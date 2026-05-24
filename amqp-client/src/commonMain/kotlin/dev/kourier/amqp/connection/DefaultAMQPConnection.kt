package dev.kourier.amqp.connection

import dev.kourier.amqp.*
import dev.kourier.amqp.channel.AMQPChannel
import dev.kourier.amqp.channel.AMQPChannels
import dev.kourier.amqp.channel.DefaultAMQPChannel
import dev.kourier.amqp.channel.PartialDelivery
import dev.kourier.amqp.serialization.ProtocolBinary
import io.ktor.network.selector.*
import io.ktor.network.sockets.*
import io.ktor.network.tls.*
import io.ktor.util.logging.*
import io.ktor.utils.io.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.io.IOException
import kotlinx.serialization.encodeToByteArray
import kotlin.concurrent.Volatile
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

open class DefaultAMQPConnection(
    override val config: AMQPConfig,
    val messageListeningScope: CoroutineScope,
) : AMQPConnection {

    companion object {

        // Frame-size ceiling applied before the broker negotiates frameMax in Tune. Only small
        // handshake frames (Start, Tune) arrive in that window; 1 MiB is far above any legitimate
        // one yet still blocks an oversized-frame DoS during the handshake.
        private const val HANDSHAKE_FRAME_MAX: Long = 1L shl 20

        // AMQP framing overhead per frame: 7-byte header (type + channel + size) + 1-byte end marker.
        private const val FRAME_OVERHEAD: Long = 8

        /**
         * Connect to broker.
         *
         * @param coroutineScope CoroutineScope on which to connect.
         * @param config Configuration data.
         *
         * @return AMQPConnection instance.
         */
        suspend fun create(
            coroutineScope: CoroutineScope,
            config: AMQPConfig,
        ): AMQPConnection {
            val amqpScope = CoroutineScope(coroutineScope.coroutineContext + SupervisorJob())
            val instance = DefaultAMQPConnection(config, amqpScope)
            instance.connect()
            return instance
        }

    }

    protected val logger = KtorSimpleLogger("AMQPConnection")

    @Volatile
    override var state = ConnectionState.CLOSED

    protected var socket: Socket? = null
    protected var readChannel: ByteReadChannel? = null
    protected var writeChannel: ByteWriteChannel? = null

    protected var socketSubscription: Job? = null
    protected var heartbeatSubscription: Job? = null

    // Watchdog that detects missed *server* heartbeats. AMQP 0.9.1: if no frame (heartbeat or
    // otherwise) is received for 2 * heartbeat, the peer is considered dead. Without this, a
    // silently-vanished broker (cable pulled, NAT timeout, kernel panic) leaves the read loop
    // blocked on readAvailable() forever and the robust connection never reconnects.
    protected var heartbeatWatchdogSubscription: Job? = null

    // Monotonic timestamp of the last frame received from the broker; updated by the read loop
    // and read by the watchdog. @Volatile for cross-coroutine visibility (same pattern as state).
    @Volatile
    private var lastFrameReceivedAt = TimeSource.Monotonic.markNow()

    private val writeMutex = Mutex()

    // Source of truth for the broker-imposed write block. Reads via writeBlocked.first { !it }
    // before each non-heartbeat write, eliminating the prior CompletableDeferred field-clearing race.
    @InternalAmqpApi
    val writeBlocked = MutableStateFlow(false)

    // Serializes close-from-failure transitions so concurrent IO errors / decoder exceptions
    // don't double-fire the SHUTTING_DOWN -> CLOSED path; the gate inside is "skip if terminal".
    private val closeMutex = Mutex()

    // Dedicated child scope for broker-Channel.Close-triggered cancelAll launches so we can
    // cancel them wholesale when the connection is terminated. Without this, a restore() whose
    // socket has just been closed keeps ticking until its `withTimeout(restoreTimeout)` fires
    // inside an unrelated later test/operation, surfacing as a phantom RestoreTimeoutException
    // that the test framework attributes to whichever test was active when the timer fired.
    // SupervisorJob so a failure in one channel's restore doesn't cascade-cancel the others.
    private val brokerCloseRestoresScope: CoroutineScope = CoroutineScope(
        messageListeningScope.coroutineContext + SupervisorJob(messageListeningScope.coroutineContext[Job])
    )

    private var channelMax: UShort = 0u
    private var frameMax: UInt = 0u
    private var heartbeat: UShort = 60u

    @InternalAmqpApi
    val connectionResponses = MutableSharedFlow<AMQPResponse>(extraBufferCapacity = Channel.UNLIMITED)

    @InternalAmqpApi
    val channels = AMQPChannels()

    override var connectionOpened = CompletableDeferred<Unit>()

    override val connectionClosed = CompletableDeferred<AMQPException.ConnectionClosed>()

    override val openedResponses: Flow<AMQPResponse.Connection.Connected> =
        connectionResponses.filterIsInstance<AMQPResponse.Connection.Connected>()

    override val closedResponses: Flow<AMQPResponse.Connection.Closed> =
        connectionResponses.filterIsInstance<AMQPResponse.Connection.Closed>()

    override val blockedResponses: Flow<AMQPResponse.Connection.Blocked> =
        connectionResponses.filterIsInstance<AMQPResponse.Connection.Blocked>()

    override val unblockedResponses: Flow<AMQPResponse.Connection.Unblocked> =
        connectionResponses.filterIsInstance<AMQPResponse.Connection.Unblocked>()

    protected open suspend fun connect() {
        if (socket != null && socket?.isActive == true) return

        val selector = SelectorManager(Dispatchers.IO)
        val tcpClient = aSocket(selector).tcp()

        val connection = config.connection
        socket = when (connection) {
            is AMQPConfig.Connection.Tls -> tcpClient
                .connect(config.server.host, config.server.port)
                .apply {
                    connection.tlsConfiguration?.let { tls(coroutineContext, it) } ?: tls(coroutineContext)
                }

            is AMQPConfig.Connection.Plain -> tcpClient
                .connect(config.server.host, config.server.port)
        }

        readChannel = socket?.openReadChannel()
        writeChannel = socket?.openWriteChannel(autoFlush = true)

        startListening()

        // Subscribe BEFORE writing the protocol header: `connectionResponses` is a SharedFlow
        // with replay=0, so a fast Connection.Start/Tune-Ok handshake could emit the
        // Connected response before our subscription is wired up under multi-thread dispatch.
        // `onSubscription` must be applied directly to the SharedFlow (it isn't defined for
        // plain Flow), so attach it first and filter afterwards.
        val response = withTimeout(config.server.timeout.inWholeMilliseconds) {
            connectionResponses
                .onSubscription { write(Protocol.PROTOCOL_START_0_9_1) }
                .filterIsInstance<AMQPResponse.Connection.Connected>()
                .first()
        }

        this.channelMax = response.channelMax
        this.frameMax = response.frameMax

        this.state = ConnectionState.OPEN
        connectionOpened.complete(Unit)
        logger.debug("Connected to AMQP broker at ${config.server.host}:${config.server.port}")
    }

    protected open fun startListening() {
        socketSubscription?.cancel()
        heartbeatSubscription?.cancel()
        heartbeatWatchdogSubscription?.cancel()
        // The heartbeat sender + watchdog are (re)started from the Tune handler once the broker's
        // heartbeat interval is negotiated — they can't run here because the interval is unknown
        // until Tune, and a negotiated 0 means heart-beating is disabled (no sender, no watchdog).
        socketSubscription = messageListeningScope.launch {
            val readChannel = this@DefaultAMQPConnection.readChannel ?: return@launch
            try {
                FrameDecoder.decodeStreaming(readChannel, maxFrameSize = ::maxReceivableFrameSize) { frame ->
                    // Any received frame (including heartbeats) resets the missed-heartbeat clock.
                    lastFrameReceivedAt = TimeSource.Monotonic.markNow()
                    logger.debug("Received AMQP frame: $frame")
                    read(frame)
                }
            } catch (e: kotlinx.coroutines.CancellationException) {
                // Normal shutdown cancels socketSubscription; let it propagate rather than
                // routing it through the broker/IO-failure close path.
                throw e
            } catch (e: Exception) {
                closeFromChannelException(e)
            }
        }
    }

    /**
     * (Re)starts the heartbeat sender and the missed-heartbeat watchdog using the broker-negotiated
     * interval. Called from the [Frame.Method.Connection.Tune] handler. A negotiated value of 0
     * disables heart-beating entirely (per AMQP 0.9.1) — neither job is started, which avoids both
     * a `delay(0)` busy-spin in the sender and a watchdog whose `2 * 0 = 0` timeout would treat the
     * connection as instantly dead.
     */
    protected open fun startHeartbeat(negotiatedHeartbeat: UShort) {
        heartbeatSubscription?.cancel()
        heartbeatWatchdogSubscription?.cancel()
        if (negotiatedHeartbeat.toUInt() == 0u) {
            logger.debug("Heartbeat disabled (negotiated 0); skipping heartbeat sender and watchdog")
            return
        }
        val interval = negotiatedHeartbeat.toInt().seconds
        // Reset the clock so the gap before the connection became OPEN isn't counted as silence.
        lastFrameReceivedAt = TimeSource.Monotonic.markNow()
        heartbeatSubscription = messageListeningScope.launch {
            while (isActive) {
                delay(interval / 2)
                // Skip the periodic heartbeat once the connection has begun shutting down.
                // close() cancels this job before sending Connection.Close, but a tick that has
                // already passed `delay` would otherwise still call sendHeartbeat() and push a
                // frame onto the wire after Close — the broker treats that as `unexpected_frame:
                // "type 8"`, floods its logs, and can spike latency for unrelated in-flight ops.
                if (state != ConnectionState.OPEN) continue
                sendHeartbeat()
            }
        }
        val timeout = interval * 2
        heartbeatWatchdogSubscription = messageListeningScope.launch {
            while (isActive) {
                delay(interval / 2)
                if (state != ConnectionState.OPEN) continue
                if (lastFrameReceivedAt.elapsedNow() > timeout) {
                    logger.debug("No frame received for >${timeout}; treating connection as dead")
                    closeFromChannelException(
                        IOException("Missed server heartbeats (interval = ${negotiatedHeartbeat}s)")
                    )
                    return@launch
                }
            }
        }
    }

    // Max payload size (bytes) a single incoming frame may advertise. AMQP's negotiated frameMax is
    // the *total* frame size, so the payload bound is frameMax minus the 8-byte framing overhead
    // (7-byte header + 1-byte end marker). After Tune we use the negotiated frameMax; before it
    // (only small handshake frames arrive), fall back to a generous bound so the size check is
    // active from the very first frame without rejecting a legitimate Start/Tune.
    private fun maxReceivableFrameSize(): Long =
        (frameMax.toLong().takeIf { it > 0 } ?: HANDSHAKE_FRAME_MAX) - FRAME_OVERHEAD

    private suspend fun read(frame: Frame) {
        val channel = channels[frame.channelId] as? DefaultAMQPChannel

        when (val payload = frame.payload) {
            is Frame.Method.Connection.Start -> {
                val clientProperties = mapOf(
                    "connection_name" to Field.LongString(config.server.connectionName),
                    "product" to Field.LongString("kourier-amqp-client"),
                    "platform" to Field.LongString("Kotlin"),
                    "version" to Field.LongString("0.4.5"),
                    "capabilities" to Field.Table(
                        mapOf(
                            "publisher_confirms" to Field.Boolean(true),
                            "exchange_exchange_bindings" to Field.Boolean(true),
                            "basic.nack" to Field.Boolean(true),
                            "per_consumer_qos" to Field.Boolean(true),
                            "authentication_failure_close" to Field.Boolean(true),
                            "consumer_cancel_notify" to Field.Boolean(true),
                            "connection.blocked" to Field.Boolean(true),
                        )
                    )
                )
                val startOk = Frame(
                    channelId = frame.channelId,
                    payload = Frame.Method.Connection.StartOk(
                        clientProperties = clientProperties,
                        mechanism = "PLAIN",
                        response = "\u0000${config.server.user}\u0000${config.server.password}",
                        locale = "en_US"
                    )
                )
                write(startOk)
            }

            is Frame.Method.Connection.StartOk -> error("Unexpected StartOk frame received: $payload")

            is Frame.Method.Connection.Secure -> TODO()
            is Frame.Method.Connection.SecureOk -> TODO()

            is Frame.Method.Connection.Tune -> {
                this@DefaultAMQPConnection.channelMax = payload.channelMax
                this@DefaultAMQPConnection.frameMax = payload.frameMax
                this@DefaultAMQPConnection.heartbeat = payload.heartbeat
                this@DefaultAMQPConnection.channels.channelMax = payload.channelMax
                val tuneOk = Frame(
                    channelId = frame.channelId,
                    payload = Frame.Method.Connection.TuneOk(
                        channelMax = payload.channelMax,
                        frameMax = payload.frameMax,
                        heartbeat = payload.heartbeat
                    )
                )
                val open = Frame(
                    channelId = frame.channelId,
                    payload = Frame.Method.Connection.Open(
                        vhost = config.server.vhost,
                    )
                )
                write(tuneOk)
                write(open)
                // Start (or restart, on reconnect) heart-beating now that the interval is known.
                startHeartbeat(payload.heartbeat)
            }

            is Frame.Method.Connection.TuneOk -> error("Unexpected TuneOk frame received: $payload")

            is Frame.Method.Connection.Open -> error("Unexpected Open frame received: $payload")
            is Frame.Method.Connection.OpenOk -> connectionResponses.emit(
                AMQPResponse.Connection.Connected(
                    channelMax = channelMax,
                    frameMax = frameMax,
                )
            )

            is Frame.Method.Connection.Close -> {
                val closeOk = Frame(
                    channelId = frame.channelId,
                    payload = Frame.Method.Connection.CloseOk
                )
                write(closeOk)
                closeFromBroker(payload)
            }

            is Frame.Method.Connection.CloseOk -> connectionResponses.emit(AMQPResponse.Connection.Closed)

            is Frame.Method.Connection.Blocked -> {
                logger.debug("Connection blocked by broker: ${payload.reason}")
                writeBlocked.value = true
                connectionResponses.emit(AMQPResponse.Connection.Blocked(payload.reason))
            }

            is Frame.Method.Connection.Unblocked -> {
                logger.debug("Connection unblocked by broker")
                writeBlocked.value = false
                connectionResponses.emit(AMQPResponse.Connection.Unblocked)
            }

            is Frame.Method.Channel.Open -> error("Unexpected Open frame received: $payload")
            is Frame.Method.Channel.OpenOk -> channel?.channelResponses?.emit(
                AMQPResponse.Channel.Opened(
                    channelId = frame.channelId,
                )
            )

            is Frame.Method.Channel.Close -> {
                // ALWAYS send Channel.CloseOk back, even if we don't track this channel id.
                // Per AMQP 0-9-1 spec section 1.4.2.6, CloseOk confirms a Close and tells the
                // peer it can release resources. If we silently drop a Close for an unknown
                // channel id, the broker keeps the id in "awaiting CloseOk" state and rejects
                // any subsequent Channel.Open on that id with "second 'channel.open' seen"
                // (CHANNEL_ERROR 504). This affects scenarios where a client-side error
                // triggers a broker-side channel error on an id we never opened, e.g. sending
                // an invalid frame referencing an unopened channel.
                val closeOk = Frame(
                    channelId = frame.channelId,
                    payload = Frame.Method.Channel.CloseOk
                )
                write(closeOk)
                channel?.let { channel ->
                    val exception = AMQPException.ChannelClosed(
                        replyCode = payload.replyCode,
                        replyText = payload.replyText,
                        isInitiatedByApplication = false
                    )
                    // Arm restore BEFORE the Channel.Closed is observable, so exactly one restore
                    // runs per broker close — consumed by the first of the two restore triggers
                    // (this launch + the in-flight RPC's `.also { cancelAll }`). See NEW-31.
                    channel.onBrokerClose()
                    channel.channelResponses.emit(
                        AMQPResponse.Channel.Closed(
                            channelId = frame.channelId,
                            replyCode = payload.replyCode,
                            replyText = payload.replyText,
                        )
                    )
                    if (channel.shouldRemoveOnBrokerClose()) channels.remove(channel.id)
                    // Launch in brokerCloseRestoresScope so cancelAll() can cancel all in-flight
                    // restores wholesale when the connection terminates — otherwise a restore()
                    // running on a now-dead socket would tick until its withTimeout fires inside
                    // an unrelated later test, surfacing as a phantom RestoreTimeoutException.
                    // Swallow non-cancellation exceptions: cancelAll() races with the restore's
                    // in-flight writeAndWaitForResponse — when the writeChannel is cancelled
                    // before our cancellation signal arrives, the restore's write() throws
                    // IOException, which would otherwise propagate up as uncaught.
                    brokerCloseRestoresScope.launch {
                        try {
                            channel.cancelAll(exception)
                        } catch (e: kotlinx.coroutines.CancellationException) {
                            throw e
                        } catch (e: Exception) {
                            logger.debug("Broker-close-triggered restore aborted: ${e.message}")
                        }
                    }
                }
            }

            is Frame.Method.Channel.CloseOk -> channel?.let { channel ->
                channel.channelResponses.emit(AMQPResponse.Channel.Closed(channelId = frame.channelId))
                channels.remove(channel.id)
            }

            is Frame.Method.Channel.Flow -> {
                val flowOk = Frame(
                    channelId = frame.channelId,
                    payload = Frame.Method.Channel.FlowOk(
                        active = payload.active,
                    )
                )
                write(flowOk)
            }

            is Frame.Method.Channel.FlowOk -> channel?.channelResponses?.emit(
                AMQPResponse.Channel.Flowed(
                    active = payload.active,
                )
            )

            is Frame.Method.Queue.Declare -> error("Unexpected Declare frame received: $payload")
            is Frame.Method.Queue.DeclareOk -> channel?.channelResponses?.emit(
                AMQPResponse.Channel.Queue.Declared(
                    queueName = payload.queueName,
                    messageCount = payload.messageCount,
                    consumerCount = payload.consumerCount
                )
            )

            is Frame.Method.Queue.Bind -> error("Unexpected Bind frame received: $payload")
            is Frame.Method.Queue.BindOk -> channel?.channelResponses?.emit(
                AMQPResponse.Channel.Queue.Bound
            )

            is Frame.Method.Queue.Purge -> error("Unexpected Purge frame received: $payload")
            is Frame.Method.Queue.PurgeOk -> channel?.channelResponses?.emit(
                AMQPResponse.Channel.Queue.Purged(
                    messageCount = payload.messageCount
                )
            )

            is Frame.Method.Queue.Delete -> error("Unexpected Delete frame received: $payload")
            is Frame.Method.Queue.DeleteOk -> channel?.channelResponses?.emit(
                AMQPResponse.Channel.Queue.Deleted(
                    messageCount = payload.messageCount,
                )
            )

            is Frame.Method.Queue.Unbind -> error("Unexpected Unbind frame received: $payload")
            is Frame.Method.Queue.UnbindOk -> channel?.channelResponses?.emit(
                AMQPResponse.Channel.Queue.Unbound
            )

            is Frame.Method.Basic.Get -> error("Unexpected Get frame received: $payload")
            is Frame.Method.Basic.GetEmpty -> channel?.channelResponses?.emit(
                AMQPResponse.Channel.Message.Get()
            )

            is Frame.Method.Basic.Deliver, is Frame.Method.Basic.GetOk, is Frame.Method.Basic.Return -> {
                channel?.nextMessage = PartialDelivery(method = payload)
            }

            is Frame.Method.Basic.RecoverAsync -> error("Unexpected RecoverAsync frame received: $payload")
            is Frame.Method.Basic.Recover -> error("Unexpected Recover frame received: $payload")
            is Frame.Method.Basic.RecoverOk -> channel?.channelResponses?.emit(
                AMQPResponse.Channel.Basic.Recovered
            )

            is Frame.Method.Basic.Consume -> error("Unexpected Consume frame received: $payload")
            is Frame.Method.Basic.ConsumeOk -> channel?.channelResponses?.emit(
                AMQPResponse.Channel.Basic.ConsumeOk(
                    consumerTag = payload.consumerTag,
                )
            )

            is Frame.Method.Basic.Cancel -> {
                channel?.channelResponses?.emit(
                    AMQPResponse.Channel.Basic.Canceled(
                        consumerTag = payload.consumerTag,
                    )
                )
                if (!payload.noWait) {
                    val cancelOk = Frame(
                        channelId = frame.channelId,
                        payload = Frame.Method.Basic.CancelOk(
                            consumerTag = payload.consumerTag,
                        )
                    )
                    write(cancelOk)
                }
            }

            is Frame.Method.Basic.CancelOk -> channel?.channelResponses?.emit(
                AMQPResponse.Channel.Basic.Canceled(
                    consumerTag = payload.consumerTag,
                )
            )

            is Frame.Method.Basic.Qos -> error("Unexpected Qos frame received: $payload")
            is Frame.Method.Basic.QosOk -> channel?.channelResponses?.emit(
                AMQPResponse.Channel.Basic.QosOk
            )

            is Frame.Method.Basic.Publish -> error("Unexpected Publish frame received: $payload")

            is Frame.Method.Basic.Ack -> channel?.channelResponses?.emit(
                AMQPResponse.Channel.Basic.PublishConfirm.Ack(
                    deliveryTag = payload.deliveryTag,
                    multiple = payload.multiple,
                )
            )

            is Frame.Method.Basic.Nack -> channel?.channelResponses?.emit(
                AMQPResponse.Channel.Basic.PublishConfirm.Nack(
                    deliveryTag = payload.deliveryTag,
                    multiple = payload.multiple,
                )
            )

            is Frame.Method.Basic.Reject -> error("Unexpected Reject frame received: $payload")

            is Frame.Method.Exchange.Declare -> error("Unexpected Declare frame received: $payload")
            is Frame.Method.Exchange.DeclareOk -> channel?.channelResponses?.emit(
                AMQPResponse.Channel.Exchange.Declared
            )

            is Frame.Method.Exchange.Delete -> error("Unexpected Delete frame received: $payload")
            is Frame.Method.Exchange.DeleteOk -> channel?.channelResponses?.emit(
                AMQPResponse.Channel.Exchange.Deleted
            )

            is Frame.Method.Exchange.Bind -> error("Unexpected Bind frame received: $payload")
            is Frame.Method.Exchange.BindOk -> channel?.channelResponses?.emit(
                AMQPResponse.Channel.Exchange.Bound
            )

            is Frame.Method.Exchange.Unbind -> error("Unexpected Unbind frame received: $payload")
            is Frame.Method.Exchange.UnbindOk -> channel?.channelResponses?.emit(
                AMQPResponse.Channel.Exchange.Unbound
            )

            is Frame.Method.Confirm.Select -> error("Unexpected Select frame received: $payload")
            is Frame.Method.Confirm.SelectOk -> channel?.channelResponses?.emit(
                AMQPResponse.Channel.Confirm.Selected
            )

            is Frame.Method.Tx.Select -> error("Unexpected Select frame received: $payload")
            is Frame.Method.Tx.SelectOk -> channel?.channelResponses?.emit(
                AMQPResponse.Channel.Tx.Selected
            )

            is Frame.Method.Tx.Commit -> error("Unexpected Commit frame received: $payload")
            is Frame.Method.Tx.CommitOk -> channel?.channelResponses?.emit(
                AMQPResponse.Channel.Tx.Committed
            )

            is Frame.Method.Tx.Rollback -> error("Unexpected Rollback frame received: $payload")
            is Frame.Method.Tx.RollbackOk -> channel?.channelResponses?.emit(
                AMQPResponse.Channel.Tx.Rollbacked
            )

            is Frame.Header -> {
                channel?.nextMessage?.setHeader(payload)
                if (channel?.nextMessage?.isComplete == true) channel.nextMessage!!.emitOnChannel(channel)
            }

            is Frame.Body -> {
                channel?.nextMessage?.addBody(payload.body)
                if (channel?.nextMessage?.isComplete == true) channel.nextMessage!!.emitOnChannel(channel)
            }

            is Frame.Heartbeat -> write(Frame(channelId = frame.channelId, payload = Frame.Heartbeat))
        }
    }

    @InternalAmqpApi
    override suspend fun write(bytes: ByteArray) {
        val writeChannel = this.writeChannel ?: return
        try {
            writeChannel.writeByteArray(bytes)
            writeChannel.flush() // Maybe not needed since autoFlush is true?
        } catch (e: ClosedWriteChannelException) {
            closeFromChannelException(e)
        }
    }

    @InternalAmqpApi
    override suspend fun write(vararg frames: Frame) {
        val isHeartbeatOnly = frames.all { it.payload is Frame.Heartbeat }
        if (!isHeartbeatOnly && writeBlocked.value) {
            // Only subscribe (allocates a Flow collector) if we actually need to wait;
            // unblocking the fast path matters here because every frame goes through it.
            writeBlocked.first { !it }
        }
        writeMutex.withLock { // Ensure that all frames are sent in order, without any other writes in between
            if (connectionClosed.isCompleted) throw connectionClosed.await()
            frames.forEach { frame ->
                logger.debug("Sent AMQP frame: $frame")
                write(ProtocolBinary.encodeToByteArray(frame))
            }
        }
    }

    @InternalAmqpApi
    suspend inline fun <reified T : AMQPResponse> writeAndWaitForResponse(vararg frames: Frame): T {
        // Subscribe BEFORE writing: `connectionResponses` is a SharedFlow with replay=0,
        // so any response emitted before our subscription would be lost. `onSubscription`
        // must be applied directly to the SharedFlow (it isn't defined for plain Flow);
        // it fires the write only after the subscription is fully wired up.
        return connectionResponses
            .onSubscription { write(*frames) }
            .filterIsInstance<T>()
            .first()
    }

    protected open fun createChannel(id: ChannelId, frameMax: UInt): AMQPChannel =
        DefaultAMQPChannel(this, id, frameMax)

    override suspend fun openChannel(): AMQPChannel {
        val channelId = channels.reserveNext() ?: throw AMQPException.TooManyOpenedChannels
        return createChannel(channelId, frameMax).also { it.open() }
    }

    override suspend fun sendHeartbeat() {
        write(Frame(channelId = 0u, payload = Frame.Heartbeat))
    }

    override suspend fun close(
        reason: String,
        code: UShort,
    ): AMQPResponse.Connection.Closed {
        if (state != ConnectionState.OPEN) return AMQPResponse.Connection.Closed
        this.state = ConnectionState.SHUTTING_DOWN
        // Stop the heartbeat ticker before sending Connection.Close: once the broker has processed
        // Close it considers the connection terminal and treats any subsequent heartbeat frame as
        // an `unexpected_frame: "type 8"` connection-level exception. That race floods the broker
        // logs with spurious errors and, more importantly, can spike broker latency enough to make
        // unrelated in-flight ops (like a concurrent channel restore) hit their timeout.
        // cancelAndJoin (not just cancel) so any in-flight sendHeartbeat() completes before we
        // start writing the Close frame — otherwise a heartbeat already past its state check
        // would still slip onto the wire after Close.
        heartbeatSubscription?.cancelAndJoin()
        heartbeatSubscription = null
        heartbeatWatchdogSubscription?.cancel()
        heartbeatWatchdogSubscription = null
        val close = Frame(
            channelId = 0u,
            payload = Frame.Method.Connection.Close(
                replyCode = code,
                replyText = reason,
                failingClassId = 0u,
                failingMethodId = 0u,
            )
        )
        val result = writeAndWaitForResponse<AMQPResponse.Connection.Closed>(close)
        cancelAll(
            AMQPException.ConnectionClosed(
                replyCode = code,
                replyText = reason,
                isInitiatedByApplication = true,
            )
        )
        return result
    }

    protected open fun cancelAll(connectionClose: AMQPException.ConnectionClosed) {
        if (state != ConnectionState.SHUTTING_DOWN) return

        socketSubscription?.cancel()
        heartbeatSubscription?.cancel()
        heartbeatWatchdogSubscription?.cancel()
        socket?.close()
        readChannel?.cancel()
        writeChannel?.cancel(IOException())

        // Cancel any in-flight broker-Channel.Close-triggered restore launches. Their socket
        // is now dead, so their writeAndWaitForResponse `.first()` would otherwise hang until
        // withTimeout(restoreTimeout) fires — long after the connection is gone, leaking a
        // RestoreTimeoutException into whatever test happens to be running by then.
        brokerCloseRestoresScope.cancel()

        socketSubscription = null
        heartbeatSubscription = null
        heartbeatWatchdogSubscription = null
        socket = null
        readChannel = null
        writeChannel = null

        this.state = ConnectionState.CLOSED
        logger.debug("Disconnected from AMQP broker at ${config.server.host}:${config.server.port}")
        connectionClosed.complete(connectionClose)
    }

    protected open suspend fun closeFromBroker(payload: Frame.Method.Connection.Close) {
        this.state = ConnectionState.SHUTTING_DOWN
        cancelAll(
            AMQPException.ConnectionClosed(
                replyCode = payload.replyCode,
                replyText = payload.replyText,
            )
        )
        connectionResponses.emit(AMQPResponse.Connection.Closed)
    }

    protected open suspend fun closeFromChannelException(exception: Exception) {
        // Serialize transition so two concurrent IO failures (read loop + writer) can't both
        // pass the OPEN gate and double-fire closeFromBroker. The gate stays "only drive
        // cleanup if currently OPEN" — non-OPEN means another caller (close(), the broker's
        // explicit Connection.Close, or the robust subclass's broker-close path) is already
        // handling it; double-firing here would re-enter closeFromBroker against a half-torn
        // connection, which in robust mode would advance the reconnect loop twice.
        closeMutex.withLock {
            if (state != ConnectionState.OPEN) return
            // Same as if the server closed the connection properly
            closeFromBroker(
                Frame.Method.Connection.Close(
                    replyCode = 500u,
                    replyText = exception.message ?: "Channel is closed",
                    failingClassId = 0u,
                    failingMethodId = 0u,
                )
            )
        }
    }

}
