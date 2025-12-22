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
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.filterIsInstance
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.io.IOException
import kotlinx.serialization.encodeToByteArray
import kotlin.time.Duration.Companion.seconds

open class DefaultAMQPConnection(
    override val config: AMQPConfig,
    val messageListeningScope: CoroutineScope,
) : AMQPConnection {

    companion object {

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

    override var state = ConnectionState.CLOSED

    protected var socket: Socket? = null
    protected var readChannel: ByteReadChannel? = null
    protected var writeChannel: ByteWriteChannel? = null

    protected var socketSubscription: Job? = null
    protected var heartbeatSubscription: Job? = null

    private val writeMutex = Mutex()

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

        write(Protocol.PROTOCOL_START_0_9_1)
        val response = withTimeout(config.server.timeout.inWholeMilliseconds) {
            connectionResponses.filterIsInstance<AMQPResponse.Connection.Connected>().first()
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
        socketSubscription = messageListeningScope.launch {
            val readChannel = this@DefaultAMQPConnection.readChannel ?: return@launch
            try {
                FrameDecoder.decodeStreaming(readChannel) { frame ->
                    logger.debug("Received AMQP frame: $frame")
                    read(frame)
                }
            } catch (e: Exception) {
                closeFromChannelException(e)
            }
        }
        heartbeatSubscription = messageListeningScope.launch {
            while (isActive) {
                delay(heartbeat.toInt().seconds.inWholeMilliseconds / 2)
                sendHeartbeat()
            }
        }
    }

    private suspend fun read(frame: Frame) {
        val channel = channels[frame.channelId] as? DefaultAMQPChannel

        when (val payload = frame.payload) {
            is Frame.Method.Connection.Start -> {
                val clientProperties = mapOf(
                    "connection_name" to Field.LongString(config.server.connectionName),
                    "product" to Field.LongString("kourier-amqp-client"),
                    "platform" to Field.LongString("Kotlin"),
                    "version" to Field.LongString("0.4.1"),
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

            is Frame.Method.Connection.Blocked -> TODO()
            is Frame.Method.Connection.Unblocked -> TODO()

            is Frame.Method.Channel.Open -> error("Unexpected Open frame received: $payload")
            is Frame.Method.Channel.OpenOk -> channel?.channelResponses?.emit(
                AMQPResponse.Channel.Opened(
                    channelId = frame.channelId,
                )
            )

            is Frame.Method.Channel.Close -> channel?.let { channel ->
                val exception = AMQPException.ChannelClosed(
                    replyCode = payload.replyCode,
                    replyText = payload.replyText,
                    isInitiatedByApplication = false
                )
                channel.channelResponses.emit(
                    AMQPResponse.Channel.Closed(
                        channelId = frame.channelId,
                        replyCode = payload.replyCode,
                        replyText = payload.replyText,
                    )
                )
                if (channel.shouldRemoveOnBrokerClose()) channels.remove(channel.id)
                val closeOk = Frame(
                    channelId = frame.channelId,
                    payload = Frame.Method.Channel.CloseOk
                )
                write(closeOk)
                // Trigger restoration asynchronously (for RobustAMQPChannel, this will restore)
                messageListeningScope.launch {
                    channel.cancelAll(exception)
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
        write(*frames)
        return connectionResponses.filterIsInstance<T>().first()
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
        socket?.close()
        readChannel?.cancel()
        writeChannel?.cancel(IOException())

        socketSubscription = null
        heartbeatSubscription = null
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
