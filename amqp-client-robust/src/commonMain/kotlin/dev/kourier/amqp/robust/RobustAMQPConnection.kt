package dev.kourier.amqp.robust

import dev.kourier.amqp.AMQPResponse
import dev.kourier.amqp.ChannelId
import dev.kourier.amqp.Frame
import dev.kourier.amqp.channel.AMQPChannel
import dev.kourier.amqp.channel.DefaultAMQPChannel
import dev.kourier.amqp.connection.AMQPConfig
import dev.kourier.amqp.connection.AMQPConnection
import dev.kourier.amqp.connection.ConnectionState
import dev.kourier.amqp.connection.DefaultAMQPConnection
import kotlinx.coroutines.*
import kotlin.concurrent.Volatile

open class RobustAMQPConnection(
    config: AMQPConfig,
    messageListeningScope: CoroutineScope,
) : DefaultAMQPConnection(config, messageListeningScope) {

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
            val instance = RobustAMQPConnection(config, amqpScope)
            instance.connect()
            return instance
        }

    }

    private var reconnectSubscription: Job? = null

    /**
     * Per-iteration close signal. Recreated at the top of each [connectionFactory] iteration
     * and completed by either [closeFromBroker] (the read loop) or [forceReconnect].
     *
     * Sidesteps the SharedFlow race where [closedResponses.first] could miss a Closed emission
     * fired before its subscriber attached. [CompletableDeferred.complete] is idempotent, so
     * concurrent completions from multiple channels and the read loop are safe.
     */
    @Volatile
    private var iterationClosed: CompletableDeferred<Unit> = CompletableDeferred()

    override suspend fun connect() {
        reconnectSubscription?.cancel()
        reconnectSubscription = messageListeningScope.launch {
            connectionFactory()
        }

        // Wait directly on the connectionOpened Deferred (completed inside super.connect())
        // instead of subscribing to connectionResponses, which would race with the launched
        // factory's emit and could miss the Connected event entirely on fast loopback.
        withTimeout(config.server.timeout.inWholeMilliseconds) {
            connectionOpened.await()
        }
    }

    /**
     * Test seam: lets a subclass replace the underlying socket-level connect with a stub
     * that throws or records timestamps without going through ktor TCP.
     * Production override is `super.connect()` — DefaultAMQPConnection.connect().
     * Calling `super.connect()` directly from [connectionFactory] would be statically bound
     * and bypass any test override, which makes the reconnect loop untestable in isolation.
     */
    protected open suspend fun openConnection() {
        super.connect()
    }

    protected suspend fun connectionFactory() {
        // Exponential backoff between failed reconnect attempts. Without this, a broker
        // outage with fast-failing connect() (ConnectException) loops in microseconds and
        // floods logs / burns CPU. Reset to initial after a successful iteration so a
        // long-lived connection that drops twice doesn't carry over an escalated delay.
        // Loop-local: when connectionFactory() exits and is later relaunched, the backoff
        // naturally restarts at the configured initial value.
        val initialDelay = config.server.reconnectInitialDelay
        val maxDelay = config.server.reconnectMaxDelay
        val multiplier = config.server.reconnectBackoffMultiplier
        var retryDelay = initialDelay
        while (!connectionClosed.isCompleted) {
            iterationClosed = CompletableDeferred()
            // Snapshot the channels to restore BEFORE openConnection(). super.connect() emits
            // Connection.Connected as soon as Connection.OpenOk is received, which can resume a
            // user awaiting `connection.openedResponses.first()` and let them call
            // `connection.openChannel()`. That new channel would land in channels.list() before
            // the restore loop reads it, causing both the user's openChannel and a phantom restore
            // to send Channel.Open concurrently — the broker rejects the second one with
            // CHANNEL_ERROR "second 'channel.open' seen" (504) and tears down the connection.
            val channelsToRestore = channels.list().filterIsInstance<RobustAMQPChannel>()
            try {
                openConnection()
                // Restore channels concurrently — sequential restore would make the worst-case
                // wall-clock per iteration N * restoreTimeout when each channel has to time out.
                // super.connect() must complete before any channel restore (single shared socket).
                coroutineScope {
                    channelsToRestore.forEach { channel ->
                        launch {
                            try {
                                channel.restore()
                            } catch (e: CancellationException) {
                                throw e
                            } catch (e: Exception) {
                                logger.error(
                                    "Failed to restore channel ${channel.id}, it will be retried on next reconnect",
                                    e,
                                )
                            }
                        }
                    }
                }
                iterationClosed.await()
                // Iteration completed normally (connection was opened and later closed).
                // Reset the backoff so the next failure path starts fresh.
                retryDelay = initialDelay
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                logger.error("Connection factory failed, will retry in $retryDelay", e)
                // delay() is a cooperative cancellation point — an in-flight close() that
                // cancels reconnectSubscription will short-circuit the sleep cleanly.
                delay(retryDelay)
                retryDelay = (retryDelay * multiplier).coerceAtMost(maxDelay)
            } finally {
                channels.list().filterIsInstance<RobustAMQPChannel>().forEach {
                    it.prepareForRestore() // Set the state to CLOSED, so that it can be reopened later.
                }
            }
        }
    }

    override fun createChannel(id: ChannelId, frameMax: UInt): AMQPChannel =
        RobustAMQPChannel(this, id, frameMax)

    override suspend fun close(reason: String, code: UShort): AMQPResponse.Connection.Closed {
        reconnectSubscription?.cancel()
        reconnectSubscription = null
        iterationClosed.complete(Unit)

        return super.close(reason, code)
    }

    override suspend fun closeFromBroker(payload: Frame.Method.Connection.Close) {
        // Capture the iteration's deferred up-front. If forceReconnect() (or another
        // closeFromBroker) races with us and connectionFactory() has already iterated and
        // reassigned `iterationClosed`, completing the new instance would spuriously unblock
        // the next iteration's await(). The `===` guard below skips completion in that case.
        val expected = iterationClosed
        this.state = ConnectionState.SHUTTING_DOWN
        // Don't cancelAll here, as it would cancel the reconnect subscription and complete the connectionClosed deferred.
        // But cancel the socket, otherwise connect won't work.
        socket?.close()
        socket = null

        // The heartbeat sender + watchdog tick against the now-dead socket until reconnect.
        // Cancel them so they don't leak; startHeartbeat() (called from the Tune handler on
        // reconnect) relaunches them. Do NOT cancel socketSubscription: the read loop is
        // already exiting via the catch that brought us here.
        heartbeatSubscription?.cancel()
        heartbeatSubscription = null
        heartbeatWatchdogSubscription?.cancel()
        heartbeatWatchdogSubscription = null

        // Wake up any per-channel writers blocked on writeAndWaitForResponse{...}.first().
        // Without this, mid-flight calls (channel.open(), exchangeDeclare(), basicQos(), ...)
        // stay suspended forever because the broker frames they expected will never arrive.
        val synthetic = AMQPResponse.Channel.Closed(
            channelId = 0u,
            replyCode = payload.replyCode,
            replyText = payload.replyText,
        )
        channels.list().filterIsInstance<DefaultAMQPChannel>().forEach { channel ->
            channel.channelResponses.emit(synthetic.copy(channelId = channel.id))
        }

        // Unblock connectionFactory() before publishing the connection-level Closed event so
        // observers see the loop iterate immediately rather than waiting on flow scheduling.
        // Only complete if no concurrent caller has caused the loop to iterate (which would
        // have replaced iterationClosed with a fresh instance for the next iteration).
        if (iterationClosed === expected) expected.complete(Unit)
        connectionResponses.emit(AMQPResponse.Connection.Closed)
    }

    /**
     * Drop the current socket so connectionFactory() iterates again.
     * Closing the socket causes the read loop to fail, which routes through
     * closeFromChannelException -> closeFromBroker. We also complete iterationClosed
     * directly to avoid relying on SharedFlow delivery timing.
     *
     * The `===` guard prevents completing the *next* iteration's deferred when a concurrent
     * caller has already advanced the loop (e.g. multiple channel restores time out at once
     * and each calls forceReconnect; the late ones must not unblock the new iteration).
     */
    internal fun forceReconnect() {
        if (connectionClosed.isCompleted) return
        val expected = iterationClosed
        socket?.close()
        if (iterationClosed === expected) expected.complete(Unit)
    }

}
