package dev.kourier.amqp.robust

import dev.kourier.amqp.AMQPResponse
import dev.kourier.amqp.ChannelId
import dev.kourier.amqp.Frame
import dev.kourier.amqp.channel.AMQPChannel
import dev.kourier.amqp.connection.AMQPConfig
import dev.kourier.amqp.connection.AMQPConnection
import dev.kourier.amqp.connection.ConnectionState
import dev.kourier.amqp.connection.DefaultAMQPConnection
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.filterIsInstance
import kotlinx.coroutines.flow.first

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

    override suspend fun connect() {
        reconnectSubscription?.cancel()
        reconnectSubscription = messageListeningScope.launch {
            connectionFactory()
        }

        withTimeout(config.server.timeout.inWholeMilliseconds) {
            connectionResponses.filterIsInstance<AMQPResponse.Connection.Connected>().first()
        }
        connectionOpened.await()
    }

    protected suspend fun connectionFactory() {
        while (!connectionClosed.isCompleted) {
            try {
                super.connect()
                channels.list().filterIsInstance<RobustAMQPChannel>().forEach { channel ->
                    try {
                        channel.restore()
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        logger.error("Failed to restore channel ${channel.id}, it will be retried on next reconnect", e)
                    }
                }
                closedResponses.first()
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                logger.error("Connection factory failed, will retry", e)
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

        return super.close(reason, code)
    }

    override suspend fun closeFromBroker(payload: Frame.Method.Connection.Close) {
        this.state = ConnectionState.SHUTTING_DOWN
        // Don't cancelAll here, as it would cancel the reconnect subscription and complete the connectionClosed deferred.
        // But cancel the socket, otherwise connect won't work.
        socket?.close()
        socket = null
        connectionResponses.emit(AMQPResponse.Connection.Closed)
    }

}
