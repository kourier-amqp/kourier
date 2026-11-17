package dev.kourier.amqp.robust

import dev.kourier.amqp.AMQPResponse
import dev.kourier.amqp.ChannelId
import dev.kourier.amqp.Frame
import dev.kourier.amqp.Table
import dev.kourier.amqp.channel.AMQPChannel
import dev.kourier.amqp.connection.AMQPConfig
import dev.kourier.amqp.connection.ConnectionState
import dev.kourier.amqp.connection.amqpConfig
import dev.kourier.amqp.connection.createAMQPConnection
import io.ktor.utils.io.core.*
import kotlinx.coroutines.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.Uuid

/**
 * A RobustAMQPChannel that runs [beforeRestoreConsume] the first time a restore re-registers a consumer, to make
 * the queue unavailable at that exact moment (e.g. the node that hosts it is down while the client reconnects to
 * another one). The broker then refuses the `basic.consume` with 404 NOT_FOUND and closes the channel.
 */
private class RefusedRestoreChannel(
    connection: RefusedRestoreConnection,
    id: ChannelId,
    frameMax: UInt,
) : RobustAMQPChannel(connection, id, frameMax) {

    var beforeRestoreConsume: (suspend () -> Unit)? = null
    val restoreConsumeRefused = CompletableDeferred<Unit>()
    var restoreConsumeAttempts = 0

    override suspend fun basicConsume(
        queue: String,
        consumerTag: String,
        noAck: Boolean,
        exclusive: Boolean,
        arguments: Table,
        onDelivery: suspend (AMQPResponse.Channel.Message.Delivery) -> Unit,
        onCanceled: suspend (AMQPResponse.Channel) -> Unit,
    ): AMQPResponse.Channel.Basic.ConsumeOk {
        if (currentChannelRestoreContext() != null) {
            restoreConsumeAttempts++
            beforeRestoreConsume?.let {
                beforeRestoreConsume = null
                it()
            }
        }
        try {
            return super.basicConsume(queue, consumerTag, noAck, exclusive, arguments, onDelivery, onCanceled)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            if (currentChannelRestoreContext() != null) restoreConsumeRefused.complete(Unit)
            throw e
        }
    }
}

private class RefusedRestoreConnection(
    config: AMQPConfig,
    scope: CoroutineScope,
) : RobustAMQPConnection(config, scope) {

    public override suspend fun connect() = super.connect()

    override fun createChannel(id: ChannelId, frameMax: UInt): AMQPChannel =
        RefusedRestoreChannel(this, id, frameMax)
}

class RefusedRestoreTest {

    /**
     * When the broker refuses part of a restore and closes the channel, the channel must not be left `OPEN` on the
     * client side: it has to report the close, and restore itself again once the queue is available, without
     * waiting for the next reconnection.
     */
    @Test
    fun channelIsRestoredAgainAfterTheBrokerRefusedItsRestore(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val config = amqpConfig {
            server {
                reconnectInitialDelay = 1.seconds
                reconnectMaxDelay = 1.seconds
            }
        }
        val connection = RefusedRestoreConnection(config, scope)
        // A separate, plain connection stands for the rest of the cluster: it owns the queue.
        val admin = createAMQPConnection(scope)
        val queue = "test-refused-restore-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            connection.connect()
            val channel = connection.openChannel() as RefusedRestoreChannel
            val receiver = channel.basicConsume(queue = queue, noAck = true, arguments = emptyMap())

            // The queue is gone when the restore re-registers its consumer.
            channel.beforeRestoreConsume = { adminChannel.queueDelete(queue) }

            // Drop the connection so the robust factory reconnects and restores the channel.
            // Heartbeat on a non-zero channel forces broker to close the connection.
            connection.write(Frame(channelId = 1u, payload = Frame.Heartbeat))
            withTimeout(10.seconds) { channel.restoreConsumeRefused.await() }

            // The broker has closed the channel: the client must not keep reporting it as open, nor restore it again
            // at once on every close while the queue is unavailable.
            delay(300.milliseconds)
            assertEquals(ConnectionState.CLOSED, channel.state, "Channel left OPEN after the broker refused its restore")
            assertTrue(
                channel.restoreConsumeAttempts <= 2,
                "Restore retried ${channel.restoreConsumeAttempts} times within 300 ms, before the retry delay"
            )

            // The queue is available again: the channel is restored on the same connection and consumes it.
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())
            withTimeoutOrNull(10.seconds) {
                while (adminChannel.consumerCount(queue) == 0u) delay(100)
            }
            adminChannel.basicPublish("After refused restore".toByteArray(), exchange = "", routingKey = queue)
            val delivery = withTimeoutOrNull(5.seconds) { receiver.receive() }
            assertNotNull(delivery, "Consumer was not restored after the broker refused the restore")
            assertEquals("After refused restore", delivery.message.body.decodeToString())
            assertEquals(ConnectionState.OPEN, channel.state)
        } finally {
            runCatching {
                withTimeout(5.seconds) {
                    admin.openChannel().queueDelete(queue)
                    admin.close()
                    connection.close()
                }
            }
            scope.cancel()
        }
    }

    /**
     * A retry the broker refuses as well is retried again, with the backoff, until the queue is back.
     */
    @Test
    fun refusedRestoreIsRetriedUntilTheQueueIsBack(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val config = amqpConfig {
            server {
                reconnectInitialDelay = 200.milliseconds
                reconnectMaxDelay = 400.milliseconds
            }
        }
        val connection = RefusedRestoreConnection(config, scope)
        val admin = createAMQPConnection(scope)
        val queue = "test-refused-restore-retried-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            connection.connect()
            val channel = connection.openChannel() as RefusedRestoreChannel
            val receiver = channel.basicConsume(queue = queue, noAck = true, arguments = emptyMap())
            channel.beforeRestoreConsume = { adminChannel.queueDelete(queue) }

            connection.write(Frame(channelId = 1u, payload = Frame.Heartbeat))
            withTimeout(10.seconds) { channel.restoreConsumeRefused.await() }

            // The queue stays unavailable across several retries, each refused in turn.
            withTimeout(10.seconds) { while (channel.restoreConsumeAttempts < 3) delay(50) }

            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())
            withTimeoutOrNull(10.seconds) {
                while (adminChannel.consumerCount(queue) == 0u) delay(100)
            }
            adminChannel.basicPublish("After retries".toByteArray(), exchange = "", routingKey = queue)
            val delivery = withTimeoutOrNull(5.seconds) { receiver.receive() }
            assertNotNull(delivery, "Consumer was not restored once the queue was back")
            assertEquals("After retries", delivery.message.body.decodeToString())
        } finally {
            runCatching {
                withTimeout(5.seconds) {
                    admin.openChannel().queueDelete(queue)
                    admin.close()
                    connection.close()
                }
            }
            scope.cancel()
        }
    }

    /**
     * A reconnect while a refused restore waits for its retry restores the channel at once, and the pending retry
     * does not run on the new connection.
     */
    @Test
    fun reconnectWhileRetryingRestoresTheChannel(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val config = amqpConfig {
            server {
                reconnectInitialDelay = 30.seconds
                reconnectMaxDelay = 30.seconds
            }
        }
        val connection = RefusedRestoreConnection(config, scope)
        val admin = createAMQPConnection(scope)
        val queue = "test-refused-restore-reconnect-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            connection.connect()
            val channel = connection.openChannel() as RefusedRestoreChannel
            val receiver = channel.basicConsume(queue = queue, noAck = true, arguments = emptyMap())
            channel.beforeRestoreConsume = { adminChannel.queueDelete(queue) }

            connection.write(Frame(channelId = 1u, payload = Frame.Heartbeat))
            withTimeout(10.seconds) { channel.restoreConsumeRefused.await() }
            delay(300.milliseconds)
            assertEquals(ConnectionState.CLOSED, channel.state)
            val attempts = channel.restoreConsumeAttempts

            // The queue is back, and the connection drops again long before the retry is due.
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())
            connection.write(Frame(channelId = 1u, payload = Frame.Heartbeat))

            withTimeoutOrNull(10.seconds) {
                while (adminChannel.consumerCount(queue) == 0u) delay(100)
            }
            adminChannel.basicPublish("After reconnect".toByteArray(), exchange = "", routingKey = queue)
            val delivery = withTimeoutOrNull(5.seconds) { receiver.receive() }
            assertNotNull(delivery, "Consumer was not restored by the reconnect")
            assertEquals("After reconnect", delivery.message.body.decodeToString())
            assertEquals(attempts + 1, channel.restoreConsumeAttempts, "Restore ran more than once on the reconnect")
            assertEquals(1u, adminChannel.consumerCount(queue))
        } finally {
            runCatching {
                withTimeout(5.seconds) {
                    admin.openChannel().queueDelete(queue)
                    admin.close()
                    connection.close()
                }
            }
            scope.cancel()
        }
    }
}
