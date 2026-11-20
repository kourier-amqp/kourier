package dev.kourier.amqp.robust

import dev.kourier.amqp.AMQPException
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
import kotlin.test.assertIs
import kotlin.test.assertNotEquals
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

    /** Restores that failed in `basic.consume`, with the connection state seen when they failed. */
    val restoreConsumeFailures = mutableListOf<Pair<Exception, ConnectionState>>()

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
            if (currentChannelRestoreContext() != null) {
                restoreConsumeFailures += e to connection.state
                restoreConsumeRefused.complete(Unit)
            }
            throw e
        }
    }
}

private class RefusedRestoreConnection(
    config: AMQPConfig,
    scope: CoroutineScope,
) : RobustAMQPConnection(config, scope) {

    /** Runs once, right after the next reconnect has opened the connection and before it restores the channels. */
    var afterOpenConnection: (suspend () -> Unit)? = null
    var openConnectionCount = 0

    public override suspend fun connect() = super.connect()

    override suspend fun openConnection() {
        super.openConnection()
        openConnectionCount++
        afterOpenConnection?.let {
            afterOpenConnection = null
            it()
        }
    }

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

    /**
     * A restore that fails because the whole connection dropped meanwhile is not a refusal: the channel is not
     * retried on that dead connection, the reconnect restores it at once.
     */
    @Test
    fun restoreInterruptedByAConnectionDropIsLeftToTheReconnect(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val config = amqpConfig {
            server {
                // Far beyond the test: only the reconnect, which does not wait after a dropped connection, restores.
                reconnectInitialDelay = 30.seconds
                reconnectMaxDelay = 30.seconds
            }
        }
        val connection = RefusedRestoreConnection(config, scope)
        val admin = createAMQPConnection(scope)
        val queue = "test-restore-connection-drop-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            connection.connect()
            val channel = connection.openChannel() as RefusedRestoreChannel
            val receiver = channel.basicConsume(queue = queue, noAck = true, arguments = emptyMap())

            // The connection drops again while the restore re-registers the consumer.
            channel.beforeRestoreConsume = { connection.write(Frame(channelId = 1u, payload = Frame.Heartbeat)) }
            connection.write(Frame(channelId = 1u, payload = Frame.Heartbeat))
            withTimeout(10.seconds) { channel.restoreConsumeRefused.await() }

            val (failure, connectionState) = channel.restoreConsumeFailures.single()
            assertIs<AMQPException.ChannelClosed>(failure)
            assertNotEquals(ConnectionState.OPEN, connectionState, "The restore failed while the connection was up")

            withTimeoutOrNull(10.seconds) {
                while (adminChannel.consumerCount(queue) == 0u) delay(100)
            }
            adminChannel.basicPublish("After connection drop".toByteArray(), exchange = "", routingKey = queue)
            val delivery = withTimeoutOrNull(5.seconds) { receiver.receive() }
            assertNotNull(delivery, "Consumer was not restored by the reconnect")
            assertEquals("After connection drop", delivery.message.body.decodeToString())
            assertEquals(2, channel.restoreConsumeAttempts, "Restore ran more than once on the reconnect")
            assertEquals(ConnectionState.OPEN, channel.state)
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

    /**
     * A channel whose retried restore went through, and whose next restore is refused in turn, is retried again:
     * the retry that has finished does not stand for a pending one.
     */
    @Test
    fun restoreRefusedAgainAfterASuccessfulRetryIsRetriedAgain(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val config = amqpConfig {
            server {
                reconnectInitialDelay = 200.milliseconds
                reconnectMaxDelay = 200.milliseconds
            }
        }
        val connection = RefusedRestoreConnection(config, scope)
        val admin = createAMQPConnection(scope)
        val queue = "test-refused-restore-twice-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            connection.connect()
            val channel = connection.openChannel() as RefusedRestoreChannel
            val receiver = channel.basicConsume(queue = queue, noAck = true, arguments = emptyMap())

            for (round in 1..2) {
                val failures = channel.restoreConsumeFailures.size
                channel.beforeRestoreConsume = { adminChannel.queueDelete(queue) }
                connection.write(Frame(channelId = 1u, payload = Frame.Heartbeat))
                withTimeout(10.seconds) { while (channel.restoreConsumeFailures.size == failures) delay(20) }
                assertEquals(ConnectionState.CLOSED, channel.state, "Round $round: channel left OPEN after the refusal")

                adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())
                withTimeoutOrNull(10.seconds) {
                    while (adminChannel.consumerCount(queue) == 0u) delay(100)
                }
                adminChannel.basicPublish("Round $round".toByteArray(), exchange = "", routingKey = queue)
                val delivery = withTimeoutOrNull(5.seconds) { receiver.receive() }
                assertNotNull(delivery, "Round $round: consumer was not restored by the retry")
                assertEquals("Round $round", delivery.message.body.decodeToString())
                assertEquals(ConnectionState.OPEN, channel.state)
            }
            // Both restores were refused on the connection the retries then restored the channel on.
            assertEquals(2, channel.restoreConsumeFailures.size)
            assertTrue(channel.restoreConsumeFailures.all { it.second == ConnectionState.OPEN })
            assertEquals(3, connection.openConnectionCount, "The connection dropped other than by the test")
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
     * A retry that falls due after the connection it was scheduled on has gone, while the reconnect has opened the
     * new connection but not restored the channel yet, does not run: the reconnect restores the channel, once. Two
     * restores on the new connection would open the channel twice, and the broker would close the connection.
     */
    @Test
    fun retryDueDuringAReconnectDoesNotRunOnTheNewConnection(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val config = amqpConfig {
            server {
                reconnectInitialDelay = 1.seconds
                reconnectMaxDelay = 1.seconds
            }
        }
        val connection = RefusedRestoreConnection(config, scope)
        val admin = createAMQPConnection(scope)
        val queue = "test-refused-restore-retry-during-reconnect-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            connection.connect()
            val channel = connection.openChannel() as RefusedRestoreChannel
            val receiver = channel.basicConsume(queue = queue, noAck = true, arguments = emptyMap())
            channel.beforeRestoreConsume = { adminChannel.queueDelete(queue) }

            connection.write(Frame(channelId = 1u, payload = Frame.Heartbeat))
            withTimeout(10.seconds) { channel.restoreConsumeRefused.await() }
            // The retry is due in 1 s. The queue is back, the connection drops again, and the reconnect holds the
            // new connection for 2 s before restoring the channel: the retry falls due in between.
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())
            val attempts = channel.restoreConsumeAttempts
            val retryDueInHold = CompletableDeferred<Unit>()
            connection.afterOpenConnection = {
                delay(2.seconds)
                retryDueInHold.complete(Unit)
            }
            connection.write(Frame(channelId = 1u, payload = Frame.Heartbeat))
            withTimeout(10.seconds) { retryDueInHold.await() }

            withTimeoutOrNull(10.seconds) {
                while (adminChannel.consumerCount(queue) == 0u) delay(100)
            }
            adminChannel.basicPublish("After reconnect".toByteArray(), exchange = "", routingKey = queue)
            val delivery = withTimeoutOrNull(5.seconds) { receiver.receive() }
            assertNotNull(delivery, "Consumer was not restored by the reconnect")
            assertEquals("After reconnect", delivery.message.body.decodeToString())
            assertEquals(attempts + 1, channel.restoreConsumeAttempts, "The retry ran on the new connection")
            assertEquals(3, connection.openConnectionCount, "The connection dropped again after the reconnect")
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

    /**
     * Closing the connection while a refused restore waits for its retry stops the retry: the channel is not
     * reopened, even once the queue is back.
     */
    @Test
    fun closingTheConnectionStopsThePendingRetry(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val config = amqpConfig {
            server {
                reconnectInitialDelay = 300.milliseconds
                reconnectMaxDelay = 300.milliseconds
            }
        }
        val connection = RefusedRestoreConnection(config, scope)
        val admin = createAMQPConnection(scope)
        val queue = "test-refused-restore-connection-closed-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            connection.connect()
            val channel = connection.openChannel() as RefusedRestoreChannel
            channel.basicConsume(queue = queue, noAck = true, arguments = emptyMap())
            channel.beforeRestoreConsume = { adminChannel.queueDelete(queue) }

            connection.write(Frame(channelId = 1u, payload = Frame.Heartbeat))
            withTimeout(10.seconds) { channel.restoreConsumeRefused.await() }
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())
            val attempts = channel.restoreConsumeAttempts

            connection.close()
            // Several retry delays go by.
            delay(1.seconds)

            assertEquals(attempts, channel.restoreConsumeAttempts, "The retry ran after the connection was closed")
            assertEquals(ConnectionState.CLOSED, channel.state)
            assertEquals(0u, adminChannel.consumerCount(queue))
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
     * Closing the channel while a refused restore waits for its retry stops the retry: the channel is not
     * reopened, even once the queue is back.
     */
    @Test
    fun closingTheChannelStopsThePendingRetry(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val config = amqpConfig {
            server {
                reconnectInitialDelay = 300.milliseconds
                reconnectMaxDelay = 300.milliseconds
            }
        }
        val connection = RefusedRestoreConnection(config, scope)
        val admin = createAMQPConnection(scope)
        val queue = "test-refused-restore-channel-closed-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            connection.connect()
            val channel = connection.openChannel() as RefusedRestoreChannel
            channel.basicConsume(queue = queue, noAck = true, arguments = emptyMap())
            channel.beforeRestoreConsume = { adminChannel.queueDelete(queue) }

            connection.write(Frame(channelId = 1u, payload = Frame.Heartbeat))
            withTimeout(10.seconds) { channel.restoreConsumeRefused.await() }
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())
            val attempts = channel.restoreConsumeAttempts

            try {
                channel.close()
            } catch (_: AMQPException.ChannelClosed) {
                // Like every call on the channel until its restore goes through, close() fails with the refusal.
            }
            // Several retry delays go by.
            delay(1.seconds)

            assertEquals(attempts, channel.restoreConsumeAttempts, "The retry ran after the channel was closed")
            assertNotEquals(ConnectionState.OPEN, channel.state)
            assertEquals(0u, adminChannel.consumerCount(queue))
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
     * After a `basic.cancel` from the broker, the robust channel consumes the queue again. While the queue is still
     * unavailable, the broker refuses that consume with 404 NOT_FOUND and closes the channel; the restore that
     * follows is refused in turn. The channel must then take the path of a refused restore: reported closed, retried
     * with the backoff rather than on every close, and restored once the queue is back.
     */
    @Test
    fun consumeAgainAfterABrokerCancelRefusedIsRetriedLikeARefusedRestore(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val config = amqpConfig {
            server {
                reconnectInitialDelay = 1.seconds
                reconnectMaxDelay = 1.seconds
            }
        }
        val connection = RefusedRestoreConnection(config, scope)
        val admin = createAMQPConnection(scope)
        val queue = "test-broker-cancel-refused-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            connection.connect()
            val channel = connection.openChannel() as RefusedRestoreChannel
            val receiver = channel.basicConsume(queue = queue, noAck = true, arguments = emptyMap())

            // The queue goes away: the broker sends basic.cancel, and the queue is still missing when the channel
            // consumes it again, then when the restore that follows the refusal does.
            adminChannel.queueDelete(queue)
            withTimeout(10.seconds) { channel.restoreConsumeRefused.await() }

            delay(300.milliseconds)
            assertEquals(ConnectionState.CLOSED, channel.state, "Channel left OPEN after the broker refused its restore")
            assertTrue(
                channel.restoreConsumeAttempts <= 2,
                "Restore retried ${channel.restoreConsumeAttempts} times within 300 ms, before the retry delay"
            )
            assertIs<AMQPException.ChannelClosed>(channel.restoreConsumeFailures.first().first)
            assertEquals(1, connection.openConnectionCount, "The refusal was handled by a reconnect")

            // The queue is back: the retry restores the channel on the same connection, and the consumer gets it.
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())
            withTimeoutOrNull(10.seconds) {
                while (adminChannel.consumerCount(queue) == 0u) delay(100)
            }
            assertEquals(1u, adminChannel.consumerCount(queue), "The queue was not consumed again once it was back")
            adminChannel.basicPublish("After refused consume".toByteArray(), exchange = "", routingKey = queue)
            val delivery = withTimeoutOrNull(5.seconds) { receiver.receive() }
            assertNotNull(delivery, "The consumer did not receive the message published once the queue was back")
            assertEquals("After refused consume", delivery.message.body.decodeToString())
            assertEquals(ConnectionState.OPEN, channel.state)
            assertEquals(1, connection.openConnectionCount, "The channel was restored by a reconnect")
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
