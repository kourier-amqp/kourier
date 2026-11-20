package dev.kourier.amqp.robust

import dev.kourier.amqp.AMQPResponse
import dev.kourier.amqp.ChannelId
import dev.kourier.amqp.Frame
import dev.kourier.amqp.Table
import dev.kourier.amqp.channel.AMQPChannel
import dev.kourier.amqp.connection.AMQPConfig
import dev.kourier.amqp.connection.AMQPConnection
import dev.kourier.amqp.connection.ConnectionState
import dev.kourier.amqp.connection.amqpConfig
import dev.kourier.amqp.connection.createAMQPConnection
import io.ktor.utils.io.core.*
import kotlinx.coroutines.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.Uuid

/**
 * A RobustAMQPChannel that counts the `basic.consume` calls made outside a restore (the application's own, and the
 * one made again after a `basic.cancel` from the broker), and those made by a restore.
 */
private class ConsumeCountingChannel(
    connection: ConsumeCountingConnection,
    id: ChannelId,
    frameMax: UInt,
) : RobustAMQPChannel(connection, id, frameMax) {

    var consumeCalls = 0
    var restoreConsumeCalls = 0

    override suspend fun basicConsume(
        queue: String,
        consumerTag: String,
        noAck: Boolean,
        exclusive: Boolean,
        arguments: Table,
        onDelivery: suspend (AMQPResponse.Channel.Message.Delivery) -> Unit,
        onCanceled: suspend (AMQPResponse.Channel) -> Unit,
    ): AMQPResponse.Channel.Basic.ConsumeOk {
        if (currentChannelRestoreContext() == null) consumeCalls++ else restoreConsumeCalls++
        return super.basicConsume(queue, consumerTag, noAck, exclusive, arguments, onDelivery, onCanceled)
    }
}

private class ConsumeCountingConnection(
    config: AMQPConfig,
    scope: CoroutineScope,
) : RobustAMQPConnection(config, scope) {

    public override suspend fun connect() = super.connect()

    override fun createChannel(id: ChannelId, frameMax: UInt): AMQPChannel =
        ConsumeCountingChannel(this, id, frameMax)
}

class BrokerConsumerCancelTest {

    /**
     * The broker cancels a consumer with `basic.cancel` when its queue becomes unavailable, e.g. when the node that
     * hosts the queue goes down while the client is connected to another node. A robust channel must consume the
     * queue again once it is back, as it does after a restore, instead of dropping the consumer for good.
     */
    @Test
    fun consumerIsConsumedAgainAfterTheBrokerCancelsIt(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val connection = createRobustAMQPConnection(scope) {
            server {
                reconnectInitialDelay = 500.milliseconds
            }
        }
        // A separate, plain connection stands for the rest of the cluster: it owns the queue.
        val admin = createAMQPConnection(scope)
        val queue = "test-broker-consumer-cancel-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            val channel = connection.openChannel()
            val receiver = channel.basicConsume(queue = queue, noAck = true, arguments = emptyMap())
            assertEquals(1u, adminChannel.consumerCount(queue))

            // The queue goes away (the broker sends basic.cancel to its consumer), then comes back.
            adminChannel.queueDelete(queue)
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            withTimeoutOrNull(10.seconds) {
                while (adminChannel.consumerCount(queue) == 0u) delay(100)
            }
            assertEquals(1u, adminChannel.consumerCount(queue), "The queue was not consumed again after basic.cancel")

            adminChannel.basicPublish("After broker cancel".toByteArray(), exchange = "", routingKey = queue)
            val delivery = withTimeoutOrNull(5.seconds) { receiver.receive() }
            assertNotNull(delivery, "The consumer did not receive the message published after basic.cancel")
            assertEquals("After broker cancel", delivery.message.body.decodeToString())
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
     * A queue the application deletes through the channel that consumes it is not consumed again: the broker's
     * `basic.cancel` is the outcome of that delete, and the consumer sees its cancellation as before.
     */
    @Test
    fun consumerIsNotConsumedAgainWhenTheApplicationDeletesItsQueue(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val connection = createRobustAMQPConnection(scope) {
            server {
                reconnectInitialDelay = 200.milliseconds
            }
        }
        val admin = createAMQPConnection(scope)
        val queue = "test-application-queue-delete-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            val channel = connection.openChannel()
            channel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())
            val receiver = channel.basicConsume(queue = queue, noAck = true, arguments = emptyMap())

            channel.queueDelete(queue)
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            assertNull(withTimeoutOrNull(5.seconds) { receiver.receiveCatching().getOrNull() })
            assertTrue(receiver.isClosedForReceive, "The consumer was not told its queue was deleted")
            delay(1.seconds)
            assertEquals(0u, adminChannel.consumerCount(queue), "A queue deleted by the application was consumed again")
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
     * A consumer the application cancels with its broker-assigned tag is not consumed again by the next restore.
     */
    @Test
    fun consumerCancelledByTheApplicationIsNotRestored(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val connection = createRobustAMQPConnection(scope)
        val admin = createAMQPConnection(scope)
        val queue = "test-application-consumer-cancel-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            val channel = connection.openChannel()
            val consumeOk = channel.basicConsume(queue = queue, noAck = true, arguments = emptyMap(), onDelivery = {})
            channel.basicCancel(consumeOk.consumerTag)
            assertEquals(0u, adminChannel.consumerCount(queue))

            // Drop the connection so the robust factory reconnects and restores the channel.
            // Heartbeat on a non-zero channel forces broker to close the connection.
            connection.write(Frame(channelId = 1u, payload = Frame.Heartbeat))
            delay(500.milliseconds)
            withTimeout(10.seconds) {
                while (true) {
                    try {
                        channel.basicQos(count = 1u)
                        break
                    } catch (e: CancellationException) {
                        throw e
                    } catch (_: Exception) {
                        delay(100)
                    }
                }
            }

            assertEquals(0u, adminChannel.consumerCount(queue), "A consumer cancelled by the application was restored")
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
     * A consumer the application cancels while it waits to be consumed again after a `basic.cancel` from the broker
     * stays cancelled: the queue is not consumed again once it is back.
     */
    @Test
    fun consumerCancelledByTheApplicationBeforeItIsConsumedAgainStaysCancelled(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val connection = createRobustAMQPConnection(scope) {
            server {
                reconnectInitialDelay = 1.seconds
            }
        }
        val admin = createAMQPConnection(scope)
        val queue = "test-broker-cancel-then-application-cancel-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            val channel = connection.openChannel()
            val consumeOk = channel.basicConsume(queue = queue, noAck = true, arguments = emptyMap(), onDelivery = {})
            adminChannel.queueDelete(queue)
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            // Within the delay before the queue is consumed again.
            channel.basicCancel(consumeOk.consumerTag)

            delay(2.seconds)
            assertEquals(0u, adminChannel.consumerCount(queue), "A consumer cancelled by the application was consumed again")
        } finally {
            cleanUp(admin, connection, queue)
            scope.cancel()
        }
    }

    /**
     * A channel the application closes while a consumer waits to be consumed again after a `basic.cancel` from the
     * broker does not try to consume it again.
     */
    @Test
    fun consumerIsNotConsumedAgainOnAChannelClosedMeanwhile(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val config = amqpConfig {
            server {
                reconnectInitialDelay = 500.milliseconds
            }
        }
        val connection = ConsumeCountingConnection(config, scope)
        val admin = createAMQPConnection(scope)
        val queue = "test-broker-cancel-then-channel-close-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            connection.connect()
            val channel = connection.openChannel() as ConsumeCountingChannel
            channel.basicConsume(queue = queue, noAck = true, arguments = emptyMap())
            assertEquals(1u, adminChannel.consumerCount(queue))

            adminChannel.queueDelete(queue)
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())
            // The broker's basic.cancel has reached the channel before it is closed.
            withTimeout(5.seconds) { while (adminChannel.consumerCount(queue) != 0u) delay(20) }
            delay(100.milliseconds)
            channel.close()

            delay(1.seconds)
            assertEquals(1, channel.consumeCalls, "The queue was consumed again on a channel closed by the application")
        } finally {
            cleanUp(admin, connection, queue)
            scope.cancel()
        }
    }

    /**
     * A consumer with a tag of its own keeps it when it is consumed again after a `basic.cancel` from the broker,
     * and is still restored by the next reconnect.
     */
    @Test
    fun consumerWithItsOwnTagIsStillRestoredAfterBeingConsumedAgain(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val connection = createRobustAMQPConnection(scope) {
            server {
                reconnectInitialDelay = 500.milliseconds
            }
        }
        val admin = createAMQPConnection(scope)
        val queue = "test-broker-cancel-own-tag-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            val channel = connection.openChannel()
            val receiver = channel.basicConsume(queue = queue, consumerTag = "own-tag", noAck = true, arguments = emptyMap())
            adminChannel.queueDelete(queue)
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())
            withTimeoutOrNull(10.seconds) {
                while (adminChannel.consumerCount(queue) == 0u) delay(100)
            }
            assertEquals(1u, adminChannel.consumerCount(queue), "The queue was not consumed again after basic.cancel")

            // Heartbeat on a non-zero channel forces broker to close the connection.
            connection.write(Frame(channelId = 1u, payload = Frame.Heartbeat))
            delay(500.milliseconds)
            awaitRestored(channel)

            assertEquals(1u, adminChannel.consumerCount(queue), "The consumer was not restored by the reconnect")
            adminChannel.basicPublish("After reconnect".toByteArray(), exchange = "", routingKey = queue)
            val delivery = withTimeoutOrNull(5.seconds) { receiver.receive() }
            assertNotNull(delivery, "The consumer did not receive the message published after the reconnect")
            assertEquals("After reconnect", delivery.message.body.decodeToString())
        } finally {
            cleanUp(admin, connection, queue)
            scope.cancel()
        }
    }

    /**
     * A queue delete the broker refuses (here, the queue is in use) keeps the consumers of the queue: the restore that
     * follows the channel close consumes the queue again.
     */
    @Test
    fun consumersOfAQueueWhoseDeleteIsRefusedAreRestored(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val connection = createRobustAMQPConnection(scope)
        val admin = createAMQPConnection(scope)
        val queue = "test-refused-queue-delete-${Uuid.random()}"
        val otherQueue = "test-refused-queue-delete-other-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())
            adminChannel.queueDeclare(otherQueue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            val channel = connection.openChannel()
            channel.basicConsume(queue = otherQueue, noAck = true, arguments = emptyMap())
            val receiver = channel.basicConsume(queue = queue, noAck = true, arguments = emptyMap())

            // PRECONDITION_FAILED: the queue has a consumer. The broker closes the channel.
            assertFails { channel.queueDelete(queue, ifUnused = true) }
            awaitRestored(channel)

            withTimeoutOrNull(10.seconds) {
                while (adminChannel.consumerCount(queue) == 0u) delay(100)
            }
            assertEquals(1u, adminChannel.consumerCount(queue), "The consumer was dropped by a refused queue delete")
            assertEquals(1u, adminChannel.consumerCount(otherQueue), "The consumer of another queue was dropped")
            adminChannel.basicPublish("After refused delete".toByteArray(), exchange = "", routingKey = queue)
            val delivery = withTimeoutOrNull(5.seconds) { receiver.receive() }
            assertNotNull(delivery, "The consumer did not receive the message published after the refused delete")
            assertEquals("After refused delete", delivery.message.body.decodeToString())
        } finally {
            runCatching { withTimeout(5.seconds) { admin.openChannel().queueDelete(otherQueue) } }
            cleanUp(admin, connection, queue)
            scope.cancel()
        }
    }

    /**
     * A queue that is deleted and not declared again is gone for good: the broker refuses the `basic.consume` made
     * after its `basic.cancel` with 404 NOT_FOUND, and closes the channel. The robust channel must then stop
     * consuming that queue and forward the cancellation to the consumer, as before, on a channel that is open
     * again: no consume of the missing queue retried in a loop, and the other consumers of the channel kept.
     */
    @Test
    fun consumerOfAQueueThatIsGoneForGoodIsCancelledOnAnOpenChannel(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val config = amqpConfig {
            server {
                reconnectInitialDelay = 300.milliseconds
                reconnectMaxDelay = 300.milliseconds
            }
        }
        val connection = ConsumeCountingConnection(config, scope)
        val admin = createAMQPConnection(scope)
        val queue = "test-broker-cancel-queue-gone-${Uuid.random()}"
        val otherQueue = "test-broker-cancel-queue-gone-other-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())
            adminChannel.queueDeclare(otherQueue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            connection.connect()
            val channel = connection.openChannel() as ConsumeCountingChannel
            val otherReceiver = channel.basicConsume(queue = otherQueue, noAck = true, arguments = emptyMap())
            val receiver = channel.basicConsume(queue = queue, noAck = true, arguments = emptyMap())

            // The queue goes away and does not come back.
            adminChannel.queueDelete(queue)

            assertNull(
                withTimeoutOrNull(5.seconds) { receiver.receiveCatching().getOrNull() },
                "The consumer of a queue that is gone received a message"
            )
            assertTrue(receiver.isClosedForReceive, "The consumer was not told its queue is gone")

            // The channel is open, on the client side and on the broker's: the other consumer is still there.
            awaitRestored(channel)
            assertEquals(ConnectionState.OPEN, channel.state, "The channel was left closed")
            assertEquals(1u, adminChannel.consumerCount(otherQueue), "The consumer of another queue was dropped")
            adminChannel.basicPublish("After queue gone".toByteArray(), exchange = "", routingKey = otherQueue)
            val delivery = withTimeoutOrNull(5.seconds) { otherReceiver.receive() }
            assertNotNull(delivery, "The other consumer of the channel did not receive its message")
            assertEquals("After queue gone", delivery.message.body.decodeToString())

            // Nothing retries the consume of the missing queue: one consume again after the broker cancel, then no
            // more, whatever the retry delay.
            val consumeCalls = channel.consumeCalls
            val restoreConsumeCalls = channel.restoreConsumeCalls
            delay(2.seconds)
            assertEquals(consumeCalls, channel.consumeCalls, "The missing queue was consumed again in a loop")
            assertEquals(restoreConsumeCalls, channel.restoreConsumeCalls, "The channel was restored in a loop")
            assertEquals(3, channel.consumeCalls, "Expected the two consumes of the application and one consume again")

            // The channel is usable by the application.
            val newQueue = channel.queueDeclare("", durable = false, exclusive = true, autoDelete = true, arguments = emptyMap())
            val newReceiver = channel.basicConsume(queue = newQueue.queueName, noAck = true, arguments = emptyMap())
            channel.basicPublish("On the open channel".toByteArray(), exchange = "", routingKey = newQueue.queueName)
            val newDelivery = withTimeoutOrNull(5.seconds) { newReceiver.receive() }
            assertNotNull(newDelivery, "The channel is not usable after its consumer was cancelled")
            assertEquals("On the open channel", newDelivery.message.body.decodeToString())
        } finally {
            runCatching { withTimeout(5.seconds) { admin.openChannel().queueDelete(otherQueue) } }
            cleanUp(admin, connection, queue)
            scope.cancel()
        }
    }

    /**
     * A queue the robust channel declared and bound itself, and another client deleted, is gone too: every consumer
     * of the queue on the channel is told, and the restore of the channel neither declares the queue again (nobody
     * would consume it) nor binds it (the broker would refuse it, and the restore would be retried in a loop).
     */
    @Test
    fun queueGoneForGoodIsNotDeclaredAgainByTheRestore(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val config = amqpConfig {
            server {
                reconnectInitialDelay = 300.milliseconds
                reconnectMaxDelay = 300.milliseconds
            }
        }
        val connection = ConsumeCountingConnection(config, scope)
        val admin = createAMQPConnection(scope)
        val queue = "test-broker-cancel-declared-queue-gone-${Uuid.random()}"
        try {
            connection.connect()
            val channel = connection.openChannel() as ConsumeCountingChannel
            channel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())
            channel.queueBind(queue, exchange = "amq.direct", routingKey = queue, arguments = emptyMap())
            val receiver = channel.basicConsume(queue = queue, noAck = true, arguments = emptyMap())
            val otherReceiver = channel.basicConsume(queue = queue, noAck = true, arguments = emptyMap())

            admin.openChannel().queueDelete(queue)

            withTimeoutOrNull(5.seconds) {
                while (!receiver.isClosedForReceive || !otherReceiver.isClosedForReceive) delay(50)
            }
            assertTrue(receiver.isClosedForReceive, "A consumer was not told its queue is gone")
            assertTrue(otherReceiver.isClosedForReceive, "Another consumer of the queue was not told it is gone")

            awaitRestored(channel)
            assertEquals(ConnectionState.OPEN, channel.state, "The channel was left closed")
            val consumeCalls = channel.consumeCalls
            val restoreConsumeCalls = channel.restoreConsumeCalls
            delay(2.seconds)
            assertEquals(consumeCalls, channel.consumeCalls, "The missing queue was consumed again in a loop")
            assertEquals(restoreConsumeCalls, channel.restoreConsumeCalls, "The channel was restored in a loop")
            assertFails("The restore declared the queue again") { admin.openChannel().queueDeclarePassive(queue) }
            assertEquals(ConnectionState.OPEN, channel.state, "The channel was closed again")
        } finally {
            cleanUp(admin, connection, queue)
            scope.cancel()
        }
    }

    /**
     * A consumer with a tag generated by the broker gets a new one when it is consumed again after a `basic.cancel`
     * from the broker. The application only knows the first one: cancelling the consumer with it must cancel the
     * consumer that runs now, which the next reconnect must not restore either.
     */
    @Test
    fun consumerConsumedAgainIsCancelledByItsFirstTag(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val connection = createRobustAMQPConnection(scope) {
            server {
                reconnectInitialDelay = 300.milliseconds
            }
        }
        val admin = createAMQPConnection(scope)
        val queue = "test-broker-cancel-first-tag-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            val channel = connection.openChannel()
            val canceled = CompletableDeferred<AMQPResponse.Channel>()
            val consumeOk = channel.basicConsume(
                queue = queue,
                noAck = true,
                arguments = emptyMap(),
                onDelivery = {},
                onCanceled = { canceled.complete(it) }
            )
            adminChannel.queueDelete(queue)
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())
            withTimeoutOrNull(10.seconds) {
                while (adminChannel.consumerCount(queue) == 0u) delay(100)
            }
            assertEquals(1u, adminChannel.consumerCount(queue), "The queue was not consumed again after basic.cancel")

            channel.basicCancel(consumeOk.consumerTag)
            assertEquals(0u, adminChannel.consumerCount(queue), "Cancelling by the first tag left the consumer running")
            assertNotNull(withTimeoutOrNull(5.seconds) { canceled.await() }, "The consumer was not told it is cancelled")

            // Heartbeat on a non-zero channel forces broker to close the connection.
            connection.write(Frame(channelId = 1u, payload = Frame.Heartbeat))
            delay(500.milliseconds)
            awaitRestored(channel)
            assertEquals(0u, adminChannel.consumerCount(queue), "A consumer cancelled by its first tag was restored")
        } finally {
            cleanUp(admin, connection, queue)
            scope.cancel()
        }
    }

    private suspend fun awaitRestored(channel: AMQPChannel) = withTimeout(10.seconds) {
        while (true) {
            try {
                channel.basicQos(count = 1u)
                break
            } catch (e: CancellationException) {
                throw e
            } catch (_: Exception) {
                delay(100)
            }
        }
    }

    private suspend fun cleanUp(admin: AMQPConnection, connection: AMQPConnection, queue: String) {
        runCatching {
            withTimeout(5.seconds) {
                admin.openChannel().queueDelete(queue)
                admin.close()
                connection.close()
            }
        }
    }
}
