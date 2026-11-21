package dev.kourier.amqp.robust

import dev.kourier.amqp.AMQPException
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
import kotlin.test.assertIs
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

    /**
     * Runs, once, in place of the next `basic.consume` made outside a restore (the consume made again after a
     * `basic.cancel` from the broker, once the application has consumed its queues), given that consume to run:
     * makes something happen at that exact moment (the application closes the channel, the connection drops).
     */
    var aroundNextConsume: (suspend (consume: suspend () -> AMQPResponse.Channel.Basic.ConsumeOk) -> AMQPResponse.Channel.Basic.ConsumeOk)? =
        null

    /**
     * Holds the next close of the channel by the application in its `SHUTTING_DOWN` state, as a close whose reply
     * has not arrived yet, until this completes. [closing] completes once the close is held.
     */
    var holdNextClose: CompletableDeferred<Unit>? = null
    val closing = CompletableDeferred<Unit>()

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
            restoreConsumeCalls++
            return super.basicConsume(queue, consumerTag, noAck, exclusive, arguments, onDelivery, onCanceled)
        }
        consumeCalls++
        val around = aroundNextConsume
            ?: return super.basicConsume(queue, consumerTag, noAck, exclusive, arguments, onDelivery, onCanceled)
        aroundNextConsume = null
        return around { super.basicConsume(queue, consumerTag, noAck, exclusive, arguments, onDelivery, onCanceled) }
    }

    override suspend fun close(reason: String, code: UShort): AMQPResponse.Channel.Closed {
        holdNextClose?.let { hold ->
            holdNextClose = null
            state = ConnectionState.SHUTTING_DOWN
            closing.complete(Unit)
            hold.await()
        }
        return super.close(reason, code)
    }
}

private class ConsumeCountingConnection(
    config: AMQPConfig,
    scope: CoroutineScope,
) : RobustAMQPConnection(config, scope) {

    public override suspend fun connect() = super.connect()

    var openConnectionCount = 0

    /** Holds the next reconnect, before it opens the connection, until this completes (the broker is unreachable). */
    var holdNextReconnect: CompletableDeferred<Unit>? = null
    val reconnecting = CompletableDeferred<Unit>()

    override suspend fun openConnection() {
        if (openConnectionCount++ > 0) holdNextReconnect?.let { hold ->
            holdNextReconnect = null
            reconnecting.complete(Unit)
            hold.await()
        }
        super.openConnection()
    }

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
     * A consume made again after a `basic.cancel` from the broker that is refused for another reason than a missing
     * queue (here 403 ACCESS_REFUSED, the queue is in exclusive use by another client) keeps the consumer: the
     * channel is restored, the restore refused in turn is retried, and the consumer gets the queue once it is free.
     */
    @Test
    fun consumerOfAQueueRefusedForAnotherReasonIsKept(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val connection = createRobustAMQPConnection(scope) {
            server {
                reconnectInitialDelay = 500.milliseconds
                reconnectMaxDelay = 500.milliseconds
            }
        }
        val admin = createAMQPConnection(scope)
        val queue = "test-broker-cancel-exclusive-use-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            val channel = connection.openChannel()
            val receiver = channel.basicConsume(queue = queue, noAck = true, arguments = emptyMap())

            // The queue comes back, but another client consumes it exclusively before the channel consumes it again.
            adminChannel.queueDelete(queue)
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())
            val exclusiveConsumer = adminChannel.basicConsume(
                queue = queue,
                noAck = true,
                exclusive = true,
                arguments = emptyMap(),
                onDelivery = {}
            )

            delay(2.seconds)
            assertTrue(!receiver.isClosedForReceive, "A consumer refused for another reason than a missing queue was cancelled")

            adminChannel.basicCancel(exclusiveConsumer.consumerTag)
            withTimeoutOrNull(10.seconds) {
                while (adminChannel.consumerCount(queue) == 0u) delay(100)
            }
            assertEquals(1u, adminChannel.consumerCount(queue), "The queue was not consumed again once it was free")
            awaitRestored(channel)
            adminChannel.basicPublish("Once free".toByteArray(), exchange = "", routingKey = queue)
            val delivery = withTimeoutOrNull(5.seconds) { receiver.receive() }
            assertNotNull(delivery, "The consumer did not receive the message published once the queue was free")
            assertEquals("Once free", delivery.message.body.decodeToString())
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

            val cancelOk = channel.basicCancel(consumeOk.consumerTag)
            assertEquals(0u, adminChannel.consumerCount(queue), "Cancelling by the first tag left the consumer running")
            val cancellation = withTimeoutOrNull(5.seconds) { canceled.await() }
            assertNotNull(cancellation, "The consumer was not told it is cancelled")
            // The application only knows the first tag: the answer to its cancel carries it.
            assertEquals(AMQPResponse.Channel.Basic.Canceled(consumeOk.consumerTag), cancellation)
            assertEquals(consumeOk.consumerTag, cancelOk.consumerTag)

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

    /**
     * Several consumers of the same queue on the channel, whose queue is gone for good: the broker cancels each of
     * them, and each is told exactly once, even when the cancellation callback of one of them throws. The channel is
     * open again, and its consumer of another queue keeps receiving.
     */
    @Test
    fun everyConsumerOfAGoneQueueIsToldOnceEvenIfACallbackThrows(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val config = amqpConfig {
            server {
                reconnectInitialDelay = 300.milliseconds
                reconnectMaxDelay = 300.milliseconds
            }
        }
        val connection = ConsumeCountingConnection(config, scope)
        val admin = createAMQPConnection(scope)
        val queue = "test-broker-cancel-gone-several-${Uuid.random()}"
        val otherQueue = "test-broker-cancel-gone-several-other-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())
            adminChannel.queueDeclare(otherQueue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            connection.connect()
            val channel = connection.openChannel() as ConsumeCountingChannel
            val otherReceiver = channel.basicConsume(queue = otherQueue, noAck = true, arguments = emptyMap())
            val cancellations = List(5) { mutableListOf<AMQPResponse.Channel>() }
            val consumerTags = cancellations.mapIndexed { index, received ->
                channel.basicConsume(
                    queue = queue,
                    noAck = true,
                    arguments = emptyMap(),
                    onDelivery = {},
                    onCanceled = {
                        received += it
                        // The first consumer's callback fails: the others must still be told.
                        if (index == 0) throw IllegalStateException("Callback failure")
                    }
                ).consumerTag
            }
            assertEquals(5u, adminChannel.consumerCount(queue))

            adminChannel.queueDelete(queue)

            withTimeoutOrNull(5.seconds) { while (cancellations.any { it.isEmpty() }) delay(50) }
            awaitRestored(channel)
            delay(1.seconds) // Leaves time for a second, wrong, cancellation.
            cancellations.forEachIndexed { index, received ->
                assertEquals(1, received.size, "Consumer $index was not told exactly once its queue is gone: $received")
                val canceled = assertIs<AMQPResponse.Channel.Basic.Canceled>(received.single())
                assertEquals(consumerTags[index], canceled.consumerTag, "Consumer $index was told with another tag")
            }

            assertEquals(ConnectionState.OPEN, channel.state, "The channel was left closed")
            adminChannel.basicPublish("After queue gone".toByteArray(), exchange = "", routingKey = otherQueue)
            val delivery = withTimeoutOrNull(5.seconds) { otherReceiver.receive() }
            assertNotNull(delivery, "The consumer of another queue did not receive its message")
            assertEquals("After queue gone", delivery.message.body.decodeToString())

            val restoreConsumeCalls = channel.restoreConsumeCalls
            delay(1.seconds)
            assertEquals(restoreConsumeCalls, channel.restoreConsumeCalls, "The channel was restored in a loop")
            // The five consumes again are made one at a time: the first is refused, and the others find their consumer
            // already forgotten.
            assertEquals(7, channel.consumeCalls, "Expected the six consumes of the application and one consume again")
        } finally {
            runCatching { withTimeout(5.seconds) { admin.openChannel().queueDelete(otherQueue) } }
            cleanUp(admin, connection, queue)
            scope.cancel()
        }
    }

    /**
     * Two queues consumed on the same channel are gone at once (e.g. a node that hosted both is lost): the restore
     * that follows the first refused consume must not be refused in a loop on the second queue, whose consumer is
     * told too.
     */
    @Test
    fun consumersOfTwoQueuesGoneAtOnceAreBothTold(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val config = amqpConfig {
            server {
                reconnectInitialDelay = 300.milliseconds
                reconnectMaxDelay = 300.milliseconds
            }
        }
        val connection = ConsumeCountingConnection(config, scope)
        val admin = createAMQPConnection(scope)
        val queue = "test-broker-cancel-two-gone-${Uuid.random()}"
        val secondQueue = "test-broker-cancel-two-gone-second-${Uuid.random()}"
        val otherQueue = "test-broker-cancel-two-gone-other-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            listOf(queue, secondQueue, otherQueue).forEach {
                adminChannel.queueDeclare(it, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())
            }

            connection.connect()
            val channel = connection.openChannel() as ConsumeCountingChannel
            val otherReceiver = channel.basicConsume(queue = otherQueue, noAck = true, arguments = emptyMap())
            val receiver = channel.basicConsume(queue = queue, noAck = true, arguments = emptyMap())
            val secondReceiver = channel.basicConsume(queue = secondQueue, noAck = true, arguments = emptyMap())

            adminChannel.queueDelete(queue)
            adminChannel.queueDelete(secondQueue)

            withTimeoutOrNull(10.seconds) {
                while (!receiver.isClosedForReceive || !secondReceiver.isClosedForReceive) delay(50)
            }
            assertTrue(receiver.isClosedForReceive, "The consumer of the first queue was not told it is gone")
            assertTrue(secondReceiver.isClosedForReceive, "The consumer of the second queue was not told it is gone")

            awaitRestored(channel)
            assertEquals(ConnectionState.OPEN, channel.state, "The channel was left closed")
            val restoreConsumeCalls = channel.restoreConsumeCalls
            delay(2.seconds)
            assertEquals(restoreConsumeCalls, channel.restoreConsumeCalls, "The channel was restored in a loop")
            assertEquals(1u, adminChannel.consumerCount(otherQueue), "The consumer of another queue was dropped")
            adminChannel.basicPublish("After two queues gone".toByteArray(), exchange = "", routingKey = otherQueue)
            val delivery = withTimeoutOrNull(5.seconds) { otherReceiver.receive() }
            assertNotNull(delivery, "The consumer of another queue did not receive its message")
        } finally {
            runCatching {
                withTimeout(5.seconds) {
                    admin.openChannel().queueDelete(secondQueue)
                    admin.openChannel().queueDelete(otherQueue)
                }
            }
            cleanUp(admin, connection, queue)
            scope.cancel()
        }
    }

    /**
     * A consumer consumed again once (the broker gave it a new tag) whose queue is then gone for good is told with the
     * tag the application received, the only one it knows.
     */
    @Test
    fun consumerOfAGoneQueueIsToldWithTheTagItWasGiven(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val connection = createRobustAMQPConnection(scope) {
            server {
                reconnectInitialDelay = 300.milliseconds
                reconnectMaxDelay = 300.milliseconds
            }
        }
        val admin = createAMQPConnection(scope)
        val queue = "test-broker-cancel-gone-first-tag-${Uuid.random()}"
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
            // Consumed again, under a new tag.
            adminChannel.queueDelete(queue)
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())
            withTimeoutOrNull(10.seconds) { while (adminChannel.consumerCount(queue) == 0u) delay(100) }
            assertEquals(1u, adminChannel.consumerCount(queue), "The queue was not consumed again after basic.cancel")

            // Then gone for good.
            adminChannel.queueDelete(queue)
            val response = withTimeoutOrNull(5.seconds) { canceled.await() }
            val cancellation = assertIs<AMQPResponse.Channel.Basic.Canceled>(response, "The consumer was not told")
            assertEquals(consumeOk.consumerTag, cancellation.consumerTag, "The consumer was told with a tag it does not know")
        } finally {
            cleanUp(admin, connection, queue)
            scope.cancel()
        }
    }

    /**
     * The application closes the channel while the consume made again after a `basic.cancel` from the broker is under
     * way: the consume fails on the closed channel, and the consumer is told, once, instead of being left silent.
     */
    @Test
    fun consumerIsToldWhenTheApplicationClosesTheChannelDuringTheConsumeAgain(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val config = amqpConfig {
            server {
                reconnectInitialDelay = 300.milliseconds
            }
        }
        val connection = ConsumeCountingConnection(config, scope)
        val admin = createAMQPConnection(scope)
        val queue = "test-broker-cancel-close-during-consume-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            connection.connect()
            val channel = connection.openChannel() as ConsumeCountingChannel
            val cancellations = mutableListOf<AMQPResponse.Channel>()
            channel.basicConsume(
                queue = queue,
                noAck = true,
                arguments = emptyMap(),
                onDelivery = {},
                onCanceled = { cancellations += it }
            )
            val consumeFailure = CompletableDeferred<Throwable>()
            channel.aroundNextConsume = { consume ->
                channel.close()
                try {
                    consume()
                } catch (e: Exception) {
                    consumeFailure.complete(e)
                    throw e
                }
            }

            adminChannel.queueDelete(queue)
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            assertIs<AMQPException.ChannelClosed>(withTimeoutOrNull(5.seconds) { consumeFailure.await() })
            withTimeoutOrNull(5.seconds) { while (cancellations.isEmpty()) delay(50) }
            delay(500.milliseconds)
            assertEquals(1, cancellations.size, "The consumer was not told exactly once: $cancellations")
            assertIs<AMQPResponse.Channel.Basic.Canceled>(cancellations.single())
            assertEquals(0, channel.restoreConsumeCalls, "A channel closed by the application was restored")
            assertEquals(0u, adminChannel.consumerCount(queue), "The queue was consumed on a closed channel")
        } finally {
            cleanUp(admin, connection, queue)
            scope.cancel()
        }
    }

    /**
     * The application is closing the channel (its close is sent, not answered yet) when the consume would be made
     * again after a `basic.cancel` from the broker: the queue is not consumed again, and the consumer is told.
     */
    @Test
    fun consumerIsToldWhenTheApplicationIsClosingTheChannel(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val config = amqpConfig {
            server {
                reconnectInitialDelay = 500.milliseconds
            }
        }
        val connection = ConsumeCountingConnection(config, scope)
        val admin = createAMQPConnection(scope)
        val queue = "test-broker-cancel-while-closing-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            connection.connect()
            val channel = connection.openChannel() as ConsumeCountingChannel
            val canceled = CompletableDeferred<AMQPResponse.Channel>()
            channel.basicConsume(
                queue = queue,
                noAck = true,
                arguments = emptyMap(),
                onDelivery = {},
                onCanceled = { canceled.complete(it) }
            )

            adminChannel.queueDelete(queue)
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())
            withTimeout(5.seconds) { while (adminChannel.consumerCount(queue) != 0u) delay(20) }

            val release = CompletableDeferred<Unit>()
            channel.holdNextClose = release
            val close = scope.launch { channel.close() }
            channel.closing.await()

            val response = withTimeoutOrNull(5.seconds) { canceled.await() }
            assertIs<AMQPResponse.Channel.Basic.Canceled>(response, "The consumer was not told while the channel closes")
            assertEquals(1, channel.consumeCalls, "The queue was consumed again on a channel being closed")

            release.complete(Unit)
            withTimeout(5.seconds) { close.join() }
            assertEquals(0u, adminChannel.consumerCount(queue))
        } finally {
            cleanUp(admin, connection, queue)
            scope.cancel()
        }
    }

    /**
     * The connection drops while the consume made again after a `basic.cancel` from the broker waits for its reply:
     * the consumer is kept, not told, and the reconnect restores it.
     */
    @Test
    fun consumerIsRestoredWhenTheConnectionDropsDuringTheConsumeAgain(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val config = amqpConfig {
            server {
                reconnectInitialDelay = 300.milliseconds
            }
        }
        val connection = ConsumeCountingConnection(config, scope)
        val admin = createAMQPConnection(scope)
        val queue = "test-broker-cancel-drop-during-consume-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            connection.connect()
            val channel = connection.openChannel() as ConsumeCountingChannel
            val canceled = CompletableDeferred<AMQPResponse.Channel>()
            val deliveries = kotlinx.coroutines.channels.Channel<String>(kotlinx.coroutines.channels.Channel.UNLIMITED)
            channel.basicConsume(
                queue = queue,
                noAck = true,
                arguments = emptyMap(),
                onDelivery = { deliveries.send(it.message.body.decodeToString()) },
                onCanceled = { canceled.complete(it) }
            )
            val consumeFailure = CompletableDeferred<Throwable>()
            channel.aroundNextConsume = { consume ->
                // Heartbeat on a non-zero channel forces broker to close the connection.
                connection.write(Frame(channelId = channel.id, payload = Frame.Heartbeat))
                try {
                    consume()
                } catch (e: Exception) {
                    consumeFailure.complete(e)
                    throw e
                }
            }

            adminChannel.queueDelete(queue)
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            assertNotNull(withTimeoutOrNull(5.seconds) { consumeFailure.await() }, "The consume again did not fail")
            withTimeoutOrNull(10.seconds) { while (adminChannel.consumerCount(queue) == 0u) delay(100) }
            assertEquals(1u, adminChannel.consumerCount(queue), "The reconnect did not restore the consumer")
            awaitRestored(channel)
            adminChannel.basicPublish("After the drop".toByteArray(), exchange = "", routingKey = queue)
            assertEquals("After the drop", withTimeoutOrNull(5.seconds) { deliveries.receive() })
            assertTrue(!canceled.isCompleted, "A consumer restored by the reconnect was told it is cancelled")
        } finally {
            cleanUp(admin, connection, queue)
            scope.cancel()
        }
    }

    /**
     * The broker refuses the consume made again with 404 NOT_FOUND, and the connection drops right after: the queue is
     * still gone, so the consumer is told, and the reconnect restores the channel without it, instead of being refused
     * in a loop.
     */
    @Test
    fun consumerOfAGoneQueueIsToldWhenTheConnectionDropsAfterTheRefusal(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val config = amqpConfig {
            server {
                reconnectInitialDelay = 300.milliseconds
                reconnectMaxDelay = 300.milliseconds
            }
        }
        val connection = ConsumeCountingConnection(config, scope)
        val admin = createAMQPConnection(scope)
        val queue = "test-broker-cancel-gone-then-drop-${Uuid.random()}"
        val otherQueue = "test-broker-cancel-gone-then-drop-other-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())
            adminChannel.queueDeclare(otherQueue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            connection.connect()
            val channel = connection.openChannel() as ConsumeCountingChannel
            val otherReceiver = channel.basicConsume(queue = otherQueue, noAck = true, arguments = emptyMap())
            val receiver = channel.basicConsume(queue = queue, noAck = true, arguments = emptyMap())
            val refusal = CompletableDeferred<AMQPException.ChannelClosed>()
            channel.aroundNextConsume = { consume ->
                val iteration = connection.currentIteration
                try {
                    consume()
                } catch (e: AMQPException.ChannelClosed) {
                    refusal.complete(e)
                    // Heartbeat on a non-zero channel forces broker to close the connection. Not on this channel,
                    // whose frames the broker discards until the client acknowledges its close.
                    connection.write(Frame(channelId = (channel.id + 1u).toUShort(), payload = Frame.Heartbeat))
                    // The reconnect may open the next connection within milliseconds: wait for this one to be gone.
                    withTimeout(5.seconds) { while (!iteration.isCompleted) delay(10) }
                    throw e
                }
            }

            adminChannel.queueDelete(queue)

            assertEquals(404.toUShort(), withTimeoutOrNull(5.seconds) { refusal.await() }?.replyCode)
            withTimeoutOrNull(5.seconds) { while (!receiver.isClosedForReceive) delay(50) }
            assertTrue(receiver.isClosedForReceive, "The consumer was not told its queue is gone")

            awaitRestored(channel)
            assertEquals(ConnectionState.OPEN, channel.state, "The channel was left closed")
            val restoreConsumeCalls = channel.restoreConsumeCalls
            delay(2.seconds)
            assertEquals(restoreConsumeCalls, channel.restoreConsumeCalls, "The channel was restored in a loop")
            adminChannel.basicPublish("After the drop".toByteArray(), exchange = "", routingKey = otherQueue)
            val delivery = withTimeoutOrNull(5.seconds) { otherReceiver.receive() }
            assertNotNull(delivery, "The consumer of another queue was not restored by the reconnect")
        } finally {
            runCatching { withTimeout(5.seconds) { admin.openChannel().queueDelete(otherQueue) } }
            cleanUp(admin, connection, queue)
            scope.cancel()
        }
    }

    /**
     * The consume made again after a `basic.cancel` from the broker gets no reply in time: the broker may still
     * register it, and nothing else would consume the queue again. The connection is recovered, as after a restore
     * that stalls, and the reconnect restores the consumer.
     */
    @Test
    fun consumeAgainLeftWithoutAReplyIsRecoveredByAReconnect(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val config = amqpConfig {
            server {
                reconnectInitialDelay = 300.milliseconds
            }
        }
        val connection = ConsumeCountingConnection(config, scope)
        val admin = createAMQPConnection(scope)
        val queue = "test-broker-cancel-consume-timeout-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            connection.connect()
            val channel = connection.openChannel() as ConsumeCountingChannel
            val canceled = CompletableDeferred<AMQPResponse.Channel>()
            val deliveries = kotlinx.coroutines.channels.Channel<String>(kotlinx.coroutines.channels.Channel.UNLIMITED)
            channel.basicConsume(
                queue = queue,
                noAck = true,
                arguments = emptyMap(),
                onDelivery = { deliveries.send(it.message.body.decodeToString()) },
                onCanceled = { canceled.complete(it) }
            )
            val timedOut = CompletableDeferred<Unit>()
            channel.aroundNextConsume = {
                timedOut.complete(Unit)
                throw AMQPException.RpcTimeout(channelId = channel.id, timeout = 1.seconds)
            }

            adminChannel.queueDelete(queue)
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())
            withTimeout(5.seconds) { timedOut.await() }

            withTimeoutOrNull(10.seconds) { while (adminChannel.consumerCount(queue) == 0u) delay(100) }
            assertEquals(1u, adminChannel.consumerCount(queue), "A consume again left without a reply was never made again")
            awaitRestored(channel)
            adminChannel.basicPublish("After the timeout".toByteArray(), exchange = "", routingKey = queue)
            assertEquals("After the timeout", withTimeoutOrNull(5.seconds) { deliveries.receive() })
            assertTrue(!canceled.isCompleted, "A consumer recovered by the reconnect was told it is cancelled")
        } finally {
            cleanUp(admin, connection, queue)
            scope.cancel()
        }
    }

    /**
     * Several consumers on the channel, one consumed again under a new tag: cancelling another one by its tag leaves
     * the consumer consumed again running, cancelling that one by its first tag stops it, a second cancel of a
     * consumer already cancelled does nothing, and the reconnect restores none of them.
     */
    @Test
    fun consumersOfAChannelAreCancelledIndependentlyByTheirTags(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val connection = createRobustAMQPConnection(scope) {
            server {
                reconnectInitialDelay = 300.milliseconds
            }
        }
        val admin = createAMQPConnection(scope)
        val queue = "test-broker-cancel-independent-${Uuid.random()}"
        val otherQueue = "test-broker-cancel-independent-other-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())
            adminChannel.queueDeclare(otherQueue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            val channel = connection.openChannel()
            val deliveries = kotlinx.coroutines.channels.Channel<String>(kotlinx.coroutines.channels.Channel.UNLIMITED)
            val consumeOk = channel.basicConsume(
                queue = queue,
                noAck = true,
                arguments = emptyMap(),
                onDelivery = { deliveries.send(it.message.body.decodeToString()) },
            )
            val firstOther = channel.basicConsume(queue = otherQueue, noAck = true, arguments = emptyMap(), onDelivery = {})
            val secondOther = channel.basicConsume(queue = otherQueue, noAck = true, arguments = emptyMap(), onDelivery = {})

            adminChannel.queueDelete(queue)
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())
            withTimeoutOrNull(10.seconds) { while (adminChannel.consumerCount(queue) == 0u) delay(100) }
            assertEquals(1u, adminChannel.consumerCount(queue), "The queue was not consumed again after basic.cancel")

            channel.basicCancel(secondOther.consumerTag)
            assertEquals(1u, adminChannel.consumerCount(otherQueue), "Cancelling a consumer stopped another one")
            assertEquals(1u, adminChannel.consumerCount(queue), "Cancelling another consumer stopped the one consumed again")
            adminChannel.basicPublish("Still consumed".toByteArray(), exchange = "", routingKey = queue)
            assertEquals("Still consumed", withTimeoutOrNull(5.seconds) { deliveries.receive() })

            channel.basicCancel(consumeOk.consumerTag)
            channel.basicCancel(firstOther.consumerTag)
            assertEquals(0u, adminChannel.consumerCount(queue), "Cancelling by the first tag left the consumer running")
            assertEquals(0u, adminChannel.consumerCount(otherQueue))

            // Already cancelled: nothing to do, and the channel stays open.
            channel.basicCancel(consumeOk.consumerTag)
            channel.basicCancel(secondOther.consumerTag)
            assertEquals(ConnectionState.OPEN, channel.state)

            // Heartbeat on a non-zero channel forces broker to close the connection.
            connection.write(Frame(channelId = 1u, payload = Frame.Heartbeat))
            delay(500.milliseconds)
            awaitRestored(channel)
            assertEquals(0u, adminChannel.consumerCount(queue), "A cancelled consumer was restored")
            assertEquals(0u, adminChannel.consumerCount(otherQueue), "A cancelled consumer was restored")
        } finally {
            runCatching { withTimeout(5.seconds) { admin.openChannel().queueDelete(otherQueue) } }
            cleanUp(admin, connection, queue)
            scope.cancel()
        }
    }

    /**
     * A delivery is being processed when the queue is gone for good: its handler, which does not let itself be
     * cancelled, acks it once the channel has been restored without the queue. That ack is from before the restore:
     * it is ignored instead of closing the channel again, and the other consumer keeps receiving.
     */
    @Test
    fun deliveryInProgressWhenTheQueueIsGoneIsAckedWithoutClosingTheChannel(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val config = amqpConfig {
            server {
                reconnectInitialDelay = 300.milliseconds
                reconnectMaxDelay = 300.milliseconds
            }
        }
        val connection = ConsumeCountingConnection(config, scope)
        val admin = createAMQPConnection(scope)
        val queue = "test-broker-cancel-gone-delivery-${Uuid.random()}"
        val otherQueue = "test-broker-cancel-gone-delivery-other-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())
            adminChannel.queueDeclare(otherQueue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            connection.connect()
            val channel = connection.openChannel() as ConsumeCountingChannel
            val otherReceiver = channel.basicConsume(queue = otherQueue, noAck = true, arguments = emptyMap())
            val started = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            val acked = CompletableDeferred<Unit>()
            val canceled = CompletableDeferred<AMQPResponse.Channel>()
            channel.basicConsume(
                queue = queue,
                noAck = false,
                arguments = emptyMap(),
                onDelivery = { delivery ->
                    withContext(NonCancellable) {
                        started.complete(Unit)
                        release.await()
                        channel.basicAck(delivery.message.deliveryTag)
                        acked.complete(Unit)
                    }
                },
                onCanceled = { canceled.complete(it) }
            )
            adminChannel.basicPublish("In progress".toByteArray(), exchange = "", routingKey = queue)
            withTimeout(5.seconds) { started.await() }

            adminChannel.queueDelete(queue)
            assertIs<AMQPResponse.Channel.Basic.Canceled>(withTimeoutOrNull(5.seconds) { canceled.await() })
            awaitRestored(channel)

            release.complete(Unit)
            withTimeout(5.seconds) { acked.await() }
            delay(500.milliseconds)
            assertEquals(ConnectionState.OPEN, channel.state, "The ack of the delivery in progress closed the channel")
            adminChannel.basicPublish("After the ack".toByteArray(), exchange = "", routingKey = otherQueue)
            val delivery = withTimeoutOrNull(5.seconds) { otherReceiver.receive() }
            assertNotNull(delivery, "The other consumer of the channel did not receive its message")
            assertEquals("After the ack", delivery.message.body.decodeToString())
        } finally {
            runCatching { withTimeout(5.seconds) { admin.openChannel().queueDelete(otherQueue) } }
            cleanUp(admin, connection, queue)
            scope.cancel()
        }
    }

    /**
     * A delivery is being processed when the queue is deleted and declared again: the handler acks it after the
     * consumer was consumed again, without closing the channel, and the consumer receives the next messages.
     */
    @Test
    fun deliveryInProgressWhenTheQueueComesBackIsAckedAndTheConsumerKeepsReceiving(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val connection = createRobustAMQPConnection(scope) {
            server {
                reconnectInitialDelay = 300.milliseconds
            }
        }
        val admin = createAMQPConnection(scope)
        val queue = "test-broker-cancel-back-delivery-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            val channel = connection.openChannel()
            val started = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            val deliveries = kotlinx.coroutines.channels.Channel<String>(kotlinx.coroutines.channels.Channel.UNLIMITED)
            channel.basicConsume(
                queue = queue,
                noAck = false,
                arguments = emptyMap(),
                onDelivery = { delivery ->
                    withContext(NonCancellable) {
                        if (started.complete(Unit)) release.await()
                        channel.basicAck(delivery.message.deliveryTag)
                        deliveries.send(delivery.message.body.decodeToString())
                    }
                },
            )
            adminChannel.basicPublish("In progress".toByteArray(), exchange = "", routingKey = queue)
            withTimeout(5.seconds) { started.await() }

            adminChannel.queueDelete(queue)
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())
            withTimeoutOrNull(10.seconds) { while (adminChannel.consumerCount(queue) == 0u) delay(100) }
            assertEquals(1u, adminChannel.consumerCount(queue), "The queue was not consumed again after basic.cancel")

            release.complete(Unit)
            assertEquals("In progress", withTimeoutOrNull(5.seconds) { deliveries.receive() })
            adminChannel.basicPublish("Next".toByteArray(), exchange = "", routingKey = queue)
            assertEquals("Next", withTimeoutOrNull(5.seconds) { deliveries.receive() }, "The consumer stopped receiving")
            delay(500.milliseconds)
            assertEquals(ConnectionState.OPEN, channel.state, "The ack of the delivery in progress closed the channel")
        } finally {
            cleanUp(admin, connection, queue)
            scope.cancel()
        }
    }

    /**
     * The connection drops while a consumer the broker cancelled waits to be consumed again, and the broker stays
     * unreachable past that wait: the consume is not made on the closed channel, and the consumer, kept, is restored
     * by the reconnect.
     */
    @Test
    fun consumerWaitingToBeConsumedAgainIsRestoredByALateReconnect(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val config = amqpConfig {
            server {
                reconnectInitialDelay = 300.milliseconds
            }
        }
        val connection = ConsumeCountingConnection(config, scope)
        val admin = createAMQPConnection(scope)
        val queue = "test-broker-cancel-late-reconnect-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            connection.connect()
            val channel = connection.openChannel() as ConsumeCountingChannel
            val canceled = CompletableDeferred<AMQPResponse.Channel>()
            val deliveries = kotlinx.coroutines.channels.Channel<String>(kotlinx.coroutines.channels.Channel.UNLIMITED)
            channel.basicConsume(
                queue = queue,
                noAck = true,
                arguments = emptyMap(),
                onDelivery = { deliveries.send(it.message.body.decodeToString()) },
                onCanceled = { canceled.complete(it) }
            )

            adminChannel.queueDelete(queue)
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())
            withTimeout(5.seconds) { while (adminChannel.consumerCount(queue) != 0u) delay(20) }
            val release = CompletableDeferred<Unit>()
            connection.holdNextReconnect = release
            // Heartbeat on a non-zero channel forces broker to close the connection.
            connection.write(Frame(channelId = channel.id, payload = Frame.Heartbeat))
            withTimeout(5.seconds) { connection.reconnecting.await() }

            // Past the wait before the consume again.
            delay(1.seconds)
            assertEquals(1, channel.consumeCalls, "The queue was consumed again on a closed connection")

            release.complete(Unit)
            withTimeoutOrNull(10.seconds) { while (adminChannel.consumerCount(queue) == 0u) delay(100) }
            assertEquals(1u, adminChannel.consumerCount(queue), "The reconnect did not restore the consumer")
            awaitRestored(channel)
            adminChannel.basicPublish("After the reconnect".toByteArray(), exchange = "", routingKey = queue)
            assertEquals("After the reconnect", withTimeoutOrNull(5.seconds) { deliveries.receive() })
            assertTrue(!canceled.isCompleted, "A consumer restored by the reconnect was told it is cancelled")
        } finally {
            cleanUp(admin, connection, queue)
            scope.cancel()
        }
    }

    /**
     * The consume made again fails once the connection it was made on has been replaced by the reconnect (e.g. its
     * write failed on the old socket): the new connection is left alone, and restores the consumer.
     */
    @Test
    fun consumeAgainFailingOnAReplacedConnectionDoesNotReconnectAgain(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val config = amqpConfig {
            server {
                reconnectInitialDelay = 300.milliseconds
            }
        }
        val connection = ConsumeCountingConnection(config, scope)
        val admin = createAMQPConnection(scope)
        val queue = "test-broker-cancel-replaced-connection-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            connection.connect()
            val channel = connection.openChannel() as ConsumeCountingChannel
            val deliveries = kotlinx.coroutines.channels.Channel<String>(kotlinx.coroutines.channels.Channel.UNLIMITED)
            channel.basicConsume(
                queue = queue,
                noAck = true,
                arguments = emptyMap(),
                onDelivery = { deliveries.send(it.message.body.decodeToString()) },
            )
            channel.aroundNextConsume = {
                val iteration = connection.currentIteration
                // Heartbeat on a non-zero channel forces broker to close the connection.
                connection.write(Frame(channelId = channel.id, payload = Frame.Heartbeat))
                withTimeout(5.seconds) { while (connection.currentIteration === iteration) delay(10) }
                throw IllegalStateException("Write failed on the previous socket")
            }

            adminChannel.queueDelete(queue)
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            withTimeoutOrNull(10.seconds) { while (adminChannel.consumerCount(queue) == 0u) delay(100) }
            assertEquals(1u, adminChannel.consumerCount(queue), "The reconnect did not restore the consumer")
            awaitRestored(channel)
            delay(1.seconds)
            assertEquals(2, connection.openConnectionCount, "The new connection was dropped again")
            adminChannel.basicPublish("After the reconnect".toByteArray(), exchange = "", routingKey = queue)
            assertEquals("After the reconnect", withTimeoutOrNull(5.seconds) { deliveries.receive() })
        } finally {
            cleanUp(admin, connection, queue)
            scope.cancel()
        }
    }

    /**
     * The application cancels the consumer, then closes the channel, while the consume made again is under way: the
     * consumer is told once, by its own cancel, and not a second time when the consume fails on the closed channel.
     */
    @Test
    fun consumerCancelledThenClosedDuringTheConsumeAgainIsToldOnce(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val config = amqpConfig {
            server {
                reconnectInitialDelay = 300.milliseconds
            }
        }
        val connection = ConsumeCountingConnection(config, scope)
        val admin = createAMQPConnection(scope)
        val queue = "test-broker-cancel-cancel-then-close-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            connection.connect()
            val channel = connection.openChannel() as ConsumeCountingChannel
            val cancellations = mutableListOf<AMQPResponse.Channel>()
            val consumeOk = channel.basicConsume(
                queue = queue,
                noAck = true,
                arguments = emptyMap(),
                onDelivery = {},
                onCanceled = { cancellations += it }
            )
            val consumeFailure = CompletableDeferred<Throwable>()
            channel.aroundNextConsume = { consume ->
                channel.basicCancel(consumeOk.consumerTag)
                channel.close()
                try {
                    consume()
                } catch (e: Exception) {
                    consumeFailure.complete(e)
                    throw e
                }
            }

            adminChannel.queueDelete(queue)
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            assertIs<AMQPException.ChannelClosed>(withTimeoutOrNull(5.seconds) { consumeFailure.await() })
            delay(500.milliseconds)
            assertEquals(1, cancellations.size, "The consumer was not told exactly once: $cancellations")
            assertEquals(0u, adminChannel.consumerCount(queue), "The queue was consumed on a closed channel")
        } finally {
            cleanUp(admin, connection, queue)
            scope.cancel()
        }
    }

    /**
     * The consumer of a server-named queue that another client deletes is cancelled by the broker, and the connection
     * drops before it is consumed again: the reconnect declares a new queue, with its bindings, and consumes it, and
     * the consume made again then leaves it alone.
     */
    @Test
    fun consumerOfAServerNamedQueueCancelledByTheBrokerIsRestoredByAReconnect(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val config = amqpConfig {
            server {
                reconnectInitialDelay = 1.seconds
            }
        }
        val connection = ConsumeCountingConnection(config, scope)
        val admin = createAMQPConnection(scope)
        val routingKey = "test-broker-cancel-server-named-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            connection.connect()
            val channel = connection.openChannel() as ConsumeCountingChannel
            val queue = channel.queueDeclare("", durable = false, exclusive = false, autoDelete = true, arguments = emptyMap())
            channel.queueBind(queue.queueName, exchange = "amq.direct", routingKey = routingKey, arguments = emptyMap())
            val canceled = CompletableDeferred<AMQPResponse.Channel>()
            val deliveries = kotlinx.coroutines.channels.Channel<String>(kotlinx.coroutines.channels.Channel.UNLIMITED)
            channel.basicConsume(
                queue = queue.queueName,
                noAck = true,
                arguments = emptyMap(),
                onDelivery = { deliveries.send(it.message.body.decodeToString()) },
                onCanceled = { canceled.complete(it) }
            )

            adminChannel.queueDelete(queue.queueName)
            delay(100.milliseconds) // The broker's basic.cancel reaches the channel.
            // Heartbeat on a non-zero channel forces broker to close the connection.
            connection.write(Frame(channelId = channel.id, payload = Frame.Heartbeat))
            delay(300.milliseconds)
            awaitRestored(channel)
            // Past the wait before the consume again, which finds the consumer restored.
            delay(1.seconds)
            assertEquals(1, channel.consumeCalls, "A consumer restored by the reconnect was consumed again")

            adminChannel.basicPublish("To the new queue".toByteArray(), exchange = "amq.direct", routingKey = routingKey)
            assertEquals("To the new queue", withTimeoutOrNull(5.seconds) { deliveries.receive() })
            assertTrue(!canceled.isCompleted, "A consumer restored by the reconnect was told it is cancelled")
        } finally {
            runCatching {
                withTimeout(5.seconds) {
                    admin.close()
                    connection.close()
                }
            }
            scope.cancel()
        }
    }

    /**
     * The application cancels a consumer that the broker cancelled and that waits to be consumed again: its listener
     * is gone with the broker's cancel, so the robust channel tells it, as the cancel of a running consumer does.
     */
    @Test
    fun consumerCancelledByTheApplicationWhileWaitingToBeConsumedAgainIsTold(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val connection = createRobustAMQPConnection(scope) {
            server {
                reconnectInitialDelay = 1.seconds
            }
        }
        val admin = createAMQPConnection(scope)
        val queue = "test-broker-cancel-pending-cancel-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            val channel = connection.openChannel()
            val cancellations = mutableListOf<AMQPResponse.Channel>()
            val consumeOk = channel.basicConsume(
                queue = queue,
                noAck = true,
                arguments = emptyMap(),
                onDelivery = {},
                onCanceled = { cancellations += it }
            )
            adminChannel.queueDelete(queue)
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())
            withTimeout(5.seconds) { while (adminChannel.consumerCount(queue) != 0u) delay(20) }
            delay(100.milliseconds) // The broker's basic.cancel reaches the channel.

            val canceled = channel.basicCancel(consumeOk.consumerTag)
            assertEquals(consumeOk.consumerTag, canceled.consumerTag)
            assertEquals(
                listOf<AMQPResponse.Channel>(AMQPResponse.Channel.Basic.Canceled(consumeOk.consumerTag)),
                cancellations,
                "The consumer was not told once of its cancel"
            )
            delay(2.seconds)
            assertEquals(1, cancellations.size, "The consumer was told again")
            assertEquals(0u, adminChannel.consumerCount(queue), "A consumer cancelled by the application was consumed again")
        } finally {
            cleanUp(admin, connection, queue)
            scope.cancel()
        }
    }

    /**
     * The application cancels the consumer while the consume made again after a `basic.cancel` from the broker is
     * under way, before the broker answers it: the consumer that the consume starts must not be left running, and the
     * consumer is told once.
     */
    @Test
    fun consumerCancelledByTheApplicationBeforeTheConsumeAgainIsAnsweredIsNotLeftRunning() =
        consumerCancelledByTheApplicationDuringTheConsumeAgainIsNotLeftRunning(beforeTheReply = true)

    /**
     * The application cancels the consumer right after the broker answered the consume made again, before the robust
     * channel has recorded it: the consumer must not be left running, and the consumer is told once.
     */
    @Test
    fun consumerCancelledByTheApplicationAfterTheConsumeAgainIsAnsweredIsNotLeftRunning() =
        consumerCancelledByTheApplicationDuringTheConsumeAgainIsNotLeftRunning(beforeTheReply = false)

    private fun consumerCancelledByTheApplicationDuringTheConsumeAgainIsNotLeftRunning(
        beforeTheReply: Boolean,
    ): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val config = amqpConfig {
            server {
                reconnectInitialDelay = 300.milliseconds
            }
        }
        val connection = ConsumeCountingConnection(config, scope)
        val admin = createAMQPConnection(scope)
        val queue = "test-broker-cancel-cancel-during-consume-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            connection.connect()
            val channel = connection.openChannel() as ConsumeCountingChannel
            val cancellations = mutableListOf<AMQPResponse.Channel>()
            val consumeOk = channel.basicConsume(
                queue = queue,
                noAck = true,
                arguments = emptyMap(),
                onDelivery = {},
                onCanceled = { cancellations += it }
            )
            val consumedAgain = CompletableDeferred<Unit>()
            channel.aroundNextConsume = { consume ->
                if (beforeTheReply) scope.launch { channel.basicCancel(consumeOk.consumerTag) }.join()
                consume().also {
                    if (!beforeTheReply) scope.launch { channel.basicCancel(consumeOk.consumerTag) }.join()
                    consumedAgain.complete(Unit)
                }
            }

            adminChannel.queueDelete(queue)
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())
            withTimeout(5.seconds) { consumedAgain.await() }

            delay(1.seconds)
            assertEquals(0u, adminChannel.consumerCount(queue), "The consumer cancelled by the application was left running")
            assertEquals(1, cancellations.size, "The consumer was not told exactly once: $cancellations")

            // Heartbeat on a non-zero channel forces broker to close the connection.
            connection.write(Frame(channelId = channel.id, payload = Frame.Heartbeat))
            delay(500.milliseconds)
            awaitRestored(channel)
            assertEquals(0u, adminChannel.consumerCount(queue), "The consumer cancelled by the application was restored")
        } finally {
            cleanUp(admin, connection, queue)
            scope.cancel()
        }
    }

    /**
     * A server-named queue that another client deletes is not gone for the robust channel, which declares it again
     * on every restore: the consume made again is refused with 404 NOT_FOUND, and the restore of the channel declares
     * a new queue, with its bindings, and consumes it.
     */
    @Test
    fun consumerOfAServerNamedQueueDeletedByAnotherClientKeepsConsuming(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val config = amqpConfig {
            server {
                reconnectInitialDelay = 300.milliseconds
            }
        }
        val connection = ConsumeCountingConnection(config, scope)
        val admin = createAMQPConnection(scope)
        val routingKey = "test-broker-cancel-server-named-deleted-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            connection.connect()
            val channel = connection.openChannel() as ConsumeCountingChannel
            val queue = channel.queueDeclare("", durable = false, exclusive = false, autoDelete = true, arguments = emptyMap())
            channel.queueBind(queue.queueName, exchange = "amq.direct", routingKey = routingKey, arguments = emptyMap())
            val canceled = CompletableDeferred<AMQPResponse.Channel>()
            val deliveries = kotlinx.coroutines.channels.Channel<String>(kotlinx.coroutines.channels.Channel.UNLIMITED)
            channel.basicConsume(
                queue = queue.queueName,
                noAck = true,
                arguments = emptyMap(),
                onDelivery = { deliveries.send(it.message.body.decodeToString()) },
                onCanceled = { canceled.complete(it) }
            )

            adminChannel.queueDelete(queue.queueName)
            withTimeoutOrNull(5.seconds) { while (channel.restoreConsumeCalls == 0) delay(50) }
            awaitRestored(channel)

            adminChannel.basicPublish("To the new queue".toByteArray(), exchange = "amq.direct", routingKey = routingKey)
            assertEquals("To the new queue", withTimeoutOrNull(5.seconds) { deliveries.receive() }, "The consumer stopped")
            assertTrue(!canceled.isCompleted, "The consumer of a server-named queue was cancelled")
        } finally {
            runCatching {
                withTimeout(5.seconds) {
                    admin.close()
                    connection.close()
                }
            }
            scope.cancel()
        }
    }

    /**
     * The application deletes the queue of a consumer that the broker cancelled and that waits to be consumed again:
     * the consumer is told, once, since its listener is gone with the broker's cancel, and is not consumed again.
     */
    @Test
    fun consumerWaitingToBeConsumedAgainIsToldWhenTheApplicationDeletesItsQueue(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val config = amqpConfig {
            server {
                reconnectInitialDelay = 1.seconds
            }
        }
        val connection = ConsumeCountingConnection(config, scope)
        val admin = createAMQPConnection(scope)
        val queue = "test-broker-cancel-pending-delete-${Uuid.random()}"
        try {
            val adminChannel = admin.openChannel()
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())

            connection.connect()
            val channel = connection.openChannel() as ConsumeCountingChannel
            val cancellations = mutableListOf<AMQPResponse.Channel>()
            val consumeOk = channel.basicConsume(
                queue = queue,
                noAck = true,
                arguments = emptyMap(),
                onDelivery = {},
                onCanceled = { cancellations += it }
            )
            adminChannel.queueDelete(queue)
            adminChannel.queueDeclare(queue, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())
            withTimeout(5.seconds) { while (adminChannel.consumerCount(queue) != 0u) delay(20) }
            delay(100.milliseconds) // The broker's basic.cancel reaches the channel.

            channel.queueDelete(queue)
            assertEquals(
                listOf<AMQPResponse.Channel>(AMQPResponse.Channel.Basic.Canceled(consumeOk.consumerTag)),
                cancellations,
                "The consumer was not told once its queue was deleted"
            )
            delay(2.seconds)
            assertEquals(1, cancellations.size, "The consumer was told again")
            assertEquals(1, channel.consumeCalls, "The queue deleted by the application was consumed again")
            assertEquals(ConnectionState.OPEN, channel.state)
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
