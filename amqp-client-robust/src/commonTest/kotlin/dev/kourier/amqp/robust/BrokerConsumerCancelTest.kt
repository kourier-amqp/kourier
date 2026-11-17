package dev.kourier.amqp.robust

import dev.kourier.amqp.Frame
import dev.kourier.amqp.connection.createAMQPConnection
import io.ktor.utils.io.core.*
import kotlinx.coroutines.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.Uuid

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
}
