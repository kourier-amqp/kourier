package dev.kourier.amqp.robust

import dev.kourier.amqp.AMQPException
import dev.kourier.amqp.BuiltinExchangeType
import dev.kourier.amqp.Field
import dev.kourier.amqp.channel.AMQPChannel
import io.ktor.utils.io.core.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first
import kotlin.test.*
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.Uuid

class RobustAMQPChannelTest {

    private suspend fun AMQPChannel.closeByBreaking() =
        assertFailsWith<AMQPException.ChannelClosed> {
            exchangeDeclare(
                "will-fail",
                "nonexistent-type",
                durable = true,
                autoDelete = false,
                internal = false,
                arguments = emptyMap()
            )
        }

    @Test
    @OptIn(DelicateCoroutinesApi::class)
    fun testDeclareAndRestoreEverything() = runBlocking {
        withConnection { connection ->
            val channel = connection.openChannel()
            val closeEvent = async { channel.closedResponses.first() }
            val reopenEvent = async { channel.openedResponses.first() }

            val queueName = "test-restore-queue"
            val exchange1 = "test-restore-exchange1"
            val exchange2 = "test-restore-exchange2"
            val routingKey = "test.key"

            // 1. Declare 2 exchanges
            channel.exchangeDeclare(
                exchange1,
                BuiltinExchangeType.DIRECT,
                durable = true,
                arguments = emptyMap()
            )
            channel.exchangeDeclare(
                exchange2,
                BuiltinExchangeType.FANOUT,
                durable = true,
                arguments = emptyMap()
            )

            // 2. Declare a queue
            channel.queueDeclare(
                queueName,
                durable = false,
                exclusive = false,
                autoDelete = true,
                arguments = emptyMap()
            )

            // 3. Bind queue to exchange1
            channel.queueBind(queueName, exchange1, routingKey, arguments = emptyMap())

            // 4. Bind exchange1 to exchange2 (fanout)
            channel.exchangeBind(exchange1, exchange2, routingKey = "", arguments = emptyMap())

            // 5. Start consumer
            val receivedMessages = channel.basicConsume(
                queue = queueName,
                consumerTag = "restore-test-consumer",
                noAck = true,
                exclusive = false,
                arguments = emptyMap()
            )

            // 6. Send a test message to exchange2
            channel.basicPublish("Before crash".toByteArray(), exchange2, routingKey)

            // 7. Assert it was received
            val msg = withTimeout(5000) { receivedMessages.receive() }
            assertEquals("Before crash", msg.message.body.decodeToString())

            // 8. Break the channel by declaring an invalid exchange type
            channel.closeByBreaking()
            closeEvent.await()
            reopenEvent.await()

            // 9. Send another message after restore
            channel.basicPublish("After restore".toByteArray(), exchange2, routingKey)

            // 10. Assert it was received again
            val msg2 = withTimeout(5.seconds) { receivedMessages.receive() }
            assertEquals("After restore", msg2.message.body.decodeToString())

            channel.close()
            assertTrue(receivedMessages.isClosedForReceive)
        }
    }

    @Test
    fun testGetQueueFail() = runBlocking {
        withConnection { connection ->
            val channel = connection.openChannel()
            val closeEvent = async { channel.closedResponses.first() }
            val reopenEvent = async { channel.openedResponses.first() }

            val name = "test-passive-queue-${Uuid.random()}"
            channel.queueDeclare(name, autoDelete = true, arguments = mapOf("x-max-length" to Field.Int(1)))
            assertFailsWith<AMQPException.ChannelClosed> {
                channel.queueDeclare(name, autoDelete = true)
            }

            closeEvent.await()
            reopenEvent.await()

            assertFailsWith<AMQPException.ChannelClosed> {
                channel.queueDeclare(name, autoDelete = true)
            }
        }
    }

    @Test
    fun testDeleteExchange() = runBlocking {
        withConnection { connection ->
            val channel = connection.openChannel()
            val closeEvent = async { channel.closedResponses.first() }
            val reopenEvent = async { channel.openedResponses.first() }

            channel.exchangeDeclare("test-delete-exchange", BuiltinExchangeType.DIRECT, durable = true)
            channel.exchangeDeclare("test-delete-exchange-2", BuiltinExchangeType.FANOUT, durable = true)

            channel.exchangeBind("test-delete-exchange-2", "test-delete-exchange", routingKey = "")
            channel.exchangeUnbind("test-delete-exchange-2", "test-delete-exchange", routingKey = "")

            channel.exchangeDelete("test-delete-exchange")
            channel.exchangeDelete("test-delete-exchange-2")

            channel.closeByBreaking()
            closeEvent.await()
            reopenEvent.await()

            assertFailsWith<AMQPException.ChannelClosed> {
                channel.exchangeDeclarePassive("test-delete-exchange")
            }

            channel.close()
        }
    }

    @Test
    fun testDeleteQueue() = runBlocking {
        withConnection { connection ->
            val channel = connection.openChannel()
            val closeEvent = async { channel.closedResponses.first() }
            val reopenEvent = async { channel.openedResponses.first() }

            channel.exchangeDeclare("test-delete-queue-exchange", BuiltinExchangeType.DIRECT, durable = true)
            channel.queueDeclare(
                "test-delete-queue",
                durable = true,
                exclusive = false,
                autoDelete = false,
                arguments = emptyMap()
            )

            channel.queueBind("test-delete-queue", "test-delete-queue-exchange", routingKey = "")
            channel.queueUnbind("test-delete-queue", "test-delete-queue-exchange", routingKey = "")

            channel.exchangeDelete("test-delete-queue-exchange")
            channel.queueDelete("test-delete-queue")

            channel.closeByBreaking()
            closeEvent.await()
            reopenEvent.await()

            assertFailsWith<AMQPException.ChannelClosed> {
                channel.queueDeclarePassive("test-delete-queue")
            }

            channel.close()
        }
    }

    @Test
    @OptIn(DelicateCoroutinesApi::class)
    fun testCancelConsume() = runBlocking {
        withConnection { connection ->
            val channel = connection.openChannel()
            val closeEvent = async { channel.closedResponses.first() }
            val reopenEvent = async { channel.openedResponses.first() }

            val queueName = "test-cancel-consume-queue"
            channel.queueDeclare(
                queueName,
                durable = false,
                exclusive = false,
                autoDelete = true,
                arguments = emptyMap()
            )

            val consumerTag = "test-cancel-consumer"
            val receivedMessages = channel.basicConsume(
                queue = queueName,
                consumerTag = consumerTag,
                noAck = true,
                exclusive = false,
                arguments = emptyMap()
            )

            channel.closeByBreaking()
            closeEvent.await()
            reopenEvent.await()

            assertFalse(receivedMessages.isClosedForReceive)
            channel.basicCancel(consumerTag)
            assertTrue(receivedMessages.isClosedForReceive)
            channel.close()
        }
    }

    /**
     * Regression test: multiple broker-assigned consumers (empty consumerTag) on the same channel
     * caused a ConcurrentModificationException in restore() when the channel was closed by the broker,
     * because basicConsume() adds new entries to consumedQueues while the forEach iteration is in progress.
     */
    @Test
    @OptIn(DelicateCoroutinesApi::class)
    fun testRestoreMultipleBrokerAssignedConsumersAfterChannelBreak() = runBlocking {
        withConnection { connection ->
            val channel = connection.openChannel()
            val closeEvent = async { channel.closedResponses.first() }
            val reopenEvent = async { channel.openedResponses.first() }

            val queue1 = "test-multi-consumer-ch-${Uuid.random()}"
            val queue2 = "test-multi-consumer-ch-${Uuid.random()}"
            val exchange = "test-multi-consumer-ch-ex-${Uuid.random()}"
            val routingKey1 = "key1"
            val routingKey2 = "key2"

            channel.exchangeDeclare(exchange, BuiltinExchangeType.DIRECT, durable = false, arguments = emptyMap())
            channel.queueDeclare(queue1, durable = false, exclusive = false, autoDelete = true, arguments = emptyMap())
            channel.queueDeclare(queue2, durable = false, exclusive = false, autoDelete = true, arguments = emptyMap())
            channel.queueBind(queue1, exchange, routingKey1, arguments = emptyMap())
            channel.queueBind(queue2, exchange, routingKey2, arguments = emptyMap())

            // Two broker-assigned consumers (empty consumerTag -> broker assigns amq.ctag-...)
            val messages1 = channel.basicConsume(queue = queue1, noAck = true, arguments = emptyMap())
            val messages2 = channel.basicConsume(queue = queue2, noAck = true, arguments = emptyMap())

            channel.basicPublish("Before crash 1".toByteArray(), exchange, routingKey1)
            channel.basicPublish("Before crash 2".toByteArray(), exchange, routingKey2)
            assertEquals("Before crash 1", withTimeout(5.seconds) { messages1.receive() }.message.body.decodeToString())
            assertEquals("Before crash 2", withTimeout(5.seconds) { messages2.receive() }.message.body.decodeToString())

            // Break the channel
            channel.closeByBreaking()
            closeEvent.await()
            reopenEvent.await()

            // Publish after restore - both consumers must be active
            channel.basicPublish("After restore 1".toByteArray(), exchange, routingKey1)
            channel.basicPublish("After restore 2".toByteArray(), exchange, routingKey2)
            assertEquals(
                "After restore 1",
                withTimeout(5.seconds) { messages1.receive() }.message.body.decodeToString()
            )
            assertEquals(
                "After restore 2",
                withTimeout(5.seconds) { messages2.receive() }.message.body.decodeToString()
            )

            channel.close()
        }
    }

    @Test
    @OptIn(DelicateCoroutinesApi::class)
    fun testConsumerTimeoutWithManualAck() = runBlocking {
        withConnection { connection ->
            val channel = connection.openChannel()
            val closeEvent = async { channel.closedResponses.first() }
            val reopenEvent = async { channel.openedResponses.first() }

            val queueName = "test-consumer-timeout-queue-${Uuid.random()}"
            val exchangeName = "test-consumer-timeout-exchange-${Uuid.random()}"
            val routingKey = "test.timeout"

            // Declare exchange
            channel.exchangeDeclare(
                exchangeName,
                BuiltinExchangeType.DIRECT,
                durable = false,
                arguments = emptyMap()
            )

            // Declare queue with 1 second consumer timeout
            channel.queueDeclare(
                queueName,
                durable = false,
                exclusive = false,
                autoDelete = true,
                arguments = mapOf("x-consumer-timeout" to Field.Long(1000)) // 1 second timeout
            )

            // Bind queue to exchange
            channel.queueBind(queueName, exchangeName, routingKey, arguments = emptyMap())

            // Start consumer with manual ack (noAck = false)
            val receivedMessages = channel.basicConsume(
                queue = queueName,
                consumerTag = "timeout-test-consumer",
                noAck = false, // Manual ack required
                exclusive = false,
                arguments = emptyMap()
            )

            // Publish a message
            channel.basicPublish("Message before timeout".toByteArray(), exchangeName, routingKey)

            // Receive the message
            val delivery = withTimeout(5.seconds) { receivedMessages.receive() }
            assertEquals("Message before timeout", delivery.message.body.decodeToString())

            // Simulate slow processing that exceeds the consumer timeout
            delay(2000) // 2 seconds > 1 second timeout

            // At this point, the channel should have been closed by the server due to timeout
            // Wait for the channel to close and reopen
            closeEvent.await()
            reopenEvent.await()

            // Try to ack the old message - this should be silently ignored (stale delivery tag)
            // The robust client now tracks delivery tags and ignores acks for tags from before restoration
            channel.basicAck(delivery.message.deliveryTag)

            // Verify the channel is functional after recovery by publishing and consuming a new message
            channel.basicPublish("Message after restore".toByteArray(), exchangeName, routingKey)

            val delivery2 = withTimeout(5.seconds) { receivedMessages.receive() }
            assertEquals("Message after restore", delivery2.message.body.decodeToString())

            // Ack the new message immediately (within timeout)
            channel.basicAck(delivery2.message.deliveryTag)

            channel.close()
            assertTrue(receivedMessages.isClosedForReceive)
        }
    }

}
