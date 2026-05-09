package dev.kourier.amqp.channel

import dev.kourier.amqp.*
import io.ktor.utils.io.core.*
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.test.*
import kotlin.time.Clock

class AMQPChannelTest {

    @Test
    fun testCanCloseChannel() = withConnection { connection ->
        val channel = connection.openChannel()
        assertFalse(channel.channelClosed.isCompleted)
        channel.close()
        assertTrue(channel.channelClosed.isCompleted)
        assertFailsWith<AMQPException.ChannelClosed> { channel.basicGet("test") }
    }

    @Test
    fun testQueue() = withConnection { connection ->
        val channel = connection.openChannel()

        val queueDeclare = channel.queueDeclare {
            name = "test-queue-not-durable"
            durable = false
        }
        assertEquals("test-queue-not-durable", queueDeclare.queueName)

        channel.queueBind {
            queue = "test-queue-not-durable"
            exchange = "amq.topic"
            routingKey = "test"
        }
        channel.queueUnbind {
            queue = "test-queue-not-durable"
            exchange = "amq.topic"
            routingKey = "test"
        }

        channel.queuePurge {
            name = "test-queue-not-durable"
        }
        channel.queueDelete {
            name = "test-queue-not-durable"
        }

        channel.close()
    }

    @Test
    fun testQueueDeclarePassive() = withConnection { connection ->
        val passiveChannel = connection.openChannel()
        val exception = assertFailsWith<AMQPException.ChannelClosed> {
            passiveChannel.queueDeclarePassive {
                name = "test"
            }
        }
        assertEquals(404u, exception.replyCode)

        val channel = connection.openChannel()
        channel.queueDeclare("test")
        channel.queueDeclarePassive {
            name = "test"
        }

        channel.queueDelete {
            name = "test"
        }

        channel.close()
    }

    @Test
    fun testExchange() = withConnection { connection ->
        val channel = connection.openChannel()

        channel.exchangeDeclare {
            name = "test1"
            type = BuiltinExchangeType.TOPIC
        }
        channel.exchangeDeclare {
            name = "test2"
            type = BuiltinExchangeType.TOPIC
        }

        channel.exchangeBind {
            destination = "test1"
            source = "test2"
            routingKey = "test"
        }
        channel.exchangeUnbind {
            destination = "test1"
            source = "test2"
            routingKey = "test"
        }

        channel.exchangeDelete {
            name = "test1"
        }
        channel.exchangeDelete {
            name = "test2"
        }

        channel.close()
    }

    @Test
    fun testExchangeDeclarePassive() = withConnection { connection ->
        val passiveChannel = connection.openChannel()
        val exception = assertFailsWith<AMQPException.ChannelClosed> {
            passiveChannel.exchangeDeclarePassive {
                name = "test"
            }
        }
        assertEquals(404u, exception.replyCode)

        val channel = connection.openChannel()
        channel.exchangeDeclare("test", BuiltinExchangeType.TOPIC)
        channel.exchangeDeclarePassive {
            name = "test"
        }

        channel.exchangeDelete {
            name = "test"
        }

        channel.close()
    }

    @Test
    fun testBasicPublish() = withConnection { connection ->
        val channel = connection.openChannel()

        channel.queueDeclare {
            name = "test"
            durable = true
        }

        val body = "{}".toByteArray()

        val result = channel.basicPublish {
            this.body = body
            exchange = ""
            routingKey = "test"
        }
        assertEquals(0u, result.deliveryTag)

        val messageCount = channel.messageCount("test")
        assertEquals(1u, messageCount)

        channel.queueDelete {
            name = "test"
        }

        channel.close()
    }

    @Test
    fun testBasicPublishFrameMaxExact() = withConnection { connection ->
        val channel = connection.openChannel()
        val frameMax = (channel as DefaultAMQPChannel).frameMax.toInt()

        channel.queueDeclare {
            name = "test_framemax"
            durable = true
        }

        val body = ByteArray(frameMax) { 'A'.code.toByte() }
        channel.basicPublish {
            this.body = body
            exchange = ""
            routingKey = "test_framemax"
        }

        val msg = channel.basicGet {
            queue = "test_framemax"
        }
        assertNotNull(msg.message)
        assertEquals(frameMax, msg.message.body.size)

        channel.queueDelete {
            name = "test_framemax"
        }
        channel.close()
    }

    @Test
    fun testBasicPublishFrameMaxMinusOne() = withConnection { connection ->
        val channel = connection.openChannel()
        val frameMax = (channel as DefaultAMQPChannel).frameMax.toInt()

        channel.queueDeclare {
            name = "test_framemax"
            durable = true
        }

        val body = ByteArray(frameMax - 1) { 'A'.code.toByte() }
        channel.basicPublish {
            this.body = body
            exchange = ""
            routingKey = "test_framemax"
        }

        val msg = channel.basicGet {
            queue = "test_framemax"
        }
        assertNotNull(msg.message)
        assertEquals(frameMax - 1, msg.message.body.size)

        channel.queueDelete {
            name = "test_framemax"
        }
        channel.close()
    }

    @Test
    fun testBasicPublishFrameMaxPlusOne() = withConnection { connection ->
        val channel = connection.openChannel()
        val frameMax = (channel as DefaultAMQPChannel).frameMax.toInt()

        channel.queueDeclare {
            name = "test_framemax"
            durable = true
        }

        val body = ByteArray(frameMax + 1) { 'A'.code.toByte() }
        channel.basicPublish {
            this.body = body
            exchange = ""
            routingKey = "test_framemax"
        }

        val msg = channel.basicGet {
            queue = "test_framemax"
        }
        assertNotNull(msg.message)
        assertEquals(frameMax + 1, msg.message.body.size)

        channel.queueDelete {
            name = "test_framemax"
        }
        channel.close()
    }

    @Test
    fun testBasicPublishTwoTimesFrameMaxExact() = withConnection { connection ->
        val channel = connection.openChannel()
        val frameMax = (channel as DefaultAMQPChannel).frameMax.toInt()

        channel.queueDeclare {
            name = "test_framemax"
            durable = true
        }

        val body = ByteArray(2 * frameMax) { 'A'.code.toByte() }
        channel.basicPublish {
            this.body = body
            exchange = ""
            routingKey = "test_framemax"
        }

        val msg = channel.basicGet {
            queue = "test_framemax"
        }
        assertNotNull(msg.message)
        assertEquals(2 * frameMax, msg.message.body.size)

        channel.queueDelete {
            name = "test_framemax"
        }
        channel.close()
    }

    @Test
    fun testBasicPublishTwoTimesFrameMaxMinusOne() = withConnection { connection ->
        val channel = connection.openChannel()
        val frameMax = (channel as DefaultAMQPChannel).frameMax.toInt()

        channel.queueDeclare {
            name = "test_framemax"
            durable = true
        }

        val body = ByteArray(2 * frameMax - 1) { 'A'.code.toByte() }
        channel.basicPublish {
            this.body = body
            exchange = ""
            routingKey = "test_framemax"
        }

        val msg = channel.basicGet {
            queue = "test_framemax"
        }
        assertNotNull(msg.message)
        assertEquals(2 * frameMax - 1, msg.message.body.size)

        channel.queueDelete {
            name = "test_framemax"
        }
        channel.close()
    }

    @Test
    fun testBasicPublishTwoTimesFrameMaxPlusOne() = withConnection { connection ->
        val channel = connection.openChannel()
        val frameMax = (channel as DefaultAMQPChannel).frameMax.toInt()

        channel.queueDeclare {
            name = "test_framemax"
            durable = true
        }

        val body = ByteArray(2 * frameMax + 1) { 'A'.code.toByte() }
        channel.basicPublish {
            this.body = body
            exchange = ""
            routingKey = "test_framemax"
        }

        val msg = channel.basicGet {
            queue = "test_framemax"
        }
        assertNotNull(msg.message)
        assertEquals(2 * frameMax + 1, msg.message.body.size)

        channel.queueDelete {
            name = "test_framemax"
        }
        channel.close()
    }

    @Test
    fun testBasicGet() = withConnection { connection ->
        val channel = connection.openChannel()

        channel.queueDeclare {
            name = "test"
            durable = true
        }

        val body = "{}".toByteArray()
        val properties = Properties(
            contentType = "application/json",
            contentEncoding = "UTF-8",
            headers = mapOf("test" to Field.LongString("test")),
            deliveryMode = 1u,
            priority = 1u,
            correlationId = "correlationID",
            replyTo = "replyTo",
            expiration = "60000",
            messageId = "messageID",
            timestamp = 100,
            type = "type",
            userId = "guest",
            appId = "appID"
        )
        channel.basicPublish {
            this.body = body
            exchange = ""
            routingKey = "test"
            this.properties = properties
        }

        val msg = channel.basicGet {
            queue = "test"
        }
        assertNotNull(msg.message)

        assertEquals(0u, msg.messageCount)
        assertEquals("{}", msg.message.body.decodeToString())
        assertEquals(properties, msg.message.properties)

        channel.queueDelete {
            name = "test"
        }

        channel.close()
    }

    @Test
    fun testBasicGetWithZeroBytesPayload() = withConnection { connection ->
        val channel = connection.openChannel()

        channel.queueDeclare {
            name = "test"
            durable = true
        }

        val body = "".toByteArray()
        channel.basicPublish {
            this.body = body
            exchange = ""
            routingKey = "test"
        }

        val msg = channel.basicGet {
            queue = "test"
        }
        assertNotNull(msg.message)

        assertEquals(0u, msg.messageCount)
        assertEquals("", msg.message.body.decodeToString())

        channel.queueDelete {
            name = "test"
        }

        channel.close()
    }

    @Test
    fun testBasicGetEmpty() = withConnection { connection ->
        val channel = connection.openChannel()

        channel.queueDeclare {
            name = "test"
            durable = true
        }

        val result = channel.basicGet {
            queue = "test"
        }
        assertEquals(null, result.message)
        assertEquals(0u, result.messageCount)

        channel.queueDelete {
            name = "test"
        }

        channel.close()
    }

    @Test
    fun testBasicTx() = withConnection { connection ->
        val channel = connection.openChannel()
        channel.txSelect {}
        channel.txCommit {}
        channel.txRollback {}
        channel.close()
    }

    @Test
    fun testConfirm() = withConnection { connection ->
        val channel = connection.openChannel()
        channel.confirmSelect {}
        channel.confirmSelect {}
        channel.close()
    }

    @Test
    fun testFlow() = withConnection { connection ->
        val channel = connection.openChannel()
        channel.flow {
            active = true
        }
        channel.close()
    }

    @Test
    fun testBasicQos() = withConnection { connection ->
        val channel = connection.openChannel()

        channel.basicQos {
            count = 100u
            global = true
        }
        channel.basicQos {
            count = 100u
            global = false
        }

        channel.close()
    }

    @Test
    fun testConsumeConfirms() = withConnection { connection ->
        val channel = connection.openChannel()

        channel.queueDeclare {
            name = "test"
            durable = true
        }

        val body = "{}".toByteArray()

        repeat(6) {
            channel.basicPublish {
                this.body = body
                exchange = ""
                routingKey = "test"
                properties = Properties()
            }
        }

        run {
            val msg = channel.basicGet {
                queue = "test"
            }.message ?: kotlin.test.fail()
            channel.basicAck(msg.deliveryTag)

            val msg2 = channel.basicGet {
                queue = "test"
            }.message ?: kotlin.test.fail()
            channel.basicAck(msg2)
        }

        run {
            val msg = channel.basicGet {
                queue = "test"
            }.message ?: kotlin.test.fail()
            channel.basicNack(msg.deliveryTag)

            val msg2 = channel.basicGet {
                queue = "test"
            }.message ?: kotlin.test.fail()
            channel.basicNack(msg2)
        }

        run {
            val msg = channel.basicGet {
                queue = "test"
            }.message ?: kotlin.test.fail()
            channel.basicReject(msg.deliveryTag)

            val msg2 = channel.basicGet {
                queue = "test"
            }.message ?: kotlin.test.fail()
            channel.basicReject(msg2)
        }

        channel.basicRecover {
            requeue = true
        }

        channel.queueDelete {
            name = "test"
        }

        channel.close()
    }

    @Test
    fun testPublishConsume() = runBlocking {
        withConnection { connection ->
            val channel = connection.openChannel()

            channel.queueDeclare {
                name = "test_publish"
                durable = true
            }

            val body = "{}".toByteArray()

            channel.confirmSelect {}

            val confirmJob = launch {
                val mutex = Mutex()
                var count = 1
                channel.publishConfirmResponses.collect {
                    mutex.withLock {
                        if (it.multiple) count = it.deliveryTag.toInt() else count++
                        if (count >= 100) cancel()
                    }
                }
            }

            for (i in 1..100) {
                val result = channel.basicPublish {
                    this.body = body
                    exchange = ""
                    routingKey = "test_publish"
                }
                assertEquals(i.toULong(), result.deliveryTag)
            }

            repeat(100) {
                val result = channel.basicGet {
                    queue = "test_publish"
                }
                channel.basicAck(result.message ?: kotlin.test.fail("No message received"))
            }

            confirmJob.join()

            channel.queueDelete {
                name = "test_publish"
            }
            channel.close()
        }
    }

    @Test
    fun testBasicConsumeManualCancel() = withConnection { connection ->
        val channel = connection.openChannel()
        channel.queueDeclare {
            name = "test_consume"
            durable = true
        }

        val body = "{}".toByteArray()
        repeat(100) {
            channel.basicPublish {
                this.body = body
                exchange = ""
                routingKey = "test_consume"
            }
        }

        val deliveryChannel = channel.basicConsume(
            queue = "test_consume",
            noAck = true
        )

        val consumerCount = channel.consumerCount("test_consume")
        assertEquals(1u, consumerCount)

        var count = 0
        runCatching {
            for (delivery in deliveryChannel) {
                count++
                if (count == 100) channel.basicCancel(deliveryChannel.consumeOk.consumerTag)
            }
        }
        assertEquals(100, count)

        channel.queueDelete {
            name = "test_consume"
        }
        channel.close()
    }

    @Test
    fun testBasicConsumeManualCancelFromReceiveChannel() = withConnection { connection ->
        val channel = connection.openChannel()
        channel.queueDeclare {
            name = "test_consume"
            durable = true
        }

        val body = "{}".toByteArray()
        repeat(100) {
            channel.basicPublish {
                this.body = body
                exchange = ""
                routingKey = "test_consume"
            }
        }

        val deliveryChannel = channel.basicConsume(
            queue = "test_consume",
            noAck = true
        )

        var count = 0
        runCatching {
            for (delivery in deliveryChannel) {
                count++
                if (count == 100) deliveryChannel.cancel()
            }
        }
        assertEquals(100, count)

        channel.queueDelete {
            name = "test_consume"
        }
        channel.close()
    }

    @Test
    fun testOpenChannelsConcurrently() = withConnection { connection ->
        val first = connection.openChannel()
        val second = connection.openChannel()

        first.close()
        second.close()
    }

    @Test
    fun testConcurrentOperationsOnChannel() = withConnection { connection ->
        repeat(1001) { run ->
            val queueName = "temp_queue_$run"
            val channel = connection.openChannel()
            channel.queueDeclare {
                name = queueName
                durable = false
                exclusive = true
            }

            val receiveChannel = channel.basicConsume(queue = queueName)
            channel.basicPublish {
                body = "baz".toByteArray()
                exchange = ""
                routingKey = queueName
            }
            channel.basicConsume(queue = queueName)
            channel.basicPublish {
                body = "baz".toByteArray()
                exchange = ""
                routingKey = queueName
            }
            channel.basicCancel(receiveChannel.consumeOk.consumerTag)
        }
    }

    @Test
    fun testConcurrentMessageProcessing() = runBlocking {
        withConnection { connection ->
            // Setup
            val queueName = "test_concurrent_consume"
            val messageCount = 10
            val handlerDelayMs = 500L
            // If concurrent: ~500ms total. If sequential: ~5000ms. Threshold: 2500ms (halfway)
            val expectedMaxTime = handlerDelayMs * messageCount / 2

            // Create channel with prefetchCount > 1
            val channel = connection.openChannel()
            channel.queueDeclare {
                name = queueName
                durable = false
                exclusive = true
            }
            channel.basicQos {
                count = 10u
            }

            // Track message processing
            val processedMessages = mutableSetOf<Int>()
            val processingTimes = mutableMapOf<Int, Long>()
            val mutex = Mutex()
            val startTime = Clock.System.now()

            // Publish messages first
            repeat(messageCount) { i ->
                channel.basicPublish {
                    body = i.toString().encodeToByteArray()
                    exchange = ""
                    routingKey = queueName
                }
            }

            // Consume messages with delay
            channel.basicConsume(
                queue = queueName,
                onDelivery = { delivery ->
                    val messageId = delivery.message.body.decodeToString().toInt()
                    val messageStartTime = Clock.System.now()

                    delay(handlerDelayMs) // Simulate long-running work

                    mutex.withLock {
                        processedMessages.add(messageId)
                        processingTimes[messageId] = (Clock.System.now() - messageStartTime).inWholeMilliseconds
                    }
                    channel.basicAck(delivery.message.deliveryTag, false)
                }
            )

            // Wait for all messages to be processed
            var allProcessed = false
            while (!allProcessed) {
                mutex.withLock {
                    allProcessed = processedMessages.size >= messageCount
                }
                if (!allProcessed) {
                    delay(50)
                }
            }

            val totalTime = (Clock.System.now() - startTime).inWholeMilliseconds

            // Assertions
            assertEquals(messageCount, processedMessages.size, "All messages should be processed")
            assertTrue(
                totalTime < expectedMaxTime,
                "Messages should process concurrently: took ${totalTime}ms, expected < ${expectedMaxTime}ms"
            )

            channel.queueDelete {
                name = queueName
            }
            channel.close()
        }
    }

    @Test
    fun testConcurrentMessageProcessingWithErrors() = runBlocking {
        withConnection { connection ->
            // Setup
            val queueName = "test_concurrent_consume_errors"
            val messageCount = 5

            // Create channel
            val channel = connection.openChannel()
            channel.queueDeclare {
                name = queueName
                durable = false
                exclusive = true
            }
            channel.basicQos {
                count = 10u
            }

            // Track message processing
            val processedMessages = mutableSetOf<Int>()
            val failedMessages = mutableSetOf<Int>()
            val mutex = Mutex()

            // Publish messages first
            repeat(messageCount) { i ->
                channel.basicPublish {
                    body = i.toString().encodeToByteArray()
                    exchange = ""
                    routingKey = queueName
                }
            }

            // Consume messages - throw error on message 2
            channel.basicConsume(
                queue = queueName,
                onDelivery = { delivery ->
                    val messageId = delivery.message.body.decodeToString().toInt()

                    if (messageId == 2) {
                        // This should be caught and logged, not crash the consumer
                        throw RuntimeException("Simulated error for message $messageId")
                    }

                    mutex.withLock {
                        processedMessages.add(messageId)
                    }
                    channel.basicAck(delivery.message.deliveryTag, false)
                }
            )

            // Wait for all messages to be processed (except the failed one)
            var allProcessed = false
            while (!allProcessed) {
                mutex.withLock {
                    // We expect 4 messages to succeed (0, 1, 3, 4) and 1 to fail (2)
                    allProcessed = processedMessages.size >= messageCount - 1
                }
                if (!allProcessed) {
                    delay(50)
                }
            }

            // Give a bit more time to ensure message 2 was attempted
            delay(100)

            // Assertions
            mutex.withLock {
                assertEquals(messageCount - 1, processedMessages.size, "4 messages should succeed")
                assertFalse(processedMessages.contains(2), "Message 2 should have failed")
                assertTrue(processedMessages.contains(0), "Message 0 should succeed")
                assertTrue(processedMessages.contains(1), "Message 1 should succeed")
                assertTrue(processedMessages.contains(3), "Message 3 should succeed")
                assertTrue(processedMessages.contains(4), "Message 4 should succeed")
            }

            channel.queueDelete {
                name = queueName
            }
            channel.close()
        }
    }

}
