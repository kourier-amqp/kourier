package dev.kourier.amqp.channel

import dev.kourier.amqp.*
import io.ktor.utils.io.core.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.test.runTest
import kotlin.test.*
import kotlin.time.Clock
import kotlin.uuid.Uuid

class AMQPChannelTest {

    @Test
    fun testCanCloseChannel() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()
            assertFalse(channel.channelClosed.isCompleted)
            channel.close()
            assertTrue(channel.channelClosed.isCompleted)
            assertFailsWith<AMQPException.ChannelClosed> { channel.basicGet("test") }
        }
    }

    @Test
    fun testQueue() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()
            val queueName = "test-queue-not-durable-${Uuid.random()}"

            try {
                val queueDeclare = channel.queueDeclare {
                    name = queueName
                    durable = false
                }
                assertEquals(queueName, queueDeclare.queueName)

                channel.queueBind {
                    queue = queueName
                    exchange = "amq.topic"
                    routingKey = "test"
                }
                channel.queueUnbind {
                    queue = queueName
                    exchange = "amq.topic"
                    routingKey = "test"
                }

                channel.queuePurge {
                    name = queueName
                }
            } finally {
                runCatching { channel.queueDelete { name = queueName } }
                channel.close()
            }
        }
    }

    @Test
    fun testQueueDeclarePassive() = runTest {
        withConnection { connection ->
            // Unique queue name so the assertFailsWith<ChannelClosed>(queueDeclarePassive)
            // probe is always against a queue that doesn't exist — a sibling test leaving
            // a "test" queue behind would otherwise turn this into a passing-passive call
            // and silently break the 404 assertion.
            val queueName = "test-queue-declare-passive-${Uuid.random()}"
            val passiveChannel = connection.openChannel()
            val exception = assertFailsWith<AMQPException.ChannelClosed> {
                passiveChannel.queueDeclarePassive {
                    name = queueName
                }
            }
            assertEquals(404u, exception.replyCode)

            val channel = connection.openChannel()
            try {
                channel.queueDeclare(queueName)
                channel.queueDeclarePassive {
                    name = queueName
                }
            } finally {
                runCatching { channel.queueDelete { name = queueName } }
                channel.close()
            }
        }
    }

    @Test
    fun testExchange() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()
            val exchange1 = "test-exchange-1-${Uuid.random()}"
            val exchange2 = "test-exchange-2-${Uuid.random()}"

            try {
                channel.exchangeDeclare {
                    name = exchange1
                    type = BuiltinExchangeType.TOPIC
                }
                channel.exchangeDeclare {
                    name = exchange2
                    type = BuiltinExchangeType.TOPIC
                }

                channel.exchangeBind {
                    destination = exchange1
                    source = exchange2
                    routingKey = "test"
                }
                channel.exchangeUnbind {
                    destination = exchange1
                    source = exchange2
                    routingKey = "test"
                }
            } finally {
                runCatching { channel.exchangeDelete { name = exchange1 } }
                runCatching { channel.exchangeDelete { name = exchange2 } }
                channel.close()
            }
        }
    }

    @Test
    fun testExchangeDeclarePassive() = runTest {
        withConnection { connection ->
            val exchangeName = "test-exchange-declare-passive-${Uuid.random()}"
            val passiveChannel = connection.openChannel()
            val exception = assertFailsWith<AMQPException.ChannelClosed> {
                passiveChannel.exchangeDeclarePassive {
                    name = exchangeName
                }
            }
            assertEquals(404u, exception.replyCode)

            val channel = connection.openChannel()
            try {
                channel.exchangeDeclare(exchangeName, BuiltinExchangeType.TOPIC)
                channel.exchangeDeclarePassive {
                    name = exchangeName
                }
            } finally {
                runCatching { channel.exchangeDelete { name = exchangeName } }
                channel.close()
            }
        }
    }

    @Test
    fun testBasicPublish() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()
            // Unique queue name per test run: messageCount() depends on broker-side state and
            // any leftover from a prior test (or a sibling test running concurrently against
            // the same broker) would skew the count. The Uuid suffix isolates this test
            // entirely from any other queue named "test*".
            val queueName = "test-basic-publish-${Uuid.random()}"

            channel.queueDeclare {
                name = queueName
                durable = true
            }

            // Publisher confirms ensures the broker has fully processed (and enqueued) the
            // publish before we ask for messageCount. Without it, queueDeclarePassive can
            // race the publish on a fast loopback and return 0 while the message is still
            // in flight on the broker side — reproducible on Kotlin/Native targets.
            channel.confirmSelect()

            val body = "{}".toByteArray()

            try {
                // Subscribe to publishConfirmResponses BEFORE publishing — replay=0 SharedFlow
                // drops emissions made while subscriberCount=0, so a fast broker Ack arriving
                // before .first() subscribed would be lost and the await would hang forever.
                // UNDISPATCHED runs the async body up to the first suspension synchronously,
                // wiring the SharedFlow subscriber before async returns.
                val confirm = async(start = CoroutineStart.UNDISPATCHED) {
                    channel.publishConfirmResponses.first()
                }
                val result = channel.basicPublish {
                    this.body = body
                    exchange = ""
                    routingKey = queueName
                }
                assertEquals(1u, result.deliveryTag)
                confirm.await()

                val messageCount = channel.messageCount(queueName)
                assertEquals(1u, messageCount)
            } finally {
                // Always delete the queue even if assertions failed, so leftover state never
                // pollutes a subsequent test run.
                runCatching { channel.queueDelete { name = queueName } }
                channel.close()
            }
        }
    }

    @Test
    fun testBasicPublishFrameMaxExact() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()
            val frameMax = (channel as DefaultAMQPChannel).frameMax.toInt()
            val queueName = "test-framemax-exact-${Uuid.random()}"

            channel.queueDeclare {
                name = queueName
                durable = true
            }
            channel.confirmSelect()

            try {
                val body = ByteArray(frameMax) { 'A'.code.toByte() }
                val confirm = async(start = CoroutineStart.UNDISPATCHED) {
                    channel.publishConfirmResponses.first()
                }
                channel.basicPublish {
                    this.body = body
                    exchange = ""
                    routingKey = queueName
                }
                confirm.await()

                val msg = channel.basicGet {
                    queue = queueName
                }
                assertNotNull(msg.message)
                assertEquals(frameMax, msg.message.body.size)
                channel.basicAck(msg.message.deliveryTag)
            } finally {
                runCatching { channel.queueDelete { name = queueName } }
                channel.close()
            }
        }
    }

    @Test
    fun testBasicPublishFrameMaxMinusOne() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()
            val frameMax = (channel as DefaultAMQPChannel).frameMax.toInt()
            val queueName = "test-framemax-minus-one-${Uuid.random()}"

            channel.queueDeclare {
                name = queueName
                durable = true
            }
            channel.confirmSelect()

            try {
                val body = ByteArray(frameMax - 1) { 'A'.code.toByte() }
                val confirm = async(start = CoroutineStart.UNDISPATCHED) {
                    channel.publishConfirmResponses.first()
                }
                channel.basicPublish {
                    this.body = body
                    exchange = ""
                    routingKey = queueName
                }
                confirm.await()

                val msg = channel.basicGet {
                    queue = queueName
                }
                assertNotNull(msg.message)
                assertEquals(frameMax - 1, msg.message.body.size)
                channel.basicAck(msg.message.deliveryTag)
            } finally {
                runCatching { channel.queueDelete { name = queueName } }
                channel.close()
            }
        }
    }

    @Test
    fun testBasicPublishFrameMaxPlusOne() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()
            val frameMax = (channel as DefaultAMQPChannel).frameMax.toInt()
            val queueName = "test-framemax-plus-one-${Uuid.random()}"

            channel.queueDeclare {
                name = queueName
                durable = true
            }
            channel.confirmSelect()

            try {
                val body = ByteArray(frameMax + 1) { 'A'.code.toByte() }
                val confirm = async(start = CoroutineStart.UNDISPATCHED) {
                    channel.publishConfirmResponses.first()
                }
                channel.basicPublish {
                    this.body = body
                    exchange = ""
                    routingKey = queueName
                }
                confirm.await()

                val msg = channel.basicGet {
                    queue = queueName
                }
                assertNotNull(msg.message)
                assertEquals(frameMax + 1, msg.message.body.size)
                channel.basicAck(msg.message.deliveryTag)
            } finally {
                runCatching { channel.queueDelete { name = queueName } }
                channel.close()
            }
        }
    }

    @Test
    fun testBasicPublishTwoTimesFrameMaxExact() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()
            val frameMax = (channel as DefaultAMQPChannel).frameMax.toInt()
            val queueName = "test-framemax-2x-exact-${Uuid.random()}"

            channel.queueDeclare {
                name = queueName
                durable = true
            }
            channel.confirmSelect()

            try {
                val body = ByteArray(2 * frameMax) { 'A'.code.toByte() }
                val confirm = async(start = CoroutineStart.UNDISPATCHED) {
                    channel.publishConfirmResponses.first()
                }
                channel.basicPublish {
                    this.body = body
                    exchange = ""
                    routingKey = queueName
                }
                confirm.await()

                val msg = channel.basicGet {
                    queue = queueName
                }
                assertNotNull(msg.message)
                assertEquals(2 * frameMax, msg.message.body.size)
                channel.basicAck(msg.message.deliveryTag)
            } finally {
                runCatching { channel.queueDelete { name = queueName } }
                channel.close()
            }
        }
    }

    @Test
    fun testBasicPublishTwoTimesFrameMaxMinusOne() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()
            val frameMax = (channel as DefaultAMQPChannel).frameMax.toInt()
            val queueName = "test-framemax-2x-minus-one-${Uuid.random()}"

            channel.queueDeclare {
                name = queueName
                durable = true
            }
            channel.confirmSelect()

            try {
                val body = ByteArray(2 * frameMax - 1) { 'A'.code.toByte() }
                val confirm = async(start = CoroutineStart.UNDISPATCHED) {
                    channel.publishConfirmResponses.first()
                }
                channel.basicPublish {
                    this.body = body
                    exchange = ""
                    routingKey = queueName
                }
                confirm.await()

                val msg = channel.basicGet {
                    queue = queueName
                }
                assertNotNull(msg.message)
                assertEquals(2 * frameMax - 1, msg.message.body.size)
                channel.basicAck(msg.message.deliveryTag)
            } finally {
                runCatching { channel.queueDelete { name = queueName } }
                channel.close()
            }
        }
    }

    @Test
    fun testBasicPublishTwoTimesFrameMaxPlusOne() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()
            val frameMax = (channel as DefaultAMQPChannel).frameMax.toInt()
            val queueName = "test-framemax-2x-plus-one-${Uuid.random()}"

            channel.queueDeclare {
                name = queueName
                durable = true
            }
            channel.confirmSelect()

            try {
                val body = ByteArray(2 * frameMax + 1) { 'A'.code.toByte() }
                val confirm = async(start = CoroutineStart.UNDISPATCHED) {
                    channel.publishConfirmResponses.first()
                }
                channel.basicPublish {
                    this.body = body
                    exchange = ""
                    routingKey = queueName
                }
                confirm.await()

                val msg = channel.basicGet {
                    queue = queueName
                }
                assertNotNull(msg.message)
                assertEquals(2 * frameMax + 1, msg.message.body.size)
                channel.basicAck(msg.message.deliveryTag)
            } finally {
                runCatching { channel.queueDelete { name = queueName } }
                channel.close()
            }
        }
    }

    @Test
    fun testBasicGet() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()
            // Unique queue name + always-delete cleanup: prior runs sharing a "test" queue could
            // leave unacked messages around (basicGet doesn't auto-ack), then redeliver them
            // on the next run and break the messageCount assertion. Isolating per-run sidesteps
            // the cross-test pollution entirely.
            val queueName = "test-basic-get-${Uuid.random()}"

            channel.queueDeclare {
                name = queueName
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

            // Confirm-mode + wait-for-ack guarantees the broker has fully enqueued the publish
            // before basicGet runs, which is otherwise racy on a fast loopback (Kotlin/Native).
            channel.confirmSelect()

            try {
                val confirm = async(start = CoroutineStart.UNDISPATCHED) {
                    channel.publishConfirmResponses.first()
                }
                channel.basicPublish {
                    this.body = body
                    exchange = ""
                    routingKey = queueName
                    this.properties = properties
                }
                // Wait for the broker's publish-confirm so the message is guaranteed enqueued
                // before basicGet() runs. See testBasicPublish for the timing rationale.
                confirm.await()

                val msg = channel.basicGet {
                    queue = queueName
                }
                assertNotNull(msg.message)

                assertEquals(0u, msg.messageCount)
                assertEquals("{}", msg.message.body.decodeToString())
                assertEquals(properties, msg.message.properties)
                channel.basicAck(msg.message.deliveryTag)
            } finally {
                runCatching { channel.queueDelete { name = queueName } }
                channel.close()
            }
        }
    }

    @Test
    fun testBasicGetWithZeroBytesPayload() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()
            val queueName = "test-basic-get-zero-${Uuid.random()}"

            channel.queueDeclare {
                name = queueName
                durable = true
            }

            channel.confirmSelect()

            try {
                val body = "".toByteArray()
                val confirm = async(start = CoroutineStart.UNDISPATCHED) {
                    channel.publishConfirmResponses.first()
                }
                channel.basicPublish {
                    this.body = body
                    exchange = ""
                    routingKey = queueName
                }
                confirm.await()

                val msg = channel.basicGet {
                    queue = queueName
                }
                assertNotNull(msg.message)

                assertEquals(0u, msg.messageCount)
                assertEquals("", msg.message.body.decodeToString())
                channel.basicAck(msg.message.deliveryTag)
            } finally {
                runCatching { channel.queueDelete { name = queueName } }
                channel.close()
            }
        }
    }

    @Test
    fun testBasicGetEmpty() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()
            val queueName = "test-basic-get-empty-${Uuid.random()}"

            channel.queueDeclare {
                name = queueName
                durable = true
            }

            try {
                val result = channel.basicGet {
                    queue = queueName
                }
                assertEquals(null, result.message)
                assertEquals(0u, result.messageCount)
            } finally {
                runCatching { channel.queueDelete { name = queueName } }
                channel.close()
            }
        }
    }

    @Test
    fun testBasicTx() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()
            channel.txSelect {}
            channel.txCommit {}
            channel.txRollback {}
            channel.close()
        }
    }

    @Test
    fun testConfirm() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()
            channel.confirmSelect {}
            channel.confirmSelect {}
            channel.close()
        }
    }

    @Test
    fun testFlow() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()
            channel.flow {
                active = true
            }
            channel.close()
        }
    }

    @Test
    fun testBasicQos() = runTest {
        withConnection { connection ->
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
    }

    @Test
    fun testConsumeConfirms() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()
            val queueName = "test-consume-confirms-${Uuid.random()}"

            channel.queueDeclare {
                name = queueName
                durable = true
            }
            channel.confirmSelect()

            try {
                val body = "{}".toByteArray()
                val publishCount = 6

                // Subscribe BEFORE publishing so we don't miss confirms that fire between
                // basicPublish() returning and our collector attaching. publishConfirmResponses
                // is a SharedFlow with replay=0 — late subscribers drop emissions.
                val confirmed = CompletableDeferred<Unit>()
                val confirmJob = launch {
                    var resolved = 0uL
                    channel.publishConfirmResponses.collect { confirm ->
                        resolved = maxOf(resolved, confirm.deliveryTag)
                        if (resolved >= publishCount.toULong()) {
                            confirmed.complete(Unit)
                            cancel()
                        }
                    }
                }

                repeat(publishCount) {
                    channel.basicPublish {
                        this.body = body
                        exchange = ""
                        routingKey = queueName
                        properties = Properties()
                    }
                }
                // Wait for every publish to be confirmed before consuming, so basicGet sees all
                // messages on Kotlin/Native (where the broker has not yet fully enqueued the
                // publishes when the next frame is sent over the same fast loopback connection).
                confirmed.await()
                confirmJob.join()

                run {
                    val msg = channel.basicGet {
                        queue = queueName
                    }.message ?: kotlin.test.fail()
                    channel.basicAck(msg.deliveryTag)

                    val msg2 = channel.basicGet {
                        queue = queueName
                    }.message ?: kotlin.test.fail()
                    channel.basicAck(msg2)
                }

                run {
                    val msg = channel.basicGet {
                        queue = queueName
                    }.message ?: kotlin.test.fail()
                    channel.basicNack(msg.deliveryTag)

                    val msg2 = channel.basicGet {
                        queue = queueName
                    }.message ?: kotlin.test.fail()
                    channel.basicNack(msg2)
                }

                run {
                    val msg = channel.basicGet {
                        queue = queueName
                    }.message ?: kotlin.test.fail()
                    channel.basicReject(msg.deliveryTag)

                    val msg2 = channel.basicGet {
                        queue = queueName
                    }.message ?: kotlin.test.fail()
                    channel.basicReject(msg2)
                }

                channel.basicRecover {
                    requeue = true
                }
            } finally {
                runCatching { channel.queueDelete { name = queueName } }
                channel.close()
            }
        }
    }

    @Test
    fun testPublishConsume() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()
            val queueName = "test-publish-consume-${Uuid.random()}"

            channel.queueDeclare {
                name = queueName
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

            try {
                for (i in 1..100) {
                    val result = channel.basicPublish {
                        this.body = body
                        exchange = ""
                        routingKey = queueName
                    }
                    assertEquals(i.toULong(), result.deliveryTag)
                }

                repeat(100) {
                    val result = channel.basicGet {
                        queue = queueName
                    }
                    channel.basicAck(result.message ?: kotlin.test.fail("No message received"))
                }

                confirmJob.join()
            } finally {
                runCatching { channel.queueDelete { name = queueName } }
                channel.close()
            }
        }
    }

    @Test
    fun testBasicConsumeManualCancel() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()
            val queueName = "test-consume-manual-cancel-${Uuid.random()}"
            channel.queueDeclare {
                name = queueName
                durable = true
            }

            try {
                val body = "{}".toByteArray()
                repeat(100) {
                    channel.basicPublish {
                        this.body = body
                        exchange = ""
                        routingKey = queueName
                    }
                }

                val deliveryChannel = channel.basicConsume(
                    queue = queueName,
                    noAck = true
                )

                val consumerCount = channel.consumerCount(queueName)
                assertEquals(1u, consumerCount)

                var count = 0
                runCatching {
                    for (delivery in deliveryChannel) {
                        count++
                        if (count == 100) channel.basicCancel(deliveryChannel.consumeOk.consumerTag)
                    }
                }
                assertEquals(100, count)
            } finally {
                runCatching { channel.queueDelete { name = queueName } }
                channel.close()
            }
        }
    }

    @Test
    fun testBasicConsumeManualCancelFromReceiveChannel() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()
            val queueName = "test-consume-manual-cancel-from-receive-${Uuid.random()}"
            channel.queueDeclare {
                name = queueName
                durable = true
            }

            try {
                val body = "{}".toByteArray()
                repeat(100) {
                    channel.basicPublish {
                        this.body = body
                        exchange = ""
                        routingKey = queueName
                    }
                }

                val deliveryChannel = channel.basicConsume(
                    queue = queueName,
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
            } finally {
                runCatching { channel.queueDelete { name = queueName } }
                channel.close()
            }
        }
    }

    @Test
    fun testOpenChannelsConcurrently() = runTest {
        withConnection { connection ->
            val first = connection.openChannel()
            val second = connection.openChannel()

            first.close()
            second.close()
        }
    }

    @Test
    fun testConcurrentOperationsOnChannel() = runTest {
        withConnection { connection ->
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
    }

    @Test
    fun testConcurrentMessageProcessing() = runTest {
        withConnection { connection ->
            // Setup
            val queueName = "test-concurrent-consume-${Uuid.random()}"
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

            try {
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
            } finally {
                runCatching { channel.queueDelete { name = queueName } }
                channel.close()
            }
        }
    }

    @Test
    fun testConcurrentMessageProcessingWithErrors() = runTest {
        withConnection { connection ->
            // Setup
            val queueName = "test-concurrent-consume-errors-${Uuid.random()}"
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

            try {
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
            } finally {
                runCatching { channel.queueDelete { name = queueName } }
                channel.close()
            }
        }
    }

}
