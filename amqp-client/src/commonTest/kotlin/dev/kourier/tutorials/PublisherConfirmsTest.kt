package dev.kourier.tutorials

import dev.kourier.amqp.AMQPResponse
import dev.kourier.amqp.Properties
import dev.kourier.amqp.connection.amqpConfig
import dev.kourier.amqp.connection.createAMQPConnection
import io.ktor.utils.io.core.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.uuid.Uuid

class PublisherConfirmsTest {

    @Test
    fun testIndividualPublishConfirm() = runTest {
        withContext(Dispatchers.Default) {
            // Unique queue name per test run: a shared queue across tests + a SharedFlow with
            // replay=0 means confirms from a prior test's still-in-flight publish could land
            // before this test's collector attaches, throwing the count off.
            val queueName = "test-individual-confirm-${Uuid.random()}"
            // Strategy 1: Publish and wait for confirm individually
            val config = amqpConfig {
                server {
                    host = "localhost"
                }
            }
            val connection = createAMQPConnection(this, config)
            val channel = connection.openChannel()

            try {
                // Declare queue
                channel.queueDeclare(
                    queueName,
                    durable = false,
                    exclusive = false,
                    autoDelete = true,
                    arguments = emptyMap()
                )

                // Enable publisher confirms
                channel.confirmSelect()

                println("[Individual] Publishing messages...")
                val messageCount = 5

                for (i in 1..messageCount) {
                    val message = "Message $i"

                    // Subscribe to confirms BEFORE publishing — replay=0 SharedFlow drops
                    // emissions made while subscriberCount=0, so a fast broker Ack arriving
                    // before .first() subscribed would be lost. UNDISPATCHED wires the
                    // subscription synchronously before async returns.
                    val confirmDeferred = async(start = CoroutineStart.UNDISPATCHED) {
                        channel.publishConfirmResponses.first()
                    }

                    // Publish message
                    channel.basicPublish(
                        message.toByteArray(),
                        exchange = "",
                        routingKey = queueName,
                        properties = Properties()
                    )

                    val confirm = confirmDeferred.await()

                    when (confirm) {
                        is AMQPResponse.Channel.Basic.PublishConfirm.Ack -> {
                            println(" [x] Message $i confirmed (deliveryTag: ${confirm.deliveryTag})")
                        }

                        is AMQPResponse.Channel.Basic.PublishConfirm.Nack -> {
                            println(" [!] Message $i nacked (deliveryTag: ${confirm.deliveryTag})")
                        }
                    }
                }

                println("[Individual] All messages confirmed")
            } finally {
                runCatching { channel.queueDelete(queueName) }
                runCatching { channel.close() }
                runCatching { connection.close() }
            }
        }
    }

    @Test
    fun testBatchPublishConfirm() = runTest {
        withContext(Dispatchers.Default) {
            val queueName = "test-batch-confirm-${Uuid.random()}"
            // Strategy 2: Publish in batches, then wait for confirms
            val config = amqpConfig {
                server {
                    host = "localhost"
                }
            }
            val connection = createAMQPConnection(this, config)
            val channel = connection.openChannel()

            try {
                channel.queueDeclare(
                    queueName,
                    durable = false,
                    exclusive = false,
                    autoDelete = true,
                    arguments = emptyMap()
                )

                // Enable publisher confirms
                channel.confirmSelect()

                println("[Batch] Publishing messages in batches...")
                val batchSize = 10
                val totalMessages = 30

                // Subscribe ONCE up front so we don't drop confirms that arrive between batches.
                // (publishConfirmResponses is a SharedFlow with replay=0 — emissions before the
                // collector attaches are silently discarded.)
                var ackCount = 0
                var nackCount = 0
                var resolvedTags = 0uL
                val allConfirmed = CompletableDeferred<Unit>()
                val confirmJob = launch(start = CoroutineStart.UNDISPATCHED) {
                    channel.publishConfirmResponses.collect { confirm ->
                        when (confirm) {
                            is AMQPResponse.Channel.Basic.PublishConfirm.Ack -> ackCount++
                            is AMQPResponse.Channel.Basic.PublishConfirm.Nack -> nackCount++
                        }
                        resolvedTags = maxOf(resolvedTags, confirm.deliveryTag)
                        if (resolvedTags >= totalMessages.toULong()) {
                            // Use a CompletableDeferred signal + explicit cancelAndJoin from the
                            // outer flow: throwing CancellationException here is not robust under
                            // runTest's leak detector — its propagation up through the SharedFlow
                            // collector can race with the test scope's shutdown on slow CI hosts
                            // and surface as UncompletedCoroutinesError.
                            allConfirmed.complete(Unit)
                        }
                    }
                }

                var published = 0
                while (published < totalMessages) {
                    val priorAcks = ackCount
                    val priorNacks = nackCount
                    val targetResolved = (published + batchSize).toULong()

                    // Publish a batch
                    for (i in 1..batchSize) {
                        val message = "Batch message ${published + i}"
                        channel.basicPublish(
                            message.toByteArray(),
                            exchange = "",
                            routingKey = queueName,
                            properties = Properties()
                        )
                    }

                    // Wait for confirms covering every tag in this batch. The broker may bulk-confirm
                    // (Ack with multiple=true) producing fewer emissions than `batchSize`, so we wait
                    // on the highest deliveryTag rather than counting emissions.
                    while (resolvedTags < targetResolved && confirmJob.isActive) {
                        yield()
                    }

                    println(" [x] Batch confirmed: ${ackCount - priorAcks} acks, ${nackCount - priorNacks} nacks")
                    published += batchSize
                }

                allConfirmed.await()
                confirmJob.cancelAndJoin()
                println("[Batch] All batches confirmed")
            } finally {
                runCatching { channel.queueDelete(queueName) }
                runCatching { channel.close() }
                runCatching { connection.close() }
            }
        }
    }

    @Test
    fun testAsyncPublishConfirm() = runTest {
        withContext(Dispatchers.Default) {
            val queueName = "test-async-confirm-${Uuid.random()}"
            // Strategy 3: Publish and handle confirms asynchronously
            val config = amqpConfig {
                server {
                    host = "localhost"
                }
            }
            val connection = createAMQPConnection(this, config)
            val channel = connection.openChannel()

            try {
                channel.queueDeclare(
                    queueName,
                    durable = false,
                    exclusive = false,
                    autoDelete = true,
                    arguments = emptyMap()
                )

                // Enable publisher confirms
                channel.confirmSelect()

                println("[Async] Publishing messages with async confirms...")

                val messageCount = 20
                val confirmedTags = mutableSetOf<ULong>()
                val nackedTags = mutableSetOf<ULong>()
                // Snapshot of the highest tag ever confirmed via multiple=true; needed because the broker
                // is permitted to send a single Ack(multiple=true) covering several pending publishes.
                var highestMultipleAck: ULong = 0u

                // Launch coroutine to handle confirms asynchronously. Signal completion via a
                // CompletableDeferred (cleaner than throwing CancellationException out of collect,
                // which can race with runTest's leak detector on slow CI hosts).
                val allConfirmed = CompletableDeferred<Unit>()
                val confirmJob = launch(start = CoroutineStart.UNDISPATCHED) {
                    channel.publishConfirmResponses.collect { confirm ->
                        when (confirm) {
                            is AMQPResponse.Channel.Basic.PublishConfirm.Ack -> {
                                if (confirm.multiple) {
                                    // Acknowledges every still-pending tag up to and including deliveryTag
                                    for (tag in (highestMultipleAck + 1u)..confirm.deliveryTag) {
                                        confirmedTags.add(tag)
                                    }
                                    highestMultipleAck = maxOf(highestMultipleAck, confirm.deliveryTag)
                                    println(" [✓] Ack(multiple) covering up to delivery tag: ${confirm.deliveryTag}")
                                } else {
                                    confirmedTags.add(confirm.deliveryTag)
                                    println(" [✓] Ack for delivery tag: ${confirm.deliveryTag}")
                                }
                            }

                            is AMQPResponse.Channel.Basic.PublishConfirm.Nack -> {
                                if (confirm.multiple) {
                                    for (tag in (highestMultipleAck + 1u)..confirm.deliveryTag) {
                                        nackedTags.add(tag)
                                    }
                                    highestMultipleAck = maxOf(highestMultipleAck, confirm.deliveryTag)
                                } else {
                                    nackedTags.add(confirm.deliveryTag)
                                }
                                println(" [✗] Nack for delivery tag: ${confirm.deliveryTag}")
                            }
                        }
                        if ((confirmedTags + nackedTags).size >= messageCount) {
                            allConfirmed.complete(Unit)
                        }
                    }
                }

                // Publish messages without waiting
                for (i in 1..messageCount) {
                    val message = "Async message $i"
                    channel.basicPublish(
                        message.toByteArray(),
                        exchange = "",
                        routingKey = queueName,
                        properties = Properties()
                    )
                }

                // Wait for all confirms to be processed, then cancel the collector explicitly.
                allConfirmed.await()
                confirmJob.cancelAndJoin()

                println("[Async] Confirmed: ${confirmedTags.size}, Nacked: ${nackedTags.size}")
                assertEquals(messageCount, confirmedTags.size, "All messages should be confirmed")
            } finally {
                runCatching { channel.queueDelete(queueName) }
                runCatching { channel.close() }
                runCatching { connection.close() }
            }
        }
    }

    @Test
    fun testMultipleConfirms() = runTest {
        withContext(Dispatchers.Default) {
            val queueName = "test-multiple-confirms-${Uuid.random()}"
            // Test the 'multiple' flag in confirms
            val config = amqpConfig {
                server {
                    host = "localhost"
                }
            }
            val connection = createAMQPConnection(this, config)
            val channel = connection.openChannel()

            try {
                channel.queueDeclare(
                    queueName,
                    durable = false,
                    exclusive = false,
                    autoDelete = true,
                    arguments = emptyMap()
                )

                // Enable publisher confirms
                channel.confirmSelect()

                // Publish several messages
                val messageCount = 10
                val confirms = mutableListOf<AMQPResponse.Channel.Basic.PublishConfirm>()

                // Subscribe BEFORE publishing so an Ack(multiple=true) covering several pending publishes
                // — which is one emission, not `messageCount` emissions — does not leave us waiting.
                val allConfirmed = CompletableDeferred<Unit>()
                val confirmJob = launch(start = CoroutineStart.UNDISPATCHED) {
                    var resolvedTags = 0uL
                    channel.publishConfirmResponses.collect { confirm ->
                        confirms += confirm
                        // Each ack/nack resolves all tags up to deliveryTag; once we've covered every
                        // publish, stop collecting.
                        resolvedTags = maxOf(resolvedTags, confirm.deliveryTag)
                        if (resolvedTags >= messageCount.toULong()) {
                            allConfirmed.complete(Unit)
                        }
                    }
                }

                for (i in 1..messageCount) {
                    channel.basicPublish(
                        "Message $i".toByteArray(),
                        exchange = "",
                        routingKey = queueName,
                        properties = Properties()
                    )
                }

                allConfirmed.await()
                confirmJob.cancelAndJoin()

                println("[Multiple] Received ${confirms.size} confirm responses")

                val multipleConfirms = confirms.filter { it.multiple }
                if (multipleConfirms.isNotEmpty()) {
                    println("[Multiple] Received ${multipleConfirms.size} confirms with multiple=true")
                }

                // Verify all messages were confirmed
                assertTrue(
                    confirms.all { it is AMQPResponse.Channel.Basic.PublishConfirm.Ack },
                    "All confirms should be acks"
                )
            } finally {
                runCatching { channel.queueDelete(queueName) }
                runCatching { channel.close() }
                runCatching { connection.close() }
            }
        }
    }

}
