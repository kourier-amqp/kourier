package dev.kourier.amqp.channel

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Stress tests for concurrent access to [AMQPChannels].
 *
 * Without synchronization, two coroutines calling reserveNext() can observe the same empty
 * slot and both win the reservation, returning the same id. The list/add/remove paths can
 * also produce ConcurrentModificationException (where the platform enforces it) or stale reads.
 *
 * These tests are stress tests: they iterate enough times that a racy implementation
 * fails frequently (>= ~10% of runs) while a synchronized one passes deterministically.
 */
class AMQPChannelsConcurrencyTest {

    @Test
    fun reserveNextDoesNotHandOutDuplicateIdsUnderConcurrency() = runBlocking {
        val iterations = 50
        repeat(iterations) {
            val channels = AMQPChannels(channelMax = 200u)
            withContext(Dispatchers.Default) {
                val results = (1..200).map {
                    async { channels.reserveNext() }
                }.awaitAll()
                val nonNull = results.filterNotNull()
                assertEquals(
                    nonNull.size,
                    nonNull.toSet().size,
                    "Expected unique channel ids; saw duplicates: ${
                        nonNull.groupingBy { it }.eachCount().filter { it.value > 1 }
                    }",
                )
            }
        }
    }

    @Test
    fun listIsStableUnderConcurrentMutation() = runBlocking {
        val channels = AMQPChannels(channelMax = 1000u)
        // Pre-populate so list() always has something to walk
        repeat(100) {
            val id = channels.reserveNext() ?: error("could not reserve")
            channels.add(StubChannel(id))
        }

        val mutationDone = Mutex(locked = true)
        withContext(Dispatchers.Default) {
            val mutator = async {
                repeat(500) {
                    val id = channels.reserveNext()
                    if (id != null) {
                        channels.add(StubChannel(id))
                        channels.remove(id)
                    }
                }
                mutationDone.unlock()
            }
            val reader = async {
                while (!mutationDone.tryLock()) {
                    // Snapshot — a racy list() that returns a live view would throw on JVM here
                    val snapshot = channels.list()
                    // Touch every entry to force any structural concurrent issue to surface
                    snapshot.forEach { it.id }
                }
            }
            mutator.await()
            reader.await()
        }
        assertTrue(true, "Reader did not throw a concurrent modification error")
    }
}

/**
 * Minimal AMQPChannel for testing AMQPChannels container — we only need stable id() access.
 */
private class StubChannel(override val id: dev.kourier.amqp.ChannelId) : AMQPChannel {
    override val state get() = error("not used")
    override val channelClosed get() = error("not used")
    override val openedResponses get() = error("not used")
    override val closedResponses get() = error("not used")
    override val publishConfirmResponses get() = error("not used")
    override val returnResponses get() = error("not used")
    override val flowResponses get() = error("not used")
    override val isConfirmMode get() = error("not used")
    override val isTxMode get() = error("not used")
    override suspend fun write(vararg frames: dev.kourier.amqp.Frame) = error("not used")
    override suspend fun open() = error("not used")
    override suspend fun close(reason: String, code: UShort) = error("not used")
    override suspend fun basicPublish(
        body: ByteArray,
        exchange: String,
        routingKey: String,
        mandatory: Boolean,
        immediate: Boolean,
        properties: dev.kourier.amqp.Properties,
    ) = error("not used")

    override suspend fun basicGet(queue: String, noAck: Boolean) = error("not used")
    override suspend fun basicConsume(
        queue: String,
        consumerTag: String,
        noAck: Boolean,
        exclusive: Boolean,
        arguments: dev.kourier.amqp.Table,
    ) = error("not used")

    override suspend fun basicConsume(
        queue: String,
        consumerTag: String,
        noAck: Boolean,
        exclusive: Boolean,
        arguments: dev.kourier.amqp.Table,
        onDelivery: suspend (dev.kourier.amqp.AMQPResponse.Channel.Message.Delivery) -> Unit,
        onCanceled: suspend (dev.kourier.amqp.AMQPResponse.Channel) -> Unit,
    ) = error("not used")

    override suspend fun basicCancel(consumerTag: String) = error("not used")
    override suspend fun basicAck(deliveryTag: ULong, multiple: Boolean) = error("not used")
    override suspend fun basicAck(message: dev.kourier.amqp.AMQPMessage, multiple: Boolean) = error("not used")
    override suspend fun basicNack(deliveryTag: ULong, multiple: Boolean, requeue: Boolean) = error("not used")
    override suspend fun basicNack(message: dev.kourier.amqp.AMQPMessage, multiple: Boolean, requeue: Boolean) =
        error("not used")

    override suspend fun basicReject(deliveryTag: ULong, requeue: Boolean) = error("not used")
    override suspend fun basicReject(message: dev.kourier.amqp.AMQPMessage, requeue: Boolean) = error("not used")
    override suspend fun basicRecover(requeue: Boolean) = error("not used")
    override suspend fun basicQos(count: UShort, global: Boolean) = error("not used")
    override suspend fun flow(active: Boolean) = error("not used")
    override suspend fun queueDeclare(
        name: String,
        durable: Boolean,
        exclusive: Boolean,
        autoDelete: Boolean,
        arguments: dev.kourier.amqp.Table,
    ) = error("not used")

    override suspend fun queueDeclarePassive(name: String) = error("not used")
    override suspend fun queueDeclare() = error("not used")
    override suspend fun messageCount(name: String) = error("not used")
    override suspend fun consumerCount(name: String) = error("not used")
    override suspend fun queueDelete(name: String, ifUnused: Boolean, ifEmpty: Boolean) = error("not used")
    override suspend fun queuePurge(name: String) = error("not used")
    override suspend fun queueBind(
        queue: String,
        exchange: String,
        routingKey: String,
        arguments: dev.kourier.amqp.Table,
    ) =
        error("not used")

    override suspend fun queueUnbind(
        queue: String,
        exchange: String,
        routingKey: String,
        arguments: dev.kourier.amqp.Table,
    ) = error("not used")

    override suspend fun exchangeDeclare(
        name: String,
        type: String,
        durable: Boolean,
        autoDelete: Boolean,
        internal: Boolean,
        arguments: dev.kourier.amqp.Table,
    ) = error("not used")

    override suspend fun exchangeDeclarePassive(name: String) = error("not used")
    override suspend fun exchangeDelete(name: String, ifUnused: Boolean) = error("not used")
    override suspend fun exchangeBind(
        destination: String,
        source: String,
        routingKey: String,
        arguments: dev.kourier.amqp.Table,
    ) = error("not used")

    override suspend fun exchangeUnbind(
        destination: String,
        source: String,
        routingKey: String,
        arguments: dev.kourier.amqp.Table,
    ) = error("not used")

    override suspend fun confirmSelect() = error("not used")
    override suspend fun txSelect() = error("not used")
    override suspend fun txCommit() = error("not used")
    override suspend fun txRollback() = error("not used")
}
