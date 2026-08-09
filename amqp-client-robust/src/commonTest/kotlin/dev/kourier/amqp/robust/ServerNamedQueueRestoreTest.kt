package dev.kourier.amqp.robust

import dev.kourier.amqp.AMQPException
import dev.kourier.amqp.BuiltinExchangeType
import dev.kourier.amqp.channel.AMQPChannel
import io.ktor.utils.io.core.*
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.Uuid

/**
 * Regression tests for consumers attached to **server-named** queues (the no-arg [AMQPChannel.queueDeclare],
 * which declares an exclusive, auto-delete, non-durable queue whose name the broker generates).
 *
 * Such a queue is *channel-scoped*: the broker deletes it as soon as the channel that declared it goes away.
 * So when the broker closes a channel (e.g. `consumer_timeout` firing on an unacked delivery), restoring the
 * consumer requires re-declaring the queue and re-binding it, and the broker hands back a **new** name.
 *
 * Before the fix, [RobustAMQPChannel.restore] re-issued `basic.consume` with the *old* generated name:
 *  - with `restoreTopology = false` the queue was never re-declared at all;
 *  - with `restoreTopology = true` it was re-declared under the *requested* name `""`, so the broker minted a
 *    fresh name while `consumedQueues` still held the stale one.
 *
 * Both paths made the broker answer `not_found`, which closed the channel again. Because that close is itself
 * a broker close, the channel was left permanently unusable until the process restarted.
 */
class ServerNamedQueueRestoreTest {

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

    /**
     * `restoreTopology = false` with a server-named reply queue bound to a topic exchange and consumed on the
     * very channel that declared it. This is the common request/reply setup.
     */
    @Test
    @OptIn(DelicateCoroutinesApi::class)
    fun testServerNamedQueueConsumerRestoredWithoutTopologyRestore() = runTest {
        withConnection({ server { restoreTopology = false } }) { connection ->
            val channel = connection.openChannel()
            val exchange = "test-server-named-no-topology-${Uuid.random()}"
            val routingKey = "test.server.named"

            channel.exchangeDeclare(exchange, BuiltinExchangeType.TOPIC, durable = false)

            val queueName = channel.queueDeclare().queueName
            channel.queueBind(queueName, exchange, routingKey)
            val deliveries = channel.basicConsume(queue = queueName, noAck = true)

            val reopenEvent = async(start = CoroutineStart.UNDISPATCHED) { channel.openedResponses.first() }
            channel.closeByBreaking()
            reopenEvent.await()

            // The consumer must still be attached after the restore.
            assertFalse(deliveries.isClosedForReceive, "Consumer was cancelled by the restore")

            channel.basicPublish("after-restore".toByteArray(), exchange, routingKey)
            val delivery = withTimeout(5.seconds) { deliveries.receive() }
            assertEquals("after-restore", delivery.message.body.decodeToString())

            channel.exchangeDelete(exchange)
            channel.close()
        }
    }

    /**
     * Same scenario with `restoreTopology = true`. The queue *is* re-declared here, but under the requested
     * name `""`, so the broker generates a different name than the one the consumer was recorded against.
     */
    @Test
    @OptIn(DelicateCoroutinesApi::class)
    fun testServerNamedQueueConsumerRestoredWithTopologyRestore() = runTest {
        withConnection({ server { restoreTopology = true } }) { connection ->
            val channel = connection.openChannel()
            val exchange = "test-server-named-topology-${Uuid.random()}"
            val routingKey = "test.server.named"

            channel.exchangeDeclare(exchange, BuiltinExchangeType.TOPIC, durable = false)

            val queueName = channel.queueDeclare().queueName
            channel.queueBind(queueName, exchange, routingKey)
            val deliveries = channel.basicConsume(queue = queueName, noAck = true)

            val reopenEvent = async(start = CoroutineStart.UNDISPATCHED) { channel.openedResponses.first() }
            channel.closeByBreaking()
            reopenEvent.await()

            assertFalse(deliveries.isClosedForReceive, "Consumer was cancelled by the restore")

            channel.basicPublish("after-restore".toByteArray(), exchange, routingKey)
            val delivery = withTimeout(5.seconds) { deliveries.receive() }
            assertEquals("after-restore", delivery.message.body.decodeToString())

            channel.exchangeDelete(exchange)
            channel.close()
        }
    }

    /**
     * The broker mints a new name on every re-declare, so the bookkeeping has to be retargeted each time
     * rather than only on the first restore. A second close must be survived just as well as the first.
     */
    @Test
    @OptIn(DelicateCoroutinesApi::class)
    fun testServerNamedQueueConsumerRestoredAcrossConsecutiveBrokerCloses() = runTest {
        withConnection({ server { restoreTopology = false } }) { connection ->
            val channel = connection.openChannel()
            val exchange = "test-server-named-twice-${Uuid.random()}"
            val routingKey = "test.server.named"

            channel.exchangeDeclare(exchange, BuiltinExchangeType.TOPIC, durable = false)

            val queueName = channel.queueDeclare().queueName
            channel.queueBind(queueName, exchange, routingKey)
            val deliveries = channel.basicConsume(queue = queueName, noAck = true)

            repeat(2) { round ->
                val reopenEvent = async(start = CoroutineStart.UNDISPATCHED) { channel.openedResponses.first() }
                channel.closeByBreaking()
                reopenEvent.await()

                assertFalse(deliveries.isClosedForReceive, "Consumer was cancelled by restore #${round + 1}")

                channel.basicPublish("round-$round".toByteArray(), exchange, routingKey)
                val delivery = withTimeout(5.seconds) { deliveries.receive() }
                assertEquals("round-$round", delivery.message.body.decodeToString())
            }

            channel.exchangeDelete(exchange)
            channel.close()
        }
    }

    /**
     * A server-named queue that is neither exclusive nor auto-delete outlives its channel, so it is ordinary
     * topology rather than channel-scoped state and must not be tracked as the latter.
     *
     * The restore is exercised on purpose: the broker reserves the `amq.*` prefix, so re-declaring such a
     * queue under the name it assigned draws `ACCESS_REFUSED` and kills the channel for good. The recorded
     * name has to stay the empty one the caller asked for.
     */
    @Test
    @OptIn(DelicateCoroutinesApi::class)
    fun testDurableServerNamedQueueIsNotTrackedAsChannelScoped() = runTest {
        withConnection({ server { restoreTopology = true } }) { connection ->
            val channel = connection.openChannel() as RobustAMQPChannel

            val queueName = channel
                .queueDeclare("", durable = false, exclusive = false, autoDelete = false)
                .queueName

            assertTrue(
                channel.serverNamedQueues.isEmpty(),
                "A queue that survives its channel must not be tracked as channel-scoped"
            )

            // The restore below re-declares this one under the empty name and so leaves an orphan behind.
            // That is pre-existing behaviour, unrelated to what this test guards, and the queue is transient.

            val reopenEvent = async(start = CoroutineStart.UNDISPATCHED) { channel.openedResponses.first() }
            channel.closeByBreaking()
            reopenEvent.await()

            // The channel must have survived the restore rather than tripping over a reserved queue name.
            val probe = "test-probe-${Uuid.random()}"
            channel.queueDeclare(probe, durable = false, exclusive = true, autoDelete = true)
            channel.queueDelete(probe)

            channel.queueDelete(queueName)
            channel.close()
        }
    }

    /**
     * The channel must stay usable after restoring a server-named consumer. Without the fix the `not_found`
     * raised during the restore closes the channel a second time, and since that is itself a broker close the
     * channel never recovers: every later operation throws [AMQPException.ChannelClosed] for the lifetime of
     * the process.
     */
    @Test
    @OptIn(DelicateCoroutinesApi::class)
    fun testChannelStaysUsableAfterServerNamedQueueRestore() = runTest {
        withConnection({ server { restoreTopology = false } }) { connection ->
            val channel = connection.openChannel()

            val queueName = channel.queueDeclare().queueName
            channel.basicConsume(queue = queueName, noAck = true)

            val reopenEvent = async(start = CoroutineStart.UNDISPATCHED) { channel.openedResponses.first() }
            channel.closeByBreaking()
            reopenEvent.await()

            // A plain operation on the restored channel must succeed rather than throw ChannelClosed.
            val probe = "test-probe-${Uuid.random()}"
            channel.queueDeclare(probe, durable = false, exclusive = true, autoDelete = true)
            channel.queueDelete(probe)

            channel.close()
        }
    }
}
