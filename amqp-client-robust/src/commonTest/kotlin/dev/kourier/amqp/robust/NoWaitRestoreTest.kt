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
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.Uuid

/**
 * Topology declared through the `no-wait` variants has to be recorded for restore exactly like the
 * awaiting ones. Were it not, it would be missing from the replay and would silently disappear on the
 * first reconnect, which is precisely the kind of failure the robust channel exists to prevent.
 */
class NoWaitRestoreTest {

    private suspend fun AMQPChannel.closeByBreaking() =
        assertFailsWith<AMQPException.ChannelClosed> {
            exchangeDeclare(
                "will-fail", "nonexistent-type",
                durable = true, autoDelete = false, internal = false, arguments = emptyMap()
            )
        }

    @Test
    fun testNoWaitDeclarationsAreRecordedForRestore() = runTest {
        withConnection({ server { restoreTopology = true } }) { connection ->
            val channel = connection.openChannel() as RobustAMQPChannel
            val exchange = "test-nowait-recorded-${Uuid.random()}"
            val queue = "test-nowait-recorded-queue-${Uuid.random()}"

            channel.exchangeDeclareNoWait(exchange, BuiltinExchangeType.TOPIC)
            channel.queueDeclareNoWait(queue, durable = false, exclusive = false, autoDelete = true)
            channel.queueBindNoWait(queue, exchange, "recorded.key")

            assertTrue(exchange in channel.declaredExchanges, "exchange not recorded for restore")
            assertTrue(queue in channel.declaredQueues, "queue not recorded for restore")
            assertTrue(
                Triple(queue, exchange, "recorded.key") in channel.boundQueues,
                "binding not recorded for restore"
            )

            channel.queueDeleteNoWait(queue)
            assertTrue(queue !in channel.declaredQueues, "deleted queue still recorded for restore")

            channel.exchangeDeleteNoWait(exchange)
            assertTrue(exchange !in channel.declaredExchanges, "deleted exchange still recorded for restore")

            channel.close()
        }
    }

    @Test
    fun testNoWaitExchangeBindingsAreRecordedForRestore() = runTest {
        withConnection({ server { restoreTopology = true } }) { connection ->
            val channel = connection.openChannel() as RobustAMQPChannel
            val source = "test-nowait-bound-source-${Uuid.random()}"
            val destination = "test-nowait-bound-destination-${Uuid.random()}"

            channel.exchangeDeclareNoWait(source, BuiltinExchangeType.DIRECT)
            channel.exchangeDeclareNoWait(destination, BuiltinExchangeType.DIRECT)
            channel.exchangeBindNoWait(destination, source, "bound.key")

            assertTrue(
                Triple(destination, source, "bound.key") in channel.boundExchanges,
                "exchange binding not recorded for restore"
            )

            channel.exchangeUnbindNoWait(destination, source, "bound.key")
            assertTrue(
                Triple(destination, source, "bound.key") !in channel.boundExchanges,
                "unbound exchange still recorded for restore"
            )

            channel.exchangeDelete(source)
            channel.exchangeDelete(destination)
            channel.close()
        }
    }

    @Test
    @OptIn(DelicateCoroutinesApi::class)
    fun testConfirmModeSelectedWithNoWaitIsReestablishedAfterRestore() = runTest {
        withConnection({ server { restoreTopology = false } }) { connection ->
            val channel = connection.openChannel()

            channel.confirmSelectNoWait()

            val reopenEvent = async(start = CoroutineStart.UNDISPATCHED) { channel.openedResponses.first() }
            channel.closeByBreaking()
            reopenEvent.await()

            // The reopened broker channel is not in confirm mode unless the restore re-issued the
            // select, which only happens if the no-wait variant recorded the intent.
            val confirm = async(start = CoroutineStart.UNDISPATCHED) { channel.publishConfirmResponses.first() }
            channel.basicPublish("after restore".toByteArray(), "", "test-nowait-confirm-${Uuid.random()}")
            withTimeout(5.seconds) { confirm.await() }

            channel.close()
        }
    }

    @Test
    @OptIn(DelicateCoroutinesApi::class)
    fun testTopologyDeclaredWithNoWaitSurvivesABrokerClose() = runTest {
        withConnection({ server { restoreTopology = true } }) { connection ->
            val channel = connection.openChannel()
            val exchange = "test-nowait-restore-${Uuid.random()}"
            val queue = "test-nowait-restore-queue-${Uuid.random()}"

            channel.exchangeDeclareNoWait(exchange, BuiltinExchangeType.TOPIC)
            // autoDelete so the broker drops the queue when the channel close cancels its consumer:
            // the restore then has to re-declare and re-bind it rather than finding it still there.
            channel.queueDeclareNoWait(queue, durable = false, exclusive = false, autoDelete = true)
            channel.queueBindNoWait(queue, exchange, "restore.key")
            val deliveries = channel.basicConsume(queue = queue, noAck = true)

            val reopenEvent = async(start = CoroutineStart.UNDISPATCHED) { channel.openedResponses.first() }
            channel.closeByBreaking()
            reopenEvent.await()

            channel.basicPublish("after restore".toByteArray(), exchange, "restore.key")
            val delivery = withTimeout(5.seconds) { deliveries.receive() }
            assertEquals("after restore", delivery.message.body.decodeToString())

            channel.exchangeDelete(exchange)
            channel.close()
        }
    }
}
