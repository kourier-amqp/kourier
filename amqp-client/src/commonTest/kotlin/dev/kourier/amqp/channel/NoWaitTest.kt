package dev.kourier.amqp.channel

import dev.kourier.amqp.AMQPException
import dev.kourier.amqp.BuiltinExchangeType
import dev.kourier.amqp.withConnection
import io.ktor.utils.io.core.*
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlin.test.*
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.Uuid

/**
 * Covers the `no-wait` variants: the broker sends no reply, so each test checks the effect through an
 * awaiting call afterwards rather than through a return value.
 */
class NoWaitTest {

    @Test
    fun testQueueDeclareNoWaitCreatesTheQueue() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()
            val queue = "test-nowait-declare-${Uuid.random()}"

            channel.queueDeclareNoWait(queue, durable = false, exclusive = true, autoDelete = true)

            // A passive declare only succeeds if the queue is really there.
            assertEquals(queue, channel.queueDeclarePassive(queue).queueName)

            channel.close()
        }
    }

    @Test
    fun testQueueDeclareNoWaitRejectsAnEmptyName() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()

            // With no reply there is no way to learn a server-generated name, so asking for one is a
            // programming error rather than something to discover at runtime.
            assertFailsWith<IllegalArgumentException> { channel.queueDeclareNoWait("") }

            channel.close()
        }
    }

    @Test
    fun testQueueBindNoWaitRoutesMessages() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()
            val exchange = "test-nowait-bind-${Uuid.random()}"
            val queue = "test-nowait-bind-queue-${Uuid.random()}"

            channel.exchangeDeclareNoWait(exchange, BuiltinExchangeType.TOPIC)
            channel.queueDeclareNoWait(queue, durable = false, exclusive = true, autoDelete = true)
            channel.queueBindNoWait(queue, exchange, "test.key")

            val deliveries = channel.basicConsume(queue = queue, noAck = true)
            channel.basicPublish("routed".toByteArray(), exchange, "test.key")

            // Nothing above was acknowledged by the broker, so this delivery is the proof that the
            // exchange, the queue and the binding all really exist.
            assertEquals("routed", withTimeout(5.seconds) { deliveries.receive() }.message.body.decodeToString())

            channel.exchangeDelete(exchange)
            channel.close()
        }
    }

    @Test
    fun testQueueDeleteNoWaitRemovesTheQueue() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()
            val queue = "test-nowait-delete-${Uuid.random()}"

            channel.queueDeclare(queue, durable = false, exclusive = true, autoDelete = false)
            channel.queueDeleteNoWait(queue)

            // Passively declaring a queue that is gone raises a channel-level 404.
            assertFailsWith<AMQPException.ChannelClosed> { channel.queueDeclarePassive(queue) }
        }
    }

    @Test
    fun testQueuePurgeNoWaitEmptiesTheQueue() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()
            val queue = "test-nowait-purge-${Uuid.random()}"

            channel.queueDeclare(queue, durable = false, exclusive = true, autoDelete = false)
            channel.basicPublish("to be purged".toByteArray(), "", queue)
            channel.basicPublish("to be purged".toByteArray(), "", queue)

            channel.queuePurgeNoWait(queue)

            // The purge is fire and forget, but frames are ordered on a channel, so by the time this
            // awaiting call is answered the purge has been applied.
            assertNull(channel.basicGet(queue, noAck = true).message)

            channel.close()
        }
    }

    @Test
    fun testExchangeDeclareAndDeleteNoWait() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()
            val exchange = "test-nowait-exchange-${Uuid.random()}"

            channel.exchangeDeclareNoWait(exchange, BuiltinExchangeType.DIRECT)
            // A passive declare only succeeds if the exchange is really there.
            channel.exchangeDeclarePassive(exchange)

            channel.exchangeDeleteNoWait(exchange)
            assertFailsWith<AMQPException.ChannelClosed> { channel.exchangeDeclarePassive(exchange) }
        }
    }

    @Test
    fun testQueueDeclarePassiveNoWaitAssertsExistence() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()
            val queue = "test-nowait-passive-queue-${Uuid.random()}"

            channel.queueDeclare(queue, durable = false, exclusive = true, autoDelete = true)
            channel.queueDeclarePassiveNoWait(queue)

            // The assertion held, so the channel is still alive and usable.
            assertEquals(queue, channel.queueDeclarePassive(queue).queueName)

            channel.close()
        }
    }

    @Test
    fun testQueueDeclarePassiveNoWaitClosesTheChannelWhenTheQueueIsMissing() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()

            channel.queueDeclarePassiveNoWait("test-nowait-absent-${Uuid.random()}")

            // Nothing is returned, so the failed assertion reaches us only as the broker closing the
            // channel. Awaiting that signal is deterministic, unlike issuing another call and hoping
            // it is rejected rather than left waiting for a reply that will never come.
            val closed = withTimeout(5.seconds) { channel.channelClosed.await() }
            assertEquals(404u.toUShort(), closed.replyCode)
        }
    }

    @Test
    fun testExchangeDeclarePassiveNoWaitAssertsExistence() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()
            val exchange = "test-nowait-passive-exchange-${Uuid.random()}"

            channel.exchangeDeclare(exchange, BuiltinExchangeType.DIRECT)
            channel.exchangeDeclarePassiveNoWait(exchange)

            channel.exchangeDeclarePassive(exchange)

            channel.exchangeDelete(exchange)
            channel.close()
        }
    }

    @Test
    fun testExchangeDeclarePassiveNoWaitClosesTheChannelWhenTheExchangeIsMissing() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()

            channel.exchangeDeclarePassiveNoWait("test-nowait-absent-${Uuid.random()}")

            val closed = withTimeout(5.seconds) { channel.channelClosed.await() }
            assertEquals(404u.toUShort(), closed.replyCode)
        }
    }

    @Test
    fun testExchangeBindAndUnbindNoWait() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()
            val source = "test-nowait-source-${Uuid.random()}"
            val destination = "test-nowait-destination-${Uuid.random()}"
            val queue = "test-nowait-chained-${Uuid.random()}"

            channel.exchangeDeclareNoWait(source, BuiltinExchangeType.DIRECT)
            channel.exchangeDeclareNoWait(destination, BuiltinExchangeType.DIRECT)
            channel.queueDeclareNoWait(queue, durable = false, exclusive = true, autoDelete = true)
            channel.queueBindNoWait(queue, destination, "chained")
            channel.exchangeBindNoWait(destination, source, "chained")

            val deliveries = channel.basicConsume(queue = queue, noAck = true)
            channel.basicPublish("through".toByteArray(), source, "chained")
            assertEquals("through", withTimeout(5.seconds) { deliveries.receive() }.message.body.decodeToString())

            channel.exchangeUnbindNoWait(destination, source, "chained")
            channel.basicPublish("dropped".toByteArray(), source, "chained")

            // The unbind is applied before this awaiting call is answered, so the queue must stay empty.
            assertNull(channel.basicGet(queue, noAck = true).message)

            channel.exchangeDelete(source)
            channel.exchangeDelete(destination)
            channel.close()
        }
    }

    @Test
    fun testConfirmSelectNoWaitStillConfirmsPublishes() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()

            channel.confirmSelectNoWait()

            // Subscribe before publishing: publishConfirmResponses is a replay=0 SharedFlow. If confirm
            // mode had not actually been applied, no Basic.Ack would come and this would time out.
            val confirm = async(start = CoroutineStart.UNDISPATCHED) { channel.publishConfirmResponses.first() }
            channel.basicPublish("confirmed".toByteArray(), "", "test-nowait-confirm-${Uuid.random()}")
            withTimeout(5.seconds) { confirm.await() }

            channel.close()
        }
    }
}
