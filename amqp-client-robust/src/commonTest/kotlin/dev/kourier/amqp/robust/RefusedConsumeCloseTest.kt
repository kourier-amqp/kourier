package dev.kourier.amqp.robust

import dev.kourier.amqp.AMQPException
import dev.kourier.amqp.connection.ConnectionState
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.Uuid

class RefusedConsumeCloseTest {

    /**
     * A `basic.consume` the broker refuses (404 NOT_FOUND) closes the channel before any `consume-ok`, so no
     * consumer exists and its listener must not stay behind. The channel is restored, and its close() joins every
     * listener of the channel: one left waiting for the consumer tag of the refused consume would never let it return.
     */
    @Test
    fun closeReturnsAfterARefusedConsume() = runTest {
        withConnection { connection ->
            val channel = connection.openChannel()
            val queueName = "test-refused-consume-close-${Uuid.random()}"
            val exception = withTimeout(10.seconds) {
                assertFailsWith<AMQPException.ChannelClosed> {
                    channel.basicConsume(queue = queueName, onDelivery = {}, onCanceled = {})
                }
            }
            assertEquals(404u, exception.replyCode)

            // The robust channel restores itself after the broker closed it.
            withTimeout(10.seconds) { while (channel.state != ConnectionState.OPEN) delay(50) }

            withTimeout(5.seconds) { channel.close() }
            assertEquals(ConnectionState.CLOSED, channel.state)
        }
    }
}
