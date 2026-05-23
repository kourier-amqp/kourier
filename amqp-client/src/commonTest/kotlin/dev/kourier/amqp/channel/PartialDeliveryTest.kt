package dev.kourier.amqp.channel

import dev.kourier.amqp.Frame
import dev.kourier.amqp.Properties
import dev.kourier.amqp.ProtocolError
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class PartialDeliveryTest {

    private fun deliver() = Frame.Method.Basic.Deliver(
        consumerTag = "c",
        deliveryTag = 1u,
        redelivered = false,
        exchange = "",
        routingKey = "k",
    )

    private fun header(bodySize: ULong) =
        Frame.Header(classID = 60u, weight = 0u, bodySize = bodySize, properties = Properties())

    // NEW-12: a body spanning more bytes than the header declared must be rejected, not silently
    // grown (the old addBody reallocated unboundedly).
    @Test
    fun testBodyOverrunIsRejected() {
        val pd = PartialDelivery(deliver())
        pd.setHeader(header(bodySize = 10uL))
        assertFailsWith<ProtocolError.Invalid> { pd.addBody(ByteArray(15)) }
    }

    // Reassembly across multiple body frames yields the exact bytes and completes precisely.
    @Test
    fun testReassemblyAcrossFrames() {
        val pd = PartialDelivery(deliver())
        pd.setHeader(header(bodySize = 5uL))
        assertFalse(pd.isComplete)
        pd.addBody(byteArrayOf(1, 2))
        assertFalse(pd.isComplete)
        pd.addBody(byteArrayOf(3, 4, 5))
        assertTrue(pd.isComplete)

        val (_, _, body) = pd.asCompletedMessage()
        assertContentEquals(byteArrayOf(1, 2, 3, 4, 5), body)
    }
}
