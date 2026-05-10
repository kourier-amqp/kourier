package dev.kourier.amqp.channel

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals

class AMQPChannelsTest {

    @Test
    fun testReservesChannelIds() = runTest {
        val channels = AMQPChannels(channelMax = 10u)
        val c1 = channels.reserveNext()
        val c2 = channels.reserveNext()
        assertEquals(1u, c1)
        assertEquals(2u, c2)
    }

    @Test
    fun testReusesChannelIdAfterRemoving() = runTest {
        val channels = AMQPChannels(channelMax = 10u)
        val c1 = channels.reserveNext()
        channels.remove(id = c1!!)
        val c2 = channels.reserveNext()
        assertEquals(1u, c1)
        assertEquals(1u, c2)
    }

    @Test
    fun testDoesNotGoPastChannelMax() = runTest {
        val channels = AMQPChannels(channelMax = 1u)
        val c1 = channels.reserveNext()
        val c2 = channels.reserveNext()
        assertEquals(1u, c1)
        assertEquals(null, c2)
    }

}
