package dev.kourier.amqp.connection

import dev.kourier.amqp.Frame
import dev.kourier.amqp.ProtocolError
import dev.kourier.amqp.serialization.ProtocolBinary
import io.ktor.utils.io.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.encodeToByteArray
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class FrameDecoderTest {

    private fun encode(frame: Frame): ByteArray = ProtocolBinary.encodeToByteArray(frame)

    // NEW-25: multiple whole frames packed into a single read must all decode. This is the path
    // the old code walked with an O(n^2) whole-buffer snapshot/copy per frame; now O(n) via peek/skip.
    @Test
    fun testDecodesMultipleFramesInOneRead() = runTest {
        withContext(Dispatchers.Default) {
            val bytes = encode(Frame(channelId = 1u, payload = Frame.Body("a".encodeToByteArray()))) +
                encode(Frame(channelId = 0u, payload = Frame.Heartbeat)) +
                encode(Frame(channelId = 2u, payload = Frame.Body("bb".encodeToByteArray())))

            val received = mutableListOf<Frame>()
            // ByteReadChannel(bytes) is closed once fully read, so decodeStreaming throws EOFException
            // at the end — expected; we only care about the frames collected before that.
            runCatching { FrameDecoder.decodeStreaming(ByteReadChannel(bytes)) { received.add(it) } }

            assertEquals(3, received.size)
            assertEquals("a", (received[0].payload as Frame.Body).body.decodeToString())
            assertTrue(received[1].payload is Frame.Heartbeat)
            assertEquals("bb", (received[2].payload as Frame.Body).body.decodeToString())
        }
    }

    // NEW-25: a frame split across reads (part of it arrives, then the rest) must not yield a frame
    // until complete, then decode exactly once. This is the correctness-critical path of the rewrite
    // (the peek/request "wait for more" branch).
    @Test
    fun testDecodesFrameSplitAcrossReads() = runTest {
        withContext(Dispatchers.Default) {
            val frame = encode(Frame(channelId = 1u, payload = Frame.Body("hello world".encodeToByteArray())))
            val channel = ByteChannel(autoFlush = true)
            val received = mutableListOf<Frame>()
            val job = launch { runCatching { FrameDecoder.decodeStreaming(channel) { received.add(it) } } }

            // First chunk: only part of the frame (mid-payload). Must not yield anything yet.
            channel.writeByteArray(frame.copyOfRange(0, 5))
            channel.flush()
            delay(100)
            assertEquals(0, received.size, "a partial frame must not produce a delivery")

            // Remaining bytes complete the frame.
            channel.writeByteArray(frame.copyOfRange(5, frame.size))
            channel.flush()
            withTimeout(5000) { while (received.isEmpty()) delay(10) }

            channel.flushAndClose()
            withTimeout(5000) { job.join() }

            assertEquals(1, received.size)
            assertEquals("hello world", (received[0].payload as Frame.Body).body.decodeToString())
        }
    }

    // NEW-25: a frame split before its 7-byte header is complete (fewer than 7 bytes in the first
    // read) must not yield anything until the header — and then the rest — arrives.
    @Test
    fun testDecodesFrameSplitMidHeader() = runTest {
        withContext(Dispatchers.Default) {
            val frame = encode(Frame(channelId = 7u, payload = Frame.Body("hi".encodeToByteArray())))
            val channel = ByteChannel(autoFlush = true)
            val received = mutableListOf<Frame>()
            val job = launch { runCatching { FrameDecoder.decodeStreaming(channel) { received.add(it) } } }

            channel.writeByteArray(frame.copyOfRange(0, 3)) // fewer than the 7-byte header
            channel.flush()
            delay(100)
            assertEquals(0, received.size, "a partial header must not produce a delivery")

            channel.writeByteArray(frame.copyOfRange(3, frame.size))
            channel.flush()
            withTimeout(5000) { while (received.isEmpty()) delay(10) }

            channel.flushAndClose()
            withTimeout(5000) { job.join() }

            assertEquals(1, received.size)
            assertEquals("hi", (received[0].payload as Frame.Body).body.decodeToString())
        }
    }

    // NEW-25 / NEW-1: a frame advertising a size larger than maxFrameSize must be rejected up front
    // (ProtocolError.Invalid) rather than buffered toward, so the connection closes instead of OOMing.
    @Test
    fun testRejectsOversizedFrame() = runTest {
        withContext(Dispatchers.Default) {
            val frame = encode(Frame(channelId = 1u, payload = Frame.Body(ByteArray(100))))
            assertFailsWith<ProtocolError.Invalid> {
                FrameDecoder.decodeStreaming(ByteReadChannel(frame), maxFrameSize = { 50L }) { }
            }
        }
    }
}
