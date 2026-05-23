package dev.kourier.amqp.connection

import dev.kourier.amqp.Frame
import dev.kourier.amqp.ProtocolError
import dev.kourier.amqp.serialization.ProtocolBinaryDecoder
import dev.kourier.amqp.serialization.serializers.frame.FrameSerializer
import io.ktor.utils.io.*
import kotlinx.io.Buffer
import kotlinx.io.EOFException

object FrameDecoder {

    // Fixed frame prefix on the wire: kind (1) + channelId (2) + size (4).
    private const val FRAME_HEADER_SIZE = 7L

    suspend fun decodeStreaming(
        channel: ByteReadChannel,
        // Resolved per decode attempt so it picks up the broker-negotiated frameMax after Tune.
        // Bounds the size a single frame may advertise.
        maxFrameSize: () -> Long = { Long.MAX_VALUE },
        onItem: suspend (Frame) -> Unit,
    ) {
        val buffer = Buffer()
        val temp = ByteArray(4096)

        while (!channel.isClosedForRead) {
            val read = channel.readAvailable(temp)
            if (read == -1) break

            // Append new data to the persistent buffer.
            buffer.write(temp, 0, read)

            // Decode every whole frame currently buffered. We peek the fixed-size header to learn
            // the frame's declared length WITHOUT consuming, decide whether the full frame has
            // arrived, and only then decode destructively from `buffer`. This avoids the previous
            // approach, which snapshotted and re-copied the entire pending buffer on every decode
            // attempt — O(n^2) in the number of frames packed into one read.
            while (true) {
                val header = buffer.peek()
                if (!header.request(FRAME_HEADER_SIZE)) break // not enough for the header yet
                header.skip(3) // kind (1) + channelId (2)
                val size = header.readInt().toUInt()

                // Reject an oversized frame as soon as the header is readable, so a peer can't make
                // us buffer toward a giant (or never-arriving) payload. Mirrors FrameSerializer's
                // bound; enforcing it here is what actually prevents unbounded accumulation.
                val max = maxFrameSize()
                if (size.toLong() > max) {
                    throw ProtocolError.Invalid(size, "Frame size $size exceeds maximum $max", this)
                }

                // Whole frame = header (7) + payload (size) + end marker (1).
                val frameSize = FRAME_HEADER_SIZE + size.toLong() + 1L
                if (!buffer.request(frameSize)) break // full frame not buffered yet

                // The full frame's bytes are buffered, so a well-formed frame won't underflow here.
                // (A structurally-malformed frame can still throw, which propagates and closes the
                // connection — same as before; the end-marker check guards that case.)
                val value = ProtocolBinaryDecoder(buffer, max).decodeSerializableValue(FrameSerializer)
                onItem(value)
            }
        }
        throw channel.closedCause ?: EOFException("Channel closed without completing frame decoding")
    }

}
