package dev.kourier.amqp.connection

import dev.kourier.amqp.Frame
import dev.kourier.amqp.ProtocolError
import dev.kourier.amqp.serialization.ProtocolBinaryDecoder
import dev.kourier.amqp.serialization.serializers.frame.FrameSerializer
import io.ktor.utils.io.*
import io.ktor.utils.io.core.*
import kotlinx.io.Buffer
import kotlinx.io.EOFException

object FrameDecoder {

    suspend fun decodeStreaming(
        channel: ByteReadChannel,
        // Resolved per decode attempt so it picks up the broker-negotiated frameMax after Tune
        // (a fresh decoder is created each iteration). Bounds the size a single frame may advertise.
        maxFrameSize: () -> Long = { Long.MAX_VALUE },
        onItem: suspend (Frame) -> Unit,
    ) {
        var buffer = Buffer()
        val temp = ByteArray(4096)

        while (!channel.isClosedForRead) {
            val read = channel.readAvailable(temp)
            if (read == -1) break

            // Append new data to buffer
            buffer.write(temp, 0, read)

            // Keep trying to decode as long as something is parsable
            while (true) {
                val snapshot = buffer.readBytes()
                val workingBuffer = Buffer().apply { write(snapshot) }

                val decoder = ProtocolBinaryDecoder(workingBuffer, maxFrameSize())

                try {
                    val value = decoder.decodeSerializableValue(FrameSerializer)
                    onItem(value)

                    // Remove consumed bytes
                    val consumed = snapshot.size - decoder.buffer.size.toInt()
                    val remaining = snapshot.copyOfRange(consumed, snapshot.size)

                    buffer = Buffer().apply { write(remaining) }
                } catch (e: Exception) {
                    if (e is ProtocolError.Incomplete || e is EOFException) {
                        // not enough data: restore full buffer
                        buffer = Buffer().apply { write(snapshot) }
                        break
                    }
                    throw e
                }
            }
        }
        throw channel.closedCause ?: EOFException("Channel closed without completing frame decoding")
    }

}
