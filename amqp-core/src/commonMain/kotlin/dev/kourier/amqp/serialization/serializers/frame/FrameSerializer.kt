package dev.kourier.amqp.serialization.serializers.frame

import dev.kourier.amqp.Frame
import dev.kourier.amqp.ProtocolError
import dev.kourier.amqp.serialization.ProtocolBinaryDecoder
import dev.kourier.amqp.serialization.ProtocolBinaryEncoder
import dev.kourier.amqp.serialization.serializers.frame.method.FrameMethodSerializer
import kotlinx.io.Buffer
import kotlinx.io.readByteArray
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.descriptors.buildSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder

object FrameSerializer : KSerializer<Frame> {

    @OptIn(InternalSerializationApi::class)
    override val descriptor: SerialDescriptor
        get() = buildSerialDescriptor("Frame", StructureKind.OBJECT)

    override fun serialize(encoder: Encoder, value: Frame) {
        require(encoder is ProtocolBinaryEncoder)

        encoder.encodeByte(value.kind.value.toByte())
        encoder.encodeShort(value.channelId.toShort())

        when (val payload = value.payload) {
            is Frame.Method -> {
                val innerEncoder = ProtocolBinaryEncoder(Buffer())
                innerEncoder.encodeSerializableValue(FrameMethodSerializer, payload)
                encoder.encodeInt(innerEncoder.buffer.size.toInt())
                innerEncoder.buffer.copyTo(encoder.buffer)
            }

            is Frame.Header -> {
                val innerEncoder = ProtocolBinaryEncoder(Buffer())
                innerEncoder.encodeSerializableValue(FrameHeaderSerializer, payload)
                encoder.encodeInt(innerEncoder.buffer.size.toInt())
                innerEncoder.buffer.copyTo(encoder.buffer)
            }

            is Frame.Body -> {
                val size = payload.body.size
                encoder.encodeInt(size)
                encoder.buffer.write(payload.body)
            }

            is Frame.Heartbeat -> {
                val size = 0
                encoder.encodeInt(size)
            }
        }

        encoder.encodeByte(206.toByte()) // endMarker
    }

    override fun deserialize(decoder: Decoder): Frame {
        require(decoder is ProtocolBinaryDecoder)

        val kind = decoder.decodeByte().toUByte().let { byte ->
            Frame.Kind.entries.first { it.value == byte }
        }
        val channelId = decoder.decodeShort().toUShort()
        // The wire size is an unsigned 32-bit int. Validate it BEFORE allocating/reading: an
        // out-of-range size (a malicious 4 GiB advertisement, or a value whose high bit makes
        // size.toInt() wrap negative) would otherwise drive a giant allocation or an undefined
        // read. Reject anything larger than the negotiated frameMax. Throwing ProtocolError.Invalid
        // (not Incomplete) makes the streaming decoder propagate it and close the connection.
        val size = decoder.decodeInt().toUInt()
        if (size.toLong() > decoder.maxFrameSize) {
            throw ProtocolError.Invalid(size, "Frame size $size exceeds maximum ${decoder.maxFrameSize}", this)
        }

        // Method/Header frames decode *structurally*, not by raw byte count, so a `size` that
        // disagrees with the real payload length would otherwise leave the stream desynced (and the
        // end-marker check at a shifted offset). Measure what the payload decode actually consumed
        // and require it to equal the declared size. (BODY consumes exactly `size` by construction;
        // HEARTBEAT consumes 0, so this also enforces a 0-size heartbeat.)
        val remainingBefore = decoder.buffer.size
        val result = when (kind) {
            Frame.Kind.METHOD -> {
                val payload = decoder.decodeSerializableValue(FrameMethodSerializer)
                Frame(channelId, payload)
            }

            Frame.Kind.HEADER -> {
                val payload = decoder.decodeSerializableValue(FrameHeaderSerializer)
                Frame(channelId, payload)
            }

            Frame.Kind.BODY -> {
                val body = decoder.buffer.readByteArray(size.toInt())
                Frame(channelId, Frame.Body(body))
            }

            Frame.Kind.HEARTBEAT -> Frame(channelId, Frame.Heartbeat)
        }
        val consumed = remainingBefore - decoder.buffer.size
        if (consumed != size.toLong()) {
            throw ProtocolError.Invalid(size, "Frame payload consumed $consumed bytes but declared size is $size", this)
        }

        val endMarker = decoder.decodeByte().toUByte()
        if (endMarker.toInt() != 206) throw ProtocolError.Invalid(endMarker, "Invalid end marker: $endMarker", this)

        return result
    }

}
