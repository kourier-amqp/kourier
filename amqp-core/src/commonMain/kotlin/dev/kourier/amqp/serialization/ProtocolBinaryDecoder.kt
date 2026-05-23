package dev.kourier.amqp.serialization

import kotlinx.io.Buffer
import kotlinx.io.readDouble
import kotlinx.io.readFloat
import kotlinx.io.readString
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.modules.EmptySerializersModule
import kotlinx.serialization.modules.SerializersModule

class ProtocolBinaryDecoder(
    val buffer: Buffer,
    // Upper bound (bytes) for a single frame's declared payload size. FrameSerializer rejects any
    // frame advertising a larger size before allocating, so a malicious/buggy peer can't drive an
    // unbounded ByteArray allocation (DoS/OOM). Default is unlimited for non-frame decoding.
    val maxFrameSize: Long = Long.MAX_VALUE,
) : Decoder {

    override val serializersModule: SerializersModule = EmptySerializersModule()

    @ExperimentalSerializationApi
    override fun decodeNotNullMark(): Boolean {
        return true
    }

    @ExperimentalSerializationApi
    override fun decodeNull(): Nothing? {
        return null
    }

    override fun decodeBoolean(): Boolean {
        return buffer.readByte().toInt() != 0
    }

    override fun decodeByte(): Byte {
        return buffer.readByte()
    }

    override fun decodeShort(): Short {
        return buffer.readShort()
    }

    override fun decodeChar(): Char {
        return buffer.readShort().toInt().toChar()
    }

    override fun decodeInt(): Int {
        return buffer.readInt()
    }

    override fun decodeLong(): Long {
        return buffer.readLong()
    }

    override fun decodeFloat(): Float {
        return buffer.readFloat()
    }

    override fun decodeDouble(): Double {
        return buffer.readDouble()
    }

    override fun decodeString(): String {
        throw UnsupportedOperationException("decodeString is not implemented. Use decodeShortString or decodeLongString instead.")
    }

    fun decodeShortString(): Pair<String, Int> {
        val size = buffer.readByte().toUByte().toInt()
        val value = buffer.readString(size.toLong())
        return Pair(value, 1 + size)
    }

    fun decodeLongString(): Pair<String, Int> {
        val size = buffer.readInt()
        val value = buffer.readString(size.toLong())
        return Pair(value, 4 + size)
    }

    override fun decodeEnum(enumDescriptor: SerialDescriptor): Int {
        throw UnsupportedOperationException()
    }

    override fun decodeInline(descriptor: SerialDescriptor): Decoder {
        throw UnsupportedOperationException()
    }

    override fun beginStructure(descriptor: SerialDescriptor): CompositeDecoder {
        throw UnsupportedOperationException()
    }

}
