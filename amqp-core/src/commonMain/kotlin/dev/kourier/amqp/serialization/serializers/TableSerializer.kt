package dev.kourier.amqp.serialization.serializers

import dev.kourier.amqp.Field
import dev.kourier.amqp.ProtocolError
import dev.kourier.amqp.Table
import dev.kourier.amqp.serialization.ProtocolBinaryDecoder
import dev.kourier.amqp.serialization.ProtocolBinaryEncoder
import kotlinx.io.Buffer
import kotlinx.io.readByteArray
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.descriptors.buildSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlin.time.Instant

object TableSerializer : KSerializer<Table> {

    // Precomputed value -> Field.Kind lookup (NEW-28): avoids a linear scan + lambda allocation
    // per table field decoded. getValue() preserves the old `entries.first { }` failure behavior.
    private val fieldKindByValue = Field.Kind.entries.associateBy { it.value }

    @OptIn(InternalSerializationApi::class)
    override val descriptor: SerialDescriptor
        get() = buildSerialDescriptor("Table", StructureKind.OBJECT)

    override fun serialize(encoder: Encoder, value: Table) {
        require(encoder is ProtocolBinaryEncoder)

        val innerEncoder = ProtocolBinaryEncoder(Buffer())
        for ((key, field) in value) {
            try {
                innerEncoder.encodeShortString(key)
            } catch (e: Exception) {
                throw ProtocolError.Encode(key, null, "Cannot write table key: $key", e)
            }
            try {
                writeField(field, innerEncoder)
            } catch (e: Exception) {
                throw ProtocolError.Encode(field, null, "Cannot write table value of key: $key", e)
            }
        }
        encoder.encodeInt(innerEncoder.buffer.size.toInt())
        encoder.buffer.transferFrom(innerEncoder.buffer)
    }

    fun writeArray(values: List<Field>, encoder: ProtocolBinaryEncoder) {
        val innerEncoder = ProtocolBinaryEncoder(Buffer())
        for (value in values) {
            try {
                writeField(value, innerEncoder)
            } catch (e: Exception) {
                throw ProtocolError.Encode(value, null, "Cannot write array value: $value", e)
            }
        }
        encoder.encodeInt(innerEncoder.buffer.size.toInt())
        encoder.buffer.transferFrom(innerEncoder.buffer)
    }

    @OptIn(ExperimentalSerializationApi::class)
    fun writeField(field: Field, encoder: ProtocolBinaryEncoder) {
        encoder.encodeByte(field.kind.value.toByte())
        when (field) {
            is Field.Boolean -> encoder.encodeByte(if (field.value) 1 else 0)
            is Field.Byte -> encoder.encodeByte(field.value)
            is Field.UByte -> encoder.encodeByte(field.value.toByte())
            is Field.Short -> encoder.encodeShort(field.value)
            is Field.UShort -> encoder.encodeShort(field.value.toShort())
            is Field.Int -> encoder.encodeInt(field.value)
            is Field.UInt -> encoder.encodeInt(field.value.toInt())
            is Field.Long -> encoder.encodeLong(field.value)
            is Field.Float -> encoder.encodeFloat(field.value)
            is Field.Double -> encoder.encodeDouble(field.value)
            is Field.LongString -> encoder.encodeLongString(field.value)
            is Field.Bytes -> {
                encoder.encodeInt(field.value.size)
                encoder.buffer.write(field.value)
            }
            is Field.Array -> writeArray(field.value, encoder)
            is Field.Timestamp -> encoder.encodeLong(field.value.toEpochMilliseconds())
            is Field.Table -> encoder.encodeSerializableValue(TableSerializer, field.value)

            is Field.Decimal -> {
                encoder.encodeByte(field.scale.toByte())
                encoder.encodeInt(field.value.toInt())
            }

            is Field.Null -> encoder.encodeNull()
        }
    }

    override fun deserialize(decoder: Decoder): Table {
        require(decoder is ProtocolBinaryDecoder)

        val (table, _) = readTable(decoder)
        return table
    }

    private fun readTable(decoder: ProtocolBinaryDecoder): Pair<Table, Int> {
        val size = decoder.decodeInt()
        val result = mutableMapOf<String, Field>()
        var bytesRead = 0

        while (bytesRead < size) {
            val (key, keySize) = decoder.decodeShortString()
            bytesRead += keySize

            val (value, valueSize) = readField(decoder)
            bytesRead += valueSize

            result[key] = value
        }
        return Pair(result, 4 + bytesRead)
    }

    private fun readArray(decoder: ProtocolBinaryDecoder): Pair<List<Field>, Int> {
        val size = decoder.decodeInt()
        val result = mutableListOf<Field>()
        var bytesRead = 0

        while (bytesRead < size) {
            val (value, valueSize) = readField(decoder)
            bytesRead += valueSize
            result.add(value)
        }

        return Pair(result, 4 + bytesRead)
    }

    private fun readField(decoder: ProtocolBinaryDecoder): Pair<Field, Int> {
        val kind = fieldKindByValue.getValue(decoder.decodeByte().toUByte())
        return when (kind) {
            Field.Kind.BOOLEAN -> {
                val value = decoder.decodeBoolean()
                Pair(Field.Boolean(value), 1 + 1)
            }

            Field.Kind.BYTE -> {
                val value = decoder.decodeByte()
                Pair(Field.Byte(value), 1 + 1)
            }

            Field.Kind.UBYTE -> {
                val value = decoder.decodeByte().toUByte()
                Pair(Field.UByte(value), 1 + 1)
            }

            Field.Kind.SHORT -> {
                val value = decoder.decodeShort()
                Pair(Field.Short(value), 1 + 2)
            }

            Field.Kind.USHORT -> {
                val value = decoder.decodeShort().toUShort()
                Pair(Field.UShort(value), 1 + 2)
            }

            Field.Kind.INT -> {
                val value = decoder.decodeInt()
                Pair(Field.Int(value), 1 + 4)
            }

            Field.Kind.UINT -> {
                val value = decoder.decodeInt().toUInt()
                Pair(Field.UInt(value), 1 + 4)
            }

            Field.Kind.LONG -> {
                val value = decoder.decodeLong()
                Pair(Field.Long(value), 1 + 8)
            }

            Field.Kind.FLOAT -> {
                val value = decoder.decodeFloat()
                Pair(Field.Float(value), 1 + 4)
            }

            Field.Kind.DOUBLE -> {
                val value = decoder.decodeDouble()
                Pair(Field.Double(value), 1 + 8)
            }

            Field.Kind.LONG_STRING -> {
                val (value, valueSize) = decoder.decodeLongString()
                Pair(Field.LongString(value), 1 + valueSize)
            }

            Field.Kind.BYTES -> {
                val size = decoder.decodeInt()
                val value = decoder.buffer.readByteArray(size)
                Pair(Field.Bytes(value), 1 + 4 + size)
            }

            Field.Kind.ARRAY -> {
                val (value, valueSize) = readArray(decoder)
                Pair(Field.Array(value), 1 + valueSize)
            }

            Field.Kind.TIMESTAMP -> {
                val timestamp = Instant.fromEpochMilliseconds(decoder.decodeLong())
                Pair(Field.Timestamp(timestamp), 1 + 8)
            }

            Field.Kind.TABLE -> {
                val (value, valueSize) = readTable(decoder)
                Pair(Field.Table(value), 1 + valueSize)
            }

            Field.Kind.DECIMAL -> {
                val scale = decoder.decodeByte().toUByte()
                val value = decoder.decodeInt().toUInt()
                Pair(Field.Decimal(scale, value), 1 + 1 + 4)
            }

            Field.Kind.NULL -> Pair(Field.Null, 1)
        }
    }

}
