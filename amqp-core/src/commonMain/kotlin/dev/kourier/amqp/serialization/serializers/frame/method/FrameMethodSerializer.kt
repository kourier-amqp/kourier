package dev.kourier.amqp.serialization.serializers.frame.method

import dev.kourier.amqp.Frame
import dev.kourier.amqp.serialization.serializers.frame.method.basic.FrameMethodBasicSerializer
import dev.kourier.amqp.serialization.serializers.frame.method.channel.FrameMethodChannelSerializer
import dev.kourier.amqp.serialization.serializers.frame.method.confirm.FrameMethodConfirmSerializer
import dev.kourier.amqp.serialization.serializers.frame.method.connection.FrameMethodConnectionSerializer
import dev.kourier.amqp.serialization.serializers.frame.method.exchange.FrameMethodExchangeSerializer
import dev.kourier.amqp.serialization.serializers.frame.method.queue.FrameMethodQueueSerializer
import dev.kourier.amqp.serialization.serializers.frame.method.tx.FrameMethodTxSerializer
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.descriptors.buildSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder

object FrameMethodSerializer : KSerializer<Frame.Method> {

    // Precomputed value -> Kind lookup (NEW-28): avoids a linear scan + lambda allocation per
    // method frame decoded. getValue() preserves the old `entries.first { }` failure behavior.
    private val kindByValue = Frame.Method.Kind.entries.associateBy { it.value }

    @OptIn(InternalSerializationApi::class)
    override val descriptor: SerialDescriptor
        get() = buildSerialDescriptor("Frame.Method", StructureKind.OBJECT)

    override fun serialize(encoder: Encoder, value: Frame.Method) {
        encoder.encodeShort(value.kind.value.toShort())
        when (value) {
            is Frame.Method.Connection -> encoder.encodeSerializableValue(FrameMethodConnectionSerializer, value)
            is Frame.Method.Channel -> encoder.encodeSerializableValue(FrameMethodChannelSerializer, value)
            is Frame.Method.Exchange -> encoder.encodeSerializableValue(FrameMethodExchangeSerializer, value)
            is Frame.Method.Queue -> encoder.encodeSerializableValue(FrameMethodQueueSerializer, value)
            is Frame.Method.Basic -> encoder.encodeSerializableValue(FrameMethodBasicSerializer, value)
            is Frame.Method.Confirm -> encoder.encodeSerializableValue(FrameMethodConfirmSerializer, value)
            is Frame.Method.Tx -> encoder.encodeSerializableValue(FrameMethodTxSerializer, value)
        }
    }

    override fun deserialize(decoder: Decoder): Frame.Method {
        val kind = kindByValue.getValue(decoder.decodeShort().toUShort())
        return when (kind) {
            Frame.Method.Kind.CONNECTION -> decoder.decodeSerializableValue(FrameMethodConnectionSerializer)
            Frame.Method.Kind.CHANNEL -> decoder.decodeSerializableValue(FrameMethodChannelSerializer)
            Frame.Method.Kind.EXCHANGE -> decoder.decodeSerializableValue(FrameMethodExchangeSerializer)
            Frame.Method.Kind.QUEUE -> decoder.decodeSerializableValue(FrameMethodQueueSerializer)
            Frame.Method.Kind.BASIC -> decoder.decodeSerializableValue(FrameMethodBasicSerializer)
            Frame.Method.Kind.CONFIRM -> decoder.decodeSerializableValue(FrameMethodConfirmSerializer)
            Frame.Method.Kind.TX -> decoder.decodeSerializableValue(FrameMethodTxSerializer)
        }
    }

}
