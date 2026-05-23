package dev.kourier.amqp.channel

import dev.kourier.amqp.AMQPMessage
import dev.kourier.amqp.AMQPResponse
import dev.kourier.amqp.Frame
import dev.kourier.amqp.Properties
import dev.kourier.amqp.ProtocolError
import io.ktor.util.logging.*

data class PartialDelivery(
    val method: Frame.Method.Basic,
) {

    private val logger = KtorSimpleLogger("PartialDelivery")

    private var header: Frame.Header? = null
    private var payload: ByteArray? = null
    private var written: Int = 0

    val isComplete: Boolean
        get() = header != null && written.toULong() == header!!.bodySize

    fun setHeader(header: Frame.Header) {
        if (this.header != null) error("Header already set")
        logger.debug("Setting PartialDelivery header: $header")
        this.header = header
        // Pre-allocate the full body once. The old approach reallocated + copied the whole payload
        // on every body frame, i.e. O(n^2) reassembly for a message split across many frames.
        // No artificial size cap is imposed: the body comes from a trusted broker we negotiated
        // with, and capping it would arbitrarily limit large legitimate messages.
        this.payload = ByteArray(header.bodySize.toInt())
    }

    fun addBody(buffer: ByteArray) {
        val payload = this.payload ?: error("Header must be set before adding body")
        // Reject a body that overruns the size the header declared, rather than silently growing.
        if (written + buffer.size > payload.size) {
            throw ProtocolError.Invalid(
                buffer.size,
                "Body overruns declared message size ${payload.size} (have $written, +${buffer.size})",
                this,
            )
        }
        logger.debug("Appending ${buffer.size} bytes to PartialDelivery body ($written/${payload.size})")
        buffer.copyInto(payload, written)
        written += buffer.size
    }

    fun asCompletedMessage(): Triple<Frame.Method.Basic, Properties, ByteArray> {
        // NOTE: this could be made a consuming func once partial is possible I think
        check(isComplete)

        // header and payloads are guaranteed to be non-null after isComplete
        return Triple(method, header!!.properties, payload ?: ByteArray(0))
    }

    suspend fun emitOnChannel(channel: DefaultAMQPChannel) {
        logger.debug("Emitting completed PartialDelivery on channel ${channel.id}")
        val (method, properties, completeBody) = asCompletedMessage()
        channel.nextMessage = null

        when (method) {
            is Frame.Method.Basic.GetOk -> channel.channelResponses.emit(
                AMQPResponse.Channel.Message.Get(
                    message = AMQPMessage(
                        exchange = method.exchange,
                        routingKey = method.routingKey,
                        deliveryTag = method.deliveryTag,
                        properties = properties,
                        redelivered = method.redelivered,
                        body = completeBody
                    ),
                    messageCount = method.messageCount
                )
            )

            is Frame.Method.Basic.Deliver -> channel.channelResponses.emit(
                AMQPResponse.Channel.Message.Delivery(
                    message = AMQPMessage(
                        exchange = method.exchange,
                        routingKey = method.routingKey,
                        deliveryTag = method.deliveryTag,
                        properties = properties,
                        redelivered = method.redelivered,
                        body = completeBody
                    ),
                    consumerTag = method.consumerTag
                ),
            )

            is Frame.Method.Basic.Return -> channel.channelResponses.emit(
                AMQPResponse.Channel.Message.Return(
                    replyCode = method.replyCode,
                    replyText = method.replyText,
                    exchange = method.exchange,
                    routingKey = method.routingKey,
                    properties = properties,
                    body = completeBody
                )
            )

            else -> error("Unexpected frame: $method")
        }
    }

}
