package dev.kourier.amqp

sealed class AMQPResponse {

    sealed class Channel : AMQPResponse() {

        data class Opened(val channelId: ChannelId) : Channel()

        data class Closed(
            val channelId: ChannelId,
            val replyCode: UShort? = null,
            val replyText: String? = null,
        ) : Channel()

        sealed class Message : Channel() {

            data class Delivery(
                val message: AMQPMessage,
                val consumerTag: String,
            ) : Message()

            data class Get(
                val message: AMQPMessage? = null,
                val messageCount: UInt = 0u,
            ) : Message()

            data class Return(
                val replyCode: UShort,
                val replyText: String,
                val exchange: String,
                val routingKey: String,
                val properties: Properties,
                val body: ByteArray,
            ) : Message()

        }

        sealed class Queue : Channel() {

            data class Declared(
                val queueName: String,
                val messageCount: UInt,
                val consumerCount: UInt,
            ) : Queue()

            data object Bound : Queue()
            data class Purged(val messageCount: UInt) : Queue()
            data class Deleted(val messageCount: UInt) : Queue()
            data object Unbound : Queue()

        }

        sealed class Exchange : Channel() {

            data object Declared : Exchange()
            data object Deleted : Exchange()
            data object Bound : Exchange()
            data object Unbound : Exchange()

        }

        sealed class Basic : Channel() {

            data object Recovered : Basic()
            data object QosOk : Basic()
            data class ConsumeOk(val consumerTag: String) : Basic()
            data class Canceled(val consumerTag: String) : Basic()

            sealed class PublishConfirm : Basic() {

                abstract val deliveryTag: ULong
                abstract val multiple: Boolean

                data class Ack(
                    override val deliveryTag: ULong,
                    override val multiple: Boolean,
                ) : PublishConfirm()

                data class Nack(
                    override val deliveryTag: ULong,
                    override val multiple: Boolean,
                ) : PublishConfirm()

            }

            data class Published(val deliveryTag: ULong) : Basic()

        }

        sealed class Confirm : Channel() {

            data object Selected : Confirm()

        }

        sealed class Tx : Channel() {

            data object Selected : Tx()
            data object Committed : Tx()
            data object Rollbacked : Tx()

        }

        data class Flowed(val active: Boolean) : Channel()

    }

    sealed class Connection : AMQPResponse() {

        data class Connected(val channelMax: UShort, val frameMax: UInt) : Connection()
        data object Closed : Connection()
        data class Blocked(val reason: String) : Connection()
        data object Unblocked : Connection()

    }

}
