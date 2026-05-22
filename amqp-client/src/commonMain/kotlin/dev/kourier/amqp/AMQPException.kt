package dev.kourier.amqp

import kotlinx.io.IOException
import kotlin.time.Duration

sealed class AMQPException : IOException() {

    /**
     * A channel-level RPC (queueDeclare, basicGet, confirmSelect, …) did not receive a response
     * within the configured `rpcTimeout`. Prevents a stalled/black-holed broker from suspending
     * the caller forever. Not a [kotlinx.coroutines.CancellationException], so robust recovery
     * treats it as a normal failure.
     */
    data class RpcTimeout(
        val channelId: ChannelId,
        val timeout: Duration,
    ) : AMQPException() {
        override val message: String get() = "RPC on channel $channelId timed out after $timeout"
    }

    data object InvalidUrl : AMQPException()

    data object InvalidUrlScheme : AMQPException()

    data class ConnectionClosed(
        val replyCode: UShort? = null,
        val replyText: String? = null,
        val isInitiatedByApplication: Boolean = false,
    ) : AMQPException()

    data class ConnectionClose(val broker: Throwable? = null, val connection: Throwable? = null) : AMQPException()

    data object ConnectionBlocked : AMQPException()

    data class ChannelClosed(
        val replyCode: UShort? = null,
        val replyText: String? = null,
        val isInitiatedByApplication: Boolean = false,
    ) : AMQPException()

    data object TooManyOpenedChannels : AMQPException()

    data object ChannelNotInConfirmMode : AMQPException()

    data object ConsumerCancelled : AMQPException()

    data object ConsumerAlreadyCancelled : AMQPException()

    data object InvalidMessage : AMQPException()

    data class InvalidResponse(val response: AMQPResponse) : AMQPException()

}
