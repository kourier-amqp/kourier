package dev.kourier.amqp.robust

import dev.kourier.amqp.ChannelId
import kotlin.time.Duration

/**
 * Thrown when [RobustAMQPChannel.restore] exceeds the configured restore timeout.
 *
 * Extends [RuntimeException] (not [kotlinx.coroutines.CancellationException]) so the
 * recovery loop in `RobustAMQPConnection.connectionFactory()` catches it as a regular
 * exception, logs it, and iterates rather than cancelling the recovery coroutine.
 */
class RestoreTimeoutException(
    val channelId: ChannelId,
    val timeout: Duration,
    cause: Throwable? = null,
) : RuntimeException("Channel $channelId restore timed out after $timeout", cause)
