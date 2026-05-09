package dev.kourier.amqp.robust

import dev.kourier.amqp.ChannelId
import kotlinx.io.IOException
import kotlin.time.Duration

/**
 * Thrown when [RobustAMQPChannel.restore] exceeds the configured restore timeout.
 *
 * Extends [IOException] (matching the rest of the AMQP exception hierarchy via
 * `AMQPException : IOException()`) so user code that catches `IOException` around
 * `basicPublish(...)` and similar calls catches this as well. `kotlinx.io.IOException`
 * does NOT extend [kotlinx.coroutines.CancellationException], so the recovery loop in
 * `RobustAMQPConnection.connectionFactory()` still catches it via `catch (e: Exception)`,
 * logs it, and iterates rather than cancelling the recovery coroutine.
 */
class RestoreTimeoutException(
    val channelId: ChannelId,
    val timeout: Duration,
    cause: Throwable? = null,
) : IOException("Channel $channelId restore timed out after $timeout", cause)
