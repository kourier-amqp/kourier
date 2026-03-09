package dev.kourier.amqp.connection

import dev.kourier.amqp.AMQPException
import dev.kourier.amqp.AMQPResponse
import dev.kourier.amqp.Frame
import dev.kourier.amqp.InternalAmqpApi
import dev.kourier.amqp.channel.AMQPChannel
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.flow.Flow

interface AMQPConnection {

    /**
     * The configuration of the connection.
     */
    val config: AMQPConfig

    /**
     * The connection state.
     */
    val state: ConnectionState

    /**
     * A deferred that completes when the connection is opened.
     */
    val connectionOpened: Deferred<Unit>

    /**
     * A deferred that completes when the connection is closed.
     */
    val connectionClosed: Deferred<AMQPException.ConnectionClosed>

    /**
     * A flow of opened responses from the connection.
     */
    val openedResponses: Flow<AMQPResponse.Connection.Connected>

    /**
     * A flow of closed responses from the connection.
     */
    val closedResponses: Flow<AMQPResponse.Connection.Closed>

    /**
     * A flow of blocked responses from the connection.
     * Emitted when the broker signals the connection is resource-constrained.
     */
    val blockedResponses: Flow<AMQPResponse.Connection.Blocked>

    /**
     * A flow of unblocked responses from the connection.
     * Emitted when the broker signals the connection is no longer resource-constrained.
     */
    val unblockedResponses: Flow<AMQPResponse.Connection.Unblocked>

    /**
     * Internal API to write raw bytes to the connection.
     */
    @InternalAmqpApi
    suspend fun write(bytes: ByteArray)

    /**
     * Internal API to write raw frames to the connection.
     */
    @InternalAmqpApi
    suspend fun write(vararg frames: Frame)

    /**
     * Opens a new channel.
     *
     * Can be used only when the connection is connected.
     * The channel ID is automatically assigned (next free one).
     *
     * @return the opened [AMQPChannel]
     */
    suspend fun openChannel(): AMQPChannel

    /**
     * Sends a heartbeat frame.
     */
    suspend fun sendHeartbeat()

    /**
     * Closes the connection.
     *
     * @param reason Reason that can be logged by the broker.
     * @param code Code that can be logged by the broker.
     *
     * @return Nothing. The connection is closed synchronously.
     */
    suspend fun close(
        reason: String = "",
        code: UShort = 200u,
    ): AMQPResponse.Connection.Closed

}
