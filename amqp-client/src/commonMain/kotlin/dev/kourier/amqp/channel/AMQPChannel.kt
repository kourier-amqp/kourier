package dev.kourier.amqp.channel

import dev.kourier.amqp.*
import dev.kourier.amqp.connection.ConnectionState
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.flow.Flow

interface AMQPChannel {

    /**
     * Unique identifier of the channel.
     */
    val id: ChannelId

    /**
     * The channel connection state.
     */
    val state: ConnectionState

    /**
     * A deferred that completes when the channel is closed.
     */
    val channelClosed: Deferred<AMQPException.ChannelClosed>

    /**
     * A flow of opened responses from the channel.
     */
    val openedResponses: Flow<AMQPResponse.Channel.Opened>

    /**
     * A flow of closed responses from the channel.
     */
    val closedResponses: Flow<AMQPResponse.Channel.Closed>

    /**
     * A flow of basic publish confirm responses.
     *
     * When channel is in confirm mode broker sends whether published message was accepted.
     */
    val publishConfirmResponses: Flow<AMQPResponse.Channel.Basic.PublishConfirm>

    /**
     * A flow of basic return responses.
     *
     * When broker cannot route message to any queue it sends a return message.
     */
    val returnResponses: Flow<AMQPResponse.Channel.Message.Return>

    /**
     * A flow of basic delivery messages.
     *
     * When broker cannot keep up with amount of published messages it sends a flow (false) message.
     * When broker is again ready to handle new messages it sends a flow (true) message.
     */
    val flowResponses: Flow<AMQPResponse.Channel.Flowed>

    /**
     * True when the channel is in confirm mode.
     */
    val isConfirmMode: Boolean

    /**
     * True when the channel is in transaction mode.
     */
    val isTxMode: Boolean

    /**
     * Internal API to write raw frames to the channel.
     */
    @InternalAmqpApi
    suspend fun write(vararg frames: Frame)

    /**
     * Opens the channel.
     *
     * This method is called automatically when the channel is created.
     * It is not necessary to call it manually.
     *
     * @return AMQPResponse.Channel.Opened confirming that broker has accepted the open request.
     */
    suspend fun open(): AMQPResponse.Channel.Opened

    /**
     * Closes the channel.
     *
     * @param reason Reason that can be logged by the broker.
     * @param code Code that can be logged by the broker.
     *
     * @return Nothing. The channel is closed synchronously.
     */
    suspend fun close(
        reason: String = "",
        code: UShort = 200u,
    ): AMQPResponse.Channel.Closed

    /**
     * Publish a ByteArray message to exchange or queue.
     *
     * @param body Message payload that can be read from ByteArray.
     * @param exchange Name of exchange on which the message is published. Can be empty.
     * @param routingKey Name of routingKey that will be attached to the message.
     *        An exchange looks at the routingKey while deciding how the message has to be routed.
     *        When exchange parameter is empty routingKey is used as queueName.
     * @param mandatory When a published message cannot be routed to any queue and mandatory is true, the message will be returned to publisher.
     *        Returned message must be handled with returnListener or returnConsumer.
     *        When a published message cannot be routed to any queue and mandatory is false, the message is discarded or republished to an alternate exchange, if any.
     * @param immediate When matching queue has at least one or more consumers and immediate is set to true, message is delivered to them immediately.
     *        When matching queue has zero active consumers and immediate is set to true, message is returned to publisher.
     *        When matching queue has zero active consumers and immediate is set to false, message will be delivered to the queue.
     * @param properties Additional message properties (check amqp documentation).
     *
     * @return DeliveryTag waiting for message write to the broker.
     *         DeliveryTag is 0 when channel is not in confirm mode.
     *         DeliveryTag is > 0 (monotonically increasing) when channel is in confirm mode.
     */
    suspend fun basicPublish(
        body: ByteArray,
        exchange: String,
        routingKey: String,
        mandatory: Boolean = false,
        immediate: Boolean = false,
        properties: Properties = Properties(),
    ): AMQPResponse.Channel.Basic.Published

    /**
     * Get a single message from a queue.
     *
     * @param queue Name of the queue.
     * @param noAck Controls whether message will be acked or nacked automatically (`true`) or manually (`false`).
     * @return AMQPResponse.Channel.Message.Get? when queue is not empty, otherwise null.
     * @deprecated EventLoopFuture based public API will be removed in first stable release, please use Async API.
     */
    suspend fun basicGet(
        queue: String,
        noAck: Boolean = false,
    ): AMQPResponse.Channel.Message.Get

    /**
     * Consume messages from a queue by sending them to the channel.
     * The consumer is automatically canceled when the channel is closed.
     *
     * @param queue Name of the queue.
     * @param consumerTag Name of the consumer; if empty, will be generated by the broker.
     * @param noAck Controls whether message will be acked or nacked automatically (`true`) or manually (`false`).
     * @param exclusive Ensures that queue can only be consumed by a single consumer.
     * @param arguments Additional arguments (check RabbitMQ documentation).
     *
     * @return AMQPResponse.Channel.Basic.ConsumeOk confirming that broker has accepted the consume request.
     */
    suspend fun basicConsume(
        queue: String,
        consumerTag: String = "",
        noAck: Boolean = false,
        exclusive: Boolean = false,
        arguments: Table = emptyMap(),
    ): AMQPReceiveChannel

    /**
     * Consume messages from a queue by sending them to registered consume listeners.
     * The caller is responsible for cancelling the consumer when done.
     *
     * @param queue Name of the queue.
     * @param consumerTag Name of the consumer; if empty, will be generated by the broker.
     * @param noAck Controls whether message will be acked or nacked automatically (`true`) or manually (`false`).
     * @param exclusive Ensures that queue can only be consumed by a single consumer.
     * @param arguments Additional arguments (check RabbitMQ documentation).
     * @param onDelivery: Callback when Delivery arrives - automatically registered.
     * @param onCanceled: Callback when consumer is canceled - automatically registered.
     *
     * @return AMQPResponse.Channel.Basic.ConsumeOk confirming that broker has accepted the consume request.
     */
    suspend fun basicConsume(
        queue: String,
        consumerTag: String = "",
        noAck: Boolean = false,
        exclusive: Boolean = false,
        arguments: Table = emptyMap(),
        onDelivery: suspend (AMQPResponse.Channel.Message.Delivery) -> Unit,
        onCanceled: suspend (AMQPResponse.Channel) -> Unit = {},
    ): AMQPResponse.Channel.Basic.ConsumeOk

    /**
     * Cancel sending messages from server to consumer.
     *
     * @param consumerTag Identifier of the consumer.
     *
     * @return AMQPResponse.Channel.Basic.Canceled confirming that broker has accepted the cancel request.
     */
    suspend fun basicCancel(
        consumerTag: String,
    ): AMQPResponse.Channel.Basic.Canceled

    /**
     * Acknowledge a message.
     *
     * @param deliveryTag Number (identifier) of the message.
     * @param multiple Controls whether only this message is acked (`false`) or additionally all others up to it (`true`).
     */
    suspend fun basicAck(
        deliveryTag: ULong,
        multiple: Boolean = false,
    )

    /**
     * Acknowledge a message.
     *
     * @param message Received message.
     * @param multiple Controls whether only this message is acked (`false`) or additionally all others up to it (`true`).
     */
    suspend fun basicAck(
        message: AMQPMessage,
        multiple: Boolean = false,
    )

    /**
     * Reject a message.
     *
     * @param deliveryTag Number (identifier) of the message.
     * @param multiple Controls whether only this message is rejected (`false`) or additionally all others up to it (`true`).
     * @param requeue Controls whether to requeue message after reject.
     */
    suspend fun basicNack(
        deliveryTag: ULong,
        multiple: Boolean = false,
        requeue: Boolean = false,
    )

    /**
     * Reject a message.
     *
     * @param message Received message.
     * @param multiple Controls whether only this message is rejected (`false`) or additionally all others up to it (`true`).
     * @param requeue Controls whether to requeue message after reject.
     */
    suspend fun basicNack(
        message: AMQPMessage,
        multiple: Boolean = false,
        requeue: Boolean = false,
    )

    /**
     * Reject a message.
     *
     * @param deliveryTag Number (identifier) of the message.
     * @param requeue Controls whether to requeue message after reject.
     */
    suspend fun basicReject(
        deliveryTag: ULong,
        requeue: Boolean = false,
    )

    /**
     * Reject a message.
     *
     * @param message Received Message.
     * @param requeue Controls whether to requeue message after reject.
     */
    suspend fun basicReject(
        message: AMQPMessage,
        requeue: Boolean = false,
    )

    /**
     * Tell the broker what to do with all unacknowledged messages.
     * Unacknowledged messages retrieved by `basicGet` are requeued regardless.
     *
     * @param requeue Controls whether to requeue all messages after rejecting them.
     * @return AMQPResponse.Channel.Basic.Recovered confirming that broker has accepted the recover request.
     */
    suspend fun basicRecover(
        requeue: Boolean = false,
    ): AMQPResponse.Channel.Basic.Recovered

    /**
     * Sets a prefetch limit when consuming messages.
     * No more messages will be delivered to the consumer until one or more messages have been acknowledged or rejected.
     *
     * @param count Size of the limit.
     * @param global Whether the limit will be shared across all consumers on the channel.
     *
     * @return AMQPResponse.Channel.Basic.QosOk confirming that broker has accepted the qos request.
     */
    suspend fun basicQos(
        count: UShort,
        global: Boolean = false,
    ): AMQPResponse.Channel.Basic.QosOk

    /**
     * Send a flow message to broker to start or stop sending messages to consumers.
     * Warning: Not supported by all brokers.
     *
     * @param active Flow enabled or disabled.
     *
     * @return AMQPResponse.Channel.Flowed confirming that broker has accepted the flow request.
     */
    suspend fun flow(active: Boolean): AMQPResponse.Channel.Flowed

    /**
     * Declares a queue.
     *
     * @param name Name of the queue.
     * @param durable If enabled, creates a queue stored on disk; otherwise, transient.
     * @param exclusive If enabled, queue will be deleted when the channel is closed.
     * @param autoDelete If enabled, queue will be deleted when the last consumer has stopped consuming.
     * @param arguments Additional arguments (check RabbitMQ documentation).
     *
     * @return AMQPResponse.Channel.Queue.Declared confirming that broker has accepted the request.
     */
    suspend fun queueDeclare(
        name: String,
        durable: Boolean = false,
        exclusive: Boolean = false,
        autoDelete: Boolean = false,
        arguments: Table = emptyMap(),
    ): AMQPResponse.Channel.Queue.Declared

    /**
     * Passively declares a queue.
     *
     * @param name Name of the queue.
     *
     * @return AMQPResponse.Channel.Queue.Declared confirming that broker has accepted the request.
     */
    suspend fun queueDeclarePassive(name: String): AMQPResponse.Channel.Queue.Declared

    /**
     * Declare a server-named exclusive, autodelete, non-durable queue.
     *
     * @return AMQPResponse.Channel.Queue.Declared confirming that broker has accepted the request.
     */
    suspend fun queueDeclare(): AMQPResponse.Channel.Queue.Declared

    /**
     * Returns the number of messages in a queue.
     *
     * @param name Name of the queue.
     *
     * @return Number of messages in the queue.
     */
    suspend fun messageCount(name: String): UInt

    /**
     * Returns the number of consumers subscribed to a queue.
     *
     * @param name Name of the queue.
     *
     * @return Number of consumers subscribed to the queue.
     */
    suspend fun consumerCount(name: String): UInt

    /**
     * Deletes a queue.
     *
     * @param name Name of the queue.
     * @param ifUnused If enabled, queue will be deleted only when there are no consumers subscribed to it.
     * @param ifEmpty If enabled, queue will be deleted only when it's empty.
     *
     * @return AMQPResponse.Channel.Queue.Deleted confirming that broker has accepted the delete request.
     */
    suspend fun queueDelete(
        name: String,
        ifUnused: Boolean = false,
        ifEmpty: Boolean = false,
    ): AMQPResponse.Channel.Queue.Deleted

    /**
     * Deletes all messages from a queue.
     *
     * @param name Name of the queue.
     *
     * @return AMQPResponse.Channel.Queue.Purged confirming that broker has accepted the delete request.
     */
    suspend fun queuePurge(
        name: String,
    ): AMQPResponse.Channel.Queue.Purged

    /**
     * Binds a queue to an exchange.
     *
     * @param queue Name of the queue.
     * @param exchange Name of the exchange.
     * @param routingKey Bind only to messages matching routingKey.
     * @param arguments Bind only to messages matching given options.
     *
     * @return AMQPResponse.Channel.Queue.Bound confirming that broker has accepted the request.
     */
    suspend fun queueBind(
        queue: String,
        exchange: String,
        routingKey: String = "",
        arguments: Table = emptyMap(),
    ): AMQPResponse.Channel.Queue.Bound

    /**
     * Unbinds a queue from an exchange.
     *
     * @param queue Name of the queue.
     * @param exchange Name of the exchange.
     * @param routingKey Unbind only from messages matching routingKey.
     * @param arguments Unbind only from messages matching given options.
     *
     * @return AMQPResponse.Channel.Queue.Unbound confirming that broker has accepted the unbind request.
     */
    suspend fun queueUnbind(
        queue: String,
        exchange: String,
        routingKey: String = "",
        arguments: Table = emptyMap(),
    ): AMQPResponse.Channel.Queue.Unbound

    /**
     * Declare an exchange.
     *
     * @param name Name of the exchange.
     * @param type Type of the exchange.
     * @param durable If enabled, creates an exchange stored on disk; otherwise, transient.
     * @param autoDelete If enabled, exchange will be deleted when the last consumer has stopped consuming.
     * @param internal Whether the exchange cannot be directly published to by client.
     * @param arguments Additional arguments (check RabbitMQ documentation).
     *
     * @return AMQPResponse.Channel.Exchange.Declared
     */
    suspend fun exchangeDeclare(
        name: String,
        type: String,
        durable: Boolean = false,
        autoDelete: Boolean = false,
        internal: Boolean = false,
        arguments: Table = emptyMap(),
    ): AMQPResponse.Channel.Exchange.Declared

    /**
     * Passively declare an exchange.
     *
     * @param name Name of the exchange.
     *
     * @return AMQPResponse.Channel.Exchange.Declared
     */
    suspend fun exchangeDeclarePassive(name: String): AMQPResponse.Channel.Exchange.Declared

    /**
     * Delete an exchange.
     *
     * @param name Name of the exchange.
     * @param ifUnused If enabled, exchange will be deleted only when it is not used.
     *
     * @return AMQPResponse.Channel.Exchange.Deleted
     */
    suspend fun exchangeDelete(
        name: String,
        ifUnused: Boolean = false,
    ): AMQPResponse.Channel.Exchange.Deleted

    /**
     * Bind an exchange to another exchange.
     *
     * @param destination Output exchange.
     * @param source Input exchange.
     * @param routingKey Bind only to messages matching routingKey.
     * @param arguments Bind only to messages matching given options.
     *
     * @return AMQPResponse.Channel.Exchange.Bound
     */
    suspend fun exchangeBind(
        destination: String,
        source: String,
        routingKey: String,
        arguments: Table = emptyMap(),
    ): AMQPResponse.Channel.Exchange.Bound

    /**
     * Unbind an exchange from another exchange.
     *
     * @param destination Output exchange.
     * @param source Input exchange.
     * @param routingKey Unbind only from messages matching routingKey.
     * @param arguments Unbind only from messages matching given options.
     *
     * @return AMQPResponse.Channel.Exchange.Unbound
     */
    suspend fun exchangeUnbind(
        destination: String,
        source: String,
        routingKey: String,
        arguments: Table = emptyMap(),
    ): AMQPResponse.Channel.Exchange.Unbound

    /**
     * Set channel in publish confirm mode, each published message will be acked or nacked.
     *
     * @return AMQPResponse.Channel.Confirm.Selected confirming that broker has accepted the confirm request.
     */
    suspend fun confirmSelect(): AMQPResponse.Channel.Confirm.Selected

    /**
     * Set channel in transaction mode.
     *
     * @return AMQPResponse.Channel.Tx.Selected confirming that broker has accepted the transaction request.
     */
    suspend fun txSelect(): AMQPResponse.Channel.Tx.Selected

    /**
     * Commit a transaction.
     *
     * @return AMQPResponse.Channel.Tx.Committed confirming that broker has committed the transaction.
     */
    suspend fun txCommit(): AMQPResponse.Channel.Tx.Committed

    /**
     * Rollback a transaction.
     *
     * @return AMQPResponse.Channel.Tx.Rollbacked confirming that broker has rolled back the transaction.
     */
    suspend fun txRollback(): AMQPResponse.Channel.Tx.Rollbacked

    // No-wait variants.
    //
    // Each of these sends the same frame as its regular counterpart but with the protocol's `no-wait`
    // flag set, so the broker sends no reply and the call returns as soon as the frame is written.
    // Declaring a large topology this way costs one round trip instead of one per declaration.
    //
    // The trade-off is that failures no longer surface as a returned error: the broker reports them by
    // closing the channel, so they arrive asynchronously as an [AMQPException.ChannelClosed] on the
    // next operation. Use the regular variants when you need to act on the outcome of each call.

    /**
     * Declares a queue without waiting for the broker to confirm it.
     *
     * @param name Name of the queue. Unlike [queueDeclare], it cannot be empty: with no reply there is
     *             no way to learn the name the broker would have generated.
     * @param durable If enabled, creates a queue stored on disk; otherwise, transient.
     * @param exclusive Announces the queue as exclusive to this connection.
     * @param autoDelete Announces the queue as auto-deleted once its last consumer leaves.
     * @param arguments Additional arguments (check RabbitMQ documentation).
     *
     * @throws IllegalArgumentException if [name] is blank.
     */
    suspend fun queueDeclareNoWait(
        name: String,
        durable: Boolean = false,
        exclusive: Boolean = false,
        autoDelete: Boolean = false,
        arguments: Table = emptyMap(),
    )

    /**
     * Passively declares a queue without waiting for the broker to confirm it.
     *
     * Since nothing is returned, this asserts the queue exists rather than reading its state: if it
     * does not, the broker closes the channel.
     *
     * @param name Name of the queue.
     */
    suspend fun queueDeclarePassiveNoWait(name: String)

    /**
     * Deletes a queue without waiting for the broker to confirm it.
     *
     * @param name Name of the queue.
     * @param ifUnused If enabled, queue will only be deleted if it has no consumers.
     * @param ifEmpty If enabled, queue will only be deleted if it has no messages.
     */
    suspend fun queueDeleteNoWait(
        name: String,
        ifUnused: Boolean = false,
        ifEmpty: Boolean = false,
    )

    /**
     * Purges a queue without waiting for the broker to confirm it.
     *
     * @param name Name of the queue.
     */
    suspend fun queuePurgeNoWait(name: String)

    /**
     * Binds a queue to an exchange without waiting for the broker to confirm it.
     *
     * @param queue Name of the queue.
     * @param exchange Name of the exchange.
     * @param routingKey Bind only to messages matching routingKey.
     * @param arguments Bind only to messages matching given options.
     */
    suspend fun queueBindNoWait(
        queue: String,
        exchange: String,
        routingKey: String = "",
        arguments: Table = emptyMap(),
    )

    /**
     * Declares an exchange without waiting for the broker to confirm it.
     *
     * @param name Name of the exchange.
     * @param type Type of the exchange.
     * @param durable If enabled, creates an exchange stored on disk; otherwise, transient.
     * @param autoDelete If enabled, exchange is deleted when the last binding is removed.
     * @param internal Whether the exchange cannot be directly published to by a client.
     * @param arguments Additional arguments (check RabbitMQ documentation).
     */
    suspend fun exchangeDeclareNoWait(
        name: String,
        type: String,
        durable: Boolean = false,
        autoDelete: Boolean = false,
        internal: Boolean = false,
        arguments: Table = emptyMap(),
    )

    /**
     * Passively declares an exchange without waiting for the broker to confirm it.
     *
     * Since nothing is returned, this asserts the exchange exists rather than reading its state: if it
     * does not, the broker closes the channel.
     *
     * @param name Name of the exchange.
     */
    suspend fun exchangeDeclarePassiveNoWait(name: String)

    /**
     * Deletes an exchange without waiting for the broker to confirm it.
     *
     * @param name Name of the exchange.
     * @param ifUnused If enabled, exchange will be deleted only if it has no bindings.
     */
    suspend fun exchangeDeleteNoWait(name: String, ifUnused: Boolean = false)

    /**
     * Binds an exchange to another exchange without waiting for the broker to confirm it.
     *
     * @param destination Name of the destination exchange.
     * @param source Name of the source exchange.
     * @param routingKey Bind only to messages matching routingKey.
     * @param arguments Bind only to messages matching given options.
     */
    suspend fun exchangeBindNoWait(
        destination: String,
        source: String,
        routingKey: String,
        arguments: Table = emptyMap(),
    )

    /**
     * Unbinds an exchange from another exchange without waiting for the broker to confirm it.
     *
     * @param destination Name of the destination exchange.
     * @param source Name of the source exchange.
     * @param routingKey Unbind only from messages matching routingKey.
     * @param arguments Unbind only from messages matching given options.
     */
    suspend fun exchangeUnbindNoWait(
        destination: String,
        source: String,
        routingKey: String,
        arguments: Table = emptyMap(),
    )

    /**
     * Puts the channel in publish confirm mode without waiting for the broker to confirm it.
     *
     * Frames are processed in order on a channel, so publishes issued after this call are covered by
     * confirm mode even though the `Confirm.SelectOk` was never awaited.
     */
    suspend fun confirmSelectNoWait()

}
