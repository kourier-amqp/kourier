package dev.kourier.amqp.robust

import dev.kourier.amqp.*
import dev.kourier.amqp.channel.*
import dev.kourier.amqp.connection.ConnectionState
import dev.kourier.amqp.states.*
import kotlinx.coroutines.CompletableDeferred

open class RobustAMQPChannel(
    override val connection: RobustAMQPConnection,
    id: ChannelId,
    frameMax: UInt,
) : DefaultAMQPChannel(connection, id, frameMax) {

    private var restoreCompleted = CompletableDeferred(Unit)

    // Adjusted delivery tag tracking (similar to Java client's RecoveryAwareChannelN):
    // Each restore increments deliveryTagOffset by the highest broker tag seen in that epoch,
    // making adjusted tags globally monotonically increasing across restores so that stale
    // acks can be detected even when the broker resets its delivery tag counter to 1.
    private var deliveryTagOffset: ULong = 0u
    private var deliveryTagOffsetBeforeRestore: ULong = 0u
    private var maxBrokerTagInCurrentEpoch: ULong = 0u

    private var declaredQos: DeclaredQos? = null
    private val declaredExchanges = mutableMapOf<String, DeclaredExchange>()
    private val declaredQueues = mutableMapOf<String, DeclaredQueue>()
    private val boundExchanges = mutableMapOf<Triple<String, String, String>, BoundExchange>()
    private val boundQueues = mutableMapOf<Triple<String, String, String>, BoundQueue>()
    private val consumedQueues = mutableMapOf<Pair<String, String>, ConsumedQueue>()

    private fun isStaleDeliveryTag(adjustedDeliveryTag: ULong): Boolean {
        // Adjusted tags from before the last restoration are stale.
        // deliveryTagOffsetBeforeRestore is the ceiling of all adjusted tags issued before the
        // most recent restore, so any tag <= it was issued on an old channel.
        return adjustedDeliveryTag <= deliveryTagOffsetBeforeRestore
    }

    /**
     * Robust channels should not be removed from registry on broker close - they restore instead
     */
    override fun shouldRemoveOnBrokerClose(): Boolean = false

    @InternalAmqpApi
    fun prepareForRestore() {
        if (restoreCompleted.isCompleted) restoreCompleted = CompletableDeferred()
        // Accumulate offset so adjusted tags never repeat across restores
        deliveryTagOffset += maxBrokerTagInCurrentEpoch
        deliveryTagOffsetBeforeRestore = deliveryTagOffset
        maxBrokerTagInCurrentEpoch = 0u
        state = ConnectionState.CLOSED
    }

    @InternalAmqpApi
    suspend fun restore() = withChannelRestoreContext {
        prepareForRestore()

        try {
            open()

            declaredQos?.let { basicQos(it) }
            declaredExchanges.values.forEach { exchangeDeclare(it) }
            declaredQueues.values.forEach { queueDeclare(it) }
            boundExchanges.values.forEach { exchangeBind(it) }
            boundQueues.values.forEach { queueBind(it) }

            // Snapshot the list and clear the map before iterating:
            // basicConsume() will re-populate consumedQueues with the broker-assigned consumer tag,
            // which would cause a ConcurrentModificationException if we iterated the live map.
            val queuesToRestore = consumedQueues.values.toList()
            consumedQueues.clear()
            queuesToRestore.forEach { consumedQueue ->
                basicConsume(
                    queue = consumedQueue.queue,
                    consumerTag = consumedQueue.consumerTag,
                    noAck = consumedQueue.noAck,
                    exclusive = consumedQueue.exclusive,
                    arguments = consumedQueue.arguments,
                    onDelivery = consumedQueue.onDelivery,
                    onCanceled = consumedQueue.onCanceled
                )
            }
            restoreCompleted.complete(Unit)
        } catch (e: Exception) {
            restoreCompleted.completeExceptionally(e)
            throw e
        }
    }

    override suspend fun write(vararg frames: Frame) {
        val channelRestoreContext = currentChannelRestoreContext()
        if (channelRestoreContext == null) restoreCompleted.await() // Wait for restore to complete if not in the restore context
        super.write(*frames)
    }

    override suspend fun cancelAll(channelClosed: AMQPException.ChannelClosed) {
        if (channelClosed.isInitiatedByApplication) return super.cancelAll(channelClosed)

        if (state == ConnectionState.CLOSED) return // Already closed
        // If the connection itself is not open, connectionFactory() will restore this channel;
        // attempting a channel-level restore here would race with that.
        if (connection.state != ConnectionState.OPEN) return
        logger.debug("Channel $id closed, attempting to restore...")
        restore()
    }

    override suspend fun basicQos(count: UShort, global: Boolean): AMQPResponse.Channel.Basic.QosOk {
        return super.basicQos(count, global).also {
            declaredQos = DeclaredQos(
                count = count,
                global = global
            )
        }
    }

    override suspend fun exchangeDeclare(
        name: String,
        type: String,
        durable: Boolean,
        autoDelete: Boolean,
        internal: Boolean,
        arguments: Table,
    ): AMQPResponse.Channel.Exchange.Declared {
        return super.exchangeDeclare(name, type, durable, autoDelete, internal, arguments).also {
            if (!internal) declaredExchanges[name] = DeclaredExchange(
                name = name,
                type = type,
                durable = durable,
                autoDelete = autoDelete,
                internal = internal,
                arguments = arguments
            )
        }
    }

    override suspend fun exchangeDelete(name: String, ifUnused: Boolean): AMQPResponse.Channel.Exchange.Deleted {
        return super.exchangeDelete(name, ifUnused).also {
            declaredExchanges.remove(name)
        }
    }

    override suspend fun exchangeBind(
        destination: String,
        source: String,
        routingKey: String,
        arguments: Table,
    ): AMQPResponse.Channel.Exchange.Bound {
        return super.exchangeBind(destination, source, routingKey, arguments).also {
            boundExchanges[Triple(destination, source, routingKey)] = BoundExchange(
                destination = destination,
                source = source,
                routingKey = routingKey,
                arguments = arguments
            )
        }
    }

    override suspend fun exchangeUnbind(
        destination: String,
        source: String,
        routingKey: String,
        arguments: Table,
    ): AMQPResponse.Channel.Exchange.Unbound {
        return super.exchangeUnbind(destination, source, routingKey, arguments).also {
            boundExchanges.remove(Triple(destination, source, routingKey))
        }
    }

    override suspend fun queueDeclare(
        name: String,
        durable: Boolean,
        exclusive: Boolean,
        autoDelete: Boolean,
        arguments: Table,
    ): AMQPResponse.Channel.Queue.Declared {
        return super.queueDeclare(name, durable, exclusive, autoDelete, arguments).also {
            declaredQueues[name] = DeclaredQueue(
                name = name,
                durable = durable,
                exclusive = exclusive,
                autoDelete = autoDelete,
                arguments = arguments
            )
        }
    }

    override suspend fun queueDelete(
        name: String,
        ifUnused: Boolean,
        ifEmpty: Boolean,
    ): AMQPResponse.Channel.Queue.Deleted {
        return super.queueDelete(name, ifUnused, ifEmpty).also {
            declaredQueues.remove(name)
        }
    }

    override suspend fun queueBind(
        queue: String,
        exchange: String,
        routingKey: String,
        arguments: Table,
    ): AMQPResponse.Channel.Queue.Bound {
        return super.queueBind(queue, exchange, routingKey, arguments).also {
            boundQueues[Triple(queue, exchange, routingKey)] = BoundQueue(
                queue = queue,
                exchange = exchange,
                routingKey = routingKey,
                arguments = arguments
            )
        }
    }

    override suspend fun queueUnbind(
        queue: String,
        exchange: String,
        routingKey: String,
        arguments: Table,
    ): AMQPResponse.Channel.Queue.Unbound {
        return super.queueUnbind(queue, exchange, routingKey, arguments).also {
            boundQueues.remove(Triple(queue, exchange, routingKey))
        }
    }

    override suspend fun basicConsume(
        queue: String,
        consumerTag: String,
        noAck: Boolean,
        exclusive: Boolean,
        arguments: Table,
        onDelivery: suspend (AMQPResponse.Channel.Message.Delivery) -> Unit,
        onCanceled: suspend (AMQPResponse.Channel) -> Unit,
    ): AMQPResponse.Channel.Basic.ConsumeOk {
        return super.basicConsume(
            queue, consumerTag, noAck, exclusive, arguments,
            onDelivery = { delivery ->
                val brokerTag = delivery.message.deliveryTag
                if (brokerTag > maxBrokerTagInCurrentEpoch) maxBrokerTagInCurrentEpoch = brokerTag
                // Adjust the delivery tag to be globally unique across restores.
                // The broker resets its counter to 1 on each new channel, but consumers store the
                // tag and use it later for basicAck — so we make it monotonically increasing here.
                val adjustedDelivery = delivery.copy(
                    message = delivery.message.copy(deliveryTag = brokerTag + deliveryTagOffset)
                )
                onDelivery(adjustedDelivery)
            },
            onCanceled = { response ->
                if (response is AMQPResponse.Channel.Closed && state == ConnectionState.OPEN) return@basicConsume
                onCanceled(response)
            }
        ).also {
            consumedQueues[Pair(queue, it.consumerTag)] = ConsumedQueue(
                queue = queue,
                consumerTag = consumerTag,
                noAck = noAck,
                exclusive = exclusive,
                arguments = arguments,
                onDelivery = onDelivery,
                onCanceled = onCanceled
            )
        }
    }

    override suspend fun basicCancel(consumerTag: String): AMQPResponse.Channel.Basic.Canceled {
        return super.basicCancel(consumerTag).also {
            val key = consumedQueues.entries.find { it.value.consumerTag == consumerTag }?.key ?: return@also
            consumedQueues.remove(key)
        }
    }

    override suspend fun basicAck(deliveryTag: ULong, multiple: Boolean) {
        // Silently ignore acks for stale delivery tags (from before channel restoration)
        // This prevents PRECONDITION_FAILED errors when acking old messages
        if (isStaleDeliveryTag(deliveryTag)) {
            logger.debug("Ignoring ack for stale delivery tag $deliveryTag (offset: $deliveryTagOffsetBeforeRestore)")
            return
        }
        super.basicAck(deliveryTag - deliveryTagOffset, multiple)
    }

    override suspend fun basicNack(deliveryTag: ULong, multiple: Boolean, requeue: Boolean) {
        // Silently ignore nacks for stale delivery tags
        if (isStaleDeliveryTag(deliveryTag)) {
            logger.debug("Ignoring nack for stale delivery tag $deliveryTag (offset: $deliveryTagOffsetBeforeRestore)")
            return
        }
        super.basicNack(deliveryTag - deliveryTagOffset, multiple, requeue)
    }

    override suspend fun basicReject(deliveryTag: ULong, requeue: Boolean) {
        // Silently ignore rejects for stale delivery tags
        if (isStaleDeliveryTag(deliveryTag)) {
            logger.debug("Ignoring reject for stale delivery tag $deliveryTag (offset: $deliveryTagOffsetBeforeRestore)")
            return
        }
        super.basicReject(deliveryTag - deliveryTagOffset, requeue)
    }

}
