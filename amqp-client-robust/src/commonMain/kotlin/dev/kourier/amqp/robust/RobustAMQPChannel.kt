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

    // Stale delivery tag tracking (similar to Java client's RecoveryAwareChannelN)
    private var maxSeenDeliveryTag: ULong = 0u
    private var deliveryTagOffsetBeforeRestore: ULong = 0u

    private var declaredQos: DeclaredQos? = null
    private val declaredExchanges = mutableMapOf<String, DeclaredExchange>()
    private val declaredQueues = mutableMapOf<String, DeclaredQueue>()
    private val boundExchanges = mutableMapOf<Triple<String, String, String>, BoundExchange>()
    private val boundQueues = mutableMapOf<Triple<String, String, String>, BoundQueue>()
    private val consumedQueues = mutableMapOf<Pair<String, String>, ConsumedQueue>()

    private fun trackDeliveryTag(deliveryTag: ULong) {
        if (deliveryTag > maxSeenDeliveryTag) maxSeenDeliveryTag = deliveryTag
    }

    private fun isStaleDeliveryTag(deliveryTag: ULong): Boolean {
        // Delivery tags from before the last restoration are stale
        return deliveryTagOffsetBeforeRestore > 0u && deliveryTag <= deliveryTagOffsetBeforeRestore
    }

    /**
     * Robust channels should not be removed from registry on broker close - they restore instead
     */
    override fun shouldRemoveOnBrokerClose(): Boolean = false

    @InternalAmqpApi
    fun prepareForRestore() {
        if (restoreCompleted.isCompleted) restoreCompleted = CompletableDeferred()
        // Save the max delivery tag seen before restoration
        // Any tags <= this value will be considered stale after restoration
        deliveryTagOffsetBeforeRestore = maxSeenDeliveryTag
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
            consumedQueues.values.forEach { consumedQueue ->
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
                // Track delivery tag to detect stale acks after restoration
                trackDeliveryTag(delivery.message.deliveryTag)
                onDelivery(delivery)
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
        super.basicAck(deliveryTag, multiple)
    }

    override suspend fun basicNack(deliveryTag: ULong, multiple: Boolean, requeue: Boolean) {
        // Silently ignore nacks for stale delivery tags
        if (isStaleDeliveryTag(deliveryTag)) {
            logger.debug("Ignoring nack for stale delivery tag $deliveryTag (offset: $deliveryTagOffsetBeforeRestore)")
            return
        }
        super.basicNack(deliveryTag, multiple, requeue)
    }

    override suspend fun basicReject(deliveryTag: ULong, requeue: Boolean) {
        // Silently ignore rejects for stale delivery tags
        if (isStaleDeliveryTag(deliveryTag)) {
            logger.debug("Ignoring reject for stale delivery tag $deliveryTag (offset: $deliveryTagOffsetBeforeRestore)")
            return
        }
        super.basicReject(deliveryTag, requeue)
    }

}
