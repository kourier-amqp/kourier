package dev.kourier.amqp.robust

import dev.kourier.amqp.*
import dev.kourier.amqp.channel.*
import dev.kourier.amqp.connection.ConnectionState
import dev.kourier.amqp.states.*
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeout
import kotlin.concurrent.Volatile

open class RobustAMQPChannel(
    override val connection: RobustAMQPConnection,
    id: ChannelId,
    frameMax: UInt,
) : DefaultAMQPChannel(connection, id, frameMax) {

    private var restoreCompleted = CompletableDeferred(Unit)

    // Deduplicates concurrent broker-`Channel.Close`-triggered restore attempts. A single
    // broker close fires *two* `cancelAll(channelClosed)` paths: (a) the read loop's
    // `messageListeningScope.launch { channel.cancelAll(exception) }`, and (b) the
    // synchronous `.also { cancelAll(it) }` inside `writeAndWaitForResponse` for any
    // in-flight RPC whose `.first()` filter caught the same `Channel.Closed` emission.
    // Without dedup, both call `prepareForRestore()` (which resets `state = CLOSED`) — so
    // even after one restore has set `state = OPEN` post-`open()`, the other's
    // `prepareForRestore()` re-flips it to CLOSED and its `open()` sends `Channel.Open`
    // AGAIN. The broker then closes the *connection* with CHANNEL_ERROR 504
    // "second 'channel.open' seen". `tryLock` lets only one restore run per close event;
    // any concurrent caller skips silently — the channel state ends up correctly restored
    // by the winning path.
    private val restoreLatch = Mutex()

    // One-shot restore token. Armed exactly once per broker Channel.Close in onBrokerClose() —
    // which the read loop calls BEFORE the close is observable to either restore trigger — and
    // consumed by the first cancelAll under restoreLatch. This makes restore idempotent *per close
    // event*: restoreLatch.tryLock alone only dedups triggers that overlap in time, but the two
    // triggers (read-loop launch + in-flight RPC's `.also { cancelAll }`) can also run sequentially,
    // and a second restore would re-send Channel.Open → broker 504 "second 'channel.open' seen"
    // → connection teardown (NEW-31).
    @Volatile
    private var pendingRestore = false

    override fun onBrokerClose() {
        pendingRestore = true
    }

    // Adjusted delivery tag tracking (similar to Java client's RecoveryAwareChannelN):
    // Each restore increments deliveryTagOffset by the highest broker tag seen in that epoch,
    // making adjusted tags globally monotonically increasing across restores so that stale
    // acks can be detected even when the broker resets its delivery tag counter to 1.
    // All three fields are guarded by deliveryTagMutex since delivery callbacks run concurrently.
    private val deliveryTagMutex = Mutex()
    private var deliveryTagOffset: ULong = 0u
    private var deliveryTagOffsetBeforeRestore: ULong = 0u
    private var maxBrokerTagInCurrentEpoch: ULong = 0u

    // Durable record of whether the *user* asked for confirm/tx mode, kept separate from the
    // base `isConfirmMode`/`isTxMode` flags. Those base flags track the *current broker channel's*
    // state and are reset by prepareForRestore() (a freshly-reopened channel is not in confirm/tx
    // mode until we re-issue the select). These intent flags survive restores so the mode is
    // re-established on every reconnect — never reset once the user enabled it.
    private var confirmModeRequested = false
    private var txModeRequested = false

    private var declaredQos: DeclaredQos? = null
    internal val declaredExchanges = mutableMapOf<String, DeclaredExchange>()
    internal val declaredQueues = mutableMapOf<String, DeclaredQueue>()

    // Queues declared with an empty name are *channel-scoped*: the broker generates the name, ties the queue's
    // lifetime to this channel, and mints a different name on every declare. They are therefore tracked apart
    // from `declaredQueues` and re-declared on every restore regardless of `restoreTopology`. That flag is
    // about the broker topology the user owns, whereas these queues are part of the channel's own state and
    // simply cease to exist when it closes. Without re-declaring them, the bindings and consumers recorded
    // against the previous generated name target a queue that is gone, the broker answers `not_found`, and the
    // freshly reopened channel is closed again. Keyed by the *broker-assigned* name so bindings and consumers,
    // which only ever saw that name, can be retargeted after the re-declare.
    internal val serverNamedQueues = mutableMapOf<String, DeclaredQueue>()
    internal val boundExchanges = mutableMapOf<Triple<String, String, String>, BoundExchange>()
    internal val boundQueues = mutableMapOf<Triple<String, String, String>, BoundQueue>()
    internal val consumedQueues = mutableMapOf<Pair<String, String>, ConsumedQueue>()

    private val restoreTopology: Boolean
        get() = connection.config.server.restoreTopology

    /**
     * Robust channels should not be removed from registry on broker close - they restore instead
     */
    override fun shouldRemoveOnBrokerClose(): Boolean = false

    @InternalAmqpApi
    fun prepareForRestore() {
        if (restoreCompleted.isCompleted) restoreCompleted = CompletableDeferred()
        // Accumulate offset so adjusted tags never repeat across restores.
        // No need to lock here: prepareForRestore() is always called before the new channel is
        // open, so no delivery callbacks can be racing at this point.
        deliveryTagOffset += maxBrokerTagInCurrentEpoch
        deliveryTagOffsetBeforeRestore = deliveryTagOffset
        maxBrokerTagInCurrentEpoch = 0u
        state = ConnectionState.CLOSED
        // The freshly-reopened broker channel is NOT in confirm/tx mode; clear the base flags so
        // restore()'s re-issued confirmSelect()/txSelect() actually send the frame (they early-return
        // if the flag is already set). The durable confirmModeRequested/txModeRequested intent flags
        // are untouched, so the mode is re-established below.
        isConfirmMode = false
        isTxMode = false
    }

    @InternalAmqpApi
    suspend fun restore() = withChannelRestoreContext {
        prepareForRestore()

        try {
            // Bound restore so a non-replying broker can't hang every writer on the channel forever.
            withTimeout(connection.config.server.restoreTimeout) {
                open()

                // Re-establish confirm/tx mode before anything is published on the new channel.
                // Without this, basicPublish keeps returning a deliveryTag (isConfirmMode is set
                // again below) but the broker, no longer in confirm mode, never sends Basic.Ack —
                // so any caller awaiting a publish confirm hangs forever.
                if (confirmModeRequested) confirmSelect()
                if (txModeRequested) txSelect()

                declaredQos?.let { basicQos(it) }

                // Channel-scoped queues have to come back before anything that references them; this also
                // retargets their bindings and consumers onto the names the broker has just handed out.
                restoreServerNamedQueues()

                if (restoreTopology) {
                    declaredExchanges.values.forEach { exchangeDeclare(it) }
                    declaredQueues.values.forEach { queueDeclare(it) }
                    boundExchanges.values.forEach { exchangeBind(it) }
                }
                restoreQueueBindings()

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
            }
            restoreCompleted.complete(Unit)
        } catch (e: TimeoutCancellationException) {
            // Restore stalled (likely broker overload / blackhole). Unblock awaiting writers,
            // tear down the socket and let connectionFactory() retry on a fresh connection.
            val timeoutException = RestoreTimeoutException(
                channelId = id,
                timeout = connection.config.server.restoreTimeout,
                cause = e,
            )
            restoreCompleted.completeExceptionally(timeoutException)
            connection.forceReconnect()
            // Throw a regular Exception (not CancellationException) so connectionFactory()
            // logs and iterates rather than cancelling the recovery coroutine.
            throw timeoutException
        } catch (e: CancellationException) {
            // Genuine cancellation (e.g. the connection was torn down, or cancelAll()
            // cancelled brokerCloseRestoresScope mid-restore) — distinct from the
            // TimeoutCancellationException case above. Unblock awaiting writers, then rethrow:
            // cooperative cancellation must propagate, never be swallowed. Handled explicitly
            // (rather than relying on the generic catch below) so this intent survives edits.
            restoreCompleted.completeExceptionally(e)
            throw e
        } catch (e: Exception) {
            restoreCompleted.completeExceptionally(e)
            throw e
        }
    }

    /**
     * Re-declares the channel-scoped server-named queues, retargeting their bindings and consumers onto the
     * name the broker assigns now, which differs from the one it assigned before the close.
     */
    private suspend fun restoreServerNamedQueues() {
        if (serverNamedQueues.isEmpty()) return
        // Iterate over a snapshot: queueDeclare() re-populates the live map under the new name. Each queue is
        // retargeted as soon as its declare succeeds, so the bookkeeping never holds a name the broker no
        // longer knows, even if a later declare throws and the restore is retried on a fresh connection.
        serverNamedQueues.toMap().forEach { (previousName, declaredQueue) ->
            val currentName = queueDeclare(declaredQueue).queueName
            if (currentName == previousName) return@forEach
            serverNamedQueues.remove(previousName)
            retargetQueue(previousName, currentName)
        }
    }

    /**
     * Points every recorded binding and consumer of [previousName] at [currentName], re-keying them so that
     * later lookups and restores see only the name the broker currently serves.
     */
    private fun retargetQueue(previousName: String, currentName: String) {
        boundQueues.filterValues { it.queue == previousName }.forEach { (previousKey, boundQueue) ->
            boundQueues.remove(previousKey)
            boundQueues[Triple(currentName, boundQueue.exchange, boundQueue.routingKey)] =
                boundQueue.copy(queue = currentName)
        }
        consumedQueues.filterKeys { it.first == previousName }.forEach { (previousKey, consumedQueue) ->
            consumedQueues.remove(previousKey)
            consumedQueues[Pair(currentName, previousKey.second)] = consumedQueue.copy(queue = currentName)
        }
    }

    /**
     * Replays the recorded queue bindings, which [restoreServerNamedQueues] has already retargeted onto the
     * current names. When [restoreTopology] is disabled only the server-named bindings are replayed: they
     * belong to the channel's own lifecycle rather than to the topology the user asked us to leave alone.
     */
    private suspend fun restoreQueueBindings() {
        if (boundQueues.isEmpty()) return
        // Names are already current here, so queueBind() rewrites each entry under its own key.
        boundQueues.values.toList().forEach { boundQueue ->
            if (!restoreTopology && boundQueue.queue !in serverNamedQueues) return@forEach
            queueBind(boundQueue)
        }
    }

    override suspend fun prepareForWrite() {
        // Wait for any in-progress restore before letting a user-initiated write proceed.
        // Invoked by:
        //   - writeAndWaitForResponse BEFORE writeMutex acquisition (the response-bearing
        //     RPC path — doing the await inside the mutex would deadlock against restore's
        //     own writes which need the same mutex).
        //   - DefaultAMQPChannel.write(), so the no-response paths (basicPublish, basicAck,
        //     basicNack, the cancel frame from produce's awaitClose) also wait for restore.
        // Restore's own writes carry [ChannelRestoreContextElement] in the coroutine context
        // and skip the await to avoid blocking the very restore we'd be waiting on.
        if (currentChannelRestoreContext() == null) restoreCompleted.await()
    }

    override suspend fun cancelAll(channelClosed: AMQPException.ChannelClosed) {
        if (channelClosed.isInitiatedByApplication) return super.cancelAll(channelClosed)

        if (state == ConnectionState.CLOSED) return // Already closed
        // If the connection itself is not open, connectionFactory() will restore this channel;
        // attempting a channel-level restore here would race with that.
        if (connection.state != ConnectionState.OPEN) return
        // Dedup concurrent restore triggers from the same Channel.Close event (read-loop
        // launch + in-flight RPC's `writeAndWaitForResponse.also { cancelAll(it) }`).
        // `tryLock` returns false if another restore is already running — we silently
        // skip; the in-progress restore will complete the state transition for us.
        if (!restoreLatch.tryLock()) return
        try {
            // Consume the one-shot token armed by onBrokerClose(). The first trigger to win the
            // latch restores; any later trigger for the SAME close — whether it raced concurrently
            // (and lost the tryLock) or arrives sequentially after this restore already finished —
            // finds the token cleared and skips, so we never send a duplicate Channel.Open. (NEW-31)
            if (!pendingRestore) return
            pendingRestore = false
            logger.debug("Channel $id closed, attempting to restore...")
            restore()
        } finally {
            restoreLatch.unlock()
        }
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
            if (!internal && restoreTopology) declaredExchanges[name] = DeclaredExchange(
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
            if (restoreTopology) boundExchanges[Triple(destination, source, routingKey)] = BoundExchange(
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
            val declaredQueue = DeclaredQueue(
                name = name,
                durable = durable,
                exclusive = exclusive,
                autoDelete = autoDelete,
                arguments = arguments
            )
            // Keep the empty requested name in the spec so the re-declare asks for a new server-generated one,
            // but key the entry by the assigned name, which is all the bindings and consumers ever saw.
            // The predicate is deliberately broad: an exclusive queue is scoped to the connection and an
            // auto-delete one only goes when its last consumer leaves, so re-declaring can leave the previous
            // queue behind. An orphan queue is a far better outcome than a channel that never recovers.
            if (name.isEmpty() && (exclusive || autoDelete)) serverNamedQueues[it.queueName] = declaredQueue
            // Anything else keeps the existing behaviour, including a server-named queue that is durable and
            // neither exclusive nor auto-delete. Such a queue does outlive its channel, so replaying the empty
            // name declares a new one on every restore, but recording the assigned name instead is not an
            // option: the broker reserves the `amq.*` prefix and answers ACCESS_REFUSED to an explicit declare.
            else if (restoreTopology) declaredQueues[name] = declaredQueue
        }
    }

    override suspend fun queueDelete(
        name: String,
        ifUnused: Boolean,
        ifEmpty: Boolean,
    ): AMQPResponse.Channel.Queue.Deleted {
        return super.queueDelete(name, ifUnused, ifEmpty).also {
            declaredQueues.remove(name)
            serverNamedQueues.remove(name)
        }
    }

    override suspend fun queueBind(
        queue: String,
        exchange: String,
        routingKey: String,
        arguments: Table,
    ): AMQPResponse.Channel.Queue.Bound {
        return super.queueBind(queue, exchange, routingKey, arguments).also {
            // Bindings onto a server-named queue are tracked even without restoreTopology: the queue is
            // re-declared on every restore, so its bindings have to be replayed or it receives nothing.
            if (restoreTopology || queue in serverNamedQueues) {
                boundQueues[Triple(queue, exchange, routingKey)] = BoundQueue(
                    queue = queue,
                    exchange = exchange,
                    routingKey = routingKey,
                    arguments = arguments
                )
            }
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

    override suspend fun basicGet(queue: String, noAck: Boolean): AMQPResponse.Channel.Message.Get {
        val result = super.basicGet(queue, noAck)
        val message = result.message ?: return result
        val adjustedMessage = deliveryTagMutex.withLock {
            val brokerTag = message.deliveryTag
            if (brokerTag > maxBrokerTagInCurrentEpoch) maxBrokerTagInCurrentEpoch = brokerTag
            message.copy(deliveryTag = brokerTag + deliveryTagOffset)
        }
        return result.copy(message = adjustedMessage)
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
                val adjustedDelivery = deliveryTagMutex.withLock {
                    val brokerTag = delivery.message.deliveryTag
                    if (brokerTag > maxBrokerTagInCurrentEpoch) maxBrokerTagInCurrentEpoch = brokerTag
                    // Adjust the delivery tag to be globally unique across restores.
                    // The broker resets its counter to 1 on each new channel, but consumers store the
                    // tag and use it later for basicAck — so we make it monotonically increasing here.
                    delivery.copy(message = delivery.message.copy(deliveryTag = brokerTag + deliveryTagOffset))
                }
                onDelivery(adjustedDelivery)
            },
            onCanceled = { response ->
                // Forward Channel.Closed to the user only on a terminal close (user-initiated
                // channel.close() or terminal connection close). Transient broker/connection
                // closes are restored — the consumer must not see a cancellation for those,
                // otherwise its receive-channel would close and never reattach to the
                // post-restore listener.
                //
                // The previous predicate `state == OPEN` was racy: prepareForRestore() flips
                // state OPEN→CLOSED on the connectionFactory thread concurrently with the
                // listener's read, so during reconnect the check could observe CLOSED and
                // (incorrectly) forward — closing the user's ReceiveChannel mid-restore.
                //
                // Race-free signals (all monotonic, set synchronously by the originator):
                //  - state == SHUTTING_DOWN: set by close() before sending the close frame.
                //  - channelClosed.isCompleted: completed by cancelAll on user-init close.
                //  - connection.connectionClosed.isCompleted: connection terminally closed.
                if (response is AMQPResponse.Channel.Closed) {
                    val terminal = state == ConnectionState.SHUTTING_DOWN
                            || channelClosed.isCompleted
                            || connection.connectionClosed.isCompleted
                    if (!terminal) return@basicConsume
                }
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
        val (stale, brokerTag) = deliveryTagMutex.withLock {
            Pair(deliveryTag <= deliveryTagOffsetBeforeRestore, deliveryTag - deliveryTagOffset)
        }
        if (stale) {
            logger.debug("Ignoring ack for stale delivery tag $deliveryTag (offset: $deliveryTagOffsetBeforeRestore)")
            return
        }
        super.basicAck(brokerTag, multiple)
    }

    override suspend fun basicNack(deliveryTag: ULong, multiple: Boolean, requeue: Boolean) {
        // Silently ignore nacks for stale delivery tags
        val (stale, brokerTag) = deliveryTagMutex.withLock {
            Pair(deliveryTag <= deliveryTagOffsetBeforeRestore, deliveryTag - deliveryTagOffset)
        }
        if (stale) {
            logger.debug("Ignoring nack for stale delivery tag $deliveryTag (offset: $deliveryTagOffsetBeforeRestore)")
            return
        }
        super.basicNack(brokerTag, multiple, requeue)
    }

    override suspend fun basicReject(deliveryTag: ULong, requeue: Boolean) {
        // Silently ignore rejects for stale delivery tags
        val (stale, brokerTag) = deliveryTagMutex.withLock {
            Pair(deliveryTag <= deliveryTagOffsetBeforeRestore, deliveryTag - deliveryTagOffset)
        }
        if (stale) {
            logger.debug("Ignoring reject for stale delivery tag $deliveryTag (offset: $deliveryTagOffsetBeforeRestore)")
            return
        }
        super.basicReject(brokerTag, requeue)
    }

    override suspend fun confirmSelect(): AMQPResponse.Channel.Confirm.Selected {
        // Record the user's intent durably so restore() re-enables confirm mode on every reconnect.
        return super.confirmSelect().also { confirmModeRequested = true }
    }

    override suspend fun txSelect(): AMQPResponse.Channel.Tx.Selected {
        return super.txSelect().also { txModeRequested = true }
    }

}
