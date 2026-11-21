package dev.kourier.amqp.robust

import dev.kourier.amqp.*
import dev.kourier.amqp.channel.*
import dev.kourier.amqp.connection.ConnectionState
import dev.kourier.amqp.states.*
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeout
import kotlin.concurrent.Volatile

// Consumers cancelled for good, each with the cancellation to forward to it.
private typealias Cancellations = List<Pair<ConsumedQueue, AMQPResponse.Channel.Basic.Canceled>>

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
    // by the winning path. Every restore runs under this latch (the reconnect one, the
    // broker-close one and the retry of a refused one), so they never overlap.
    private val restoreLatch = Mutex()

    // Retries a restore the broker refused (see [restoreRefused]), with the reconnect backoff,
    // as long as the connection it was refused on is up. Cancelled by the next reconnect,
    // which restores the channel itself.
    private var restoreRetry: Job? = null

    // The refusal of the restore being retried that was last reported as a warning: the retries refused for the same
    // reason are logged at debug level, so a queue unavailable for a while does not flood the logs. Reset by a
    // restore that succeeds and by a reconnect.
    private var reportedRestoreRefusal: String? = null

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

    // Consumer tags the application is cancelling through [basicCancel]: their `Basic.Canceled` is the answer to
    // that cancel, forwarded to the application, whereas any other one is a `basic.cancel` sent by the broker.
    private val cancelingConsumerTags = mutableSetOf<String>()

    // Queues the application is deleting through [queueDelete], with the number of deletes in flight: the broker
    // cancels their consumers, and that `basic.cancel` is forwarded to the application rather than recovered from.
    private val deletingQueues = mutableMapOf<String, Int>()

    // Consumers consumed again for a consumer the application cancelled meanwhile, and cancelled in turn by the robust
    // channel: the application has been told already, their `Basic.Canceled` is not forwarded.
    private val silentlyCancelingConsumerTags = mutableSetOf<String>()
    private val cancelingMutex = Mutex()

    // Broker-generated consumer tags of consumers consumed again (after a restore, or after a `basic.cancel` from the
    // broker), mapped to the tag the application received when it consumed the queue: the broker generates a new tag
    // on every consume, while the application only knows the first one, and cancels the consumer with it.
    // Guarded by consumersMutex.
    private val firstConsumerTags = mutableMapOf<String, String>()

    // Consumers the broker cancelled with a `basic.cancel`, not consumed again yet: their listener is gone with that
    // cancel. A restore refused with 404 NOT_FOUND on one of them has found its queue gone (see [restoreConsumers]),
    // and the robust channel tells one that the application cancels itself (see [basicCancel]). Guarded by
    // consumersMutex.
    private val brokerCancelledConsumers = mutableSetOf<Pair<String, String>>()

    // Consumers of [brokerCancelledConsumers] that the application cancelled, or whose queue it deleted, while a
    // `basic.consume` may be consuming them again: that consume, once answered, is cancelled instead of recorded (see
    // [consumedAgain]). Guarded by consumersMutex.
    private val cancelledWhileConsumedAgain = mutableSetOf<Pair<String, String>>()
    private val consumersMutex = Mutex()

    // The restore has just forgotten a queue it found gone (see [restoreConsumers]): the refusal of the restore is
    // handled by restoring the channel again at once, without that queue. The caller holds [restoreLatch].
    private var goneQueueForgotten = false

    private val restoreTopology: Boolean
        get() = connection.config.server.restoreTopology

    private companion object {
        // Reply code of a `basic.consume` on a queue the broker does not know.
        const val NOT_FOUND: UShort = 404u
    }

    /**
     * Whether the broker refused a `basic.consume` of [queue] with [e] because the queue is gone: 404 NOT_FOUND, on
     * a queue that is not one of the server-named queues, which the restore declares again.
     */
    private fun isGone(queue: String, e: AMQPException.ChannelClosed): Boolean =
        e.replyCode == NOT_FOUND && queue !in serverNamedQueues

    // The application closes the channel, or has closed it.
    private val closedByTheApplication: Boolean
        get() = state == ConnectionState.SHUTTING_DOWN || channelClosed.isCompleted

    /** Whether the connection of [iteration] is going down, or gone: the reconnect restores the channel then. */
    private fun connectionLost(iteration: CompletableDeferred<Unit>): Boolean =
        iteration.isCompleted || connection.state != ConnectionState.OPEN

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
    suspend fun restore() {
        // The reconnect restores the channel itself: a retry of a restore refused on the previous connection
        // is moot, and must not run on the new one alongside this restore.
        restoreRetry?.cancelAndJoin()
        restoreLatch.withLock {
            reportedRestoreRefusal = null
            restoreLocked()
        }
    }

    /**
     * Restores the channel. The caller holds [restoreLatch].
     */
    private suspend fun restoreLocked() = withChannelRestoreContext {
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

                restoreConsumers()
            }
            reportedRestoreRefusal = null
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
        } catch (e: AMQPException.ChannelClosed) {
            val goneQueue = goneQueueForgotten
            goneQueueForgotten = false
            // A close of the whole connection also fails the restore with ChannelClosed: the reconnect handles it.
            if (connection.state != ConnectionState.OPEN) {
                restoreCompleted.completeExceptionally(e)
                throw e
            }
            if (!goneQueue) {
                restoreRefused(e)
                throw e
            }
            // Refused on a queue the restore has forgotten as gone: restore the channel again at once, without it. The
            // writers waiting for the restore keep waiting for that one.
            logger.debug("Restore of channel $id refused on a queue that is gone, restoring it again without the queue")
            pendingRestore = false
            restoreLocked()
        } catch (e: Exception) {
            restoreCompleted.completeExceptionally(e)
            throw e
        }
    }

    /**
     * The broker refused part of the restore and closed the channel, e.g. a `basic.consume` answered 404 NOT_FOUND
     * because the node that hosts the queue is down while the client has reconnected to another node.
     *
     * `open()` has already marked the channel `OPEN`: mark it closed again, since the broker has closed it, fail the
     * writers waiting for the restore, and try the restore again later on the same connection. Retrying at once, on
     * every close, would only hammer the broker while the queue is unavailable.
     */
    private fun restoreRefused(e: AMQPException.ChannelClosed) {
        state = ConnectionState.CLOSED
        // The close that refused the restore is handled here: it must not trigger another restore of its own.
        pendingRestore = false
        restoreCompleted.completeExceptionally(e)
        val reason = "${e.replyText} (${e.replyCode})"
        val message = "Restore of channel $id refused by the broker: $reason, it will be retried"
        // Reported once per refusal reason, not on every retry.
        if (reportedRestoreRefusal == reason) logger.debug(message) else logger.warn(message)
        reportedRestoreRefusal = reason
        scheduleRestoreRetry()
    }

    private fun scheduleRestoreRetry() {
        if (restoreRetry?.isActive == true) return // Already retrying: the running retry loops on.
        val iteration = connection.currentIteration
        val server = connection.config.server
        restoreRetry = connection.messageListeningScope.launch {
            var retryDelay = server.reconnectInitialDelay
            while (true) {
                delay(retryDelay)
                retryDelay = (retryDelay * server.reconnectBackoffMultiplier).coerceAtMost(server.reconnectMaxDelay)
                // The connection the restore was refused on is gone: the reconnect restores the channel.
                if (connection.currentIteration !== iteration || iteration.isCompleted) return@launch
                if (connection.state != ConnectionState.OPEN) return@launch
                if (!restoreLatch.tryLock()) continue // Another restore is running, check again later.
                try {
                    // Restored meanwhile, or closed by the application.
                    if (state != ConnectionState.CLOSED || channelClosed.isCompleted) return@launch
                    logger.debug("Retrying the restore of channel $id")
                    restoreLocked()
                    return@launch
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    logger.debug("Retry of the restore of channel $id failed: ${e.message}")
                } finally {
                    restoreLatch.unlock()
                }
            }
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
    private suspend fun retargetQueue(previousName: String, currentName: String) {
        boundQueues.filterValues { it.queue == previousName }.forEach { (previousKey, boundQueue) ->
            boundQueues.remove(previousKey)
            boundQueues[Triple(currentName, boundQueue.exchange, boundQueue.routingKey)] =
                boundQueue.copy(queue = currentName)
        }
        consumedQueues.filterKeys { it.first == previousName }.forEach { (previousKey, consumedQueue) ->
            val currentKey = Pair(currentName, previousKey.second)
            consumedQueues.remove(previousKey)
            consumedQueues[currentKey] = consumedQueue.copy(queue = currentName)
            // The restore consumes it right after, under its current name.
            consumersMutex.withLock { brokerCancelledConsumers.remove(previousKey) }
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

    /**
     * Re-registers the recorded consumers. An entry is only replaced once its consumer is back, so a restore
     * that fails or times out midway leaves the remaining consumers in place for the next attempt.
     */
    private suspend fun restoreConsumers() {
        pruneCancelledWhileConsumedAgain()
        // Iterate over a snapshot: basicConsume() records the restored consumer under the tag the broker hands
        // back, which differs from the previous one for broker-assigned tags.
        consumedQueues.toMap().forEach { (previousKey, consumedQueue) ->
            val consumeOk = try {
                consumeAgain(consumedQueue)
            } catch (e: AMQPException.ChannelClosed) {
                // A consumer the broker cancelled, refused with 404 NOT_FOUND: its queue is gone, as when the consume
                // made again after the cancel is refused (see consumeAgainLocked), e.g. when several queues of the
                // channel are gone at once, or the connection dropped meanwhile. Forget the queue, so that the
                // restore does not consume it again in a loop, and tell its consumers. Any other consumer refused
                // with 404 keeps the path of a refused restore: its queue may only be unavailable.
                if (isGone(consumedQueue.queue, e) && consumersMutex.withLock { previousKey in brokerCancelledConsumers }) {
                    val cancelled = queueGone(consumedQueue.queue, e)
                    goneQueueForgotten = true
                    connection.messageListeningScope.launch { forwardCancellations(cancelled) }
                }
                throw e
            }
            consumedAgain(previousKey, consumeOk.consumerTag)
        }
    }

    /** Consumes the queue of [consumedQueue] again, with the settings it was consumed with. */
    private suspend fun consumeAgain(consumedQueue: ConsumedQueue): AMQPResponse.Channel.Basic.ConsumeOk = basicConsume(
        queue = consumedQueue.queue,
        consumerTag = consumedQueue.consumerTag,
        noAck = consumedQueue.noAck,
        exclusive = consumedQueue.exclusive,
        arguments = consumedQueue.arguments,
        onDelivery = consumedQueue.onDelivery,
        onCanceled = consumedQueue.onCanceled
    )

    /**
     * Forgets the consumers of [cancelledWhileConsumedAgain] that are no longer registered: no consume can be
     * consuming them again, since the caller holds [restoreLatch].
     */
    private suspend fun pruneCancelledWhileConsumedAgain() = consumersMutex.withLock {
        cancelledWhileConsumedAgain.retainAll { it in consumedQueues }
    }

    /**
     * Records that the consumer registered under [previousKey] runs again with [consumerTag], which [basicConsume]
     * has just registered: drops the previous entry if the broker generated a new tag, and remembers the tag the
     * application knows the consumer by. Returns false if the application cancelled the consumer meanwhile, and has
     * been told: the consumer that runs again is then cancelled, and not recorded.
     */
    private suspend fun consumedAgain(previousKey: Pair<String, String>, consumerTag: String): Boolean {
        val cancelledMeanwhile = consumersMutex.withLock {
            brokerCancelledConsumers.remove(previousKey)
            cancelledWhileConsumedAgain.remove(previousKey).also { cancelled ->
                if (!cancelled && consumerTag != previousKey.second) {
                    firstConsumerTags[consumerTag] = firstConsumerTags.remove(previousKey.second) ?: previousKey.second
                }
            }
        }
        if (cancelledMeanwhile) {
            consumedQueues.remove(Pair(previousKey.first, consumerTag))
            cancelSilently(consumerTag)
            return false
        }
        if (consumerTag != previousKey.second) consumedQueues.remove(previousKey)
        return true
    }

    /** Cancels the consumer [consumerTag] on the broker, without telling the application, which knows already. */
    private suspend fun cancelSilently(consumerTag: String) {
        cancelingMutex.withLock {
            cancelingConsumerTags += consumerTag
            silentlyCancelingConsumerTags += consumerTag
        }
        try {
            super.basicCancel(consumerTag)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            // The channel is closed: the broker has dropped the consumer with it, and nothing records it any more.
            logger.debug("Failed to cancel consumer $consumerTag on channel $id: ${e.message}")
        } finally {
            cancelingMutex.withLock {
                cancelingConsumerTags -= consumerTag
                silentlyCancelingConsumerTags -= consumerTag
            }
        }
    }

    private suspend fun removeConsumer(key: Pair<String, String>) {
        consumedQueues.remove(key)
        consumersMutex.withLock {
            brokerCancelledConsumers.remove(key)
            firstConsumerTags.remove(key.second)
        }
    }

    /** The tag the application received for the consumer that runs under [consumerTag]. */
    private suspend fun firstConsumerTag(consumerTag: String): String =
        consumersMutex.withLock { firstConsumerTags[consumerTag] } ?: consumerTag

    /**
     * Drops the consumer registered under [key], which is cancelled for good, and returns it with the cancellation to
     * forward to it, carrying the tag the application received.
     */
    private suspend fun cancelConsumer(key: Pair<String, String>): Pair<ConsumedQueue, AMQPResponse.Channel.Basic.Canceled>? {
        val consumedQueue = consumedQueues[key] ?: return null
        val canceled = AMQPResponse.Channel.Basic.Canceled(firstConsumerTag(key.second))
        removeConsumer(key)
        return consumedQueue to canceled
    }

    /**
     * Takes over, among [keys], the consumers the broker cancelled and that wait to be consumed again, which the
     * application is cancelling: a consume that may be consuming one again is cancelled once answered. Their listener
     * is gone with the broker's cancel, so the caller tells them. Returns them.
     */
    private suspend fun takeOverBrokerCancelled(keys: List<Pair<String, String>>): List<Pair<String, String>> =
        consumersMutex.withLock {
            keys.filter { brokerCancelledConsumers.remove(it) }.onEach { cancelledWhileConsumedAgain += it }
        }

    /**
     * Calls the cancellation callback of each consumer in [cancelled], which are cancelled for good. Not under
     * [restoreLatch]: the application may use the channel from its callbacks.
     */
    private suspend fun forwardCancellations(cancelled: Cancellations) {
        cancelled.forEach { (consumedQueue, response) ->
            try {
                consumedQueue.onCanceled(response)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                logger.error("Error in the cancellation of consumer ${response.consumerTag} on channel $id", e)
            }
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
            restoreLocked()
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
        // The broker cancels the consumers of a queue it deletes: that cancel, asked for by the application, must not
        // be mistaken for one to recover from. The consumers stay registered until the delete succeeds: a refused
        // delete closes the channel, and the restore that follows, which may run before this call fails, must
        // consume the queue again.
        cancelingMutex.withLock { deletingQueues[name] = (deletingQueues[name] ?: 0) + 1 }
        val deleted = try {
            super.queueDelete(name, ifUnused, ifEmpty)
        } finally {
            cancelingMutex.withLock {
                val count = deletingQueues.getValue(name) - 1
                if (count == 0) deletingQueues.remove(name) else deletingQueues[name] = count
            }
        }
        removeConsumers(name)
        declaredQueues.remove(name)
        serverNamedQueues.remove(name)
        return deleted
    }

    /**
     * Drops the consumers of [queue], which the application deletes. Those the broker cancelled already, which wait
     * to be consumed again, are told: their listener is gone with that cancel.
     */
    private suspend fun removeConsumers(queue: String) {
        val keys = consumedQueues.keys.filter { it.first == queue }
        val cancelled = takeOverBrokerCancelled(keys).mapNotNull { cancelConsumer(it) }
        keys.forEach { removeConsumer(it) }
        forwardCancellations(cancelled)
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
                if (response is AMQPResponse.Channel.Basic.Canceled) {
                    // Cancelled for a consumer the application had cancelled already, and been told.
                    if (cancelingMutex.withLock { response.consumerTag in silentlyCancelingConsumerTags }) {
                        return@basicConsume
                    }
                    // A consumer the broker cancelled (its queue became unavailable, e.g. the node that hosts it went
                    // down) is consumed again, like after a restore: the consumer must not see a cancellation either.
                    if (consumeAgainAfterBrokerCancel(queue, response)) return@basicConsume
                    // With the tag the application received, the only one it knows.
                    onCanceled(AMQPResponse.Channel.Basic.Canceled(firstConsumerTag(response.consumerTag)))
                    return@basicConsume
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

    /**
     * Schedules a new `basic.consume` for the consumer the broker cancelled with [canceled], if it is still
     * registered and the application did not cancel it itself. Returns whether it did.
     *
     * The queue is unavailable right now, so the consume waits for the reconnect delay first. A queue deleted and
     * declared again meanwhile is consumed again. A queue that is gone (the broker refuses the consume with 404
     * NOT_FOUND and closes the channel) is not consumed any more: the channel is restored without it, and the
     * cancellation is forwarded to its consumers, as when the robust channel did not consume queues again.
     */
    private suspend fun consumeAgainAfterBrokerCancel(
        queue: String,
        canceled: AMQPResponse.Channel.Basic.Canceled,
    ): Boolean {
        val askedForByTheApplication = cancelingMutex.withLock {
            canceled.consumerTag in cancelingConsumerTags || queue in deletingQueues
        }
        if (askedForByTheApplication) return false
        val key = Pair(queue, canceled.consumerTag)
        val consumedQueue = consumersMutex.withLock {
            consumedQueues[key]?.also { brokerCancelledConsumers += key }
        } ?: return false
        logger.warn("Consumer ${canceled.consumerTag} of queue $queue on channel $id cancelled by the broker, consuming it again")
        connection.messageListeningScope.launch {
            delay(connection.config.server.reconnectInitialDelay)
            // Under the restore latch, so that no restore runs meanwhile: if the broker refuses the consume, the
            // restore of the channel it closes is the one consumeAgainLocked() runs.
            val cancelled = restoreLatch.withLock {
                pruneCancelledWhileConsumedAgain()
                consumeAgainLocked(key, consumedQueue)
            }
            forwardCancellations(cancelled)
        }
        return true
    }

    /**
     * Consumes again the queue of [consumedQueue], registered under [key], after the broker cancelled it. Returns the
     * consumers cancelled for good, with the cancellation to forward to each. The caller holds [restoreLatch].
     */
    private suspend fun consumeAgainLocked(
        key: Pair<String, String>,
        consumedQueue: ConsumedQueue,
    ): Cancellations {
        // Cancelled by the application, or already consumed again by a restore, meanwhile.
        if (consumedQueues[key] !== consumedQueue) return emptyList()
        // Closed by the application meanwhile, or being closed: the consumer is gone with the channel.
        if (closedByTheApplication) return listOfNotNull(cancelConsumer(key))
        // The restore that follows consumes the queue again, since the consumer stays registered.
        if (state != ConnectionState.OPEN || connection.state != ConnectionState.OPEN) return emptyList()
        // The connection the consume is made on.
        val iteration = connection.currentIteration
        try {
            val consumeOk = consumeAgain(consumedQueue)
            if (consumedAgain(key, consumeOk.consumerTag)) {
                logger.info("Queue ${consumedQueue.queue} consumed again on channel $id after the broker cancelled its consumer")
            }
            return emptyList()
        } catch (e: CancellationException) {
            throw e
        } catch (e: AMQPException.ChannelClosed) {
            // Closed by the application during the consume.
            if (closedByTheApplication) return listOfNotNull(cancelConsumer(key))
            // No close of this channel by the broker armed the restore token (which no restore can have consumed,
            // since this holds the latch): the connection is going down, and the reconnect restores the channel and
            // the consumer.
            if (!pendingRestore) {
                logger.debug("Failed to consume queue ${consumedQueue.queue} again on channel $id: ${e.message}")
                return emptyList()
            }
            // The broker closed the channel. Refused with 404 NOT_FOUND, the queue is gone. Refused for another
            // reason, the consumer stays registered, and the restore consumes the queue again, or takes the path of
            // a refused restore.
            val cancelled = if (isGone(consumedQueue.queue, e)) queueGone(consumedQueue.queue, e) else emptyList()
            // The close is handled here: the triggers of the close that could not take the latch find the restore
            // token consumed, and skip it.
            pendingRestore = false
            if (connectionLost(iteration)) {
                // The connection is going down too, or already replaced: the reconnect restores the channel, without
                // the queue if it is gone. Restoring it here as well would open it twice on the new connection.
                logger.debug("Failed to consume queue ${consumedQueue.queue} again on channel $id: ${e.message}")
                return cancelled
            }
            // Restore the channel the broker closed.
            try {
                restoreLocked()
            } catch (restoreError: CancellationException) {
                throw restoreError
            } catch (restoreError: Exception) {
                logger.debug("Restore of channel $id after the refused consume failed: ${restoreError.message}")
            }
            return cancelled
        } catch (e: Exception) {
            // No reply in time, or the write failed: the broker may still register the consumer, and nothing else
            // would consume the queue again. Reconnect, as when a restore stalls: the broker drops whatever it
            // registered with the connection, and the reconnect restores the channel and the consumer, which stays
            // registered.
            // Only the connection the consume was made on: one that is already going down, or replaced by the
            // reconnect, is left alone.
            logger.warn("Failed to consume queue ${consumedQueue.queue} again on channel $id: ${e.message}")
            if (!connectionLost(iteration)) connection.forceReconnect()
            return emptyList()
        }
    }

    /**
     * Forgets [queue], which the broker no longer knows: its consumers on this channel, which the broker cancelled
     * with it, and the declare and bindings recorded for it, which the restore would replay, re-creating a queue
     * nobody consumes, or binding a missing queue and being refused in turn. Returns the consumers, with the
     * cancellation to forward to each.
     */
    private suspend fun queueGone(queue: String, e: AMQPException.ChannelClosed): Cancellations {
        logger.warn("Queue $queue on channel $id is gone (${e.replyText} (${e.replyCode})), its consumers are cancelled")
        val cancelled = consumedQueues.keys.filter { it.first == queue }.mapNotNull { cancelConsumer(it) }
        declaredQueues.remove(queue)
        boundQueues.keys.removeAll { it.first == queue }
        return cancelled
    }

    override suspend fun basicCancel(consumerTag: String): AMQPResponse.Channel.Basic.Canceled {
        // The application cancels a consumer with the tag it received; consumed again since, the consumer may run
        // under another tag the broker generated. The answer carries the tag the application knows.
        val canceled = AMQPResponse.Channel.Basic.Canceled(consumerTag)
        val currentConsumerTag = consumersMutex.withLock {
            firstConsumerTags.entries.find { it.value == consumerTag }?.key
        } ?: consumerTag
        // Entries are keyed by the broker-assigned tag.
        val key = consumedQueues.keys.find { it.second == currentConsumerTag }
        if (key != null && takeOverBrokerCancelled(listOf(key)).isNotEmpty()) {
            // The broker has cancelled this consumer already, and its listener is gone with that cancel: there is
            // nothing to cancel on the broker, and the robust channel tells the consumer, as the cancel of a running
            // consumer does before it returns.
            forwardCancellations(listOfNotNull(cancelConsumer(key)))
            return canceled
        }
        cancelingMutex.withLock { cancelingConsumerTags.add(currentConsumerTag) }
        try {
            super.basicCancel(currentConsumerTag)
            consumedQueues.keys.find { it.second == currentConsumerTag }?.let { removeConsumer(it) }
            return canceled
        } finally {
            cancelingMutex.withLock { cancelingConsumerTags.remove(currentConsumerTag) }
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

    // No-wait variants record exactly what their awaiting counterparts record. Without this, topology
    // declared through them would be missing from a restore and silently disappear on the first
    // reconnect. Restore itself keeps using the awaiting variants, since it has no one to report to.

    override suspend fun exchangeDeclareNoWait(
        name: String,
        type: String,
        durable: Boolean,
        autoDelete: Boolean,
        internal: Boolean,
        arguments: Table,
    ) {
        super.exchangeDeclareNoWait(name, type, durable, autoDelete, internal, arguments)
        if (!internal && restoreTopology) declaredExchanges[name] = DeclaredExchange(
            name = name,
            type = type,
            durable = durable,
            autoDelete = autoDelete,
            internal = internal,
            arguments = arguments
        )
    }

    override suspend fun exchangeDeleteNoWait(name: String, ifUnused: Boolean) {
        super.exchangeDeleteNoWait(name, ifUnused)
        declaredExchanges.remove(name)
    }

    override suspend fun exchangeBindNoWait(
        destination: String,
        source: String,
        routingKey: String,
        arguments: Table,
    ) {
        super.exchangeBindNoWait(destination, source, routingKey, arguments)
        if (restoreTopology) boundExchanges[Triple(destination, source, routingKey)] = BoundExchange(
            destination = destination,
            source = source,
            routingKey = routingKey,
            arguments = arguments
        )
    }

    override suspend fun exchangeUnbindNoWait(
        destination: String,
        source: String,
        routingKey: String,
        arguments: Table,
    ) {
        super.exchangeUnbindNoWait(destination, source, routingKey, arguments)
        boundExchanges.remove(Triple(destination, source, routingKey))
    }

    override suspend fun queueDeclareNoWait(
        name: String,
        durable: Boolean,
        exclusive: Boolean,
        autoDelete: Boolean,
        arguments: Table,
    ) {
        super.queueDeclareNoWait(name, durable, exclusive, autoDelete, arguments)
        // A no-wait declare always carries a name, so it can never be one of the server-named queues.
        if (restoreTopology) declaredQueues[name] = DeclaredQueue(
            name = name,
            durable = durable,
            exclusive = exclusive,
            autoDelete = autoDelete,
            arguments = arguments
        )
    }

    override suspend fun queueDeleteNoWait(
        name: String,
        ifUnused: Boolean,
        ifEmpty: Boolean,
    ) {
        // See queueDelete. With no reply, the consumers are dropped before the delete: its basic.cancel is forwarded,
        // and a refused delete cannot keep them.
        removeConsumers(name)
        super.queueDeleteNoWait(name, ifUnused, ifEmpty)
        declaredQueues.remove(name)
        serverNamedQueues.remove(name)
    }

    override suspend fun queueBindNoWait(
        queue: String,
        exchange: String,
        routingKey: String,
        arguments: Table,
    ) {
        super.queueBindNoWait(queue, exchange, routingKey, arguments)
        if (restoreTopology || queue in serverNamedQueues) {
            boundQueues[Triple(queue, exchange, routingKey)] = BoundQueue(
                queue = queue,
                exchange = exchange,
                routingKey = routingKey,
                arguments = arguments
            )
        }
    }

    override suspend fun confirmSelectNoWait() {
        super.confirmSelectNoWait()
        confirmModeRequested = true
    }

}
