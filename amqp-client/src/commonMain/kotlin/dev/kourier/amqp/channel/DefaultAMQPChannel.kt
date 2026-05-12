package dev.kourier.amqp.channel

import dev.kourier.amqp.*
import dev.kourier.amqp.connection.ConnectionState
import dev.kourier.amqp.connection.DefaultAMQPConnection
import io.ktor.util.logging.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.concurrent.Volatile

open class DefaultAMQPChannel(
    open val connection: DefaultAMQPConnection,
    override val id: ChannelId,
    val frameMax: UInt,
) : AMQPChannel {

    protected val logger = KtorSimpleLogger("AMQPChannel")

    override var isConfirmMode: Boolean = false
    override var isTxMode: Boolean = false

    private val deliveryTagMutex = Mutex()
    private var deliveryTag: ULong = 1u

    @Volatile
    override var state = ConnectionState.CLOSED
    private val stateMutex = Mutex()

    @InternalAmqpApi
    val writeMutex = Mutex()

    @InternalAmqpApi
    var nextMessage: PartialDelivery? = null

    @InternalAmqpApi
    val channelResponses = MutableSharedFlow<AMQPResponse>(extraBufferCapacity = Channel.UNLIMITED)

    // Active consumer listening jobs. `cancelAll` joins these so that callers of `close()`
    // observe the consumer's `onCanceled` side effects (e.g. closing a produce channel)
    // before close() returns, instead of racing the listener coroutine on Dispatchers.Default.
    // Guarded by listeningJobsMutex; jobs auto-deregister via invokeOnCompletion.
    //
    // `listeningJobsByConsumer` (broker-assigned consumerTag -> listeningJob) lets
    // `basicCancel(consumerTag)` join the specific consumer's listener after CancelOk
    // arrives, so the user's `onCanceled` callback has run by the time basicCancel returns.
    // Without this, basicCancel races the listener's `channelResponses.collect{}` on the
    // same Channel.Basic.Canceled emission — both fire concurrently, basicCancel returns
    // before the listener calls onCanceled, and a caller immediately observing side
    // effects (e.g. `assertTrue(receiveChannel.isClosedForReceive)`) can see them
    // as not-yet-applied.
    private val listeningJobsMutex = Mutex()
    private val listeningJobs = mutableSetOf<Job>()
    private val listeningJobsByConsumer = mutableMapOf<String, Job>()

    override val channelClosed = CompletableDeferred<AMQPException.ChannelClosed>()

    override val openedResponses: Flow<AMQPResponse.Channel.Opened> =
        channelResponses.filterIsInstance<AMQPResponse.Channel.Opened>()

    override val closedResponses: Flow<AMQPResponse.Channel.Closed> =
        channelResponses.filterIsInstance<AMQPResponse.Channel.Closed>()

    override val publishConfirmResponses: Flow<AMQPResponse.Channel.Basic.PublishConfirm> =
        channelResponses.filterIsInstance<AMQPResponse.Channel.Basic.PublishConfirm>()

    override val returnResponses: Flow<AMQPResponse.Channel.Message.Return> =
        channelResponses.filterIsInstance<AMQPResponse.Channel.Message.Return>()

    override val flowResponses: Flow<AMQPResponse.Channel.Flowed> =
        channelResponses.filterIsInstance<AMQPResponse.Channel.Flowed>()

    /**
     * Hook called by [writeAndWaitForResponse] *before* acquiring [writeMutex]. Subclasses can
     * override to perform any waiting that must NOT happen while the mutex is held — e.g.
     * `RobustAMQPChannel` awaits its `restoreCompleted` deferred here, because doing so inside
     * the mutex would deadlock against restore's own writes (which need the same mutex).
     */
    @InternalAmqpApi
    open suspend fun prepareForWrite() {
    }

    @InternalAmqpApi
    override suspend fun write(vararg frames: Frame) {
        if (channelClosed.isCompleted) throw channelClosed.await()
        prepareForWrite()
        connection.write(*frames)
    }

    @InternalAmqpApi
    suspend inline fun <reified T : AMQPResponse> writeAndWaitForResponse(vararg frames: Frame): T {
        // Fast-fail on closed channel before any other work — same check that lives in [write],
        // duplicated here because we bypass the virtual `write` inside the mutex below.
        if (channelClosed.isCompleted) throw channelClosed.await()
        prepareForWrite()
        val firstResponse = writeMutex.withLock { // Ensure the response is synchronized with the write operation
            // Subscribe BEFORE writing: `channelResponses` is a SharedFlow with replay=0,
            // so any response emitted before our subscription would be lost. `onSubscription`
            // must be applied directly to the SharedFlow (it isn't defined for plain Flow),
            // so attach it first and filter afterwards. The action runs only after the
            // subscription is fully wired up.
            //
            // Call `connection.write` directly (not the virtual `write`) so an overridden
            // `write` in a subclass (e.g. RobustAMQPChannel awaiting restoreCompleted) can't
            // re-enter the mutex chain and deadlock against an in-progress restore. The
            // pre-mutex `prepareForWrite()` hook handles any required wait state.
            channelResponses
                .onSubscription { connection.write(*frames) }
                .filter { it is T || it is AMQPResponse.Channel.Closed }
                .first()
        }
        if (firstResponse is T) return firstResponse
        if (firstResponse is AMQPResponse.Channel.Closed) throw AMQPException.ChannelClosed(
            replyCode = firstResponse.replyCode,
            replyText = firstResponse.replyText
        ).also { cancelAll(it) }
        error("Expected response of type ${T::class}, but got ${firstResponse::class}")
    }

    /**
     * Determines whether this channel should be removed from the connection's channel registry
     * when the broker closes it. Robust channels override this to return false since they restore.
     */
    @InternalAmqpApi
    open fun shouldRemoveOnBrokerClose(): Boolean = true

    open suspend fun cancelAll(channelClosed: AMQPException.ChannelClosed) {
        if (state == ConnectionState.CLOSED) return // Already closed
        // Complete the channelClosed deferred BEFORE flipping state to CLOSED. Reason: the
        // RobustAMQPChannel.basicConsume listener wrapper uses `state == SHUTTING_DOWN ||
        // channelClosed.isCompleted` to decide whether a Channel.Closed event is terminal (and
        // therefore worth forwarding to the user's onCanceled). If we set state=CLOSED first,
        // there's a window where state has moved past SHUTTING_DOWN but channelClosed isn't
        // completed yet — the wrapper sees both as false and incorrectly skips onCanceled,
        // leaving the consumer's produce-channel open. Completing the deferred first preserves
        // the monotonic guarantee documented in the wrapper.
        logger.debug("Channel $id closed: ${channelClosed.replyText} (${channelClosed.replyCode})")
        this@DefaultAMQPChannel.channelClosed.complete(channelClosed)
        this.state = ConnectionState.CLOSED
        // Snapshot then join: listener coroutines self-cancel upon receiving Channel.Closed,
        // but their `onCanceled` callbacks (e.g. closing a produce channel) run AFTER the
        // SharedFlow emit returned to close()'s writeAndWaitForResponse. Joining here makes
        // close() synchronous with consumer-side cleanup, so e.g. `channel.basicConsume(...)`
        // receive-channels are guaranteed closed by the time close() returns.
        listeningJobsMutex.withLock { listeningJobs.toList() }.joinAll()
    }

    override suspend fun open(): AMQPResponse.Channel.Opened = stateMutex.withLock {
        if (state == ConnectionState.OPEN) return AMQPResponse.Channel.Opened(channelId = id)
        val channelOpen = Frame(
            channelId = id,
            payload = Frame.Method.Channel.Open(
                reserved1 = ""
            )
        )
        connection.channels.add(this)
        return writeAndWaitForResponse<AMQPResponse.Channel.Opened>(channelOpen).also {
            state = ConnectionState.OPEN
            logger.debug("Channel $id opened")
        }
    }

    override suspend fun close(
        reason: String,
        code: UShort,
    ): AMQPResponse.Channel.Closed {
        this.state = ConnectionState.SHUTTING_DOWN
        val close = Frame(
            channelId = id,
            payload = Frame.Method.Channel.Close(
                replyCode = code,
                replyText = reason,
                classId = 0u,
                methodId = 0u,
            )
        )
        return writeAndWaitForResponse<AMQPResponse.Channel.Closed>(close).also {
            cancelAll(
                AMQPException.ChannelClosed(
                    replyCode = code,
                    replyText = reason,
                    isInitiatedByApplication = true
                )
            )
        }
    }

    override suspend fun basicPublish(
        body: ByteArray,
        exchange: String,
        routingKey: String,
        mandatory: Boolean,
        immediate: Boolean,
        properties: Properties,
    ): AMQPResponse.Channel.Basic.Published {
        val publish = Frame.Method.Basic.Publish(
            reserved1 = 0u,
            exchange = exchange,
            routingKey = routingKey,
            mandatory = mandatory,
            immediate = immediate
        )
        val classID = publish.kind.value
        val header = Frame.Header(
            classID = classID,
            weight = 0u,
            bodySize = body.size.toULong(),
            properties = properties
        )

        val payloads = mutableListOf<Frame.Payload>()
        payloads.add(publish)
        payloads.add(header)
        when {
            body.isEmpty() -> {} // Do not send body
            body.size <= frameMax.toInt() -> payloads.add(Frame.Body(body)) // Send all at once
            else -> { // Split body into multiple frames
                var offset = 0
                while (offset < body.size) {
                    val length = minOf(frameMax.toInt(), body.size - offset)
                    val slice = body.copyOfRange(offset, offset + length)
                    payloads.add(Frame.Body(slice))
                    offset += length
                }
            }
        }

        write(*payloads.map { Frame(channelId = id, payload = it) }.toTypedArray())

        return if (isConfirmMode) {
            val count = deliveryTagMutex.withLock { deliveryTag++ }
            AMQPResponse.Channel.Basic.Published(deliveryTag = count)
        } else {
            AMQPResponse.Channel.Basic.Published(deliveryTag = 0u)
        }
    }

    override suspend fun basicGet(
        queue: String,
        noAck: Boolean,
    ): AMQPResponse.Channel.Message.Get {
        val get = Frame(
            channelId = id,
            payload = Frame.Method.Basic.Get(
                reserved1 = 0u,
                queue = queue,
                noAck = noAck
            )
        )
        return writeAndWaitForResponse(get)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    override suspend fun basicConsume(
        queue: String,
        consumerTag: String,
        noAck: Boolean,
        exclusive: Boolean,
        arguments: Table,
    ): AMQPReceiveChannel {
        val deferredConsumeOk = CompletableDeferred<AMQPResponse.Channel.Basic.ConsumeOk>()
        val receiveChannel = connection.messageListeningScope.produce(capacity = Channel.UNLIMITED) {
            val consumeOk = basicConsume(
                queue = queue,
                consumerTag = consumerTag,
                noAck = noAck,
                exclusive = exclusive,
                arguments = arguments,
                onDelivery = { trySend(it) },
                onCanceled = { close() }
            )
            deferredConsumeOk.complete(consumeOk)
            awaitClose {
                runBlocking {
                    if (state != ConnectionState.OPEN) return@runBlocking
                    val cancel = Frame(
                        channelId = id,
                        payload = Frame.Method.Basic.Cancel(
                            consumerTag = consumeOk.consumerTag,
                            noWait = true
                        )
                    )
                    write(cancel)
                }
            }
        }
        return AMQPReceiveChannel(
            consumeOk = deferredConsumeOk.await(),
            receiveChannel = receiveChannel,
        )
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
        val deferredConsumerTag = CompletableDeferred<String>()
        val deferredListeningJob = CompletableDeferred<Job>()
        val listeningJob = connection.messageListeningScope.launch {
            channelResponses.collect { response ->
                runCatching { // Catch to avoid cancelling messageListeningScope
                    val consumerTag = deferredConsumerTag.await()
                    when (response) {
                        is AMQPResponse.Channel.Closed -> {
                            deferredListeningJob.await().cancel()
                            logger.debug("Consumer $consumerTag on channel $id canceled due to channel closed")
                            onCanceled(response)
                        }

                        is AMQPResponse.Channel.Basic.Canceled -> if (response.consumerTag == consumerTag) {
                            deferredListeningJob.await().cancel()
                            logger.debug("Consumer $consumerTag on channel $id canceled")
                            onCanceled(response)
                        }

                        is AMQPResponse.Channel.Message.Delivery -> if (response.consumerTag == consumerTag) launch {
                            runCatching {
                                logger.debug("Consumer $consumerTag on channel $id received delivery ${response.message.deliveryTag}")
                                onDelivery(response)
                            }.onFailure { exception ->
                                logger.error(
                                    "Error processing delivery ${response.message.deliveryTag} on channel $id",
                                    exception
                                )
                            }
                        }

                        else -> {} // Ignore other responses
                    }
                }.onFailure { exception ->
                    logger.error("Error in consumer $consumerTag on channel $id", exception)
                }
            }
        }
        // Track the listening job so that cancelAll can await its completion before close()
        // returns, ensuring consumer cleanup (e.g. closing a produce channel) is observable
        // synchronously by close()'s caller. Auto-deregister when the job completes (whether
        // by self-cancel on Channel.Closed or by external cancellation).
        listeningJobsMutex.withLock { listeningJobs.add(listeningJob) }
        listeningJob.invokeOnCompletion {
            // invokeOnCompletion runs on the completing coroutine context; use launch to avoid
            // requiring suspend semantics here (mutex.withLock requires suspension).
            connection.messageListeningScope.launch {
                listeningJobsMutex.withLock {
                    listeningJobs.remove(listeningJob)
                    // Also drop any per-consumer-tag entry pointing at this job. We don't know
                    // the tag here (registration is by-value), so scan; the map is small (one
                    // entry per active consumer on the channel).
                    listeningJobsByConsumer.entries.removeAll { it.value === listeningJob }
                }
            }
        }
        deferredListeningJob.complete(listeningJob)
        val consume = Frame(
            channelId = id,
            payload = Frame.Method.Basic.Consume(
                reserved1 = 0u,
                queue = queue,
                consumerTag = consumerTag,
                noLocal = false,
                noAck = noAck,
                exclusive = exclusive,
                noWait = false,
                arguments = arguments
            )
        )
        val result = writeAndWaitForResponse<AMQPResponse.Channel.Basic.ConsumeOk>(consume)
        deferredConsumerTag.complete(result.consumerTag)
        // Index by broker-confirmed consumerTag so basicCancel can join the right listener.
        listeningJobsMutex.withLock { listeningJobsByConsumer[result.consumerTag] = listeningJob }
        logger.debug("Consumer ${result.consumerTag} on channel $id started")
        return result
    }

    override suspend fun basicCancel(
        consumerTag: String,
    ): AMQPResponse.Channel.Basic.Canceled {
        val cancel = Frame(
            channelId = id,
            payload = Frame.Method.Basic.Cancel(
                consumerTag = consumerTag,
                noWait = false
            )
        )
        val result = writeAndWaitForResponse<AMQPResponse.Channel.Basic.Canceled>(cancel)
        // Wait for the listening job to fully process the Channel.Basic.Canceled emission —
        // which is what invokes user's onCanceled (e.g. closes the receive channel of the
        // convenience basicConsume(...) -> AMQPReceiveChannel overload). Without this join,
        // basicCancel races the listener's collect{} on the same SharedFlow emission and
        // returns first, so callers immediately observing side effects can see them as not
        // yet applied. The listener self-cancels inside its Channel.Basic.Canceled handler
        // (after calling onCanceled), so `join()` returns as soon as that branch finishes.
        val listener = listeningJobsMutex.withLock { listeningJobsByConsumer.remove(consumerTag) }
        listener?.join()
        return result
    }

    override suspend fun basicAck(
        deliveryTag: ULong,
        multiple: Boolean,
    ) {
        val ack = Frame(
            channelId = id,
            payload = Frame.Method.Basic.Ack(
                deliveryTag = deliveryTag,
                multiple = multiple
            )
        )
        return write(ack)
    }

    override suspend fun basicAck(
        message: AMQPMessage,
        multiple: Boolean,
    ) {
        return basicAck(message.deliveryTag, multiple)
    }

    override suspend fun basicNack(
        deliveryTag: ULong,
        multiple: Boolean,
        requeue: Boolean,
    ) {
        val nack = Frame(
            channelId = id,
            payload = Frame.Method.Basic.Nack(
                deliveryTag = deliveryTag,
                multiple = multiple,
                requeue = requeue
            )
        )
        return write(nack)
    }

    override suspend fun basicNack(
        message: AMQPMessage,
        multiple: Boolean,
        requeue: Boolean,
    ) {
        return basicNack(message.deliveryTag, multiple, requeue)
    }

    override suspend fun basicReject(
        deliveryTag: ULong,
        requeue: Boolean,
    ) {
        val reject = Frame(
            channelId = id,
            payload = Frame.Method.Basic.Reject(
                deliveryTag = deliveryTag,
                requeue = requeue
            )
        )
        return write(reject)
    }

    override suspend fun basicReject(
        message: AMQPMessage,
        requeue: Boolean,
    ) {
        return basicReject(message.deliveryTag, requeue)
    }

    override suspend fun basicRecover(
        requeue: Boolean,
    ): AMQPResponse.Channel.Basic.Recovered {
        val recover = Frame(
            channelId = id,
            payload = Frame.Method.Basic.Recover(
                requeue = requeue
            )
        )
        return writeAndWaitForResponse(recover)
    }

    override suspend fun basicQos(
        count: UShort,
        global: Boolean,
    ): AMQPResponse.Channel.Basic.QosOk {
        val qos = Frame(
            channelId = id,
            payload = Frame.Method.Basic.Qos(
                prefetchSize = 0u,
                prefetchCount = count,
                global = global
            )
        )
        return writeAndWaitForResponse(qos)
    }

    override suspend fun flow(active: Boolean): AMQPResponse.Channel.Flowed {
        val flow = Frame(
            channelId = id,
            payload = Frame.Method.Channel.Flow(
                active = active
            )
        )
        return writeAndWaitForResponse(flow)
    }

    override suspend fun queueDeclare(
        name: String,
        durable: Boolean,
        exclusive: Boolean,
        autoDelete: Boolean,
        arguments: Table,
    ): AMQPResponse.Channel.Queue.Declared {
        val declare = Frame(
            channelId = id,
            payload = Frame.Method.Queue.Declare(
                reserved1 = 0u,
                queueName = name,
                passive = false,
                durable = durable,
                exclusive = exclusive,
                autoDelete = autoDelete,
                noWait = false,
                arguments = arguments
            )
        )
        return writeAndWaitForResponse(declare)
    }

    override suspend fun queueDeclarePassive(name: String): AMQPResponse.Channel.Queue.Declared {
        val declare = Frame(
            channelId = id,
            payload = Frame.Method.Queue.Declare(
                reserved1 = 0u,
                queueName = name,
                passive = true,
                durable = false,
                exclusive = true,
                autoDelete = true,
                noWait = false,
                arguments = emptyMap()
            )
        )
        return writeAndWaitForResponse(declare)
    }

    override suspend fun queueDeclare(): AMQPResponse.Channel.Queue.Declared {
        return queueDeclare(
            name = "",
            durable = false,
            exclusive = true,
            autoDelete = true,
        )
    }

    override suspend fun messageCount(name: String): UInt {
        return queueDeclarePassive(name).messageCount
    }

    override suspend fun consumerCount(name: String): UInt {
        return queueDeclarePassive(name).consumerCount
    }

    override suspend fun queueDelete(
        name: String,
        ifUnused: Boolean,
        ifEmpty: Boolean,
    ): AMQPResponse.Channel.Queue.Deleted {
        val delete = Frame(
            channelId = id,
            payload = Frame.Method.Queue.Delete(
                reserved1 = 0u,
                queueName = name,
                ifUnused = ifUnused,
                ifEmpty = ifEmpty,
                noWait = false
            )
        )
        return writeAndWaitForResponse(delete)
    }

    override suspend fun queuePurge(
        name: String,
    ): AMQPResponse.Channel.Queue.Purged {
        val purge = Frame(
            channelId = id,
            payload = Frame.Method.Queue.Purge(
                reserved1 = 0u,
                queueName = name,
                noWait = false
            )
        )
        return writeAndWaitForResponse(purge)
    }

    override suspend fun queueBind(
        queue: String,
        exchange: String,
        routingKey: String,
        arguments: Table,
    ): AMQPResponse.Channel.Queue.Bound {
        val bind = Frame(
            channelId = id,
            payload = Frame.Method.Queue.Bind(
                reserved1 = 0u,
                queueName = queue,
                exchangeName = exchange,
                routingKey = routingKey,
                noWait = false,
                arguments = arguments
            )
        )
        return writeAndWaitForResponse(bind)
    }

    override suspend fun queueUnbind(
        queue: String,
        exchange: String,
        routingKey: String,
        arguments: Table,
    ): AMQPResponse.Channel.Queue.Unbound {
        val unbind = Frame(
            channelId = id,
            payload = Frame.Method.Queue.Unbind(
                reserved1 = 0u,
                queueName = queue,
                exchangeName = exchange,
                routingKey = routingKey,
                arguments = arguments
            )
        )
        return writeAndWaitForResponse(unbind)
    }

    override suspend fun exchangeDeclare(
        name: String,
        type: String,
        durable: Boolean,
        autoDelete: Boolean,
        internal: Boolean,
        arguments: Table,
    ): AMQPResponse.Channel.Exchange.Declared {
        val declare = Frame(
            channelId = id,
            payload = Frame.Method.Exchange.Declare(
                reserved1 = 0u,
                exchangeName = name,
                exchangeType = type,
                passive = false,
                durable = durable,
                autoDelete = autoDelete,
                internal = internal,
                noWait = false,
                arguments = arguments
            )
        )
        return writeAndWaitForResponse(declare)
    }

    override suspend fun exchangeDeclarePassive(name: String): AMQPResponse.Channel.Exchange.Declared {
        val declare = Frame(
            channelId = id,
            payload = Frame.Method.Exchange.Declare(
                reserved1 = 0u,
                exchangeName = name,
                exchangeType = "",
                passive = true,
                durable = false,
                autoDelete = false,
                internal = false,
                noWait = false,
                arguments = emptyMap()
            )
        )
        return writeAndWaitForResponse(declare)
    }

    override suspend fun exchangeDelete(
        name: String,
        ifUnused: Boolean,
    ): AMQPResponse.Channel.Exchange.Deleted {
        val delete = Frame(
            channelId = id,
            payload = Frame.Method.Exchange.Delete(
                reserved1 = 0u,
                exchangeName = name,
                ifUnused = ifUnused,
                noWait = false
            )
        )
        return writeAndWaitForResponse(delete)
    }

    override suspend fun exchangeBind(
        destination: String,
        source: String,
        routingKey: String,
        arguments: Table,
    ): AMQPResponse.Channel.Exchange.Bound {
        val bind = Frame(
            channelId = id,
            payload = Frame.Method.Exchange.Bind(
                reserved1 = 0u,
                destination = destination,
                source = source,
                routingKey = routingKey,
                noWait = false,
                arguments = arguments
            )
        )
        return writeAndWaitForResponse(bind)
    }

    override suspend fun exchangeUnbind(
        destination: String,
        source: String,
        routingKey: String,
        arguments: Table,
    ): AMQPResponse.Channel.Exchange.Unbound {
        val unbind = Frame(
            channelId = id,
            payload = Frame.Method.Exchange.Unbind(
                reserved1 = 0u,
                destination = destination,
                source = source,
                routingKey = routingKey,
                noWait = false,
                arguments = arguments
            )
        )
        return writeAndWaitForResponse(unbind)
    }

    override suspend fun confirmSelect(): AMQPResponse.Channel.Confirm.Selected {
        if (isConfirmMode) return AMQPResponse.Channel.Confirm.Selected
        val select = Frame(
            channelId = id,
            payload = Frame.Method.Confirm.Select(
                noWait = false
            )
        )
        return writeAndWaitForResponse<AMQPResponse.Channel.Confirm.Selected>(select).also {
            isConfirmMode = true
        }
    }

    override suspend fun txSelect(): AMQPResponse.Channel.Tx.Selected {
        if (isTxMode) return AMQPResponse.Channel.Tx.Selected
        val select = Frame(
            channelId = id,
            payload = Frame.Method.Tx.Select
        )
        return writeAndWaitForResponse<AMQPResponse.Channel.Tx.Selected>(select).also {
            isTxMode = true
        }
    }

    override suspend fun txCommit(): AMQPResponse.Channel.Tx.Committed {
        val commit = Frame(
            channelId = id,
            payload = Frame.Method.Tx.Commit
        )
        return writeAndWaitForResponse(commit)
    }

    override suspend fun txRollback(): AMQPResponse.Channel.Tx.Rollbacked {
        val rollback = Frame(
            channelId = id,
            payload = Frame.Method.Tx.Rollback
        )
        return writeAndWaitForResponse(rollback)
    }

}
