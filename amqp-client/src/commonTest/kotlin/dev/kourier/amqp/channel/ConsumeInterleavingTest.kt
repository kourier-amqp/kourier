package dev.kourier.amqp.channel

import dev.kourier.amqp.AMQPResponse
import dev.kourier.amqp.connection.DefaultAMQPConnection
import dev.kourier.amqp.connection.amqpConfig
import kotlinx.coroutines.*
import kotlin.coroutines.CoroutineContext
import kotlin.test.*

/**
 * Orders of events that a real broker cannot produce on demand, driven step by step: the channel runs on a
 * connection that is never connected (frames written to it go nowhere), the responses of the broker are emitted
 * by the test, and every coroutine runs on a dispatcher that only runs its queue when the test asks.
 */
class ConsumeInterleavingTest {

    private class ManualDispatcher : CoroutineDispatcher() {
        private val queue = ArrayDeque<Runnable>()
        override fun dispatch(context: CoroutineContext, block: Runnable) {
            queue.addLast(block)
        }

        fun runAll() {
            while (queue.isNotEmpty()) queue.removeFirst().run()
        }
    }

    private val connectionDispatcher = ManualDispatcher()
    private val callerDispatcher = ManualDispatcher()
    private val connectionJob = SupervisorJob()
    private val callerJob = SupervisorJob()
    private val channel = DefaultAMQPChannel(
        connection = DefaultAMQPConnection(amqpConfig {}, CoroutineScope(connectionJob + connectionDispatcher)),
        id = 1u,
        frameMax = 4096u,
    )

    @AfterTest
    fun tearDown() {
        connectionJob.cancel()
        callerJob.cancel()
        connectionDispatcher.runAll()
        callerDispatcher.runAll()
    }

    /**
     * The broker confirms the consume and closes the channel right after. The listener of the consume starts
     * between the two: it sees the close before basicConsume got the consumer tag. The consume did succeed, so
     * the consumer must still be told the channel closed.
     */
    private fun consumeConfirmedThenChannelClosed(onCanceled: suspend (AMQPResponse.Channel) -> Unit): Deferred<AMQPResponse.Channel.Basic.ConsumeOk> {
        val consume = CoroutineScope(callerJob + callerDispatcher).async {
            channel.basicConsume(
                queue = "queue",
                consumerTag = "",
                noAck = true,
                exclusive = false,
                arguments = emptyMap(),
                onDelivery = {},
                onCanceled = onCanceled,
            )
        }
        // basic.consume is written, its RPC waits for the answer; the listener is launched but has not started.
        callerDispatcher.runAll()
        assertEquals(1, channel.channelResponses.subscriptionCount.value)

        // consume-ok reaches the RPC (resumed later), before the listener subscribes.
        assertTrue(channel.channelResponses.tryEmit(AMQPResponse.Channel.Basic.ConsumeOk(consumerTag = "tag")))
        connectionDispatcher.runAll()
        assertEquals(2, channel.channelResponses.subscriptionCount.value)

        // The close reaches the listener before basicConsume completes the consumer tag.
        val closed = AMQPResponse.Channel.Closed(channelId = 1u, replyCode = 406u, replyText = "PRECONDITION_FAILED")
        assertTrue(channel.channelResponses.tryEmit(closed))
        connectionDispatcher.runAll()

        // basicConsume returns, then whatever it resumed on the connection side runs.
        callerDispatcher.runAll()
        connectionDispatcher.runAll()
        assertTrue(consume.isCompleted)
        return consume
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun consumerConfirmedRightBeforeTheChannelClosesIsCanceled() {
        val canceled = mutableListOf<AMQPResponse.Channel>()
        val consume = consumeConfirmedThenChannelClosed { canceled += it }

        assertEquals("tag", consume.getCompleted().consumerTag)
        assertEquals(
            listOf<AMQPResponse.Channel>(
                AMQPResponse.Channel.Closed(channelId = 1u, replyCode = 406u, replyText = "PRECONDITION_FAILED")
            ),
            canceled
        )
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun failingOnCanceledOfAConsumerConfirmedRightBeforeTheChannelClosesIsContained() {
        var calls = 0
        val consume = consumeConfirmedThenChannelClosed {
            calls++
            error("onCanceled failed")
        }

        assertEquals("tag", consume.getCompleted().consumerTag)
        assertEquals(1, calls)
        assertTrue(connectionJob.isActive, "A failing onCanceled must not cancel the connection scope")
    }

    /**
     * The receive-channel basicConsume consumes from a coroutine of the connection scope. When that coroutine is
     * cancelled before the broker answers (the connection scope is cancelled), the caller must not wait forever.
     */
    @Test
    fun receiveChannelConsumeFailsWhenTheConnectionScopeIsCancelled() {
        val consume = CoroutineScope(callerJob + callerDispatcher).async {
            channel.basicConsume(queue = "queue", consumerTag = "", noAck = true, exclusive = false, arguments = emptyMap())
        }
        callerDispatcher.runAll()
        connectionDispatcher.runAll()
        // basic.consume is written and waits for its answer; its listener is subscribed.
        assertEquals(2, channel.channelResponses.subscriptionCount.value)

        connectionJob.cancel()
        connectionDispatcher.runAll()
        callerDispatcher.runAll()

        assertTrue(consume.isCompleted, "basicConsume did not return after its consume was cancelled")
        assertTrue(consume.isCancelled)
    }
}
