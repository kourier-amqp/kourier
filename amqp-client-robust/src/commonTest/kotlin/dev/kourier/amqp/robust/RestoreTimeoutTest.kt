package dev.kourier.amqp.robust

import dev.kourier.amqp.AMQPResponse
import dev.kourier.amqp.ChannelId
import dev.kourier.amqp.Frame
import dev.kourier.amqp.channel.AMQPChannel
import dev.kourier.amqp.connection.AMQPConfig
import dev.kourier.amqp.connection.amqpConfig
import io.ktor.utils.io.core.*
import kotlinx.coroutines.*
import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.Uuid

/**
 * A RobustAMQPChannel whose basicQos hangs forever when called inside a restore context,
 * to simulate a broker that accepts a restore frame but never replies (overload, network blackhole, etc.).
 */
private class HangingRestoreChannel(
    connection: HangingRestoreConnection,
    id: ChannelId,
    frameMax: UInt,
) : RobustAMQPChannel(connection, id, frameMax) {

    val restoreEnteredBasicQos = CompletableDeferred<Unit>()

    override suspend fun basicQos(count: UShort, global: Boolean): AMQPResponse.Channel.Basic.QosOk {
        if (currentChannelRestoreContext() != null) {
            restoreEnteredBasicQos.complete(Unit)
            // Mimic a broker that never replies: hang until cancelled.
            awaitCancellation()
        }
        return super.basicQos(count, global)
    }
}

private class HangingRestoreConnection(
    config: AMQPConfig,
    scope: CoroutineScope,
) : RobustAMQPConnection(config, scope) {

    public override suspend fun connect() = super.connect()

    override fun createChannel(id: ChannelId, frameMax: UInt): AMQPChannel =
        HangingRestoreChannel(this, id, frameMax)
}

class RestoreTimeoutTest {

    /**
     * Test A: when restore hangs on a synthetic basicQos that never returns, a concurrent publish
     * on the same channel must not block beyond the configured restore timeout. Today: blocks forever.
     */
    @Test
    fun publishMustNotBlockForeverWhenRestoreHangs(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val config = amqpConfig {
            server {
                restoreTimeout = 500.milliseconds
            }
        }
        val connection = HangingRestoreConnection(config, scope)
        try {
            connection.connect()
            val channel = connection.openChannel() as HangingRestoreChannel

            // Record QoS so it's part of the restore plan
            channel.basicQos(count = 1u)

            // Drop the connection so the robust factory tries to reconnect & restore.
            // Heartbeat on a non-zero channel forces broker to close the connection.
            connection.write(Frame(channelId = 1u, payload = Frame.Heartbeat))

            // Wait for restore to enter the hanging basicQos, with a sane outer cap.
            withTimeout(10.seconds) { channel.restoreEnteredBasicQos.await() }

            // Now: try to publish. With the bug, this blocks forever waiting on restoreCompleted.
            // After the fix: publish should return / throw within ~restoreTimeout.
            // Discriminating try/catch (not runCatching) so CancellationException from the
            // surrounding withTimeoutOrNull propagates and we don't silently swallow timeouts.
            val publishOutcome: Result<Unit>? = withTimeoutOrNull(5.seconds) {
                try {
                    channel.basicPublish("hello".toByteArray(), exchange = "", routingKey = "no-op-${Uuid.random()}")
                    Result.success(Unit)
                } catch (e: Exception) {
                    Result.failure<Unit>(e)
                }
            }
            assertNotNull(
                publishOutcome,
                "Publish must not hang past 5s — restore must time out and unblock writers"
            )
        } finally {
            runCatching { withTimeout(5.seconds) { connection.close() } }
            scope.cancel()
        }
    }

    /**
     * Test B: after the restore timeout fires, the connection must be torn down and reconnect
     * attempted again. We observe this via a SECOND post-drop Connection.Connected event:
     *   - Drop 1 -> reconnect 1 (restore enters and hangs)
     *   - Restore times out -> connection torn down -> reconnect 2 (a fresh socket, fresh channel)
     */
    @Test
    fun restoreTimeoutTearsDownConnectionAndReconnects(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val config = amqpConfig {
            server {
                restoreTimeout = 300.milliseconds
            }
        }
        val connection = HangingRestoreConnection(config, scope)
        try {
            connection.connect()
            val channel = connection.openChannel() as HangingRestoreChannel
            channel.basicQos(count = 1u)

            // We will see two more Connection.Connected events after the initial connect:
            // one for the post-drop reconnect, one for the post-timeout reconnect.
            val postDropReconnects = mutableListOf<AMQPResponse.Connection.Connected>()
            val collectorJob = scope.launch {
                connection.openedResponses.collect { postDropReconnects.add(it) }
            }

            // Drop the connection
            connection.write(Frame(channelId = 1u, payload = Frame.Heartbeat))

            withTimeout(10.seconds) { channel.restoreEnteredBasicQos.await() }

            // Wait for the SECOND post-drop reconnect (i.e. the timeout-driven one).
            // Today (bug): never happens — restore() blocks forever, the loop never iterates.
            withTimeout(5.seconds) {
                while (postDropReconnects.size < 2) {
                    delay(50)
                }
            }

            collectorJob.cancel()
            assertTrue(
                postDropReconnects.size >= 2,
                "Expected at least 2 reconnect events after drop; saw ${postDropReconnects.size}"
            )
        } finally {
            runCatching { withTimeout(5.seconds) { connection.close() } }
            scope.cancel()
        }
    }

    /**
     * Test C: a writer that was already awaiting restoreCompleted when the timeout fires
     * must wake up with an exception, not silently hang.
     */
    @Test
    fun pendingWriterReceivesExceptionOnRestoreTimeout(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val config = amqpConfig {
            server {
                restoreTimeout = 400.milliseconds
            }
        }
        val connection = HangingRestoreConnection(config, scope)
        try {
            connection.connect()
            val channel = connection.openChannel() as HangingRestoreChannel
            channel.basicQos(count = 1u)

            // Drop the connection
            connection.write(Frame(channelId = 1u, payload = Frame.Heartbeat))

            withTimeout(10.seconds) { channel.restoreEnteredBasicQos.await() }

            // Start a publish AFTER restore is hanging — restoreCompleted is uncompleted.
            // Discriminating try/catch (not runCatching) so CancellationException propagates
            // instead of being captured as a Result.failure.
            val publishJob = scope.async {
                try {
                    channel.basicPublish("payload".toByteArray(), exchange = "", routingKey = "x")
                    Result.success(Unit)
                } catch (e: Exception) {
                    Result.failure<Unit>(e)
                }
            }

            // The publish should resolve within ~restoreTimeout + a small slack.
            val outcome = withTimeoutOrNull(3.seconds) { publishJob.await() }
            assertNotNull(outcome, "Publish should not hang past 3s after restore times out")
            assertTrue(
                outcome.isFailure,
                "Publish must surface the restore failure to the caller, but got success: $outcome"
            )
            val cause = outcome.exceptionOrNull()
            assertTrue(
                cause is RestoreTimeoutException,
                "Publish must fail with RestoreTimeoutException, got: ${cause?.let { it::class.simpleName }} — $cause"
            )
        } finally {
            runCatching { withTimeout(5.seconds) { connection.close() } }
            scope.cancel()
        }
    }

    /**
     * Test D (Fix 4 — concurrent restore): with N channels that all hang in restore,
     * the restore launches must run concurrently rather than serialize on the first
     * channel's hanging call.
     *
     * We assert this structurally: every channel must enter its hanging `basicQos`
     * (inside its own restore context) before the first `restoreTimeout` fires.
     * This is sufficient because a sequential restore loop would `await` channel 1's
     * restore — which never returns — and channels 2..N would never get the chance
     * to enter `basicQos`, so [HangingRestoreChannel.restoreEnteredBasicQos] for those
     * channels would never complete and the outer `withTimeout` would fail.
     *
     * Wall-clock bounds on the post-timeout reconnect were intentionally dropped:
     * they were flaky on busy CI machines (observed 1.07s against a 1s budget) and
     * the structural check above already discriminates parallel vs. sequential.
     */
    @Test
    fun multipleChannelsRestoreInParallel(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val restoreTimeout = 500.milliseconds
        val numHangingChannels = 3
        val config = amqpConfig {
            server {
                this.restoreTimeout = restoreTimeout
            }
        }
        val connection = HangingRestoreConnection(config, scope)
        try {
            connection.connect()

            // Open three channels. Each will record QoS so it's part of every channel's restore plan.
            val channels = (1..numHangingChannels).map {
                (connection.openChannel() as HangingRestoreChannel).also { ch ->
                    ch.basicQos(count = 1u)
                }
            }

            // Drop the connection
            connection.write(Frame(channelId = 1u, payload = Frame.Heartbeat))

            // Structural concurrency assertion: ALL channels must enter their hanging
            // basicQos inside the restore context before the first restoreTimeout fires.
            // Sequential restore would deadlock awaiting channel 1's restore() and the
            // remaining channels' deferreds would never complete — this withTimeout would
            // then fail with a TimeoutCancellationException.
            withTimeout(restoreTimeout) {
                channels.forEach { it.restoreEnteredBasicQos.await() }
            }
        } finally {
            runCatching { withTimeout(5.seconds) { connection.close() } }
            scope.cancel()
        }
    }
}
