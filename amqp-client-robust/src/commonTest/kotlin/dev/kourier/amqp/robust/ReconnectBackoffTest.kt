package dev.kourier.amqp.robust

import dev.kourier.amqp.connection.AMQPConfig
import dev.kourier.amqp.connection.amqpConfig
import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * Test subclass that intercepts the underlying socket-level connect and records the
 * monotonic timestamp of every attempt. By default every call throws [SimulatedConnectFailure],
 * which connectionFactory catches and (with the fix) follows with delay(retryDelay).
 *
 * If [successAtAttempt] is non-null, the call at that 0-based index completes
 * connectionOpened and returns instead of throwing. The loop's iterationClosed.await()
 * suspension can then be tripped externally with [publicForceReconnect].
 */
private class BackoffCountingConnection(
    config: AMQPConfig,
    scope: CoroutineScope,
    /**
     * If non-null, attempt index N (0-based) is a "successful" connect; all other
     * attempts throw. We use exactly-one success rather than "first N fail then all succeed"
     * so the post-success attempt is itself a failure — exposing the post-reset delay.
     */
    private val successAtAttempt: Int? = null,
) : RobustAMQPConnection(config, scope) {

    val attemptTimestamps = mutableListOf<TimeSource.Monotonic.ValueTimeMark>()
    val successSignal = CompletableDeferred<Unit>()
    private var attemptIndex = 0

    /**
     * Cap recorded attempts so an absent-backoff tight loop doesn't OOM the JVM before
     * the assertion fires. The cap is well above the legitimate post-fix attempt count
     * (~5-7 in 2.5s) but far below what an unbounded loop would produce in ms.
     */
    val attemptCapForTest = 200

    override suspend fun openConnection() {
        // yield() makes the loop cancellable in the absence of any other suspension point —
        // the buggy reconnect loop has no delay(), and our stub doesn't touch a real socket,
        // so without yield() the factory job can't be cancelled by the test driver.
        yield()
        if (attemptTimestamps.size < attemptCapForTest) {
            attemptTimestamps.add(TimeSource.Monotonic.markNow())
        }
        val current = attemptIndex++
        if (current == successAtAttempt) {
            successSignal.complete(Unit)
            // Mark connectionOpened so that anyone awaiting it unblocks. We avoid
            // touching real sockets entirely; the loop will then await iterationClosed,
            // which we trip externally to force the next iteration.
            connectionOpened.complete(Unit)
            return
        }
        throw sharedFailure
    }

    private val sharedFailure = SimulatedConnectFailure("simulated failure")

    public override suspend fun connect() = super.connect()

    /** Drives the reconnect loop without going through connect()'s connectionOpened.await(). */
    public suspend fun runFactoryForTest() = connectionFactory()

    public fun publicForceReconnect() = forceReconnect()
}

/**
 * No-stack-trace exception. The tight reconnect loop (pre-fix) would otherwise allocate
 * a stack trace per attempt and OOM the JVM before assertions can run.
 */
private class SimulatedConnectFailure(message: String) : RuntimeException(message) {
    override fun fillInStackTrace(): Throwable = this
}

class ReconnectBackoffTest {

    /**
     * The reconnect loop must space out failed attempts using exponential backoff.
     * Without the fix, attempts fire in a tight loop with sub-millisecond gaps.
     */
    @Test
    fun reconnectAttemptsAreSpacedByExponentialBackoff(): Unit = runTest {
        withContext(Dispatchers.Default) {
            val scope = CoroutineScope(coroutineContext + SupervisorJob())
            val initialDelay = 100.milliseconds
            val maxDelay = 1.seconds
            val multiplier = 2.0
            val config = amqpConfig {
                server {
                    reconnectInitialDelay = initialDelay
                    reconnectMaxDelay = maxDelay
                    reconnectBackoffMultiplier = multiplier
                }
            }
            val connection = BackoffCountingConnection(config, scope)
            val factoryJob = scope.launch { connection.runFactoryForTest() }
            try {
                // Expected gaps under backoff: 100, 200, 400, 800, 1000ms (capped) ≈ 2.5s total
                // for 5 gaps after 6 attempts.
                //
                // Without backoff: the loop spins in microseconds. We poll for either the
                // attempt cap (signalling tight loop) or the wall-clock budget — whichever
                // comes first — so a buggy build fails the assertion below instead of OOM-ing.
                withTimeoutOrNull(2_500) {
                    while (connection.attemptTimestamps.size < connection.attemptCapForTest) {
                        delay(50)
                    }
                }
            } finally {
                factoryJob.cancel()
                factoryJob.join()
                scope.cancel()
            }

            val timestamps = connection.attemptTimestamps
            // Without backoff, attemptCap (200) trips within milliseconds. With backoff,
            // 2.5s yields ~5-7 attempts. The upper bound discriminates the bug from the fix.
            assertTrue(
                timestamps.size in 3..15,
                "Expected ~3-15 attempts under backoff in 2.5s; saw ${timestamps.size}. " +
                        "Without backoff, the attempt cap trips immediately (size = attemptCap)."
            )

            // Compute deltas between consecutive attempts.
            val deltas: List<Duration> = timestamps.zipWithNext { a, b -> b - a }

            // Expected backoff curve (capped at maxDelay):
            //   gap_0 ~ initialDelay      (100ms)
            //   gap_1 ~ initialDelay * 2  (200ms)
            //   gap_2 ~ initialDelay * 4  (400ms)
            //   gap_3 ~ initialDelay * 8  (800ms)
            //   gap_n ~ maxDelay          (1000ms, capped)
            val expectedGaps = generateSequence(initialDelay) { prev ->
                (prev * multiplier).coerceAtMost(maxDelay)
            }.take(deltas.size).toList()

            // Generous slack — CI scheduler jitter routinely stretches small delays.
            // Lower bound 0.7x catches the bug (a zero-delay tight loop has near-zero gaps).
            // Upper bound 3.0x absorbs jitter without being meaningless.
            deltas.forEachIndexed { i, delta ->
                val expected = expectedGaps[i]
                val lower = expected * 0.7
                val upper = expected * 3.0
                assertTrue(
                    delta in lower..upper,
                    "Gap $i was $delta; expected in [$lower, $upper] (target $expected). All deltas: $deltas"
                )
            }
        }
    }

    /**
     * After a successful iteration completes, the backoff must reset to initialDelay.
     * Otherwise, a long-lived connection that drops twice in succession would hit max delay
     * on the second drop with no chance to recover quickly.
     */
    @Test
    fun backoffResetsAfterSuccessfulIteration(): Unit = runTest {
        withContext(Dispatchers.Default) {
            val scope = CoroutineScope(coroutineContext + SupervisorJob())
            val initialDelay = 100.milliseconds
            val maxDelay = 1.seconds
            val multiplier = 2.0
            val config = amqpConfig {
                server {
                    reconnectInitialDelay = initialDelay
                    reconnectMaxDelay = maxDelay
                    reconnectBackoffMultiplier = multiplier
                }
            }
            // Plan: fail 3 times (gaps grow to ~400ms), succeed once at attempt 3, fail again post-reset.
            // The gap following the success should be ~initialDelay, not the escalated value.
            val connection = BackoffCountingConnection(config, scope, successAtAttempt = 3)
            val factoryJob = scope.launch { connection.runFactoryForTest() }
            try {
                // Wait for the success attempt to land.
                withTimeout(5.seconds) { connection.successSignal.await() }
                // The successful iteration is now blocked on iterationClosed.await().
                // Trip it so the loop iterates again and the next openConnection() call
                // exposes the post-reset delay.
                connection.publicForceReconnect()
                // Wait for at least two post-success failure attempts, so the gap BETWEEN
                // those two attempts (= the post-reset retryDelay) is observable.
                // With reset: gap ≈ initialDelay (100ms). Without reset: gap ≈ 800ms+.
                delay(800)
            } finally {
                factoryJob.cancel()
                factoryJob.join()
                scope.cancel()
            }

            val timestamps = connection.attemptTimestamps
            // We need: 3 failures (0,1,2) + 1 success (3) + 2 post-success failures (4,5).
            // The gap between attempts 4 and 5 reflects the retryDelay used after the
            // first post-success catch — this is what must reset to initialDelay.
            assertTrue(
                timestamps.size >= 6,
                "Expected at least 6 attempts (3 fail, 1 success, ≥2 post-success fail); " +
                        "saw ${timestamps.size}"
            )

            val postResetGap = timestamps[5] - timestamps[4]
            // Lower bound (≥ 0.7 * initialDelay) catches the absent-backoff bug
            //   (a tight loop has near-zero gaps).
            // Upper bound (≤ 3 * initialDelay) catches a missing-reset bug (the gap
            //   would be the previously escalated value: ~800ms after 3 failures, or higher).
            val resetLower = initialDelay * 0.7
            val resetUpper = initialDelay * 3.0
            assertTrue(
                postResetGap in resetLower..resetUpper,
                "Post-reset gap (between attempts 4 and 5) was $postResetGap; " +
                        "expected in [$resetLower, $resetUpper] (target ~$initialDelay). " +
                        "Without backoff: gap ≈ 0. " +
                        "Without reset: gap ≈ ${initialDelay * multiplier * multiplier * multiplier} or capped."
            )
        }
    }
}
