package dev.kourier.amqp.connection

import dev.kourier.amqp.Frame
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * Stress tests for the write-block path (ISSUE-BONUS-1).
 *
 * Pre-fix, `writeBlockSignal: CompletableDeferred<Unit>?` was overwritten on every
 * Connection.Blocked. A second Blocked arriving before Unblocked completed the first
 * left the original deferred orphaned — any writer that had captured it would hang forever.
 *
 * The fix replaces the field with `MutableStateFlow<Boolean>` so writers re-evaluate the
 * current state via `first { !it }`. Tests below: (a) a structural demonstration that the
 * original CompletableDeferred-based pattern hangs under storm, (b) the same storm against
 * the new MutableStateFlow gate completes, (c) a real DefaultAMQPConnection (without a
 * broker — writeChannel is null so the byte write is a no-op) progresses through the gate
 * under storm.
 */
class WriteBlockedStormTest {

    /**
     * Demonstrates the pre-fix algorithm hangs deterministically under a Blocked->Blocked storm.
     * This is the algorithmic shape of the original code in DefaultAMQPConnection (lines ~234-244,
     * ~467-477 pre-fix). Without the fix, the writer's captured deferred is orphaned and never
     * completes. With the fix's algorithm (next test), it always completes.
     */
    @Test
    fun preFixCompletableDeferredAlgorithmCanOrphanWriter(): Unit = runTest {
        withContext(Dispatchers.Default) {
            var writeBlockSignal: CompletableDeferred<Unit>? = CompletableDeferred()
            val firstBlockedSet = CompletableDeferred<Unit>()

            // Writer captures the current deferred and awaits it — the bug is exactly this pattern.
            val writer = async {
                withTimeoutOrNull(2.seconds) {
                    firstBlockedSet.await()
                    @Suppress("ktlint:standard:annotation")
                    val captured: CompletableDeferred<Unit>? = writeBlockSignal
                    captured?.await()
                }
            }
            firstBlockedSet.complete(Unit)
            // Storm: another "Blocked" arrives and overwrites the original deferred.
            // The first deferred is orphaned: nothing will ever complete it.
            delay(50)
            writeBlockSignal = CompletableDeferred()
            // "Unblocked" arrives — but only completes the *current* (second) deferred, not the orphan.
            writeBlockSignal.complete(Unit)
            writeBlockSignal = null

            val outcome = writer.await()
            assertTrue(
                outcome == null,
                "Pre-fix algorithm must orphan the captured deferred under Blocked->Blocked->Unblocked storm",
            )
        }
    }

    /**
     * The fix's algorithm: a MutableStateFlow<Boolean> that writers re-evaluate via
     * first { !it }. Same storm pattern as above, but every writer re-evaluates state on
     * every change and so always completes once the value goes false.
     */
    @Test
    fun postFixStateFlowAlgorithmAlwaysReleasesWriter(): Unit = runTest {
        withContext(Dispatchers.Default) {
            val writeBlocked = MutableStateFlow(true)
            val writer = async {
                withTimeout(5.seconds) { writeBlocked.first { !it } }
            }
            // Storm: toggle several times. The writer keeps re-evaluating because StateFlow
            // re-emits each distinct value.
            repeat(50) {
                yield()
                writeBlocked.value = true
                yield()
                writeBlocked.value = false
            }
            writeBlocked.value = false

            val outcome = withTimeoutOrNull(5.seconds) { writer.await() }
            assertTrue(outcome != null, "Post-fix StateFlow gate must release writers under storm")
        }
    }

    private fun newDisconnectedConnection(scope: CoroutineScope): DefaultAMQPConnection =
        DefaultAMQPConnection(amqpConfig {}, scope)

    /**
     * End-to-end: a real DefaultAMQPConnection (no broker — writeChannel is null so the
     * underlying byte write is a no-op). The gate in write(vararg) still runs, and writers
     * must progress through it under a Blocked/Unblocked toggle storm.
     */
    @Test
    fun writersProgressThroughGateUnderStorm(): Unit = runTest {
        withContext(Dispatchers.Default) {
            val scope = CoroutineScope(coroutineContext + SupervisorJob())
            val conn = newDisconnectedConnection(scope)
            try {
                conn.writeBlocked.value = true

                val numWriters = 50
                val writers = (1..numWriters).map {
                    scope.async {
                        withTimeout(10.seconds) {
                            conn.write(Frame(channelId = 0u, payload = Frame.Method.Connection.CloseOk))
                        }
                    }
                }

                // Toggle storm; pre-fix this would have orphaned a captured deferred.
                repeat(300) {
                    conn.writeBlocked.value = true
                    yield()
                    conn.writeBlocked.value = false
                    yield()
                }
                conn.writeBlocked.value = false

                val results = withTimeoutOrNull(15.seconds) { writers.awaitAll() }
                assertTrue(
                    results != null && results.size == numWriters,
                    "All $numWriters writers must complete after final unblock under Blocked/Unblocked storm",
                )
            } finally {
                scope.cancel()
            }
        }
    }
}
