package dev.kourier.amqp.connection

import kotlinx.coroutines.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * Tests for [DefaultAMQPConnection.closeFromChannelException] (ISSUE-NEW-3).
 *
 * Pre-fix, the gate `if (state != ConnectionState.OPEN) return` was unsynchronized; two
 * concurrent IO failures (read loop + writer) could both observe state == OPEN before
 * either transitioned it, and both call closeFromBroker -> cancelAll. Post-fix, the gate
 * is unchanged ("only drive cleanup if currently OPEN") but is serialized under
 * [closeMutex] so the second caller waits, observes state != OPEN, and bails.
 *
 * These tests use a test subclass that exposes the protected [closeFromChannelException]
 * directly, with a writeChannel that's null (so the underlying byte writes are no-ops).
 */
class CloseFromChannelExceptionTest {

    /** Test subclass that exposes the protected close path and counts cancelAll() invocations. */
    private class TestableConnection(
        config: AMQPConfig,
        scope: CoroutineScope,
    ) : DefaultAMQPConnection(config, scope) {

        var cancelAllCount = 0
            private set

        public override suspend fun closeFromChannelException(exception: Exception) =
            super.closeFromChannelException(exception)

        override fun cancelAll(connectionClose: dev.kourier.amqp.AMQPException.ConnectionClosed) {
            cancelAllCount++
            super.cancelAll(connectionClose)
        }

        /** Drive the connection into OPEN without going through a real socket connect. */
        fun forceOpen() {
            this.state = ConnectionState.OPEN
        }
    }

    @Test
    fun concurrentCloseFromChannelExceptionDoesNotDoubleFire(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob() + Dispatchers.Default)
        val conn = TestableConnection(amqpConfig {}, scope)
        try {
            conn.forceOpen()

            // Fire many concurrent decoder/IO exceptions; mutex+gate must coalesce them.
            val numConcurrent = 30
            val jobs = (1..numConcurrent).map {
                scope.async {
                    withTimeout(5.seconds) {
                        conn.closeFromChannelException(RuntimeException("simulated decoder failure $it"))
                    }
                }
            }
            jobs.awaitAll()

            assertTrue(conn.connectionClosed.isCompleted, "connectionClosed must complete after close path")
            assertEquals(ConnectionState.CLOSED, conn.state, "exit state must be CLOSED")
            // cancelAll must run exactly once: the first caller drove the SHUTTING_DOWN -> CLOSED
            // transition; subsequent callers wait on closeMutex, observe state != OPEN, and bail.
            assertEquals(
                1,
                conn.cancelAllCount,
                "cancelAll must fire exactly once across $numConcurrent concurrent failures",
            )
        } finally {
            scope.cancel()
        }
    }

    @Test
    fun decoderExceptionMidCloseDoesNotPreventTerminalState(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob() + Dispatchers.Default)
        val conn = TestableConnection(amqpConfig {}, scope)
        try {
            conn.forceOpen()
            // Simulate a single decoder exception followed quickly by another (e.g., a
            // close path that fires after state has been advanced to SHUTTING_DOWN).
            conn.closeFromChannelException(RuntimeException("first decoder failure"))
            conn.closeFromChannelException(RuntimeException("second decoder failure (mid-close)"))

            assertEquals(ConnectionState.CLOSED, conn.state)
            assertTrue(conn.connectionClosed.isCompleted)
            assertEquals(1, conn.cancelAllCount, "cancelAll must run exactly once")
        } finally {
            scope.cancel()
        }
    }

    /**
     * Stress: large concurrent fan-out of decoder exceptions hitting the gate.
     * Pre-fix, two callers could both pass the `state != OPEN` gate and both call
     * closeFromBroker -> cancelAll, leading to multiple emissions on connectionResponses
     * (and on the JVM, NPEs from double-cancelling already-null fields).
     */
    @Test
    fun stressManyConcurrentDecoderExceptionsCoalesce(): Unit = runBlocking {
        repeat(20) { iter ->
            val scope = CoroutineScope(coroutineContext + SupervisorJob() + Dispatchers.Default)
            val conn = TestableConnection(amqpConfig {}, scope)
            try {
                conn.forceOpen()
                val jobs = (1..50).map {
                    scope.async {
                        conn.closeFromChannelException(RuntimeException("err-$iter-$it"))
                    }
                }
                jobs.awaitAll()
                assertEquals(1, conn.cancelAllCount, "iter=$iter: cancelAll must fire exactly once")
                assertEquals(ConnectionState.CLOSED, conn.state, "iter=$iter: state must be CLOSED")
                assertTrue(conn.connectionClosed.isCompleted, "iter=$iter: connectionClosed must be completed")
            } finally {
                scope.cancel()
            }
        }
    }
}
