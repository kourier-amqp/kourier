package dev.kourier.amqp.connection

import dev.kourier.amqp.AMQPException
import dev.kourier.amqp.Frame
import dev.kourier.amqp.withConnection
import io.ktor.http.*
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlin.test.*
import kotlin.time.Duration.Companion.seconds

class AMQPConnectionTest {

    @Test
    fun testConnectionWithUrl() = runTest {
        withContext(Dispatchers.Default) {
            val urlString = "amqp://guest:guest@localhost:5672/"
            createAMQPConnection(this, urlString).close()
            createAMQPConnection(this, Url(urlString)).close()
            createAMQPConnection(this, amqpConfig(urlString)).close()
            createAMQPConnection(this, amqpConfig(Url(urlString))).close()
        }
    }

    @Test
    fun testCanOpenChannelAndShutdown() = runTest {
        withConnection { connection ->
            val channel1 = connection.openChannel()
            assertEquals(1u, channel1.id)

            val channel2 = connection.openChannel()
            assertEquals(2u, channel2.id)

            val channel3 = connection.openChannel()
            assertEquals(3u, channel3.id)

            val channel4 = connection.openChannel()
            assertEquals(4u, channel4.id)
        }
    }

    @Test
    fun testCloseMultipleTimes() = runTest {
        withConnection { connection ->
            assertFalse(connection.connectionClosed.isCompleted)
            connection.close()
            assertTrue(connection.connectionClosed.isCompleted)
            connection.close()
            assertTrue(connection.connectionClosed.isCompleted)
            assertFailsWith<AMQPException.ConnectionClosed> { connection.openChannel() }
        }
    }

    @Test
    fun testHeartbeat() = runTest {
        withConnection { connection ->
            connection.sendHeartbeat()
        }
    }

    @Test
    fun testConnectionDrops() = runTest {
        withConnection { connection ->
            val closeEvent = async(start = CoroutineStart.UNDISPATCHED) { connection.closedResponses.first() }

            // Write invalid frame to close connection (heartbeat frame is only allowed on channel 0)
            connection.write(
                Frame(
                    channelId = 1u,
                    payload = Frame.Heartbeat
                )
            )

            closeEvent.await()
            assertFailsWith<AMQPException.ConnectionClosed> { connection.sendHeartbeat() }
            assertTrue(connection.connectionClosed.isCompleted)
        }
    }

    @Test
    fun testConnectionClosedExternally() = runTest {
        withContext(Dispatchers.Default) {
            val fakeServer = FakeServer(this)
            fakeServer.serverReady.await()
            val connection = createAMQPConnection(this) {
                server {
                    port = 5673
                }
            }
            try {
                fakeServer.serverClosed.await()
                assertFailsWith<AMQPException.ConnectionClosed> { connection.sendHeartbeat() }
                assertTrue(connection.connectionClosed.isCompleted)
            } finally {
                connection.close()
                fakeServer.serverJob.cancel()
            }
        }
    }

    // NEW-2: the broker advertises a 1s heartbeat then goes silent. Per AMQP 0.9.1 the client
    // must treat the connection as dead after 2 * heartbeat with no frame received, so
    // connectionClosed completes (which is what drives robust reconnect). Without the watchdog
    // the read loop blocks on readAvailable() forever and this never completes.
    @Test
    fun testMissedServerHeartbeatsClosesConnection() = runTest {
        withContext(Dispatchers.Default) {
            val fakeServer = FakeServer(
                this,
                heartbeat = 1u,
                behavior = FakeServer.Behavior.StaySilent,
                port = 5674,
            )
            fakeServer.serverReady.await()
            val connection = createAMQPConnection(this) {
                server { port = 5674 }
            }
            try {
                val closed = withTimeout(8000) { connection.connectionClosed.await() }
                assertTrue(
                    closed.replyText?.contains("heartbeat", ignoreCase = true) == true,
                    "expected a missed-heartbeat close, got: ${closed.replyText}"
                )
            } finally {
                connection.close()
                fakeServer.serverJob.cancel()
            }
        }
    }

    // NEW-11: a negotiated heartbeat of 0 disables heart-beating. The client must NOT start the
    // watchdog (a 2 * 0 = 0s timeout would mark the connection dead instantly) nor a delay(0)
    // busy-spin sender. The connection must stay open.
    @Test
    fun testZeroHeartbeatDisablesWatchdog() = runTest {
        withContext(Dispatchers.Default) {
            val fakeServer = FakeServer(
                this,
                heartbeat = 0u,
                behavior = FakeServer.Behavior.StaySilent,
                port = 5675,
            )
            fakeServer.serverReady.await()
            val connection = createAMQPConnection(this) {
                server { port = 5675 }
            }
            try {
                // Give a wrongly-started watchdog ample time to misfire.
                delay(3000)
                assertFalse(
                    connection.connectionClosed.isCompleted,
                    "connection must stay open when heartbeat is disabled (0)"
                )
            } finally {
                // The silent server never answers Connection.Close, so a graceful close() would
                // block on CloseOk. Drop the server socket first (tears down the client read loop)
                // and time-box the close as a safety net.
                fakeServer.serverJob.cancel()
                runCatching { withTimeout(2000) { connection.close() } }
            }
        }
    }

    // ISSUE-3: a channel-level RPC must not hang forever when the broker stalls. The server here
    // completes the connection handshake then goes silent; openChannel() sends Channel.Open and
    // waits for OpenOk that never arrives, so it must throw RpcTimeout once rpcTimeout elapses.
    @Test
    fun testRpcTimesOutWhenBrokerStalls() = runTest {
        withContext(Dispatchers.Default) {
            val fakeServer = FakeServer(
                this,
                behavior = FakeServer.Behavior.StaySilent,
                port = 5676,
            )
            fakeServer.serverReady.await()
            val connection = createAMQPConnection(this) {
                server {
                    port = 5676
                    rpcTimeout = 2.seconds
                }
            }
            try {
                assertFailsWith<AMQPException.RpcTimeout> {
                    withTimeout(8000) { connection.openChannel() }
                }
            } finally {
                fakeServer.serverJob.cancel()
                runCatching { withTimeout(2000) { connection.close() } }
            }
        }
    }

}
