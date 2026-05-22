package dev.kourier.amqp.connection

import dev.kourier.amqp.Frame
import dev.kourier.amqp.serialization.ProtocolBinary
import io.ktor.network.selector.*
import io.ktor.network.sockets.*
import io.ktor.utils.io.*
import kotlinx.coroutines.*
import kotlinx.serialization.encodeToByteArray

/**
 * Minimal fake AMQP server for connection-level tests. Completes a stripped-down handshake
 * (skips Connection.Start: reads the client's protocol header, replies Tune then OpenOk), then
 * behaves according to [behavior].
 *
 * @param heartbeat the heartbeat interval (seconds) advertised in the Tune frame; the client
 *   echoes it in TuneOk, so this drives the negotiated value.
 * @param behavior what to do after the handshake.
 * @param port the TCP port to bind (use distinct ports across tests to avoid rebind races).
 */
class FakeServer(
    coroutineScope: CoroutineScope,
    heartbeat: UShort = 60u,
    behavior: Behavior = Behavior.CloseAbruptly,
    port: Int = 5673,
) {

    enum class Behavior {
        /** Close the socket abruptly shortly after the handshake. */
        CloseAbruptly,

        /** Keep the socket open but send no further frames — exercises missed-heartbeat detection. */
        StaySilent,
    }

    val serverReady = CompletableDeferred<Unit>()
    val serverClosed = CompletableDeferred<Unit>()

    val serverJob = coroutineScope.launch {
        val selectorManager = SelectorManager(Dispatchers.IO)
        val server = aSocket(selectorManager).tcp().bind("127.0.0.1", port)
        serverReady.complete(Unit)
        val clientSocket = server.accept()

        val readChannel = clientSocket.openReadChannel()
        val writeChannel = clientSocket.openWriteChannel(autoFlush = true)

        readChannel.readAvailable(ByteArray(1024))
        writeChannel.writeByteArray(
            ProtocolBinary.encodeToByteArray(
                Frame(
                    channelId = 0u,
                    payload = Frame.Method.Connection.Tune(
                        channelMax = 1u,
                        frameMax = 4096u,
                        heartbeat = heartbeat,
                    )
                )
            )
        )
        readChannel.readAvailable(ByteArray(1024))
        writeChannel.writeByteArray(
            ProtocolBinary.encodeToByteArray(
                Frame(
                    channelId = 0u,
                    payload = Frame.Method.Connection.OpenOk(
                        reserved1 = ""
                    )
                )
            )
        )

        when (behavior) {
            Behavior.CloseAbruptly -> {
                delay(100)
                clientSocket.close() // Simulate server closing connection abruptly
                server.close()
                delay(100)
                serverClosed.complete(Unit)
            }

            Behavior.StaySilent -> {
                // Send nothing further. Drain whatever the client writes (e.g. its own heartbeats)
                // so its write buffer can't fill, until the client disconnects or we're cancelled.
                try {
                    val buffer = ByteArray(1024)
                    while (isActive) {
                        if (readChannel.readAvailable(buffer) == -1) break
                    }
                } finally {
                    clientSocket.close()
                    server.close()
                    serverClosed.complete(Unit)
                }
            }
        }
    }

}
