package dev.kourier.amqp.robust

import dev.kourier.amqp.AMQPResponse
import dev.kourier.amqp.ChannelId
import dev.kourier.amqp.Frame
import dev.kourier.amqp.Table
import dev.kourier.amqp.channel.AMQPChannel
import dev.kourier.amqp.connection.AMQPConfig
import dev.kourier.amqp.connection.amqpConfig
import io.ktor.utils.io.core.*
import kotlinx.coroutines.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.Uuid

/**
 * A RobustAMQPChannel whose basicConsume on [hangingQueue] hangs the first time it is called inside a restore
 * context, to simulate a broker that does not answer `basic.consume` within the restore timeout (e.g. a quorum
 * queue that has temporarily lost its majority). Later restores go through normally.
 */
private class HangingConsumeRestoreChannel(
    connection: HangingConsumeRestoreConnection,
    id: ChannelId,
    frameMax: UInt,
) : RobustAMQPChannel(connection, id, frameMax) {

    var hangingQueue: String? = null
    val restoreEnteredHangingConsume = CompletableDeferred<Unit>()

    override suspend fun basicConsume(
        queue: String,
        consumerTag: String,
        noAck: Boolean,
        exclusive: Boolean,
        arguments: Table,
        onDelivery: suspend (AMQPResponse.Channel.Message.Delivery) -> Unit,
        onCanceled: suspend (AMQPResponse.Channel) -> Unit,
    ): AMQPResponse.Channel.Basic.ConsumeOk {
        if (currentChannelRestoreContext() != null && queue == hangingQueue && !restoreEnteredHangingConsume.isCompleted) {
            restoreEnteredHangingConsume.complete(Unit)
            // Mimic a broker that never replies: hang until the restore timeout cancels us.
            awaitCancellation()
        }
        return super.basicConsume(queue, consumerTag, noAck, exclusive, arguments, onDelivery, onCanceled)
    }
}

private class HangingConsumeRestoreConnection(
    config: AMQPConfig,
    scope: CoroutineScope,
) : RobustAMQPConnection(config, scope) {

    public override suspend fun connect() = super.connect()

    override fun createChannel(id: ChannelId, frameMax: UInt): AMQPChannel =
        HangingConsumeRestoreChannel(this, id, frameMax)
}

class ConsumerRestoreTimeoutTest {

    /**
     * When a restore times out while re-registering consumers, the consumers that were not re-registered yet
     * must not be dropped: the next restore, on the fresh connection, has to bring every one of them back.
     */
    @Test
    fun consumersAreRestoredAfterRestoreTimesOutDuringBasicConsume(): Unit = runBlocking {
        val scope = CoroutineScope(coroutineContext + SupervisorJob())
        val config = amqpConfig {
            server {
                restoreTimeout = 500.milliseconds
            }
        }
        val connection = HangingConsumeRestoreConnection(config, scope)
        val queues = (1..3).map { "test-consumer-restore-timeout-$it-${Uuid.random()}" }
        try {
            connection.connect()
            val channel = connection.openChannel() as HangingConsumeRestoreChannel

            queues.forEach {
                channel.queueDeclare(it, durable = false, exclusive = false, autoDelete = false, arguments = emptyMap())
            }
            // Broker-assigned consumer tags, so a restored consumer comes back under a different tag.
            val receivers = queues.map { channel.basicConsume(queue = it, noAck = true, arguments = emptyMap()) }

            // The first consumer is restored, the second hangs past the restore timeout, the third is never reached.
            channel.hangingQueue = queues[1]

            // Drop the connection so the robust factory reconnects and restores the channel.
            // Heartbeat on a non-zero channel forces broker to close the connection.
            connection.write(Frame(channelId = 1u, payload = Frame.Heartbeat))
            withTimeout(10.seconds) { channel.restoreEnteredHangingConsume.await() }

            // The restore times out, the connection is torn down and the next restore goes through.
            // Publishes fail with RestoreTimeoutException until that second restore has completed.
            withTimeout(10.seconds) {
                while (true) {
                    try {
                        channel.basicPublish("probe".toByteArray(), exchange = "", routingKey = queues[0])
                        break
                    } catch (e: CancellationException) {
                        throw e
                    } catch (_: Exception) {
                        delay(100)
                    }
                }
            }
            assertEquals("probe", withTimeout(5.seconds) { receivers[0].receive() }.message.body.decodeToString())

            queues.forEachIndexed { index, queue ->
                channel.basicPublish("After restore $index".toByteArray(), exchange = "", routingKey = queue)
                val delivery = withTimeoutOrNull(5.seconds) { receivers[index].receive() }
                assertNotNull(delivery, "Consumer of queue #$index was not restored after the restore timeout")
                assertEquals("After restore $index", delivery.message.body.decodeToString())
            }
        } finally {
            runCatching {
                withTimeout(5.seconds) {
                    val cleanup = connection.openChannel()
                    queues.forEach { cleanup.queueDelete(it) }
                    connection.close()
                }
            }
            scope.cancel()
        }
    }
}
