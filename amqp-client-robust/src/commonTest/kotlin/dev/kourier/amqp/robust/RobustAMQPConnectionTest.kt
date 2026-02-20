package dev.kourier.amqp.robust

import dev.kourier.amqp.BuiltinExchangeType
import dev.kourier.amqp.Frame
import dev.kourier.amqp.connection.ConnectionState
import dev.kourier.amqp.connection.amqpConfig
import io.ktor.http.*
import io.ktor.utils.io.core.*
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.Uuid

class RobustAMQPConnectionTest {

    @Test
    fun testConnectionWithUrl(): Unit = runBlocking {
        val urlString = "amqp://guest:guest@localhost:5672/"
        createRobustAMQPConnection(this, urlString).close()
        createRobustAMQPConnection(this, Url(urlString)).close()
        createRobustAMQPConnection(this, amqpConfig(urlString)).close()
        createRobustAMQPConnection(this, amqpConfig(Url(urlString))).close()
    }

    @Test
    fun testConnectionDrops() = runBlocking {
        withConnection { connection ->
            val closeEvent = async { connection.closedResponses.first() }
            val reopenEvent = async { connection.openedResponses.first() }

            // Write invalid frame to close connection (heartbeat frame is only allowed on channel 0)
            connection.write(
                Frame(
                    channelId = 1u,
                    payload = Frame.Heartbeat
                )
            )

            closeEvent.await()
            reopenEvent.await()

            val channel = connection.openChannel()
            channel.close()
        }
    }

    @Test
    @OptIn(DelicateCoroutinesApi::class)
    fun testRestoreConsumeAfterConnectionDrops() = runBlocking {
        withConnection { connection ->
            val channel = connection.openChannel()
            val closeEvent = async { connection.closedResponses.first() }
            val reopenEvent = async { channel.openedResponses.first() }

            val queueName = "test-connection-restore-queue"
            val exchange = "test-connection-restore-exchange"
            val routingKey = "test.key"

            channel.exchangeDeclare(
                exchange,
                BuiltinExchangeType.DIRECT,
                durable = true,
                arguments = emptyMap()
            )
            channel.queueDeclare(
                queueName,
                durable = true,
                arguments = emptyMap()
            )
            channel.queueBind(queueName, exchange, routingKey, arguments = emptyMap())
            val receivedMessages = channel.basicConsume(
                queue = queueName,
                consumerTag = "restore-test-consumer",
                noAck = true,
                arguments = emptyMap()
            )
            channel.basicPublish("Before crash".toByteArray(), exchange, routingKey)

            val msg = withTimeout(5000) { receivedMessages.receive() }
            assertEquals("Before crash", msg.message.body.decodeToString())

            // Write invalid frame to close connection (heartbeat frame is only allowed on channel 0)
            connection.write(
                Frame(
                    channelId = 1u,
                    payload = Frame.Heartbeat
                )
            )
            closeEvent.await()
            reopenEvent.await()

            channel.basicPublish("After restore".toByteArray(), exchange, routingKey)

            val msg2 = withTimeout(5.seconds) { receivedMessages.receive() }
            assertEquals("After restore", msg2.message.body.decodeToString())

            channel.close()
        }
    }

    @Test
    @OptIn(DelicateCoroutinesApi::class)
    fun testRestoreConsumeNoTaAfterConnectionDrops() = runBlocking {
        withConnection { connection ->
            val channel = connection.openChannel()
            val closeEvent = async { connection.closedResponses.first() }
            val reopenEvent = async { channel.openedResponses.first() }

            val queueName = "test-connection-restore-queue"
            val exchange = "test-connection-restore-exchange"
            val routingKey = "test.key"

            channel.exchangeDeclare(
                exchange,
                BuiltinExchangeType.DIRECT,
                durable = true,
                arguments = emptyMap()
            )
            channel.queueDeclare(
                queueName,
                durable = true,
                arguments = emptyMap()
            )
            channel.queueBind(queueName, exchange, routingKey, arguments = emptyMap())
            val receivedMessages = channel.basicConsume(
                queue = queueName,
                noAck = true,
                arguments = emptyMap()
            )
            channel.basicPublish("Before crash".toByteArray(), exchange, routingKey)

            val msg = withTimeout(5000) { receivedMessages.receive() }
            assertEquals("Before crash", msg.message.body.decodeToString())

            // Write invalid frame to close connection (heartbeat frame is only allowed on channel 0)
            connection.write(
                Frame(
                    channelId = 1u,
                    payload = Frame.Heartbeat
                )
            )
            closeEvent.await()
            reopenEvent.await()

            channel.basicPublish("After restore".toByteArray(), exchange, routingKey)

            val msg2 = withTimeout(5.seconds) { receivedMessages.receive() }
            assertEquals("After restore", msg2.message.body.decodeToString())

            channel.close()
        }
    }

    @Test
    fun testConnectionIsOpenAfterCreate() = runBlocking {
        withConnection { connection ->
            assertEquals(ConnectionState.OPEN, connection.state)
        }
    }

    /**
     * Regression test: multiple broker-assigned consumers (empty consumerTag) on the same channel
     * caused a ConcurrentModificationException in restore() because basicConsume() adds new entries
     * to consumedQueues while the forEach iteration is in progress.
     */
    @Test
    @OptIn(DelicateCoroutinesApi::class)
    fun testRestoreMultipleBrokerAssignedConsumersAfterConnectionDrop() = runBlocking {
        withConnection { connection ->
            val channel = connection.openChannel()
            val closeEvent = async { connection.closedResponses.first() }
            val reopenEvent = async { channel.openedResponses.first() }

            val queue1 = "test-multi-consumer-conn-${Uuid.random()}"
            val queue2 = "test-multi-consumer-conn-${Uuid.random()}"
            val exchange = "test-multi-consumer-conn-ex-${Uuid.random()}"
            val routingKey1 = "key1"
            val routingKey2 = "key2"

            channel.exchangeDeclare(exchange, BuiltinExchangeType.DIRECT, durable = false, arguments = emptyMap())
            channel.queueDeclare(queue1, durable = false, exclusive = false, autoDelete = true, arguments = emptyMap())
            channel.queueDeclare(queue2, durable = false, exclusive = false, autoDelete = true, arguments = emptyMap())
            channel.queueBind(queue1, exchange, routingKey1, arguments = emptyMap())
            channel.queueBind(queue2, exchange, routingKey2, arguments = emptyMap())

            // Two broker-assigned consumers (empty consumerTag -> broker assigns amq.ctag-...)
            val messages1 = channel.basicConsume(queue = queue1, noAck = true, arguments = emptyMap())
            val messages2 = channel.basicConsume(queue = queue2, noAck = true, arguments = emptyMap())

            channel.basicPublish("Before crash 1".toByteArray(), exchange, routingKey1)
            channel.basicPublish("Before crash 2".toByteArray(), exchange, routingKey2)
            assertEquals("Before crash 1", withTimeout(5.seconds) { messages1.receive() }.message.body.decodeToString())
            assertEquals("Before crash 2", withTimeout(5.seconds) { messages2.receive() }.message.body.decodeToString())

            // Drop connection
            connection.write(Frame(channelId = 1u, payload = Frame.Heartbeat))
            closeEvent.await()
            reopenEvent.await()

            // Publish after restore - both consumers must be active
            channel.basicPublish("After restore 1".toByteArray(), exchange, routingKey1)
            channel.basicPublish("After restore 2".toByteArray(), exchange, routingKey2)
            assertEquals(
                "After restore 1",
                withTimeout(5.seconds) { messages1.receive() }.message.body.decodeToString()
            )
            assertEquals(
                "After restore 2",
                withTimeout(5.seconds) { messages2.receive() }.message.body.decodeToString()
            )

            channel.close()
        }
    }

}
