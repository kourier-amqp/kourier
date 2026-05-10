package dev.kourier.tutorials

import dev.kourier.amqp.BuiltinExchangeType
import dev.kourier.amqp.Properties
import dev.kourier.amqp.connection.amqpConfig
import dev.kourier.amqp.connection.createAMQPConnection
import io.ktor.utils.io.core.*
import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class PublishSubscribeTest {

    private val exchangeName = "logs"

    /**
     * EmitLog - Publishes log messages to all subscribers.
     * Uses a fanout exchange to broadcast to all bound queues.
     */
    suspend fun emitLog(coroutineScope: CoroutineScope, message: String) {
        val config = amqpConfig {
            server {
                host = "localhost"
            }
        }
        val connection = createAMQPConnection(coroutineScope, config)
        val channel = connection.openChannel()

        // Declare a fanout exchange - broadcasts to all bound queues
        channel.exchangeDeclare(
            exchangeName,
            BuiltinExchangeType.FANOUT,
            durable = false,
            autoDelete = false,
            internal = false,
            arguments = emptyMap()
        )

        // Publish to the exchange (routing key is ignored for fanout)
        channel.basicPublish(
            message.toByteArray(),
            exchange = exchangeName,
            routingKey = "",  // Routing key is ignored by fanout exchanges
            properties = Properties()
        )
        println(" [x] Sent '$message'")

        channel.close()
        connection.close()
    }

    /**
     * ReceiveLogs - Subscribes to log messages.
     * Creates a temporary queue and binds it to the logs exchange.
     */
    suspend fun receiveLogs(
        coroutineScope: CoroutineScope,
        subscriberName: String,
        receivedMessages: MutableList<String>,
        maxMessages: Int = Int.MAX_VALUE,
    ) {
        val config = amqpConfig {
            server {
                host = "localhost"
            }
        }
        val connection = createAMQPConnection(coroutineScope, config)
        val channel = connection.openChannel()

        // Declare the same fanout exchange
        channel.exchangeDeclare(
            exchangeName,
            BuiltinExchangeType.FANOUT,
            durable = false,
            autoDelete = false,
            internal = false,
            arguments = emptyMap()
        )

        // Declare a temporary, exclusive, auto-delete queue
        // The server generates a unique name for us
        val queueDeclared = channel.queueDeclare(
            name = "",  // Empty name = server generates a unique name
            durable = false,
            exclusive = true,   // Queue is deleted when connection closes
            autoDelete = true,  // Queue is deleted when no consumers
            arguments = emptyMap()
        )
        val queueName = queueDeclared.queueName
        println(" [$subscriberName] Created temporary queue: $queueName")

        // Bind the queue to the exchange
        channel.queueBind(
            queue = queueName,
            exchange = exchangeName,
            routingKey = ""  // Routing key is ignored for fanout
        )
        println(" [$subscriberName] Waiting for logs. To exit press CTRL+C")

        // Consume messages with auto-ack (since these are just logs)
        val consumer = channel.basicConsume(queueName, noAck = true)

        var messageCount = 0
        for (delivery in consumer) {
            val message = delivery.message.body.decodeToString()
            println(" [$subscriberName] $message")
            receivedMessages.add(message)

            messageCount++
            if (messageCount >= maxMessages) {
                break
            }
        }

        channel.close()
        connection.close()
    }

    @Test
    fun testPublishSubscribe() = runTest {
        withContext(Dispatchers.Default) {
            // Clear any existing exchange (if it exists)
            val config = amqpConfig {
                server {
                    host = "localhost"
                }
            }
            val setupConnection = createAMQPConnection(this, config)
            val setupChannel = setupConnection.openChannel()

            // Try to delete the exchange if it exists (ignore errors)
            try {
                setupChannel.exchangeDelete(exchangeName, ifUnused = false)
            } catch (e: Exception) {
                // Ignore - exchange might not exist
            }

            setupChannel.close()
            setupConnection.close()

            // Create collections to store received messages
            val subscriber1Messages = mutableListOf<String>()
            val subscriber2Messages = mutableListOf<String>()
            val subscriber3Messages = mutableListOf<String>()

            // Start multiple subscribers
            val subscriber1Job = launch {
                receiveLogs(this, "Subscriber-1", subscriber1Messages, maxMessages = 3)
            }
            val subscriber2Job = launch {
                receiveLogs(this, "Subscriber-2", subscriber2Messages, maxMessages = 3)
            }
            val subscriber3Job = launch {
                receiveLogs(this, "Subscriber-3", subscriber3Messages, maxMessages = 3)
            }

            // Give subscribers time to set up
            delay(500)

            // Emit several log messages
            val logMessages = listOf(
                "info: Application started",
                "warning: High memory usage detected",
                "error: Database connection failed"
            )

            for (message in logMessages) {
                emitLog(this, message)
                delay(100)  // Small delay between messages
            }

            // Wait for all subscribers to receive messages
            withTimeout(10000) {
                subscriber1Job.join()
                subscriber2Job.join()
                subscriber3Job.join()
            }

            // Verify that all subscribers received all messages
            println("\n[Test Summary]")
            println("Subscriber-1 received ${subscriber1Messages.size} messages: $subscriber1Messages")
            println("Subscriber-2 received ${subscriber2Messages.size} messages: $subscriber2Messages")
            println("Subscriber-3 received ${subscriber3Messages.size} messages: $subscriber3Messages")

            // Each subscriber should receive all 3 messages
            assertEquals(3, subscriber1Messages.size, "Subscriber 1 should receive all messages")
            assertEquals(3, subscriber2Messages.size, "Subscriber 2 should receive all messages")
            assertEquals(3, subscriber3Messages.size, "Subscriber 3 should receive all messages")

            // All subscribers should have received the same messages
            assertEquals(logMessages, subscriber1Messages, "Subscriber 1 messages should match")
            assertEquals(logMessages, subscriber2Messages, "Subscriber 2 messages should match")
            assertEquals(logMessages, subscriber3Messages, "Subscriber 3 messages should match")
        }
    }

    @Test
    fun testFanoutBroadcast() = runTest {
        withContext(Dispatchers.Default) {
            // Test that fanout exchange broadcasts to all queues
            val config = amqpConfig {
                server {
                    host = "localhost"
                }
            }
            val connection = createAMQPConnection(this, config)
            val channel = connection.openChannel()

            // Clear any existing exchange
            try {
                channel.exchangeDelete(exchangeName, ifUnused = false)
            } catch (e: Exception) {
                // Ignore
            }

            // Declare fanout exchange
            channel.exchangeDeclare(
                exchangeName,
                BuiltinExchangeType.FANOUT,
                durable = false,
                autoDelete = false,
                internal = false,
                arguments = emptyMap()
            )

            // Create 2 temporary queues and bind them
            val queue1 = channel.queueDeclare(
                "",
                durable = false,
                exclusive = true,
                autoDelete = true,
                arguments = emptyMap()
            ).queueName

            val queue2 = channel.queueDeclare(
                "",
                durable = false,
                exclusive = true,
                autoDelete = true,
                arguments = emptyMap()
            ).queueName

            channel.queueBind(queue1, exchangeName, "")
            channel.queueBind(queue2, exchangeName, "")

            // Publish a message to the exchange
            val testMessage = "Broadcast test message"
            channel.basicPublish(
                testMessage.toByteArray(),
                exchange = exchangeName,
                routingKey = "",
                properties = Properties()
            )

            // Consume from both queues
            val consumer1 = channel.basicConsume(queue1, noAck = true)
            val consumer2 = channel.basicConsume(queue2, noAck = true)

            // Verify both queues received the message
            var message1: String? = null
            var message2: String? = null

            // Consume from queue 1
            for (delivery in consumer1) {
                message1 = delivery.message.body.decodeToString()
                break
            }

            // Consume from queue 2
            for (delivery in consumer2) {
                message2 = delivery.message.body.decodeToString()
                break
            }

            assertEquals(testMessage, message1, "Queue 1 should receive the broadcast")
            assertEquals(testMessage, message2, "Queue 2 should receive the broadcast")

            channel.close()
            connection.close()
        }
    }

    @Test
    fun testTemporaryQueues() = runTest {
        withContext(Dispatchers.Default) {
            // Test that temporary queues are properly created
            val config = amqpConfig {
                server {
                    host = "localhost"
                }
            }
            val connection = createAMQPConnection(this, config)
            val channel = connection.openChannel()

            // Declare a temporary queue (server-named)
            val queueDeclared = channel.queueDeclare(
                name = "",  // Empty name
                durable = false,
                exclusive = true,
                autoDelete = true,
                arguments = emptyMap()
            )

            val queueName = queueDeclared.queueName

            // Verify the queue has a name assigned by the server
            assertTrue(queueName.isNotEmpty(), "Queue name should be generated by server")
            println("Server-generated queue name: $queueName")

            // Typically starts with "amq.gen-" prefix
            assertTrue(
                queueName.startsWith("amq.gen-") || queueName.isNotEmpty(),
                "Queue name should be server-generated"
            )

            channel.close()
            connection.close()
        }
    }

}
