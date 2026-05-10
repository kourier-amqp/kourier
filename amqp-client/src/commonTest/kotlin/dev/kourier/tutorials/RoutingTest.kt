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

class RoutingTest {

    private val exchangeName = "direct_logs"

    /**
     * EmitLogDirect - Publishes log messages with a specific severity (routing key).
     * Uses a direct exchange to route messages based on severity.
     */
    suspend fun emitLogDirect(coroutineScope: CoroutineScope, severity: String, message: String) {
        val config = amqpConfig {
            server {
                host = "localhost"
            }
        }
        val connection = createAMQPConnection(coroutineScope, config)
        val channel = connection.openChannel()

        // Declare a direct exchange - routes based on exact routing key match
        channel.exchangeDeclare(
            exchangeName,
            BuiltinExchangeType.DIRECT,
            durable = false,
            autoDelete = false,
            internal = false,
            arguments = emptyMap()
        )

        // Publish with severity as the routing key
        channel.basicPublish(
            message.toByteArray(),
            exchange = exchangeName,
            routingKey = severity,  // Routing key determines which queues receive the message
            properties = Properties()
        )
        println(" [x] Sent '$severity':'$message'")

        channel.close()
        connection.close()
    }

    /**
     * ReceiveLogsDirect - Subscribes to log messages of specific severities.
     * Creates a temporary queue and binds it with specified routing keys.
     */
    suspend fun receiveLogsDirect(
        coroutineScope: CoroutineScope,
        subscriberName: String,
        severities: List<String>,
        receivedMessages: MutableList<Pair<String, String>>,  // Pair of (severity, message)
        maxMessages: Int = Int.MAX_VALUE,
    ) {
        val config = amqpConfig {
            server {
                host = "localhost"
            }
        }
        val connection = createAMQPConnection(coroutineScope, config)
        val channel = connection.openChannel()

        // Declare the same direct exchange
        channel.exchangeDeclare(
            exchangeName,
            BuiltinExchangeType.DIRECT,
            durable = false,
            autoDelete = false,
            internal = false,
            arguments = emptyMap()
        )

        // Declare a temporary queue
        val queueDeclared = channel.queueDeclare(
            name = "",
            durable = false,
            exclusive = true,
            autoDelete = true,
            arguments = emptyMap()
        )
        val queueName = queueDeclared.queueName
        println(" [$subscriberName] Created temporary queue: $queueName")

        // Bind the queue to the exchange with each severity
        for (severity in severities) {
            channel.queueBind(
                queue = queueName,
                exchange = exchangeName,
                routingKey = severity  // Only receive messages with this routing key
            )
            println(" [$subscriberName] Binding queue to '$severity'")
        }
        println(" [$subscriberName] Waiting for ${severities.joinToString(", ")} logs. To exit press CTRL+C")

        // Consume messages
        val consumer = channel.basicConsume(queueName, noAck = true)

        var messageCount = 0
        for (delivery in consumer) {
            val routingKey = delivery.message.routingKey
            val message = delivery.message.body.decodeToString()
            println(" [$subscriberName] Received '$routingKey':'$message'")
            receivedMessages.add(Pair(routingKey, message))

            messageCount++
            if (messageCount >= maxMessages) {
                break
            }
        }

        channel.close()
        connection.close()
    }

    @Test
    fun testRouting() = runTest {
        withContext(Dispatchers.Default) {
            // Clear any existing exchange
            val config = amqpConfig {
                server {
                    host = "localhost"
                }
            }
            val setupConnection = createAMQPConnection(this, config)
            val setupChannel = setupConnection.openChannel()

            try {
                setupChannel.exchangeDelete(exchangeName, ifUnused = false)
            } catch (e: Exception) {
                // Ignore - exchange might not exist
            }

            setupChannel.close()
            setupConnection.close()

            // Create collections to store received messages
            val errorOnlyMessages = mutableListOf<Pair<String, String>>()
            val allMessages = mutableListOf<Pair<String, String>>()

            // Start subscribers with different severity filters
            // Subscriber 1: Only errors
            val errorSubscriberJob = launch {
                receiveLogsDirect(
                    this,
                    "Error-Logger",
                    listOf("error"),
                    errorOnlyMessages,
                    maxMessages = 2
                )
            }

            // Subscriber 2: All severities
            val allLogsSubscriberJob = launch {
                receiveLogsDirect(
                    this,
                    "All-Logger",
                    listOf("info", "warning", "error"),
                    allMessages,
                    maxMessages = 5
                )
            }

            // Give subscribers time to set up
            delay(500)

            // Emit log messages with different severities
            launch {
                emitLogDirect(this, "info", "Application started successfully")
                delay(100)
                emitLogDirect(this, "warning", "Memory usage at 80%")
                delay(100)
                emitLogDirect(this, "error", "Failed to connect to database")
                delay(100)
                emitLogDirect(this, "info", "Processing request #1234")
                delay(100)
                emitLogDirect(this, "error", "Disk space critically low")
            }

            // Wait for subscribers to finish
            withTimeout(10000) {
                errorSubscriberJob.join()
                allLogsSubscriberJob.join()
            }

            // Verify routing worked correctly
            println("\n[Test Summary]")
            println("Error-Logger received ${errorOnlyMessages.size} messages: $errorOnlyMessages")
            println("All-Logger received ${allMessages.size} messages: $allMessages")

            // Error subscriber should only receive error messages
            assertEquals(2, errorOnlyMessages.size, "Error-Logger should receive 2 error messages")
            assertTrue(
                errorOnlyMessages.all { it.first == "error" },
                "Error-Logger should only receive 'error' severity"
            )

            // All logs subscriber should receive all 5 messages
            assertEquals(5, allMessages.size, "All-Logger should receive all 5 messages")

            // Verify specific messages
            assertTrue(
                errorOnlyMessages.any { it.second.contains("database") },
                "Error-Logger should receive database error"
            )
            assertTrue(
                errorOnlyMessages.any { it.second.contains("Disk space") },
                "Error-Logger should receive disk error"
            )

            // Verify all severities in all logs
            val severities = allMessages.map { it.first }.toSet()
            assertEquals(setOf("info", "warning", "error"), severities, "All-Logger should receive all severities")
        }
    }

    @Test
    fun testMultipleBindings() = runTest {
        withContext(Dispatchers.Default) {
            // Test that a queue can have multiple bindings with different routing keys
            val config = amqpConfig {
                server {
                    host = "localhost"
                }
            }
            val connection = createAMQPConnection(this, config)
            val channel = connection.openChannel()

            // Clear exchange
            try {
                channel.exchangeDelete(exchangeName, ifUnused = false)
            } catch (e: Exception) {
                // Ignore
            }

            // Declare direct exchange
            channel.exchangeDeclare(
                exchangeName,
                BuiltinExchangeType.DIRECT,
                durable = false,
                autoDelete = false,
                internal = false,
                arguments = emptyMap()
            )

            // Create a queue and bind it with multiple routing keys
            val queueDeclared = channel.queueDeclare(
                "",
                durable = false,
                exclusive = true,
                autoDelete = true,
                arguments = emptyMap()
            )
            val queueName = queueDeclared.queueName

            // Bind with warning and error
            channel.queueBind(queueName, exchangeName, "warning")
            channel.queueBind(queueName, exchangeName, "error")

            // Publish messages with different routing keys
            channel.basicPublish(
                "Info message".toByteArray(),
                exchange = exchangeName,
                routingKey = "info",
                properties = Properties()
            )
            channel.basicPublish(
                "Warning message".toByteArray(),
                exchange = exchangeName,
                routingKey = "warning",
                properties = Properties()
            )
            channel.basicPublish(
                "Error message".toByteArray(),
                exchange = exchangeName,
                routingKey = "error",
                properties = Properties()
            )

            // Consume and verify we only get warning and error
            val consumer = channel.basicConsume(queueName, noAck = true)
            val receivedMessages = mutableListOf<Pair<String, String>>()

            var count = 0
            for (delivery in consumer) {
                val routingKey = delivery.message.routingKey
                val message = delivery.message.body.decodeToString()
                receivedMessages.add(Pair(routingKey, message))
                count++
                if (count >= 2) break
            }

            // Should only receive warning and error, not info
            assertEquals(2, receivedMessages.size, "Should receive 2 messages")
            assertTrue(
                receivedMessages.none { it.first == "info" },
                "Should not receive info messages"
            )
            assertTrue(
                receivedMessages.any { it.first == "warning" },
                "Should receive warning message"
            )
            assertTrue(
                receivedMessages.any { it.first == "error" },
                "Should receive error message"
            )

            channel.close()
            connection.close()
        }
    }

    @Test
    fun testExactRoutingKeyMatch() = runTest {
        withContext(Dispatchers.Default) {
            // Test that routing keys must match exactly (not pattern matching)
            val config = amqpConfig {
                server {
                    host = "localhost"
                }
            }
            val connection = createAMQPConnection(this, config)
            val channel = connection.openChannel()

            // Clear exchange
            try {
                channel.exchangeDelete(exchangeName, ifUnused = false)
            } catch (e: Exception) {
                // Ignore
            }

            // Declare direct exchange
            channel.exchangeDeclare(
                exchangeName,
                BuiltinExchangeType.DIRECT,
                durable = false,
                autoDelete = false,
                internal = false,
                arguments = emptyMap()
            )

            // Create queue and bind to exact key "error.database"
            val queueDeclared = channel.queueDeclare(
                "",
                durable = false,
                exclusive = true,
                autoDelete = true,
                arguments = emptyMap()
            )
            val queueName = queueDeclared.queueName

            channel.queueBind(queueName, exchangeName, "error.database")

            // Publish messages with different keys
            channel.basicPublish(
                "Exact match".toByteArray(),
                exchange = exchangeName,
                routingKey = "error.database",
                properties = Properties()
            )
            channel.basicPublish(
                "Different key".toByteArray(),
                exchange = exchangeName,
                routingKey = "error",
                properties = Properties()
            )
            channel.basicPublish(
                "Partial match".toByteArray(),
                exchange = exchangeName,
                routingKey = "error.disk",
                properties = Properties()
            )

            delay(200)

            // Should only receive the exact match
            val consumer = channel.basicConsume(queueName, noAck = true)
            val receivedMessages = mutableListOf<String>()

            var count = 0
            for (delivery in consumer) {
                val message = delivery.message.body.decodeToString()
                receivedMessages.add(message)
                count++
                if (count >= 1) break  // Only expecting 1 message
            }

            assertEquals(1, receivedMessages.size, "Should receive exactly 1 message")
            assertEquals("Exact match", receivedMessages[0], "Should receive the exact match message")

            channel.close()
            connection.close()
        }
    }

}
