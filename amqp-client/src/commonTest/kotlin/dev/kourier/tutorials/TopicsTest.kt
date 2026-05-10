package dev.kourier.tutorials

import dev.kourier.amqp.BuiltinExchangeType
import dev.kourier.amqp.Properties
import dev.kourier.amqp.connection.amqpConfig
import dev.kourier.amqp.connection.createAMQPConnection
import io.ktor.utils.io.core.*
import kotlinx.coroutines.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class TopicsTest {

    private val exchangeName = "topic_logs"

    /**
     * EmitLogTopic - Publishes log messages with a topic routing key.
     * Uses a topic exchange for pattern-based routing.
     */
    suspend fun emitLogTopic(coroutineScope: CoroutineScope, routingKey: String, message: String) {
        val config = amqpConfig {
            server {
                host = "localhost"
            }
        }
        val connection = createAMQPConnection(coroutineScope, config)
        val channel = connection.openChannel()

        // Declare a topic exchange - routes based on pattern matching
        channel.exchangeDeclare(
            exchangeName,
            BuiltinExchangeType.TOPIC,
            durable = false,
            autoDelete = false,
            internal = false,
            arguments = emptyMap()
        )

        // Publish with a topic routing key (dot-separated words)
        channel.basicPublish(
            message.toByteArray(),
            exchange = exchangeName,
            routingKey = routingKey,  // Format: <facility>.<severity> or similar
            properties = Properties()
        )
        println(" [x] Sent '$routingKey':'$message'")

        channel.close()
        connection.close()
    }

    /**
     * ReceiveLogsTopic - Subscribes to log messages using topic patterns.
     * Supports wildcards: * (one word), # (zero or more words)
     */
    suspend fun receiveLogsTopic(
        coroutineScope: CoroutineScope,
        subscriberName: String,
        bindingKeys: List<String>,
        receivedMessages: MutableList<Pair<String, String>>,
        maxMessages: Int = Int.MAX_VALUE,
    ) {
        val config = amqpConfig {
            server {
                host = "localhost"
            }
        }
        val connection = createAMQPConnection(coroutineScope, config)
        val channel = connection.openChannel()

        // Declare the same topic exchange
        channel.exchangeDeclare(
            exchangeName,
            BuiltinExchangeType.TOPIC,
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

        // Bind the queue with topic patterns
        for (bindingKey in bindingKeys) {
            channel.queueBind(
                queue = queueName,
                exchange = exchangeName,
                routingKey = bindingKey  // Pattern with wildcards: * or #
            )
            println(" [$subscriberName] Binding queue to pattern '$bindingKey'")
        }
        println(" [$subscriberName] Waiting for messages matching ${bindingKeys.joinToString(", ")}")

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
    fun testTopicPatterns() = runBlocking {
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
        val kernelMessages = mutableListOf<Pair<String, String>>()      // kern.*
        val criticalMessages = mutableListOf<Pair<String, String>>()    // *.critical
        val allMessages = mutableListOf<Pair<String, String>>()         // #

        // Start subscribers with different patterns
        val kernelSubscriberJob = launch {
            receiveLogsTopic(
                this,
                "Kernel-Logger",
                listOf("kern.*"),
                kernelMessages,
                maxMessages = 2
            )
        }

        val criticalSubscriberJob = launch {
            receiveLogsTopic(
                this,
                "Critical-Logger",
                listOf("*.critical"),
                criticalMessages,
                maxMessages = 2
            )
        }

        val allLogsSubscriberJob = launch {
            receiveLogsTopic(
                this,
                "All-Logger",
                listOf("#"),
                allMessages,
                maxMessages = 5
            )
        }

        // Give subscribers time to set up
        delay(500)

        // Emit log messages with various routing keys
        launch {
            emitLogTopic(this, "kern.info", "Kernel info message")
            delay(100)
            emitLogTopic(this, "kern.critical", "Kernel critical error")
            delay(100)
            emitLogTopic(this, "app.critical", "Application critical error")
            delay(100)
            emitLogTopic(this, "app.warning", "Application warning")
            delay(100)
            emitLogTopic(this, "disk.info", "Disk info message")
        }

        // Wait for subscribers to finish
        withTimeout(10000) {
            kernelSubscriberJob.join()
            criticalSubscriberJob.join()
            allLogsSubscriberJob.join()
        }

        // Verify pattern matching worked correctly
        println("\n[Test Summary]")
        println("Kernel-Logger received ${kernelMessages.size} messages: $kernelMessages")
        println("Critical-Logger received ${criticalMessages.size} messages: $criticalMessages")
        println("All-Logger received ${allMessages.size} messages: $allMessages")

        // Kernel logger (kern.*) should receive both kern.info and kern.critical
        assertEquals(2, kernelMessages.size, "Kernel-Logger should receive 2 messages")
        assertTrue(
            kernelMessages.all { it.first.startsWith("kern.") },
            "Kernel-Logger should only receive kern.* messages"
        )

        // Critical logger (*.critical) should receive kern.critical and app.critical
        assertEquals(2, criticalMessages.size, "Critical-Logger should receive 2 critical messages")
        assertTrue(
            criticalMessages.all { it.first.endsWith(".critical") },
            "Critical-Logger should only receive *.critical messages"
        )

        // All logger (#) should receive all 5 messages
        assertEquals(5, allMessages.size, "All-Logger should receive all 5 messages")
    }

    @Test
    fun testWildcardStar() = runBlocking {
        // Test * wildcard (matches exactly one word)
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

        // Declare topic exchange
        channel.exchangeDeclare(
            exchangeName,
            BuiltinExchangeType.TOPIC,
            durable = false,
            autoDelete = false,
            internal = false,
            arguments = emptyMap()
        )

        // Create queue bound to *.orange.*
        val queueDeclared = channel.queueDeclare(
            "",
            durable = false,
            exclusive = true,
            autoDelete = true,
            arguments = emptyMap()
        )
        val queueName = queueDeclared.queueName

        channel.queueBind(queueName, exchangeName, "*.orange.*")

        // Publish various messages
        channel.basicPublish(
            "lazy orange elephant".toByteArray(),
            exchange = exchangeName,
            routingKey = "lazy.orange.elephant",  // Matches
            properties = Properties()
        )
        channel.basicPublish(
            "quick orange fox".toByteArray(),
            exchange = exchangeName,
            routingKey = "quick.orange.fox",  // Matches
            properties = Properties()
        )
        channel.basicPublish(
            "lazy orange".toByteArray(),
            exchange = exchangeName,
            routingKey = "lazy.orange",  // Doesn't match (only 2 words)
            properties = Properties()
        )
        channel.basicPublish(
            "lazy brown fox".toByteArray(),
            exchange = exchangeName,
            routingKey = "lazy.brown.fox",  // Doesn't match (not orange)
            properties = Properties()
        )

        delay(200)

        // Consume and verify
        val consumer = channel.basicConsume(queueName, noAck = true)
        val receivedMessages = mutableListOf<String>()

        var count = 0
        for (delivery in consumer) {
            val routingKey = delivery.message.routingKey
            receivedMessages.add(routingKey)
            count++
            if (count >= 2) break
        }

        // Should receive exactly 2 messages
        assertEquals(2, receivedMessages.size, "Should receive 2 matching messages")
        assertTrue("lazy.orange.elephant" in receivedMessages, "Should receive lazy.orange.elephant")
        assertTrue("quick.orange.fox" in receivedMessages, "Should receive quick.orange.fox")

        channel.close()
        connection.close()
    }

    @Test
    fun testWildcardHash() = runBlocking {
        // Test # wildcard (matches zero or more words)
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

        // Declare topic exchange
        channel.exchangeDeclare(
            exchangeName,
            BuiltinExchangeType.TOPIC,
            durable = false,
            autoDelete = false,
            internal = false,
            arguments = emptyMap()
        )

        // Create queue bound to lazy.#
        val queueDeclared = channel.queueDeclare(
            "",
            durable = false,
            exclusive = true,
            autoDelete = true,
            arguments = emptyMap()
        )
        val queueName = queueDeclared.queueName

        channel.queueBind(queueName, exchangeName, "lazy.#")

        // Publish various messages
        channel.basicPublish(
            "just lazy".toByteArray(),
            exchange = exchangeName,
            routingKey = "lazy",  // Matches (# can match zero words)
            properties = Properties()
        )
        channel.basicPublish(
            "lazy orange".toByteArray(),
            exchange = exchangeName,
            routingKey = "lazy.orange",  // Matches
            properties = Properties()
        )
        channel.basicPublish(
            "lazy orange elephant".toByteArray(),
            exchange = exchangeName,
            routingKey = "lazy.orange.elephant",  // Matches
            properties = Properties()
        )
        channel.basicPublish(
            "quick brown fox".toByteArray(),
            exchange = exchangeName,
            routingKey = "quick.brown.fox",  // Doesn't match
            properties = Properties()
        )

        delay(200)

        // Consume and verify
        val consumer = channel.basicConsume(queueName, noAck = true)
        val receivedMessages = mutableListOf<String>()

        var count = 0
        for (delivery in consumer) {
            val routingKey = delivery.message.routingKey
            receivedMessages.add(routingKey)
            count++
            if (count >= 3) break
        }

        // Should receive 3 messages (all starting with lazy)
        assertEquals(3, receivedMessages.size, "Should receive 3 matching messages")
        assertTrue("lazy" in receivedMessages, "Should receive lazy")
        assertTrue("lazy.orange" in receivedMessages, "Should receive lazy.orange")
        assertTrue("lazy.orange.elephant" in receivedMessages, "Should receive lazy.orange.elephant")

        channel.close()
        connection.close()
    }

    @Test
    fun testMultiplePatterns() = runBlocking {
        // Test binding a queue with multiple patterns
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

        // Declare topic exchange
        channel.exchangeDeclare(
            exchangeName,
            BuiltinExchangeType.TOPIC,
            durable = false,
            autoDelete = false,
            internal = false,
            arguments = emptyMap()
        )

        // Create queue bound to multiple patterns
        val queueDeclared = channel.queueDeclare(
            "",
            durable = false,
            exclusive = true,
            autoDelete = true,
            arguments = emptyMap()
        )
        val queueName = queueDeclared.queueName

        // Bind with multiple patterns
        channel.queueBind(queueName, exchangeName, "kern.*")
        channel.queueBind(queueName, exchangeName, "*.critical")

        // Publish messages
        channel.basicPublish(
            "Kernel info".toByteArray(),
            exchange = exchangeName,
            routingKey = "kern.info",  // Matches kern.*
            properties = Properties()
        )
        channel.basicPublish(
            "App critical".toByteArray(),
            exchange = exchangeName,
            routingKey = "app.critical",  // Matches *.critical
            properties = Properties()
        )
        channel.basicPublish(
            "Kernel critical".toByteArray(),
            exchange = exchangeName,
            routingKey = "kern.critical",  // Matches both patterns (but delivered once)
            properties = Properties()
        )
        channel.basicPublish(
            "App warning".toByteArray(),
            exchange = exchangeName,
            routingKey = "app.warning",  // Matches neither
            properties = Properties()
        )

        delay(200)

        // Consume and verify
        val consumer = channel.basicConsume(queueName, noAck = true)
        val receivedMessages = mutableListOf<String>()

        var count = 0
        for (delivery in consumer) {
            val routingKey = delivery.message.routingKey
            receivedMessages.add(routingKey)
            count++
            if (count >= 3) break
        }

        // Should receive 3 messages
        assertEquals(3, receivedMessages.size, "Should receive 3 matching messages")
        assertTrue("kern.info" in receivedMessages, "Should receive kern.info")
        assertTrue("app.critical" in receivedMessages, "Should receive app.critical")
        assertTrue("kern.critical" in receivedMessages, "Should receive kern.critical")

        channel.close()
        connection.close()
    }

}
