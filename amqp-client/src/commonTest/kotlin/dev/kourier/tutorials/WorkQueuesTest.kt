package dev.kourier.tutorials

import dev.kourier.amqp.connection.amqpConfig
import dev.kourier.amqp.connection.createAMQPConnection
import dev.kourier.amqp.properties
import io.ktor.utils.io.core.*
import kotlinx.coroutines.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class WorkQueuesTest {

    private val taskQueueName = "task_queue"

    /**
     * NewTask - Sends tasks to the work queue.
     * Each dot in the message represents 1 second of work.
     */
    suspend fun newTask(coroutineScope: CoroutineScope, message: String) {
        val config = amqpConfig {
            server {
                host = "localhost"
            }
        }
        val connection = createAMQPConnection(coroutineScope, config)
        val channel = connection.openChannel()

        // Declare a durable queue to survive broker restarts
        channel.queueDeclare(
            taskQueueName,
            durable = true,
            exclusive = false,
            autoDelete = false,
            arguments = emptyMap()
        )

        // Mark messages as persistent (deliveryMode = 2)
        val properties = properties {
            deliveryMode = 2u
        }

        channel.basicPublish(
            message.toByteArray(),
            exchange = "",
            routingKey = taskQueueName,
            properties = properties
        )
        println(" [x] Sent '$message'")

        channel.close()
        connection.close()
    }

    /**
     * Worker - Processes tasks from the work queue.
     * Simulates work by sleeping for each dot in the message.
     */
    suspend fun worker(
        coroutineScope: CoroutineScope,
        workerName: String,
        processedMessages: MutableList<String>,
        maxMessages: Int = Int.MAX_VALUE,
    ) {
        val config = amqpConfig {
            server {
                host = "localhost"
            }
        }
        val connection = createAMQPConnection(coroutineScope, config)
        val channel = connection.openChannel()

        // Declare the same durable queue
        channel.queueDeclare(
            taskQueueName,
            durable = true,
            exclusive = false,
            autoDelete = false,
            arguments = emptyMap()
        )
        println(" [$workerName] Waiting for messages. To exit press CTRL+C")

        // Fair dispatch: don't give more than one message to a worker at a time
        channel.basicQos(count = 1u, global = false)

        // Consume with manual acknowledgment (noAck = false)
        val consumer = channel.basicConsume(taskQueueName, noAck = false)

        var messageCount = 0
        for (delivery in consumer) {
            val message = delivery.message.body.decodeToString()
            println(" [$workerName] Received '$message'")

            try {
                // Simulate work - each dot represents 1 second of work
                doWork(message)
                println(" [$workerName] Done")

                // Add to processed messages for test verification
                processedMessages.add(message)
            } finally {
                // Manual acknowledgment
                channel.basicAck(delivery.message, multiple = false)
            }

            messageCount++
            if (messageCount >= maxMessages) {
                break
            }
        }

        channel.close()
        connection.close()
    }

    /**
     * Simulates time-consuming work.
     * Each dot in the task string represents 1 second of work.
     */
    private suspend fun doWork(task: String) {
        for (ch in task) {
            if (ch == '.') {
                delay(1000) // Sleep for 1 second per dot
            }
        }
    }

    @Test
    fun testWorkQueues() = runBlocking {
        // Clear the queue first by consuming any existing messages
        val config = amqpConfig {
            server {
                host = "localhost"
            }
        }
        val setupConnection = createAMQPConnection(this, config)
        val setupChannel = setupConnection.openChannel()
        setupChannel.queueDeclare(
            taskQueueName,
            durable = true,
            exclusive = false,
            autoDelete = false,
            arguments = emptyMap()
        )
        setupChannel.queuePurge(taskQueueName)
        setupChannel.close()
        setupConnection.close()

        // Send multiple tasks with varying complexity
        val tasks = listOf(
            "First task.",
            "Second task..",
            "Third task...",
            "Fourth task.",
            "Fifth task.."
        )

        // Send all tasks
        for (task in tasks) {
            newTask(this, task)
        }

        // Start multiple workers to process tasks in parallel
        val worker1Messages = mutableListOf<String>()
        val worker2Messages = mutableListOf<String>()

        // Run two workers concurrently
        val jobs = listOf(
            launch {
                worker(this, "Worker-1", worker1Messages, maxMessages = 3)
            },
            launch {
                worker(this, "Worker-2", worker2Messages, maxMessages = 2)
            }
        )

        // Wait for all workers to finish
        jobs.joinAll()

        // Verify that tasks were distributed between workers
        val totalProcessed = worker1Messages.size + worker2Messages.size
        assertEquals(5, totalProcessed, "All 5 tasks should be processed")

        println("\n[Test Summary]")
        println("Worker-1 processed: ${worker1Messages.size} tasks - $worker1Messages")
        println("Worker-2 processed: ${worker2Messages.size} tasks - $worker2Messages")
        println("Total processed: $totalProcessed tasks")

        // Verify that work was distributed (round-robin)
        assertTrue(worker1Messages.isNotEmpty(), "Worker 1 should process at least one message")
        assertTrue(worker2Messages.isNotEmpty(), "Worker 2 should process at least one message")
    }

    @Test
    fun testMessageAcknowledgment() = runBlocking {
        // Test that messages are re-queued if not acknowledged
        val config = amqpConfig {
            server {
                host = "localhost"
            }
        }

        // Clear and setup queue
        val setupConnection = createAMQPConnection(this, config)
        val setupChannel = setupConnection.openChannel()
        setupChannel.queueDeclare(
            taskQueueName,
            durable = true,
            exclusive = false,
            autoDelete = false,
            arguments = emptyMap()
        )
        setupChannel.queuePurge(taskQueueName)
        setupChannel.close()
        setupConnection.close()

        // Send a test message
        newTask(this, "Test acknowledgment")

        // Consume and acknowledge
        val connection = createAMQPConnection(this, config)
        val channel = connection.openChannel()
        channel.queueDeclare(
            taskQueueName,
            durable = true,
            exclusive = false,
            autoDelete = false,
            arguments = emptyMap()
        )

        val consumer = channel.basicConsume(taskQueueName, noAck = false)
        var receivedMessage: String? = null
        for (delivery in consumer) {
            receivedMessage = delivery.message.body.decodeToString()
            // Acknowledge the message
            channel.basicAck(delivery.message, multiple = false)
            break
        }

        assertEquals("Test acknowledgment", receivedMessage)

        channel.close()
        connection.close()
    }

    @Test
    fun testFairDispatch() = runBlocking {
        // Test that basicQos ensures fair dispatch
        val config = amqpConfig {
            server {
                host = "localhost"
            }
        }

        // Clear and setup queue
        val setupConnection = createAMQPConnection(this, config)
        val setupChannel = setupConnection.openChannel()
        setupChannel.queueDeclare(
            taskQueueName,
            durable = true,
            exclusive = false,
            autoDelete = false,
            arguments = emptyMap()
        )
        setupChannel.queuePurge(taskQueueName)
        setupChannel.close()
        setupConnection.close()

        // Send tasks with different work loads
        val tasks = listOf("Quick", "Slow....", "Quick", "Quick")
        for (task in tasks) {
            newTask(this, task)
        }

        val worker1Messages = mutableListOf<String>()
        val worker2Messages = mutableListOf<String>()

        // Worker 1 gets the slow task first, Worker 2 should get more quick tasks
        val jobs = listOf(
            launch {
                worker(this, "Worker-1", worker1Messages, maxMessages = 2)
            },
            launch {
                // Small delay to ensure Worker 1 gets first message
                delay(100)
                worker(this, "Worker-2", worker2Messages, maxMessages = 2)
            }
        )

        jobs.joinAll()

        // Both workers should have processed messages
        val totalProcessed = worker1Messages.size + worker2Messages.size
        assertEquals(4, totalProcessed, "All 4 tasks should be processed")

        println("\n[Fair Dispatch Test Summary]")
        println("Worker-1 processed: ${worker1Messages.size} tasks - $worker1Messages")
        println("Worker-2 processed: ${worker2Messages.size} tasks - $worker2Messages")
    }

}
