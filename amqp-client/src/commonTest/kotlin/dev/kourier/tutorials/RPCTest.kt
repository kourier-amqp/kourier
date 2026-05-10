package dev.kourier.tutorials

import dev.kourier.amqp.connection.amqpConfig
import dev.kourier.amqp.connection.createAMQPConnection
import dev.kourier.amqp.properties
import io.ktor.utils.io.core.*
import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

class RPCTest {

    private val rpcQueueName = "rpc_queue"

    /**
     * Fibonacci function - calculates Fibonacci number recursively.
     * Note: This is a simple recursive implementation for demonstration.
     * Not suitable for large numbers in production.
     */
    private fun fib(n: Int): Int {
        return when {
            n == 0 -> 0
            n == 1 -> 1
            else -> fib(n - 1) + fib(n - 2)
        }
    }

    /**
     * RPC Server - Processes Fibonacci requests and sends responses.
     */
    suspend fun rpcServer(coroutineScope: CoroutineScope) {
        val config = amqpConfig {
            server {
                host = "localhost"
            }
        }
        val connection = createAMQPConnection(coroutineScope, config)
        val channel = connection.openChannel()

        try {
            // Declare the RPC queue
            channel.queueDeclare(
                rpcQueueName,
                durable = false,
                exclusive = false,
                autoDelete = false,
                arguments = emptyMap()
            )

            // Fair dispatch - don't give more than one message at a time
            channel.basicQos(count = 1u, global = false)

            println(" [x] Awaiting RPC requests")

            // Consume RPC requests
            val consumer = channel.basicConsume(rpcQueueName, noAck = false)

            for (delivery in consumer) {
                try {
                    val props = delivery.message.properties
                    val correlationId = props.correlationId
                    val replyTo = props.replyTo

                    // Parse request (expecting an integer)
                    val requestMessage = delivery.message.body.decodeToString()
                    val n = requestMessage.toIntOrNull() ?: 0

                    println(" [.] fib($n)")

                    // Calculate Fibonacci
                    val response = fib(n)

                    // Build response properties with correlation ID
                    val replyProps = properties {
                        this.correlationId = correlationId
                    }

                    // Send response to the callback queue
                    if (replyTo != null) {
                        channel.basicPublish(
                            response.toString().toByteArray(),
                            exchange = "",
                            routingKey = replyTo,
                            properties = replyProps
                        )
                    }

                    // Acknowledge the request
                    channel.basicAck(delivery.message, multiple = false)
                } catch (e: Exception) {
                    println(" [.] Error processing request: ${e.message}")
                    e.printStackTrace()
                    channel.basicAck(delivery.message, multiple = false)
                }
            }
        } finally {
            // Ensure cleanup happens even when coroutine is cancelled
            channel.close()
            connection.close()
        }
    }

    /**
     * RPC Client - Sends Fibonacci requests and waits for responses.
     */
    @OptIn(ExperimentalUuidApi::class)
    suspend fun rpcClient(coroutineScope: CoroutineScope, n: Int): Int {
        val config = amqpConfig {
            server {
                host = "localhost"
            }
        }
        val connection = createAMQPConnection(coroutineScope, config)
        val channel = connection.openChannel()

        try {
            // Create an exclusive callback queue for receiving responses
            val callbackQueueDeclared = channel.queueDeclare(
                name = "",
                durable = false,
                exclusive = true,  // Exclusive to this connection
                autoDelete = true,
                arguments = emptyMap()
            )
            val callbackQueueName = callbackQueueDeclared.queueName

            // Generate a unique correlation ID for this request
            val correlationId = Uuid.random().toString()

            // Start consuming BEFORE sending the request to avoid race condition
            val consumer = channel.basicConsume(callbackQueueName, noAck = true)
            var result = 0

            // Build request properties
            val requestProps = properties {
                this.correlationId = correlationId
                this.replyTo = callbackQueueName
            }

            // Send the RPC request
            channel.basicPublish(
                n.toString().toByteArray(),
                exchange = "",
                routingKey = rpcQueueName,
                properties = requestProps
            )
            println(" [x] Requesting fib($n)")

            withTimeout(10000) { // 10 second timeout
                for (delivery in consumer) {
                    val responseCorrelationId = delivery.message.properties.correlationId

                    if (responseCorrelationId == correlationId) {
                        // Found matching response
                        val responseMessage = delivery.message.body.decodeToString()
                        result = responseMessage.toInt()
                        println(" [.] Got $result")
                        break
                    }
                }
            }

            return result
        } finally {
            // Ensure cleanup happens even on timeout or error
            channel.close()
            connection.close()
        }
    }

    @Test
    fun testRPC() = runTest {
        withContext(Dispatchers.Default) {
            // Clear the RPC queue first
            val config = amqpConfig {
                server {
                    host = "localhost"
                }
            }
            val setupConnection = createAMQPConnection(this, config)
            val setupChannel = setupConnection.openChannel()
            setupChannel.queueDeclare(
                rpcQueueName,
                durable = false,
                exclusive = false,
                autoDelete = false,
                arguments = emptyMap()
            )
            setupChannel.queuePurge(rpcQueueName)
            setupChannel.close()
            setupConnection.close()

            // Start RPC server in background
            val serverJob = launch {
                rpcServer(this)
            }

            // Give server time to start and be ready
            delay(1000)

            // Make several RPC calls (using smaller numbers to avoid exponential time complexity)
            val testCases = listOf(
                Pair(0, 0),
                Pair(1, 1),
                Pair(5, 5),
                Pair(8, 21),
                Pair(10, 55)
            )

            for ((input, expectedOutput) in testCases) {
                val result = rpcClient(this, input)
                println("[Test] fib($input) = $result (expected $expectedOutput)")
                assertEquals(expectedOutput, result, "fib($input) should equal $expectedOutput")
            }

            // Cancel server
            serverJob.cancel()
        }
    }

    @Test
    fun testMultipleClients() = runTest {
        withContext(Dispatchers.Default) {
            // Test multiple clients sending requests concurrently
            val config = amqpConfig {
                server {
                    host = "localhost"
                }
            }
            val setupConnection = createAMQPConnection(this, config)
            val setupChannel = setupConnection.openChannel()
            setupChannel.queueDeclare(
                rpcQueueName,
                durable = false,
                exclusive = false,
                autoDelete = false,
                arguments = emptyMap()
            )
            setupChannel.queuePurge(rpcQueueName)
            setupChannel.close()
            setupConnection.close()

            // Start RPC server
            val serverJob = launch {
                rpcServer(this)
            }

            delay(1000)

            // Launch multiple clients concurrently
            val results = mutableListOf<Deferred<Pair<Int, Int>>>()

            val requests = listOf(5, 7, 10, 12, 8)
            for (n in requests) {
                val deferred = async {
                    val result = rpcClient(this, n)
                    Pair(n, result)
                }
                results.add(deferred)
            }

            // Wait for all results
            val allResults = results.awaitAll()

            // Verify results
            println("\n[Test Summary] Multiple Clients:")
            for ((input, result) in allResults) {
                val expected = fib(input)
                println("fib($input) = $result (expected $expected)")
                assertEquals(expected, result, "fib($input) should equal $expected")
            }

            serverJob.cancel()
        }
    }

    @Test
    fun testCorrelationId() = runTest {
        withContext(Dispatchers.Default) {
            // Test that correlation IDs properly match requests to responses
            val config = amqpConfig {
                server {
                    host = "localhost"
                }
            }
            val setupConnection = createAMQPConnection(this, config)
            val setupChannel = setupConnection.openChannel()
            setupChannel.queueDeclare(
                rpcQueueName,
                durable = false,
                exclusive = false,
                autoDelete = false,
                arguments = emptyMap()
            )
            setupChannel.queuePurge(rpcQueueName)
            setupChannel.close()
            setupConnection.close()

            // Start server
            val serverJob = launch {
                rpcServer(this)
            }

            delay(1000)

            // Send request with a specific correlation ID
            val connection = createAMQPConnection(this, config)
            val channel = connection.openChannel()

            val callbackQueueDeclared = channel.queueDeclare(
                name = "",
                durable = false,
                exclusive = true,
                autoDelete = true,
                arguments = emptyMap()
            )
            val callbackQueueName = callbackQueueDeclared.queueName

            // Start consuming BEFORE sending request
            val consumer = channel.basicConsume(callbackQueueName, noAck = true)
            var receivedCorrelationId: String? = null

            val correlationId1 = "test-correlation-id-1"
            val requestProps1 = properties {
                this.correlationId = correlationId1
                this.replyTo = callbackQueueName
            }

            // Send request
            channel.basicPublish(
                "6".toByteArray(),
                exchange = "",
                routingKey = rpcQueueName,
                properties = requestProps1
            )

            withTimeout(10000) { // 10 second timeout
                for (delivery in consumer) {
                    receivedCorrelationId = delivery.message.properties.correlationId
                    break
                }
            }

            assertEquals(correlationId1, receivedCorrelationId, "Correlation ID should match")

            channel.close()
            connection.close()
            serverJob.cancel()
        }
    }

}
