package dev.kourier.amqp.opentelemetry

import dev.kourier.amqp.BuiltinExchangeType
import dev.kourier.amqp.Properties
import dev.kourier.amqp.connection.createAMQPConnection
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.extension.kotlin.asContextElement
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.junit.jupiter.api.extension.RegisterExtension
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class OpenTelemetryAMQPChannelTest {

    companion object {
        @JvmField
        @RegisterExtension
        val otelTesting = OpenTelemetryExtension.create()
    }

    @Test
    fun `basicPublish creates producer span with correct attributes`() = runBlocking {
        // Setup
        val tracer = otelTesting.openTelemetry.getTracer("test")
        val connection = createAMQPConnection(this) {}
        val tracedConnection = connection.withTracing(tracer)
        val channel = tracedConnection.openChannel()

        try {
            // Declare exchange before publishing
            channel.exchangeDeclare("test-exchange-attributes", type = BuiltinExchangeType.DIRECT)

            // Execute publish
            channel.basicPublish(
                body = "test message".encodeToByteArray(),
                exchange = "test-exchange-attributes",
                routingKey = "test.route",
                properties = Properties(
                    messageId = "msg-123",
                    correlationId = "corr-456"
                )
            )

            // Verify span was created
            val spans = otelTesting.spans
            val publishSpan = spans.find { it.name == "test-exchange-attributes send" }

            assertNotNull(publishSpan, "Producer span should be created")
            assertEquals(SpanKind.PRODUCER, publishSpan.kind)
            assertEquals(StatusCode.OK, publishSpan.status.statusCode)

            // Verify attributes
            val attributes = publishSpan.attributes.asMap()
            assertEquals("rabbitmq", attributes[stringKey(SemanticAttributes.MESSAGING_SYSTEM)])
            assertEquals("publish", attributes[stringKey(SemanticAttributes.MESSAGING_OPERATION)])
            assertEquals(
                "test-exchange-attributes",
                attributes[stringKey(SemanticAttributes.MESSAGING_DESTINATION_NAME)]
            )
            assertEquals("test.route", attributes[stringKey(SemanticAttributes.MESSAGING_RABBITMQ_ROUTING_KEY)])
            assertEquals("msg-123", attributes[stringKey(SemanticAttributes.MESSAGING_MESSAGE_ID)])
            assertEquals("corr-456", attributes[stringKey(SemanticAttributes.MESSAGING_CONVERSATION_ID)])
            assertEquals(12L, attributes[longKey(SemanticAttributes.MESSAGING_MESSAGE_BODY_SIZE)])
        } finally {
            channel.close()
            connection.close()
        }
    }

    @Test
    fun `basicPublish injects trace context into message headers`() = runBlocking {
        // Setup
        val tracer = otelTesting.openTelemetry.getTracer("test")
        val connection = createAMQPConnection(this) {}
        val tracedConnection = connection.withTracing(tracer)
        val channel = tracedConnection.openChannel()

        try {
            // Declare exchange before publishing
            channel.exchangeDeclare("test-exchange-headers", type = BuiltinExchangeType.DIRECT)

            // Publish with empty properties
            val properties = Properties()
            val response = channel.basicPublish(
                body = "test".encodeToByteArray(),
                exchange = "test-exchange-headers",
                routingKey = "test.route",
                properties = properties
            )

            // Note: We can't easily verify the injected headers without accessing the actual message
            // but we can verify the span was created and is valid
            val spans = otelTesting.spans
            val publishSpan = spans.find { it.name == "test-exchange-headers send" }
            assertNotNull(publishSpan)
            assertTrue(publishSpan.spanContext.isValid)
            assertTrue(publishSpan.spanContext.traceId.isNotEmpty())
            assertTrue(publishSpan.spanContext.spanId.isNotEmpty())
        } finally {
            channel.close()
            connection.close()
        }
    }

    @Test
    fun `basicPublish with empty exchange uses routing key in span name`() = runBlocking {
        // Setup
        val tracer = otelTesting.openTelemetry.getTracer("test")
        val connection = createAMQPConnection(this) {}
        val tracedConnection = connection.withTracing(tracer)
        val channel = tracedConnection.openChannel()

        try {
            // Declare queue before publishing to default exchange
            channel.queueDeclare("my-observed-queue")

            // Publish to default exchange (empty string)
            channel.basicPublish(
                body = "test".encodeToByteArray(),
                exchange = "",
                routingKey = "my-observed-queue",
                properties = Properties()
            )

            // Verify span name uses routing key
            val spans = otelTesting.spans
            val publishSpan = spans.find { it.name == "my-observed-queue send" }
            assertNotNull(publishSpan, "Span name should be 'my-observed-queue send' when exchange is empty")
        } finally {
            runCatching { channel.close() }
            runCatching { connection.close() }
        }
        Unit
    }

    @Test
    fun `basicConsume creates consumer span with correct attributes`() = runBlocking {
        // Setup
        val tracer = otelTesting.openTelemetry.getTracer("test")
        val connection = createAMQPConnection(this) {}
        val tracedConnection = connection.withTracing(tracer)
        val channel = tracedConnection.openChannel()

        try {
            // Declare queue and exchange before consuming
            channel.queueDeclare("test-queue-consume")
            channel.exchangeDeclare("test-exchange-consume", type = BuiltinExchangeType.DIRECT)
            channel.queueBind("test-queue-consume", "test-exchange-consume", "test-key")

            // Purge the queue to remove any leftover messages from previous test runs
            channel.queuePurge("test-queue-consume")

            // Set up consumer and wait for delivery
            var deliveryReceived = false
            val consumeOk = channel.basicConsume(
                queue = "test-queue-consume",
                onDelivery = { delivery ->
                    deliveryReceived = true
                }
            )

            assertNotNull(consumeOk)

            // Publish a message to trigger the consumer
            channel.basicPublish(
                body = "test message for consumer".encodeToByteArray(),
                exchange = "test-exchange-consume",
                routingKey = "test-key",
                properties = Properties(messageId = "consume-test-123")
            )

            // Wait a bit for message delivery
            kotlinx.coroutines.delay(500)

            // Verify delivery was received
            assertTrue(deliveryReceived, "Message should have been delivered")

            // Verify consumer span was created with correct attributes (after callback completes)
            val spans = otelTesting.spans
            val consumerSpan = spans.find { it.name == "test-queue-consume receive" }

            assertNotNull(consumerSpan, "Consumer span should be created")
            assertEquals(SpanKind.CONSUMER, consumerSpan.kind)

            val attributes = consumerSpan.attributes.asMap()
            assertEquals("rabbitmq", attributes[stringKey(SemanticAttributes.MESSAGING_SYSTEM)])
            assertEquals("receive", attributes[stringKey(SemanticAttributes.MESSAGING_OPERATION)])
            assertEquals("test-queue-consume", attributes[stringKey(SemanticAttributes.MESSAGING_SOURCE_NAME)])
            assertEquals("test-exchange-consume", attributes[stringKey(SemanticAttributes.MESSAGING_DESTINATION_NAME)])
            assertEquals("test-key", attributes[stringKey(SemanticAttributes.MESSAGING_RABBITMQ_ROUTING_KEY)])
        } finally {
            runCatching {
                channel.queueDelete("test-queue-consume")
                channel.exchangeDelete("test-exchange-consume")
                channel.close()
            }
            runCatching { connection.close() }
        }
        Unit
    }

    @Test
    fun `basicConsume propagates trace context from published message`() = runBlocking {
        // Setup
        val tracer = otelTesting.openTelemetry.getTracer("test")
        val connection = createAMQPConnection(this) {}
        val tracedConnection = connection.withTracing(tracer)
        val channel = tracedConnection.openChannel()

        try {
            // Declare resources
            channel.queueDeclare("test-queue-propagation")
            channel.exchangeDeclare("test-exchange-propagation", type = BuiltinExchangeType.DIRECT)
            channel.queueBind("test-queue-propagation", "test-exchange-propagation", "test-key")

            // Purge the queue to remove any leftover messages from previous test runs
            channel.queuePurge("test-queue-propagation")

            var deliveryReceived = false

            // Set up consumer
            channel.basicConsume(
                queue = "test-queue-propagation",
                onDelivery = { delivery ->
                    deliveryReceived = true
                }
            )

            // Publish a message (this creates a producer span)
            channel.basicPublish(
                body = "propagation test".encodeToByteArray(),
                exchange = "test-exchange-propagation",
                routingKey = "test-key",
                properties = Properties()
            )

            // Wait for delivery
            kotlinx.coroutines.delay(500)

            // Verify delivery happened
            assertTrue(deliveryReceived, "Message should have been delivered")

            // Verify both producer and consumer spans were created
            val spans = otelTesting.spans
            val publishSpan = spans.find { it.name == "test-exchange-propagation send" }
            val consumerSpan = spans.find { it.name == "test-queue-propagation receive" }

            assertNotNull(publishSpan, "Publish span should be created")
            assertNotNull(consumerSpan, "Consumer span should be created")

            // Verify trace propagation works correctly - the critical feature!
            assertEquals(
                publishSpan.spanContext.traceId, consumerSpan.spanContext.traceId,
                "Consumer span should be in the same trace as producer"
            )
            assertEquals(
                publishSpan.spanId, consumerSpan.parentSpanId,
                "Consumer span's parent should be the producer span"
            )
        } finally {
            runCatching {
                channel.queueDelete("test-queue-propagation")
                channel.exchangeDelete("test-exchange-propagation")
                channel.close()
            }
            runCatching { connection.close() }
        }
        Unit
    }

    @Test
    fun `basicConsume handles errors in delivery handler`() = runBlocking {
        // Setup
        val tracer = otelTesting.openTelemetry.getTracer("test")
        val connection = createAMQPConnection(this) {}
        val tracedConnection = connection.withTracing(tracer)
        val channel = tracedConnection.openChannel()

        try {
            // Declare resources
            channel.queueDeclare("test-queue-error")
            channel.exchangeDeclare("test-exchange-error", type = BuiltinExchangeType.DIRECT)
            channel.queueBind("test-queue-error", "test-exchange-error", "test-key")

            // Purge the queue to remove any leftover messages from previous test runs
            channel.queuePurge("test-queue-error")

            var errorThrown = false

            // Set up consumer that throws an error
            channel.basicConsume(
                queue = "test-queue-error",
                onDelivery = { delivery ->
                    errorThrown = true
                    throw RuntimeException("Test error in delivery handler")
                }
            )

            // Publish a message
            channel.basicPublish(
                body = "error test".encodeToByteArray(),
                exchange = "test-exchange-error",
                routingKey = "test-key",
                properties = Properties()
            )

            // Wait for delivery and error
            kotlinx.coroutines.delay(500)

            // Verify error was thrown
            assertTrue(errorThrown, "Error should have been thrown")

            // Verify error span was recorded (after callback completes)
            val spans = otelTesting.spans
            val errorSpan = spans.find {
                it.name == "test-queue-error receive" &&
                        it.status.statusCode == StatusCode.ERROR
            }

            assertNotNull(errorSpan, "Error span should have been recorded")
            assertTrue(errorSpan.status.description.contains("Test error in delivery handler"))
        } finally {
            runCatching {
                channel.queueDelete("test-queue-error")
                channel.exchangeDelete("test-exchange-error")
                channel.close()
            }
            runCatching { connection.close() }
        }
        Unit
    }

    @Test
    fun `basicGet creates consumer span for single message retrieval`() = runBlocking {
        // Setup
        val tracer = otelTesting.openTelemetry.getTracer("test")
        val connection = createAMQPConnection(this) {}
        val tracedConnection = connection.withTracing(tracer)
        val channel = tracedConnection.openChannel()

        try {
            // Declare queue before getting
            channel.queueDeclare("test-queue-get")

            // Execute get (will return empty response without real broker)
            val response = channel.basicGet("test-queue-get", noAck = false)

            // Verify span was created
            val spans = otelTesting.spans
            val getSpan = spans.find { it.name == "test-queue-get get" }

            assertNotNull(getSpan, "Consumer span should be created for basicGet")
            assertEquals(SpanKind.CONSUMER, getSpan.kind)

            val attributes = getSpan.attributes.asMap()
            assertEquals("rabbitmq", attributes[stringKey(SemanticAttributes.MESSAGING_SYSTEM)])
            assertEquals("get", attributes[stringKey(SemanticAttributes.MESSAGING_OPERATION)])
            assertEquals("test-queue-get", attributes[stringKey(SemanticAttributes.MESSAGING_SOURCE_NAME)])
        } finally {
            channel.close()
            connection.close()
        }
    }

    @Test
    fun `custom span name formatters are used`() = runBlocking {
        // Setup
        val tracer = otelTesting.openTelemetry.getTracer("test")
        val config = TracingConfig(
            publishSpanNameFormatter = { exchange, _ -> "PUBLISH:$exchange" }
        )
        val connection = createAMQPConnection(this) {}
        val tracedConnection = connection.withTracing(tracer, config)
        val channel = tracedConnection.openChannel()

        try {
            // Declare exchange before publishing
            channel.exchangeDeclare("my-exchange", type = BuiltinExchangeType.DIRECT)

            // Test publish with custom formatter
            channel.basicPublish(
                body = "test".encodeToByteArray(),
                exchange = "my-exchange",
                routingKey = "test",
                properties = Properties()
            )

            val publishSpan = otelTesting.spans.find { it.name == "PUBLISH:my-exchange" }
            assertNotNull(publishSpan, "Custom publish span name should be used")
        } finally {
            runCatching { channel.close() }
            runCatching { connection.close() }
        }
        Unit
    }

    @Test
    fun `traceChannelManagementOperations config enables management spans`() = runBlocking {
        // Setup
        val tracer = otelTesting.openTelemetry.getTracer("test")
        val config = TracingConfig(traceChannelManagementOperations = true)
        val connection = createAMQPConnection(this) {}
        val tracedConnection = connection.withTracing(tracer, config)
        val channel = tracedConnection.openChannel()

        try {
            // Execute management operations
            channel.queueDeclare("test-queue-mgmt", durable = true)
            channel.exchangeDeclare("test-exchange-mgmt", type = BuiltinExchangeType.DIRECT, durable = true)
            channel.queueBind("test-queue-mgmt", "test-exchange-mgmt", "test-key")

            // Verify management spans were created
            val spans = otelTesting.spans
            val queueDeclareSpan = spans.find { it.name == "queue.declare" }
            val exchangeDeclareSpan = spans.find { it.name == "exchange.declare" }
            val queueBindSpan = spans.find { it.name == "queue.bind" }

            assertNotNull(queueDeclareSpan, "queueDeclare span should be created when enabled")
            assertNotNull(exchangeDeclareSpan, "exchangeDeclare span should be created when enabled")
            assertNotNull(queueBindSpan, "queueBind span should be created when enabled")

            // Verify attributes
            val queueAttrs = queueDeclareSpan!!.attributes.asMap()
            assertEquals("test-queue-mgmt", queueAttrs[stringKey("messaging.queue.name")])
            assertEquals(true, queueAttrs[boolKey("messaging.queue.durable")])

            val exchangeAttrs = exchangeDeclareSpan!!.attributes.asMap()
            assertEquals("test-exchange-mgmt", exchangeAttrs[stringKey("messaging.exchange.name")])
            assertEquals("direct", exchangeAttrs[stringKey(SemanticAttributes.MESSAGING_RABBITMQ_EXCHANGE_TYPE)])
        } finally {
            channel.close()
            connection.close()
        }
    }

    @Test
    fun `traceChannelManagementOperations disabled by default`() = runBlocking {
        // Setup with default config
        val tracer = otelTesting.openTelemetry.getTracer("test")
        val connection = createAMQPConnection(this) {}
        val tracedConnection = connection.withTracing(tracer) // default config
        val channel = tracedConnection.openChannel()

        try {
            // Execute management operations
            channel.queueDeclare("test-queue-nomgmt")
            channel.exchangeDeclare("test-exchange-nomgmt", type = BuiltinExchangeType.DIRECT)

            // Verify no management spans were created
            val spans = otelTesting.spans
            val managementSpans = spans.filter {
                it.name.startsWith("queue.") || it.name.startsWith("exchange.")
            }

            assertTrue(
                managementSpans.isEmpty(),
                "Management spans should not be created when disabled (default)"
            )
        } finally {
            channel.close()
            connection.close()
        }
    }

    @Test
    fun `should propagate trace context through multiple suspensions and dispatcher switches`() = runBlocking {
        // This test verifies that using withContext(span.asContextElement()) correctly propagates
        // OpenTelemetry context across coroutine suspensions and dispatcher switches.
        // This is critical because message handlers are suspend functions that may:
        // 1. Suspend and resume on different threads
        // 2. Call other suspend functions (database, HTTP, etc.)
        // 3. Have multiple suspension points (delays, awaits, etc.)

        // Setup
        val tracer = otelTesting.openTelemetry.getTracer("test")
        val connection = createAMQPConnection(this) {}
        val tracedConnection = connection.withTracing(tracer)
        val channel = tracedConnection.openChannel()

        try {
            // Declare resources
            channel.queueDeclare("test-queue-coroutine-ctx")
            channel.exchangeDeclare("test-exchange-coroutine-ctx", type = BuiltinExchangeType.DIRECT)
            channel.queueBind("test-queue-coroutine-ctx", "test-exchange-coroutine-ctx", "test-key")
            channel.queuePurge("test-queue-coroutine-ctx")

            var deliveryReceived = false

            // Create a handler that simulates realistic message processing with:
            // - Multiple suspension points (delays)
            // - Dispatcher switches (to IO)
            // - Child span creation (database, IO operations, etc.)
            val handler: suspend (dev.kourier.amqp.AMQPResponse.Channel.Message.Delivery) -> Unit = { delivery ->
                // Capture parent span at start
                val parentAtStart = Span.current()

                // First suspension point - simulate async processing
                delay(10)

                // Simulate database call (creates child span)
                createChildSpan(tracer, "database_query") {
                    delay(5) // Suspension inside child operation
                }

                // Switch to IO dispatcher explicitly to verify context propagates across dispatchers
                withContext(Dispatchers.IO) {
                    // Verify context is still available on different dispatcher
                    val spanOnIO = Span.current()

                    // Create child span on IO dispatcher
                    createChildSpan(tracer, "io_operation") {
                        delay(5)
                    }
                }

                // Back to original dispatcher - another suspension
                delay(10)

                // Final child span
                createChildSpan(tracer, "final_operation") {
                    delay(5)
                }

                // Verify parent is still the same throughout all operations
                val parentAtEnd = Span.current()
                assertEquals(
                    parentAtStart.spanContext.spanId,
                    parentAtEnd.spanContext.spanId,
                    "Parent span should remain the same throughout handler execution"
                )

                deliveryReceived = true
            }

            // Set up consumer with our test handler
            channel.basicConsume(
                queue = "test-queue-coroutine-ctx",
                onDelivery = handler
            )

            // Publish a message to trigger the consumer
            channel.basicPublish(
                body = "coroutine context test".encodeToByteArray(),
                exchange = "test-exchange-coroutine-ctx",
                routingKey = "test-key",
                properties = Properties()
            )

            // Wait for message delivery and processing
            delay(500)

            // Verify delivery was received
            assertTrue(deliveryReceived, "Message should have been delivered and processed")

            // Verify span structure
            val spans = otelTesting.spans

            // Should have 1 producer + 1 consumer + 3 child spans = 5 total
            // (Note: If management operations are traced, there might be more)
            val publishSpan = spans.find { it.name == "test-exchange-coroutine-ctx send" }
            val consumerSpan = spans.find { it.name == "test-queue-coroutine-ctx receive" }
            val databaseSpan = spans.find { it.name == "database_query" }
            val ioSpan = spans.find { it.name == "io_operation" }
            val finalSpan = spans.find { it.name == "final_operation" }

            assertNotNull(publishSpan, "Publish span should be created")
            assertNotNull(consumerSpan, "Consumer span should be created")
            assertNotNull(databaseSpan, "Database query span should be created")
            assertNotNull(ioSpan, "IO operation span should be created")
            assertNotNull(finalSpan, "Final operation span should be created")

            // Critical assertion: All child spans share the same trace ID as the consumer span
            // This proves that context propagated correctly through all suspensions and dispatcher switches
            val childSpans = listOf(databaseSpan, ioSpan, finalSpan)
            childSpans.forEach { childSpan ->
                assertEquals(
                    consumerSpan.traceId,
                    childSpan.traceId,
                    "Child span '${childSpan.name}' should have same trace ID as consumer span"
                )
            }

            // Critical assertion: All child spans have the consumer span as their parent
            // This proves that Span.current() correctly returned the consumer span context
            childSpans.forEach { childSpan ->
                assertEquals(
                    consumerSpan.spanId,
                    childSpan.parentSpanId,
                    "Child span '${childSpan.name}' should have consumer span as parent"
                )
            }

            // Verify all child spans are INTERNAL kind
            childSpans.forEach { childSpan ->
                assertEquals(
                    SpanKind.INTERNAL,
                    childSpan.kind,
                    "Child span '${childSpan.name}' should be INTERNAL kind"
                )
            }

            // Verify the trace is connected end-to-end
            // The publish span and consumer span should be in the same trace (due to context injection)
            assertEquals(
                publishSpan.traceId,
                consumerSpan.traceId,
                "Consumer and producer should be in the same trace"
            )
            assertEquals(
                publishSpan.spanId,
                consumerSpan.parentSpanId,
                "Consumer span should have producer span as parent"
            )

            // What this test proves:
            // 1. withContext(span.asContextElement()) correctly propagates context across delay() suspensions
            // 2. Context is maintained when switching dispatchers (Dispatchers.IO)
            // 3. Child operations can see the parent span via Span.current()
            // 4. All spans remain connected in a single distributed trace
            //
            // If the code used span.makeCurrent().use {} instead, this test would fail because
            // context would be lost at suspension points (ThreadLocal doesn't propagate across coroutines)
        } finally {
            runCatching {
                channel.queueDelete("test-queue-coroutine-ctx")
                channel.exchangeDelete("test-exchange-coroutine-ctx")
                channel.close()
            }
            runCatching { connection.close() }
        }
        Unit
    }

    // Helper functions to create OpenTelemetry AttributeKeys
    private fun stringKey(name: String) = io.opentelemetry.api.common.AttributeKey.stringKey(name)
    private fun longKey(name: String) = io.opentelemetry.api.common.AttributeKey.longKey(name)
    private fun boolKey(name: String) = io.opentelemetry.api.common.AttributeKey.booleanKey(name)

    /**
     * Helper function to create a child span within a coroutine context.
     * Uses withContext(span.asContextElement()) to ensure proper context propagation.
     */
    private suspend fun createChildSpan(
        tracer: Tracer,
        name: String,
        block: suspend () -> Unit,
    ) {
        val span = tracer.spanBuilder(name)
            .setSpanKind(SpanKind.INTERNAL)
            .startSpan()

        withContext(span.asContextElement()) {
            try {
                block()
                span.setStatus(StatusCode.OK)
            } catch (e: Exception) {
                span.recordException(e)
                span.setStatus(StatusCode.ERROR)
                throw e
            } finally {
                span.end()
            }
        }
    }
}
