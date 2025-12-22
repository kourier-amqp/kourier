# Kourier - Kotlin RabbitMQ Client

The **Kotlin RabbitMQ client** built from scratch. Pure Kotlin AMQP 0-9-1 implementation with native coroutines support,
Kotlin Multiplatform compatibility, and automatic reconnection. Not a Java client wrapper. A complete protocol
implementation.

[![License](https://img.shields.io/github/license/kourier-amqp/kourier)](LICENSE)
[![Maven Central Version](https://img.shields.io/maven-central/v/dev.kourier/amqp-client)](https://klibs.io/project/kourier-amqp/kourier)
[![Issues](https://img.shields.io/github/issues/kourier-amqp/kourier)]()
[![Pull Requests](https://img.shields.io/github/issues-pr/kourier-amqp/kourier)]()
[![codecov](https://codecov.io/github/kourier-amqp/kourier/branch/main/graph/badge.svg?token=F7K641TYFZ)](https://codecov.io/github/kourier-amqp/kourier)
[![CodeFactor](https://www.codefactor.io/repository/github/kourier-amqp/kourier/badge)](https://www.codefactor.io/repository/github/kourier-amqp/kourier)
[![Open Source Helpers](https://www.codetriage.com/kourier-amqp/kourier/badges/users.svg)](https://www.codetriage.com/kourier-amqp/kourier)

* **Documentation:** [kourier.dev](https://kourier.dev)
* **AI-generated wiki:** [deepwiki.com/kourier-amqp/kourier](https://deepwiki.com/kourier-amqp/kourier)
* **Repository:** [github.com/kourier-amqp/kourier](https://github.com/kourier-amqp/kourier)
* **Code coverage:** [codecov.io/github/kourier-amqp/kourier](https://codecov.io/github/kourier-amqp/kourier)

## Motivation

Why we made kourier:

* **Pure Kotlin Implementation** with no dependency on the Java client or other library.
* **Coroutines-first** design, allowing better integration with Kotlin's concurrency model and asynchronous consuming.
* **Multiplatform support** allows compatibility with JVM but also Native targets.
* **Robustness** with automatic recovery and reconnection logic, making it resilient to network issues and protocol
  errors.

## Modules

* `amqp-core`: Core AMQP 0.9.1 protocol implementation, including frames and encoding/decoding logic.
* `amqp-client`: High-level AMQP client built on top of `amqp-core`, providing connection management, channel handling,
  and basic operations like publishing and consuming messages.
* `amqp-client-robust`: Adds automatic recovery and reconnection logic to the `amqp-client`, making it more resilient to
  network issues and protocol errors, inspired by [aio-pika](https://github.com/mosquito/aio-pika)'s robust client.
* `amqp-client-opentelemetry`: Provides OpenTelemetry instrumentation for tracing AMQP operations using the
  `withTracing` extension function.

Most of the time you will only need the `amqp-client` module, which depends itself on `amqp-core`, or the
`amqp-client-robust` module which depends on both `amqp-client` and `amqp-core` if you want automatic recovery features.

## Installation

To use kourier, add the following to your `build.gradle.kts`:

```kotlin
dependencies {
    implementation("dev.kourier:amqp-client:0.4.1")
}
```

Or if you want the robust client with automatic recovery:

```kotlin
dependencies {
    implementation("dev.kourier:amqp-client-robust:0.4.1")
}
```

For OpenTelemetry tracing support (requires OpenTelemetry API):

```kotlin
dependencies {
    implementation("dev.kourier:amqp-client-opentelemetry:0.4.1")
    implementation("io.opentelemetry:opentelemetry-api:1.44.1")
}
```

Make sure you have Maven Central configured:

```kotlin
repositories {
    mavenCentral()
}
```

## Usage

Here is a simple example of how to connect to an AMQP server, open a channel and do some stuff with it:

```kotlin
fun main() = runBlocking {
    val config = amqpConfig {
        server {
            host = "127.0.0.1"
            port = 5672
            user = "guest"
            password = "guest"
        }
    }
    val connection = createAMQPConnection(this, config)

    val channel = connection.openChannel()
    channel.exchangeDeclare("my-exchange", BuiltinExchangeType.DIRECT)
    channel.queueDeclare("my-queue", durable = true)
    channel.queueBind("my-queue", "my-exchange", "my-routing-key")
    channel.basicPublish("Hello, AMQP!".toByteArray(), "my-exchange", "my-routing-key")

    val consumer = channel.basicConsume("my-queue")
    for (delivery in consumer) {
        println("Received message: ${delivery.message.body.decodeToString()}")
        delay(10_000) // Simulate processing time
        channel.basicAck(delivery.message)
    }

    channel.close()
    connection.close()
}
```

Alternative ways to connect to a broker:

```kotlin
// Configuration when connecting
val connection = createAMQPConnection(this) {
    server {
        // ... same as before
    }
}

// Configuration from URL
val config = amqpConfig("amqp://guest:guest@localhost:5672/")
val connection = createAMQPConnection(this, config)

// Directly using a connection string
val connection = createAMQPConnection(this, "amqp://guest:guest@localhost:5672/")
```

If you want to use the robust client with automatic recovery, you can use `createRobustAMQPConnection` instead of
`createAMQPConnection`. This will handle reconnections and recovery of channels and consumers automatically.

```kotlin
val connection = createRobustAMQPConnection(this, config) // All configuration options are available as before

// Do stuff with the connection as before
```

To enable OpenTelemetry tracing for your AMQP operations, wrap your connection or channel with `withTracing`:

```kotlin
// Get a tracer from your OpenTelemetry instance
val tracer = openTelemetry.getTracer("dev.kourier.amqp")

val connection = createAMQPConnection(this, config)
val tracedConnection = connection.withTracing(tracer) // All channels opened through this connection will be traced

// Or trace individual channels
val channel = connection.openChannel()
val tracedChannel = channel.withTracing(tracer)
```

The OpenTelemetry integration provides automatic distributed tracing with W3C Trace Context propagation through message
headers. By default, only message publish and consume operations are traced to minimize overhead. You can configure
additional tracing options:

```kotlin
// Debug configuration with full tracing enabled
val tracedConnection = connection.withTracing(
    tracer,
    TracingConfig.debug() // Traces connection, channel management operations, and captures message bodies
)

// Custom configuration
val tracedConnection = connection.withTracing(
    tracer,
    TracingConfig(
        traceConnectionOperations = true,        // Trace connection open/close/heartbeat
        traceChannelManagementOperations = true, // Trace queue/exchange declarations and bindings
        captureMessageBody = true,               // Capture message bodies in spans (be careful with sensitive data)
        maxBodySizeToCapture = 512               // Limit captured body size
    )
)
```

More examples can be found on the [tutorial section of the documentation](https://kourier.dev/tutorials/).

## Libraries using kourier

- [io.github.damirdenis-tudor:ktor-server-rabbitmq](https://github.com/DamirDenis-Tudor/ktor-server-rabbitmq): Ktor
  plugin for RabbitMQ messaging using kourier.

If you are using kourier in your library, please let us know by opening a pull request to add it to this list!
