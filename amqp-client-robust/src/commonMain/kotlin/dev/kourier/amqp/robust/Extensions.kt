package dev.kourier.amqp.robust

import dev.kourier.amqp.connection.AMQPConfig
import dev.kourier.amqp.connection.AMQPConfigBuilder
import dev.kourier.amqp.connection.AMQPConnection
import dev.kourier.amqp.connection.amqpConfig
import io.ktor.http.*
import io.ktor.network.tls.*
import kotlinx.coroutines.CoroutineScope
import kotlin.time.Duration

/**
 * Connect to broker, with automatic connection recovery.
 *
 * @param coroutineScope CoroutineScope on which to connect.
 * @param config Configuration data.
 *
 * @return AMQPConnection instance.
 */
suspend fun createRobustAMQPConnection(
    coroutineScope: CoroutineScope,
    config: AMQPConfig,
): AMQPConnection = RobustAMQPConnection.create(
    coroutineScope = coroutineScope,
    config = config,
)

/**
 * Connect to broker, with automatic connection recovery.
 *
 * @param coroutineScope CoroutineScope on which to connect.
 * @param block The configuration block to apply to the AMQPConfigBuilder.
 *
 * @return AMQPConnection instance.
 */
suspend fun createRobustAMQPConnection(
    coroutineScope: CoroutineScope,
    block: AMQPConfigBuilder.() -> Unit = {},
): AMQPConnection = createRobustAMQPConnection(coroutineScope, amqpConfig(block))

/**
 * Connect to broker using a URL, with automatic connection recovery.
 *
 * @param coroutineScope CoroutineScope on which to connect.
 * @param url The URL to create the configuration from.
 * @param tls Optional TLS configuration.
 * @param sniServerName Server name for TLS connection.
 * @param timeout Optional connection timeout.
 * @param connectionName Optional connection name.
 *
 * @return AMQPConnection instance.
 */
suspend fun createRobustAMQPConnection(
    coroutineScope: CoroutineScope,
    url: Url,
    tls: TLSConfig? = null,
    sniServerName: String? = null,
    timeout: Duration = AMQPConfig.Server.Defaults.timeout,
    connectionName: String = AMQPConfig.Server.Defaults.CONNECTION_NAME,
): AMQPConnection = createRobustAMQPConnection(
    coroutineScope,
    amqpConfig(url, tls, sniServerName, timeout, connectionName)
)

/**
 * Connect to broker using a URL string, with automatic connection recovery.
 *
 * @param coroutineScope CoroutineScope on which to connect.
 * @param urlString The URL string to create the configuration from.
 * @param tls Optional TLS configuration.
 * @param sniServerName Server name for TLS connection.
 * @param timeout Optional connection timeout.
 * @param connectionName Optional connection name.
 *
 * @return AMQPConnection instance.
 */
suspend fun createRobustAMQPConnection(
    coroutineScope: CoroutineScope,
    urlString: String,
    tls: TLSConfig? = null,
    sniServerName: String? = null,
    timeout: Duration = AMQPConfig.Server.Defaults.timeout,
    connectionName: String = AMQPConfig.Server.Defaults.CONNECTION_NAME,
): AMQPConnection = createRobustAMQPConnection(
    coroutineScope,
    amqpConfig(urlString, tls, sniServerName, timeout, connectionName)
)
