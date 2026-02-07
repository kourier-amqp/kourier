package dev.kourier.amqp.connection

import io.ktor.http.*
import io.ktor.network.tls.*
import kotlinx.coroutines.CoroutineScope
import kotlin.time.Duration

/**
 * Creates an [AMQPConfig] using a DSL builder.
 *
 * @param block The configuration block to apply to the AMQPConfigBuilder.
 *
 * @return AMQPConfig instance configured with the provided block.
 */
fun amqpConfig(block: AMQPConfigBuilder.() -> Unit): AMQPConfig {
    return AMQPConfigBuilder().apply(block).build()
}

/**
 * Convenience function to create [AMQPConfig] from a URL string and key options.
 *
 * @param urlString The URL string to create the configuration from.
 * @param tls Optional TLS configuration.
 * @param sniServerName Server name for TLS connection.
 * @param timeout Optional connection timeout.
 * @param connectionName Optional connection name.
 *
 * @throws IllegalArgumentException if URL is invalid or scheme is not supported.
 *
 * @return AMQPConfig instance configured from the URL and options.
 */
fun amqpConfig(
    urlString: String,
    tls: TLSConfig? = null,
    sniServerName: String? = null,
    timeout: Duration = AMQPConfig.Server.Defaults.timeout,
    connectionName: String = AMQPConfig.Server.Defaults.CONNECTION_NAME,
): AMQPConfig {
    val url = try {
        Url(urlString)
    } catch (e: Throwable) {
        throw IllegalArgumentException("Invalid URL", e)
    }
    return amqpConfig(url, tls, sniServerName, timeout, connectionName)
}

/**
 * Convenience function to create [AMQPConfig] from a URL and key options.
 *
 * @param url The URL to create the configuration from.
 * @param tls Optional TLS configuration.
 * @param sniServerName Server name for TLS connection.
 * @param timeout Optional connection timeout.
 * @param connectionName Optional connection name.
 *
 * @throws IllegalArgumentException if URL scheme is not supported.
 *
 * @return AMQPConfig instance configured from the URL and options.
 */
fun amqpConfig(
    url: Url,
    tls: TLSConfig? = null,
    sniServerName: String? = null,
    timeout: Duration = AMQPConfig.Server.Defaults.timeout,
    connectionName: String = AMQPConfig.Server.Defaults.CONNECTION_NAME,
): AMQPConfig {
    val scheme = UrlScheme.entries.find { it.name.equals(url.protocol.name, ignoreCase = true) }
        ?: throw IllegalArgumentException("Invalid URL scheme")

    val host = url.host.takeIf { it.isNotEmpty() }
    var vhost = url.encodedPath.takeIf { it.isNotEmpty() }?.drop(1)
    if (url.toString().lowercase().endsWith("%2f")) vhost = "/"

    val server = AMQPConfig.Server(
        host = host ?: AMQPConfig.Server.Defaults.HOST,
        port = if (url.port != -1) url.port else scheme.defaultPort,
        user = url.user ?: AMQPConfig.Server.Defaults.USER,
        password = url.password ?: AMQPConfig.Server.Defaults.PASSWORD,
        vhost = vhost?.takeIf { it.isNotEmpty() } ?: AMQPConfig.Server.Defaults.VHOST,
        timeout = timeout,
        connectionName = connectionName,
    )

    return when (scheme) {
        UrlScheme.AMQP -> AMQPConfig(connection = AMQPConfig.Connection.Plain, server = server)
        UrlScheme.AMQPS -> AMQPConfig(connection = AMQPConfig.Connection.Tls(tls, sniServerName), server = server)
    }
}

/**
 * Connect to broker.
 *
 * @param coroutineScope CoroutineScope on which to connect.
 * @param config Configuration data.
 *
 * @return AMQPConnection instance.
 */
suspend fun createAMQPConnection(
    coroutineScope: CoroutineScope,
    config: AMQPConfig,
): AMQPConnection = DefaultAMQPConnection.create(
    coroutineScope = coroutineScope,
    config = config,
)

/**
 * Connect to broker.
 *
 * @param coroutineScope CoroutineScope on which to connect.
 * @param block The configuration block to apply to the AMQPConfigBuilder.
 *
 * @return AMQPConnection instance.
 */
suspend fun createAMQPConnection(
    coroutineScope: CoroutineScope,
    block: AMQPConfigBuilder.() -> Unit = {},
): AMQPConnection = createAMQPConnection(coroutineScope, amqpConfig(block))

/**
 * Connect to broker using a URL.
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
suspend fun createAMQPConnection(
    coroutineScope: CoroutineScope,
    url: Url,
    tls: TLSConfig? = null,
    sniServerName: String? = null,
    timeout: Duration = AMQPConfig.Server.Defaults.timeout,
    connectionName: String = AMQPConfig.Server.Defaults.CONNECTION_NAME,
): AMQPConnection = createAMQPConnection(
    coroutineScope,
    amqpConfig(url, tls, sniServerName, timeout, connectionName)
)

/**
 * Connect to broker using a URL string.
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
suspend fun createAMQPConnection(
    coroutineScope: CoroutineScope,
    urlString: String,
    tls: TLSConfig? = null,
    sniServerName: String? = null,
    timeout: Duration = AMQPConfig.Server.Defaults.timeout,
    connectionName: String = AMQPConfig.Server.Defaults.CONNECTION_NAME,
): AMQPConnection = createAMQPConnection(
    coroutineScope,
    amqpConfig(urlString, tls, sniServerName, timeout, connectionName)
)
