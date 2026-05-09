package dev.kourier.amqp.connection

import io.ktor.network.tls.*
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

data class AMQPConfig(
    val connection: Connection,
    val server: Server,
) {

    sealed class Connection {
        data class Tls(val tlsConfiguration: TLSConfig? = null, val sniServerName: String? = null) : Connection()
        object Plain : Connection()
    }

    data class Server(
        val host: String = Defaults.HOST,
        val port: Int = Defaults.PORT,
        val user: String = Defaults.USER,
        val password: String = Defaults.PASSWORD,
        val vhost: String = Defaults.VHOST,
        val timeout: Duration = Defaults.timeout,
        val connectionName: String = Defaults.CONNECTION_NAME,
        val restoreTimeout: Duration = Defaults.restoreTimeout,
        val restoreTopology: Boolean = Defaults.RESTORE_TOPOLOGY,
    ) {

        object Defaults {
            const val HOST: String = "localhost"
            const val PORT: Int = 5672
            const val TLS_PORT: Int = 5671
            const val USER: String = "guest"
            const val PASSWORD: String = "guest"
            const val VHOST: String = "/"
            val timeout: Duration = 60.seconds
            val restoreTimeout: Duration = 15.seconds
            const val CONNECTION_NAME: String = "Kourier AMQP Client"
            const val RESTORE_TOPOLOGY: Boolean = true
        }

    }

}
