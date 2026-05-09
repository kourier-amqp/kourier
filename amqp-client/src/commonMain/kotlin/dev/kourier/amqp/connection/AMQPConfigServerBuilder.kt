package dev.kourier.amqp.connection

import kotlin.time.Duration

class AMQPConfigServerBuilder {

    var host: String = AMQPConfig.Server.Defaults.HOST
    var port: Int = AMQPConfig.Server.Defaults.PORT
    var user: String = AMQPConfig.Server.Defaults.USER
    var password: String = AMQPConfig.Server.Defaults.PASSWORD
    var vhost: String = AMQPConfig.Server.Defaults.VHOST
    var timeout: Duration = AMQPConfig.Server.Defaults.timeout
    var connectionName: String = AMQPConfig.Server.Defaults.CONNECTION_NAME
    var restoreTimeout: Duration = AMQPConfig.Server.Defaults.restoreTimeout

    fun build(): AMQPConfig.Server {
        return AMQPConfig.Server(
            host = host,
            port = port,
            user = user,
            password = password,
            vhost = vhost,
            timeout = timeout,
            connectionName = connectionName,
            restoreTimeout = restoreTimeout,
        )
    }

}
