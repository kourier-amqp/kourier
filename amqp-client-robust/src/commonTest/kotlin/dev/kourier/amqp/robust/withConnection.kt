package dev.kourier.amqp.robust

import dev.kourier.amqp.connection.AMQPConfigBuilder
import dev.kourier.amqp.connection.AMQPConnection
import kotlinx.coroutines.runBlocking

fun withConnection(
    block: suspend (AMQPConnection) -> Unit,
) = withConnection({}, block)

fun withConnection(
    configure: AMQPConfigBuilder.() -> Unit,
    block: suspend (AMQPConnection) -> Unit,
) = runBlocking {
    val connection = createRobustAMQPConnection(this, configure)
    try {
        block(connection)
    } finally {
        connection.close()
    }
}
