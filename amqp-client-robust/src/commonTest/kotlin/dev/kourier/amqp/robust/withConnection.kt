package dev.kourier.amqp.robust

import dev.kourier.amqp.connection.AMQPConfigBuilder
import dev.kourier.amqp.connection.AMQPConnection
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext

suspend fun withConnection(
    block: suspend (AMQPConnection) -> Unit,
) = withConnection({}, block)

suspend fun withConnection(
    configure: AMQPConfigBuilder.() -> Unit,
    block: suspend (AMQPConnection) -> Unit,
) = withContext(Dispatchers.Default) {
    coroutineScope {
        val connection = createRobustAMQPConnection(this, configure)
        try {
            block(connection)
        } finally {
            connection.close()
        }
    }
}
