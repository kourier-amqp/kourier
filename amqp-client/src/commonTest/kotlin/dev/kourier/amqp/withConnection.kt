package dev.kourier.amqp

import dev.kourier.amqp.connection.AMQPConnection
import dev.kourier.amqp.connection.createAMQPConnection
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext

suspend fun withConnection(block: suspend (AMQPConnection) -> Unit) = withContext(Dispatchers.Default) {
    coroutineScope {
        val connection = createAMQPConnection(this) {}
        try {
            block(connection)
        } finally {
            connection.close()
        }
    }
}
