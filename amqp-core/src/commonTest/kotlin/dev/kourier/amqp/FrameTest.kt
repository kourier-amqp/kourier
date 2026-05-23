package dev.kourier.amqp

import dev.kourier.amqp.serialization.ProtocolBinary
import dev.kourier.amqp.serialization.ProtocolBinaryDecoder
import dev.kourier.amqp.serialization.serializers.frame.FrameSerializer
import kotlinx.io.Buffer
import kotlinx.io.write
import kotlinx.serialization.decodeFromByteArray
import kotlinx.serialization.encodeToByteArray
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class FrameTest {

    @Test
    fun testFrameMethodConnectionStart() {
        val frame = Frame(
            channelId = 0u,
            payload = Frame.Method.Connection.Start(
                versionMajor = 0u,
                versionMinor = 9u,
                serverProperties = mapOf(
                    "capabilities" to Field.Table(
                        mapOf(
                            "publisher_confirms" to Field.Boolean(true),
                            "basic.nack" to Field.Boolean(true)
                        )
                    )
                ),
                mechanisms = "AMQPLAIN PLAIN",
                locales = "en_US"
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodConnectionStartOk() {
        val frame = Frame(
            channelId = 0u,
            payload = Frame.Method.Connection.StartOk(
                clientProperties = mapOf(
                    "product" to Field.LongString("TestClient")
                ),
                mechanism = "PLAIN",
                response = "\u0000guest\u0000guest",
                locale = "en_US"
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodConnectionSecure() {
        val frame = Frame(
            channelId = 0u,
            payload = Frame.Method.Connection.Secure(
                challenge = "challengeData"
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodConnectionSecureOk() {
        val frame = Frame(
            channelId = 0u,
            payload = Frame.Method.Connection.SecureOk(
                response = "responseData"
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodConnectionTune() {
        val frame = Frame(
            channelId = 0u,
            payload = Frame.Method.Connection.Tune(
                channelMax = 100u,
                frameMax = 131072u,
                heartbeat = 60u
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodConnectionTuneOk() {
        val frame = Frame(
            channelId = 0u,
            payload = Frame.Method.Connection.TuneOk(
                channelMax = 100u,
                frameMax = 131072u,
                heartbeat = 60u
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodConnectionOpen() {
        val frame = Frame(
            channelId = 0u,
            payload = Frame.Method.Connection.Open(
                vhost = "/",
                reserved1 = "",
                reserved2 = false
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodConnectionOpenOk() {
        val frame = Frame(
            channelId = 0u,
            payload = Frame.Method.Connection.OpenOk(
                reserved1 = ""
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodConnectionClose() {
        val frame = Frame(
            channelId = 0u,
            payload = Frame.Method.Connection.Close(
                replyCode = 200u,
                replyText = "Normal shutdown",
                failingClassId = 0u,
                failingMethodId = 0u
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodConnectionCloseOk() {
        val frame = Frame(
            channelId = 0u,
            payload = Frame.Method.Connection.CloseOk
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodConnectionBlocked() {
        val frame = Frame(
            channelId = 0u,
            payload = Frame.Method.Connection.Blocked(
                reason = "Low on memory"
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodConnectionUnblocked() {
        val frame = Frame(
            channelId = 0u,
            payload = Frame.Method.Connection.Unblocked
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodChannelOpen() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Channel.Open(
                reserved1 = ""
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodChannelOpenOk() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Channel.OpenOk(
                reserved1 = ""
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodChannelFlow() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Channel.Flow(
                active = true
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodChannelFlowOk() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Channel.FlowOk(
                active = true
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodChannelClose() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Channel.Close(
                replyCode = 200u,
                replyText = "Normal shutdown",
                classId = 0u,
                methodId = 0u
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodChannelCloseOk() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Channel.CloseOk
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodExchangeDeclare() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Exchange.Declare(
                reserved1 = 0u,
                exchangeName = "testExchange",
                exchangeType = BuiltinExchangeType.DIRECT,
                passive = false,
                durable = true,
                autoDelete = false,
                internal = false,
                noWait = false,
                arguments = emptyMap()
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodExchangeDeclareOk() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Exchange.DeclareOk
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodExchangeDelete() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Exchange.Delete(
                reserved1 = 0u,
                exchangeName = "testExchange",
                ifUnused = false,
                noWait = false,
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodExchangeDeleteOk() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Exchange.DeleteOk
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodExchangeBind() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Exchange.Bind(
                reserved1 = 0u,
                destination = "testExchangeDestination",
                source = "testExchangeSource",
                routingKey = "testRoutingKey",
                noWait = false,
                arguments = emptyMap()
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodExchangeBindOk() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Exchange.BindOk
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodExchangeUnbind() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Exchange.Unbind(
                reserved1 = 0u,
                destination = "testExchangeDestination",
                source = "testExchangeSource",
                routingKey = "testRoutingKey",
                noWait = false,
                arguments = emptyMap()
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodExchangeUnbindOk() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Exchange.UnbindOk
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodQueueDeclare() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Queue.Declare(
                reserved1 = 0u,
                queueName = "testQueue",
                passive = false,
                durable = true,
                exclusive = false,
                autoDelete = false,
                noWait = false,
                arguments = emptyMap()
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodQueueDeclareOk() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Queue.DeclareOk(
                queueName = "testQueue",
                messageCount = 0u,
                consumerCount = 0u,
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodQueueBind() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Queue.Bind(
                reserved1 = 0u,
                queueName = "testQueue",
                exchangeName = "testExchange",
                routingKey = "testRoutingKey",
                noWait = false,
                arguments = emptyMap()
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodQueueBindOk() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Queue.BindOk
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodQueuePurge() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Queue.Purge(
                reserved1 = 0u,
                queueName = "testQueue",
                noWait = false,
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodQueuePurgeOk() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Queue.PurgeOk(
                messageCount = 0u,
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodQueueDelete() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Queue.Delete(
                reserved1 = 0u,
                queueName = "testQueue",
                ifUnused = false,
                ifEmpty = false,
                noWait = false,
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodQueueDeleteOk() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Queue.DeleteOk(
                messageCount = 0u,
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodQueueUnbind() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Queue.Unbind(
                reserved1 = 0u,
                queueName = "testQueue",
                exchangeName = "testExchange",
                routingKey = "testRoutingKey",
                arguments = emptyMap()
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodQueueUnbindOk() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Queue.UnbindOk
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodBasicQos() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Basic.Qos(
                prefetchSize = 0u,
                prefetchCount = 1u,
                global = false
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodBasicQosOk() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Basic.QosOk
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodBasicConsume() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Basic.Consume(
                reserved1 = 0u,
                queue = "testQueue",
                consumerTag = "testConsumer",
                noLocal = false,
                noAck = true,
                exclusive = false,
                noWait = false,
                arguments = emptyMap()
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodBasicConsumeOk() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Basic.ConsumeOk(
                consumerTag = "testConsumer",
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodBasicCancel() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Basic.Cancel(
                consumerTag = "testConsumer",
                noWait = false,
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodBasicCancelOk() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Basic.CancelOk(
                consumerTag = "testConsumer",
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodBasicPublish() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Basic.Publish(
                reserved1 = 0u,
                exchange = "testExchange",
                routingKey = "testRoutingKey",
                mandatory = false,
                immediate = true,
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodBasicReturn() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Basic.Return(
                replyCode = 200u,
                replyText = "OK",
                exchange = "testExchange",
                routingKey = "testRoutingKey",
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodBasicDeliver() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Basic.Deliver(
                consumerTag = "testConsumer",
                deliveryTag = 1u,
                redelivered = false,
                exchange = "testExchange",
                routingKey = "testRoutingKey",
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodBasicGet() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Basic.Get(
                reserved1 = 0u,
                queue = "testQueue",
                noAck = true
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodBasicGetOk() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Basic.GetOk(
                deliveryTag = 1u,
                redelivered = false,
                exchange = "testExchange",
                routingKey = "testRoutingKey",
                messageCount = 0u
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodBasicGetEmpty() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Basic.GetEmpty(
                reserved1 = ""
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodBasicAck() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Basic.Ack(
                deliveryTag = 1u,
                multiple = false
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodBasicReject() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Basic.Reject(
                deliveryTag = 1u,
                requeue = false
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodBasicRecoverAsync() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Basic.RecoverAsync(
                requeue = false
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodBasicRecover() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Basic.Recover(
                requeue = false
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodBasicRecoverOk() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Basic.RecoverOk
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodBasicNack() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Basic.Nack(
                deliveryTag = 1u,
                multiple = false,
                requeue = false
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodConfirmSelect() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Confirm.Select(
                noWait = false
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodConfirmSelectOk() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Confirm.SelectOk
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodTxSelect() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Tx.Select
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodTxSelectOk() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Tx.SelectOk
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodTxCommit() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Tx.Commit
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodTxCommitOk() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Tx.CommitOk
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodTxRollback() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Tx.Rollback
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameMethodTxRollbackOk() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Method.Tx.RollbackOk
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameHeader() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Header(
                classID = 60u,
                weight = 0u,
                bodySize = 100u,
                properties = Properties()
            )
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameBody() {
        val frame = Frame(
            channelId = 1u,
            payload = Frame.Body(byteArrayOf(1, 2, 3, 4, 5))
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    @Test
    fun testFrameHeartbeat() {
        val frame = Frame(
            channelId = 0u,
            payload = Frame.Heartbeat
        )
        val encoded = ProtocolBinary.encodeToByteArray(frame)
        val decoded = ProtocolBinary.decodeFromByteArray<Frame>(encoded)
        assertEquals(frame, decoded)
    }

    // NEW-1: a frame whose declared size exceeds maxFrameSize must be rejected BEFORE allocating,
    // so a malicious/buggy peer advertising a giant size can't drive an unbounded allocation.
    @Test
    fun testFrameExceedingMaxFrameSizeIsRejected() {
        val body = ByteArray(100) { it.toByte() }
        val bytes = ProtocolBinary.encodeToByteArray(Frame(channelId = 1u, payload = Frame.Body(body)))

        // Declared size (100) exceeds the 50-byte cap → reject (same branch a 4 GiB size hits).
        assertFailsWith<ProtocolError.Invalid> {
            ProtocolBinaryDecoder(Buffer().apply { write(bytes) }, maxFrameSize = 50)
                .decodeSerializableValue(FrameSerializer)
        }

        // With a sufficient cap the same frame decodes normally.
        val frame = ProtocolBinaryDecoder(Buffer().apply { write(bytes) }, maxFrameSize = 1000)
            .decodeSerializableValue(FrameSerializer)
        assertEquals(100, (frame.payload as Frame.Body).body.size)
    }

    // NEW-29: a Method/Header frame whose declared size overstates its real payload must be rejected
    // (ProtocolError.Invalid), not silently accepted with the stream left desynced.
    @Test
    fun testMethodFrameWithOverstatedSizeIsRejected() {
        val bytes = ProtocolBinary.encodeToByteArray(Frame(channelId = 1u, payload = Frame.Method.Basic.QosOk))

        // The 4-byte big-endian size is at indices 3..6. QosOk's real payload is small (4 bytes:
        // classId + methodId), so bumping the low byte makes the declared size overstate it.
        val tampered = bytes.copyOf()
        tampered[6] = (tampered[6] + 4).toByte()

        assertFailsWith<ProtocolError.Invalid> {
            ProtocolBinaryDecoder(Buffer().apply { write(tampered) })
                .decodeSerializableValue(FrameSerializer)
        }

        // Sanity: the untampered frame still decodes fine.
        ProtocolBinaryDecoder(Buffer().apply { write(bytes) }).decodeSerializableValue(FrameSerializer)
    }

}
