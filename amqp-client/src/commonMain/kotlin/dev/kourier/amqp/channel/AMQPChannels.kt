package dev.kourier.amqp.channel

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

class AMQPChannels(
    var channelMax: UShort = 0u,
) {

    private sealed class ChannelSlot {
        data object Reserved : ChannelSlot()
        data class Channel(val channel: AMQPChannel) : ChannelSlot()
    }

    private val channels: MutableMap<UShort, ChannelSlot> = mutableMapOf()
    private val mutex = Mutex()

    suspend operator fun get(id: UShort): AMQPChannel? = mutex.withLock {
        when (val slot = channels[id]) {
            is ChannelSlot.Channel -> slot.channel
            else -> null
        }
    }

    suspend fun list(): List<AMQPChannel> = mutex.withLock {
        // Snapshot under the lock; the returned list is independent of the backing map.
        channels.values.filterIsInstance<ChannelSlot.Channel>().map { it.channel }
    }

    suspend fun reserveNext(): UShort? = mutex.withLock {
        if (channels.size >= channelMax.toInt()) return@withLock null
        for (i in 1u..channelMax.toUInt()) {
            val index = i.toUShort()
            if (channels[index] == null) {
                channels[index] = ChannelSlot.Reserved
                return@withLock index
            }
        }
        null
    }

    suspend fun add(channel: AMQPChannel) = mutex.withLock {
        channels[channel.id] = ChannelSlot.Channel(channel)
    }

    suspend fun remove(id: UShort) = mutex.withLock {
        channels.remove(id)
        Unit
    }

}
