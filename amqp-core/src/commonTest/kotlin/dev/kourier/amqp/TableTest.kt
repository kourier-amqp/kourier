package dev.kourier.amqp

import dev.kourier.amqp.serialization.ProtocolBinary
import dev.kourier.amqp.serialization.serializers.TableSerializer
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class TableTest {

    @Test
    fun testEncodeDecodeEmptyTable() {
        val table: Table = emptyMap()

        val encoded = ProtocolBinary.encodeToByteArray(TableSerializer, table)
        val decoded = ProtocolBinary.decodeFromByteArray(TableSerializer, encoded)

        assertEquals(table, decoded)
    }

    @Test
    fun testEncodeDecodeSimpleTable() {
        val table: Table = mapOf(
            "boolean" to Field.Boolean(true),
        )

        val encoded = ProtocolBinary.encodeToByteArray(TableSerializer, table)
        val decoded = ProtocolBinary.decodeFromByteArray(TableSerializer, encoded)

        assertEquals(table, decoded)
    }

    @Test
    fun testTableFromAndToMap() {
        val originalMap = mapOf(
            "boolean" to true,
            "int" to 42,
            "string" to "Hello",
            "nullValue" to null
        )

        val table = originalMap.toTable()
        assertEquals(
            mapOf(
                "boolean" to Field.Boolean(true),
                "int" to Field.Int(42),
                "string" to Field.LongString("Hello"),
                "nullValue" to Field.Null
            ),
            table
        )

        val convertedMap = table.toMap()
        assertEquals(originalMap, convertedMap)
    }

    @Test
    fun testTableFromTableOf() {
        val table = tableOf("boolean" to true)
        assertEquals(mapOf("boolean" to Field.Boolean(true)), table)
    }

    // NEW-26: Field.Bytes used to decode to an all-zero array (the decoded bytes were discarded),
    // and encode hit the unsupported beginStructure. Both sides now use a 4-byte length + raw bytes,
    // so a Field.Bytes value round-trips intact.
    @Test
    fun testTableBytesFieldRoundTrip() {
        val payload = byteArrayOf(1, 2, 3, 4, 5)
        val table: Table = mapOf("payload" to Field.Bytes(payload))

        val encoded = ProtocolBinary.encodeToByteArray(TableSerializer, table)
        val decoded = ProtocolBinary.decodeFromByteArray(TableSerializer, encoded)

        val field = decoded["payload"]
        assertTrue(field is Field.Bytes)
        assertContentEquals(payload, field.value)
    }

    // NEW-26: empty byte array round-trips (size == 0 edge).
    @Test
    fun testTableEmptyBytesFieldRoundTrip() {
        val table: Table = mapOf("empty" to Field.Bytes(byteArrayOf()))

        val encoded = ProtocolBinary.encodeToByteArray(TableSerializer, table)
        val decoded = ProtocolBinary.decodeFromByteArray(TableSerializer, encoded)

        val field = decoded["empty"]
        assertTrue(field is Field.Bytes)
        assertContentEquals(byteArrayOf(), field.value)
    }

    // NEW-26: a Field.Bytes followed by another field verifies the decode's bytesRead accounting
    // stays aligned — a wrong byte count would misparse the trailing field.
    @Test
    fun testTableBytesFieldKeepsTrailingFieldAligned() {
        val table: Table = mapOf(
            "payload" to Field.Bytes(byteArrayOf(9, 8, 7)),
            "after" to Field.Int(42),
        )

        val encoded = ProtocolBinary.encodeToByteArray(TableSerializer, table)
        val decoded = ProtocolBinary.decodeFromByteArray(TableSerializer, encoded)

        assertContentEquals(byteArrayOf(9, 8, 7), (decoded["payload"] as Field.Bytes).value)
        assertEquals(Field.Int(42), decoded["after"])
    }

}
