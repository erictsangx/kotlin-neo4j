package github.etx.neo4j

import github.etx.test.Rand
import helper.Gender
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.OffsetDateTime
import java.time.OffsetTime
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import kotlin.test.assertEquals

class DefaultNeoSerializerTest {

    val subject = DefaultNeoSerializer()

    @Test
    fun serializePrimitiveTypes() {
        val params = mapOf(
                "a" to Rand.int.toShort(),
                "b" to Rand.int,
                "c" to Rand.long,
                "d" to Rand.float,
                "e" to Rand.double,
                "f" to Rand.str.first(),
                "g" to Rand.str,
                "h" to Rand.bool
        )

        val result = subject.serialize(params)
        assertEquals(params, result)
    }

    @Test
    fun serializeJava8Time() {
        val params = mapOf(
                "a" to OffsetTime.now(),
                "b" to OffsetDateTime.now(),
                "c" to ZonedDateTime.now(),
                "d" to Instant.now()
        )

        val result = subject.serialize(params)
        assertEquals(mapOf(
                "a" to (params["a"] as OffsetTime).format(DateTimeFormatter.ISO_OFFSET_TIME),
                "b" to (params["b"] as OffsetDateTime).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                "c" to (params["c"] as ZonedDateTime).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                "d" to (params["d"] as Instant).toString()
        ), result)
    }

    @Test
    fun serializeEnum() {
        val params = mapOf(
                "a" to Gender.MALE,
                "b" to Gender.FEMALE
        )

        val result = subject.serialize(params)
        assertEquals(params.mapValues { it.value.name }, result)
    }

    @Test
    fun serializeCollection() {
        val params = mapOf(
                "a" to (1..10).map { OffsetTime.now().plusHours(it.toLong()) },
                "b" to (1..10).map { OffsetDateTime.now().plusDays(it.toLong()) },
                "c" to (1..10).map { it },
                "d" to (1..10).map(Int::toString)
        )

        val result = subject.serialize(params)
        @Suppress("UNCHECKED_CAST")
        (assertEquals(mapOf(
                "a" to (params["a"] as List<OffsetTime>).map { it.format(DateTimeFormatter.ISO_OFFSET_TIME) },
                "b" to (params["b"] as List<OffsetDateTime>).map { it.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) },
                "c" to (params["c"] as List<Int>).map { it },
                "d" to (params["d"] as List<String>).map { it }
        ), result))
    }

}