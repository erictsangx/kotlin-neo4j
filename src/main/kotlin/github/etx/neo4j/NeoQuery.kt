package github.etx.neo4j

import org.neo4j.driver.internal.InternalRecord
import org.neo4j.driver.v1.Driver
import java.time.Instant
import java.time.OffsetDateTime
import java.time.OffsetTime
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter


/**
 * see [http://neo4j.com/docs/developer-manual/current/#driver-types] for supported type
 * It also support:
 *  [OffsetTime], [OffsetDateTime], [ZonedDateTime] [Instant] (ISO8601)
 *  [Enum] (Enum.name)
 *  [Collection]<Any type above>
 */
class NeoQuery(private val driver: Driver) {

    fun submit(query: String, parameters: Map<String, Any?> = mapOf()): CursorWrapper {
        val session = driver.session()

        return session.use {
            val statementResult = session.run(query, serialize(parameters))
            if (statementResult.hasNext()) {
                CursorWrapper(statementResult.peek(), statementResult)
            } else {
                CursorWrapper(InternalRecord(listOf(), arrayOf()), statementResult)
            }
        }
    }

    fun serialize(parameters: Map<String, *>): Map<String, *> {
        return parameters.mapValues {
            val value = it.value
            return@mapValues when (value) {
                is Collection<*> -> transform(value)
                is Map<*, *> -> serialize(value as Map<String, *>)
                else -> toNeo4jType(value)
            }
        }
    }

    private fun toNeo4jType(value: Any?): Any? {
        return when (value) {
            null -> null
            is OffsetTime -> serializeTime(value)
            is OffsetDateTime -> serializeTime(value)
            is ZonedDateTime -> serializeTime(value)
            is Instant -> serializeTime(value)
            is Enum<*> -> value.name
            is Collection<*> -> transform(value)
            else -> value
        }
    }

    private fun transform(list: Collection<*>): Collection<Any?> {
        return list.map {
            when (it) {
                null -> null
                is OffsetTime -> serializeTime(it)
                is OffsetDateTime -> serializeTime(it)
                is Instant -> serializeTime(it)
                is Enum<*> -> it.name
                else -> it
            }
        }
    }

    private fun serializeTime(time: OffsetTime): String {
        return time.format(DateTimeFormatter.ISO_OFFSET_TIME)
    }

    private fun serializeTime(time: OffsetDateTime): String {
        return time.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
    }

    private fun serializeTime(time: ZonedDateTime): String {
        return time.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
    }

    private fun serializeTime(time: Instant): String {
        return time.toString()
    }

}