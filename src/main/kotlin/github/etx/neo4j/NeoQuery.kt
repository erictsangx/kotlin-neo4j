package github.etx.neo4j

import org.neo4j.driver.internal.InternalRecord
import org.neo4j.driver.internal.value.NullValue
import org.neo4j.driver.v1.Driver
import org.neo4j.driver.v1.Record
import org.neo4j.driver.v1.Value
import java.time.Instant
import java.time.OffsetDateTime
import java.time.OffsetTime
import java.time.format.DateTimeFormatter


/**
 * see [http://neo4j.com/docs/developer-manual/current/#driver-types] for supported type
 * Support:
 *  [Short], [Int], [Long], [Float], [Double]
 *  [Char], [String]
 *  [Boolean]
 *  [OffsetTime], [OffsetDateTime], [Instant]
 *  [Enum]
 *  [Collection]<Any type above>
 * @throws [UnsupportedParameterTypeException]
 */
class NeoQuery(private val driver: Driver) {

    fun submit(query: String, parameters: Map<String, Any?> = mapOf()): CursorWrapper {
        val session = driver.session()

        //TODO: kotlin 1.1 add support [AutoClosable].use
        return try {
            val sr = session.run(query, serialize(parameters))
            if (sr.hasNext()) {
                CursorWrapper(sr.peek(), sr)
            } else {
                CursorWrapper(InternalRecord(listOf(), arrayOf()), sr)
            }
        } catch(e: Exception) {
            throw e
        } finally {
            session.close()
        }
    }


    fun serialize(parameters: Map<String, Any?>): Map<String, Any?> {
        return parameters.mapValues {
            val value = it.value
            when (value) {
                null -> null
                is Short, is Int, is Long, is Float, is Double -> value
                is Char, is String -> value
                is Boolean -> value
                is OffsetTime -> serializeTime(value)
                is OffsetDateTime -> serializeTime(value)
                is Instant -> serializeTime(value)
                is Enum<*> -> value.name
                is Collection<*> -> transform(value)
                else -> throw UnsupportedParameterTypeException("Unsupported type: ${value.javaClass.typeName} -> $value")
            }
        }
    }

    private fun serializeTime(time: OffsetTime): String {
        return time.format(DateTimeFormatter.ISO_OFFSET_TIME)
    }

    private fun serializeTime(time: OffsetDateTime): String {
        return time.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
    }


    private fun serializeTime(time: Instant): String {
        return time.toString()
    }

    private fun transform(list: Collection<*>): Collection<Any?> {
        return list.map {
            when (it) {
                null -> null
                is Short, is Int, is Long, is Float, is Double -> it
                is Char, is String -> it
                is Boolean -> it
                is OffsetTime -> serializeTime(it)
                is OffsetDateTime -> serializeTime(it)
                is Enum<*> -> it.name
                else -> throw UnsupportedParameterTypeException("Unsupported type: ${it.javaClass.typeName} -> $it")
            }
        }
    }

}