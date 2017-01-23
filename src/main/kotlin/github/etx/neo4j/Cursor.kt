package github.etx.neo4j

import org.neo4j.driver.internal.value.*
import org.neo4j.driver.v1.Value


class Cursor(private val value: Value) {
    private fun Value.nullable(): Value? {
        return if (this.isNull) {
            null
        } else {
            this
        }
    }

    val isNull: Boolean = value.isNull

    fun unwrap(key: String): Cursor {
        return Cursor(value[key])
    }

    fun string(key: String): String = stringOrNull(key)!!
    fun stringOrNull(key: String): String? = value[key].nullable()?.asString()
    val string: String by lazy {
        stringOrNull!!
    }
    val stringOrNull: String? by lazy {
        value.nullable()?.asString()
    }

    fun int(key: String): Int = intOrNull(key)!!
    fun intOrNull(key: String): Int? = value[key]?.nullable()?.asInt()
    val int: Int by lazy {
        intOrNull!!
    }
    val intOrNull: Int? by lazy {
        value.nullable()?.asInt()
    }

    fun long(key: String): Long = longOrNull(key)!!
    fun longOrNull(key: String): Long? = value[key]?.nullable()?.asLong()
    val long: Long by lazy {
        longOrNull!!
    }
    val longOrNull: Long? by lazy {
        value.nullable()?.asLong()
    }

    fun double(key: String): Double = doubleOrNull(key)!!
    fun doubleOrNull(key: String): Double? = value[key]?.nullable()?.asDouble()
    val double: Double by lazy {
        doubleOrNull!!
    }
    val doubleOrNull: Double? by lazy {
        value.nullable()?.asDouble()
    }

    fun bool(key: String): Boolean = boolOrNull(key)!!
    fun boolOrNull(key: String): Boolean? = value[key]?.nullable()?.asBoolean()
    val bool: Boolean by lazy {
        boolOrNull!!
    }
    val boolOrNull: Boolean? by lazy {
        value.nullable()?.asBoolean()
    }

    fun asMap(): Map<String, Any?> = this.asMapOrNull()!!
    fun asMapOrNull(): Map<String, Any?>? {
        return value.nullable()?.asMap { transform(it) }
    }

    fun asSequence(): Sequence<Cursor> {
        return value.asList(::Cursor).asSequence()
    }

    private fun transform(value: Value): Any? {
        return when (value) {
            is NullValue -> null
            is ListValue -> value.asList { transform(it) }.toList()
            is MapValue -> value.asMap { transform(it) }
            is NodeValue -> value.asMap { transform(it) }
            is RelationshipValue -> value.asMap { transform(it) }
            else -> {
                value.asObject()
            }
        }
    }

}

