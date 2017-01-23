package github.etx.neo4j

import kotlin.reflect.memberProperties

fun Any.destruct(): Map<String, Any?> {
    return this.javaClass.kotlin.memberProperties.map {
        it.name to it.get(this)
    }.toMap()
}