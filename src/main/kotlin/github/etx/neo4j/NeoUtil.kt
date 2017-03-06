package github.etx.neo4j

import kotlin.reflect.full.memberProperties


inline fun <reified T : Any> T.destruct(): Map<String, Any?> {
    return T::class.memberProperties.map {
        it.name to it.get(this)
    }.toMap()
}