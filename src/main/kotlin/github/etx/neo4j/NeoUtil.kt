package github.etx.neo4j

import kotlin.reflect.full.memberProperties


/**
 * Convert class properties to a 2D [Map]
 */
inline fun <reified T : Any> T.destruct(): Map<String, Any?> {
    return T::class.memberProperties.map {
        it.name to it.get(this)
    }.toMap()
}

inline fun <reified R : Any, reified T : Collection<R>> T.destruct(): Collection<Map<String, Any?>> {
    return this.map { ele ->
        R::class.memberProperties.map {
            it.name to it.get(ele)
        }.toMap()
    }
}