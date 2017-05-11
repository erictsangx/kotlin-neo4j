package github.etx.neo4j


interface INeoSerializer {
    fun serialize(parameters: Map<String, Any?>): Map<String, Any?>
}