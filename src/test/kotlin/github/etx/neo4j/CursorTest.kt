package github.etx.neo4j

import github.etx.test.Rand
import org.junit.jupiter.api.Test
import org.neo4j.driver.internal.InternalNode
import org.neo4j.driver.internal.InternalRelationship
import org.neo4j.driver.internal.value.*
import kotlin.test.assertEquals

class CursorTest {
    val nodeA = InternalNode(
        Rand.long,
        listOf("nodeA"),
        mapOf(
            "a" to StringValue("a")
        )
    )

    val nodeB = InternalNode(
        Rand.long,
        listOf("nodeB"),
        mapOf(
            "b" to IntegerValue(1)
        )
    )

    val relationship = InternalRelationship(
        Rand.long,
        Rand.long,
        Rand.long,
        "relationship",
        mapOf("d" to FloatValue(3.0), "e" to NullValue.NULL)
    )

    val nestedNode = InternalNode(
        Rand.long,
        listOf("node"),
        mapOf(
            "x" to NodeValue(nodeA),
            "y" to ListValue(NodeValue(nodeB), NodeValue(nodeB)),
            "z" to MapValue(mapOf("c" to BooleanValue.TRUE)),
            "w" to RelationshipValue(relationship)
        )
    )

    @Test
    fun transformList() {
        val list = Cursor(ListValue(NodeValue(nodeB), NodeValue(nodeB)))
            .asSequence()
            .map(Cursor::asMap)
            .toList()
        assertEquals(listOf(mapOf("b" to 1.toLong()), mapOf("b" to 1.toLong())), list)
    }


    @Test
    fun transformMap() {
        val map = Cursor(NodeValue(nestedNode)).asMap()
        assertEquals(mapOf("a" to "a"), map["x"])
        assertEquals(listOf(mapOf("b" to 1.toLong()), mapOf("b" to 1.toLong())), map["y"])
        assertEquals(mapOf("c" to true), map["z"])
        assertEquals(mapOf("d" to 3.0, "e" to null), map["w"])
    }
}