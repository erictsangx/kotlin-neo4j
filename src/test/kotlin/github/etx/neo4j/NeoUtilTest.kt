package github.etx.neo4j

import github.etx.test.Rand
import helper.person
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class NeoUtilTest {
    @Test
    fun destruct() {
        val person = Rand.person
        val result = person.destruct()

        assertEquals(
            mapOf(
                "name" to person.name,
                "age" to person.age,
                "gender" to person.gender
            ),
            result
        )

    }


    @Test
    fun destructCollection() {
        val person = List(Rand.int(1, 10)) { Rand.person }
        val result = person.destruct()

        assertEquals(
            person.map {
                mapOf(
                    "name" to it.name,
                    "age" to it.age,
                    "gender" to it.gender
                )
            },
            result
        )
    }
}