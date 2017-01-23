package github.etx.neo4j

import com.etx.test.Rand
import helper.Foo
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class NeoUtilTest {
    @Test
    @DisplayName("should destruct object to map")
    fun destruct() {
        val foo = Foo(name = Rand.str, age = Rand.int)
        val result = foo.destruct()

        assertEquals(
                mapOf(
                        Foo::name.name to foo.name,
                        Foo::age.name to foo.age
                ),
                result
        )
    }
}