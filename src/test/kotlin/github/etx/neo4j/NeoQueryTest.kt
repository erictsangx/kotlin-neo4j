package github.etx.neo4j

import com.etx.test.Rand
import helper.Bar
import helper.Foo
import helper.Person
import mu.KLogging
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.neo4j.driver.v1.AuthTokens
import org.neo4j.driver.v1.Config
import org.neo4j.driver.v1.Driver
import org.neo4j.driver.v1.GraphDatabase
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.logging.slf4j.Slf4jLogProvider
import org.neo4j.test.TestGraphDatabaseFactory
import java.io.File
import java.time.Instant
import java.time.OffsetDateTime
import java.time.OffsetTime
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class NeoQueryTest {

    companion object : KLogging()

    val key = Rand.str
    val port = Rand.int(40000, 60000)

    val driver: Driver = GraphDatabase.driver(
        "bolt://127.0.0.1:$port",
        AuthTokens.basic("neo4j", "123")
        , Config.build().withLogging(NeoLogging(logger)).toConfig())
    val subject: NeoQuery = NeoQuery(driver)


    val nameA = Rand.str
    val nameB = Rand.str
    val role = Rand.str

    init {
        TestGraphDatabaseFactory()
            .setUserLogProvider(Slf4jLogProvider())
            .newEmbeddedDatabaseBuilder(File("/tmp/neo-${UUID.randomUUID()}"))
            .loadPropertiesFromFile(NeoQueryTest::class.java.classLoader.getResource("neo4j.conf").file)
            .setConfig(GraphDatabaseSettings.boltConnector(key).type, "BOLT")
            .setConfig(GraphDatabaseSettings.boltConnector(key).enabled, "true")
            .setConfig(GraphDatabaseSettings.boltConnector(key).encryption_level, "OPTIONAL")
            .setConfig(GraphDatabaseSettings.boltConnector(key).address, "0.0.0.0:$port")
            .newGraphDatabase()
    }


    @BeforeEach
    fun reset() {
        driver.session().let {
            it.run("MATCH (n) DETACH DELETE n")
            it.close()
        }

        subject.submit("""
                    CREATE (c:Company { name: {nameA} })<-[r:WORK { role: {role} }]-(p:Person { name: {nameB} })
            """, mapOf("nameA" to nameA, "nameB" to nameB, "role" to role))
    }


    @Test
    fun submit() {
        val f = Foo(Rand.str, Rand.int)
        val c = subject.submit("CREATE (n:Foo { name: {name}, age: {age} }) RETURN n",
            mapOf("name" to f.name, "age" to f.age))
        val result = c
            .map {
                it.unwrap("n")
            }
            .map {
                Foo(it.string("name"), it.int("age"))
            }
            .toList()
        assertEquals(listOf(f), result)
    }

    @Test
    fun serializePrimitiveTypes() {
        val params = mapOf(
            "a" to Rand.int.toShort(),
            "b" to Rand.int,
            "c" to Rand.long,
            "d" to Rand.float,
            "e" to Rand.double,
            "f" to Rand.str.first(),
            "g" to Rand.str,
            "h" to Rand.bool
        )

        val result = subject.serialize(params)
        assertEquals(params, result)
    }

    @Test
    fun serializeJava8Time() {
        val params = mapOf(
            "a" to OffsetTime.now(),
            "b" to OffsetDateTime.now(),
            "c" to ZonedDateTime.now(),
            "d" to Instant.now()
        )

        val result = subject.serialize(params)
        assertEquals(mapOf(
            "a" to (params["a"] as OffsetTime).format(DateTimeFormatter.ISO_OFFSET_TIME),
            "b" to (params["b"] as OffsetDateTime).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME),
            "c" to (params["c"] as ZonedDateTime).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME),
            "d" to (params["d"] as Instant).toString()
        ), result)
    }

    @Test
    fun serializeEnum() {
        val params = mapOf(
            "a" to Bar.A,
            "b" to Bar.B
        )

        val result = subject.serialize(params)
        assertEquals(params.mapValues { it.value.name }, result)
    }

    @Test
    fun serializeCollection() {
        val params = mapOf(
            "a" to (1..10).map { OffsetTime.now().plusHours(it.toLong()) },
            "b" to (1..10).map { OffsetDateTime.now().plusDays(it.toLong()) },
            "c" to (1..10).map { it },
            "d" to (1..10).map(Int::toString)
        )

        val result = subject.serialize(params)
        @Suppress("UNCHECKED_CAST")
        (assertEquals(mapOf(
            "a" to (params["a"] as List<OffsetTime>).map { it.format(DateTimeFormatter.ISO_OFFSET_TIME) },
            "b" to (params["b"] as List<OffsetDateTime>).map { it.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) },
            "c" to (params["c"] as List<Int>).map { it },
            "d" to (params["d"] as List<String>).map { it }
        ), result))
    }

    @Test
    fun nestedString() {
        val test = Rand.str
        val result = subject
            .submit("""CREATE (n:Foo {name: {test}}) RETURN n""",
                mapOf("test" to test))
            .map {
                it.unwrap("n")
            }
            .map {
                it.string("name") to it.stringOrNull("nothing")
            }
            .toList()
        assertEquals(listOf(test to null), result)
    }

    @Test
    fun rawString() {
        val test = Rand.str
        val result = subject
            .submit("""CREATE (n:Foo {name: {test}, nothing: null }) RETURN n.name, n.nothing""",
                mapOf("test" to test))
            .map {
                it.unwrap("n.name") to it.unwrap("n.nothing")
            }
            .map {
                it.first.string to it.second.stringOrNull
            }
            .toList()
        assertEquals(listOf(test to null), result)
    }

    @Test
    fun nestedInt() {
        val test = Rand.int
        val result = subject
            .submit("""CREATE (n:Foo {test: {test}}) RETURN n""",
                mapOf("test" to test))
            .map {
                it.unwrap("n")
            }
            .map {
                it.int("test") to it.intOrNull("nothing")
            }
            .toList()
        assertEquals(listOf(test to null), result)
    }

    @Test
    fun rawInt() {
        val test = Rand.int
        val result = subject
            .submit("""CREATE (n:Foo {test: {test}, nothing: null }) RETURN n.test, n.nothing""",
                mapOf("test" to test))
            .map {
                it.unwrap("n.test") to it.unwrap("n.nothing")
            }
            .map {
                it.first.int to it.second.intOrNull
            }
            .toList()
        assertEquals(listOf(test to null), result)
    }

    @Test
    fun nestedLong() {
        val test = Rand.long
        val result = subject
            .submit("""CREATE (n:Foo {test: {test}}) RETURN n""",
                mapOf("test" to test))
            .map {
                it.unwrap("n")
            }
            .map {
                it.long("test") to it.longOrNull("nothing")
            }
            .toList()
        assertEquals(listOf(test to null), result)
    }

    @Test
    fun rawLong() {
        val test = Rand.long
        val result = subject
            .submit("""CREATE (n:Foo {test: {test}, nothing: null }) RETURN n.test, n.nothing""",
                mapOf("test" to test))
            .map {
                it.unwrap("n.test") to it.unwrap("n.nothing")
            }
            .map {
                it.first.long to it.second.longOrNull
            }
            .toList()
        assertEquals(listOf(test to null), result)
    }

    @Test
    fun nestedDouble() {
        val test = Rand.double
        val result = subject
            .submit("""CREATE (n:Foo {test: {test}}) RETURN n""",
                mapOf("test" to test))
            .map {
                it.unwrap("n")
            }
            .map {
                it.double("test") to it.doubleOrNull("nothing")
            }
            .toList()
        assertEquals(listOf(test to null), result)
    }

    @Test
    fun rawDouble() {
        val test = Rand.double
        val result = subject
            .submit("""CREATE (n:Foo {test: {test}, nothing: null }) RETURN n.test, n.nothing""",
                mapOf("test" to test))
            .map {
                it.unwrap("n.test") to it.unwrap("n.nothing")
            }
            .map {
                it.first.double to it.second.doubleOrNull
            }
            .toList()
        assertEquals(listOf(test to null), result)
    }

    @Test
    fun nestedBool() {
        val test = Rand.bool
        val result = subject
            .submit("""CREATE (n:Foo {test: {test}}) RETURN n""",
                mapOf("test" to test))
            .map {
                it.unwrap("n")
            }
            .map {
                it.bool("test") to it.boolOrNull("nothing")
            }
            .toList()
        assertEquals(listOf(test to null), result)
    }

    @Test
    fun rawBool() {
        val test = Rand.bool
        val result = subject
            .submit("""CREATE (n:Foo {test: {test}, nothing: null }) RETURN n.test, n.nothing""",
                mapOf("test" to test))
            .map {
                it.unwrap("n.test") to it.unwrap("n.nothing")
            }
            .map {
                it.first.bool to it.second.boolOrNull
            }
            .toList()
        assertEquals(listOf(test to null), result)
    }

    @Test
    fun node() {
        val test = Rand.str

        subject.submit("""CREATE (n:Foo {name: "$test"})""")

        val result = subject
            .submit("""MATCH (n:Foo) RETURN n""")
            .map {
                it.unwrap("n")
            }
            .map {
                it.string("name")
            }
            .toList()
        assertEquals(listOf(test), result)
    }

    @Test
    fun nodeCollection() {
        val testA = Rand.str
        val testB = Rand.str

        subject.submit("""CREATE (n:Foo {name: "$testA"})""")
        subject.submit("""CREATE (n:Foo {name: "$testB"})""")

        val result = subject
            .submit("""MATCH (n:Foo) RETURN collect(n) as rows""")
            .map {
                it.unwrap("rows")
            }
            .flatMap(Cursor::asSequence)
            .map {
                it.string("name")
            }
            .toList()
        assertEquals(listOf(testA, testB), result)
    }

    @Test
    fun nodeMap() {
        val testA = Rand.str
        val testB = Rand.str

        subject.submit("""CREATE (n:Foo { name: {testA} })""", mapOf("testA" to testA))
        subject.submit("""CREATE (n:Bar { name: {testB} })""", mapOf("testB" to testB))

        val result = subject
            .submit("""
                        MATCH (f:Foo {name: {testA} } )
                        MATCH (b:Bar {name: {testB} } )
                        RETURN {foo: f, bar: b.name} as foobar
                    """, mapOf("testA" to testA, "testB" to testB))
            .map {
                it.unwrap("foobar")
            }
            .map {
                it.unwrap("foo").string("name") to it.string("bar")
            }
            .toList()
        assertEquals(listOf(testA to testB), result)
    }

    @Test
    fun relationship() {
        val result = subject
            .submit("""
                    MATCH (c:Company)<-[r:WORK]-(p:Person)
                    RETURN r
                """)
            .map {
                it.unwrap("r")
            }
            .map {
                it.string("role")
            }
            .toList()

        assertEquals(listOf(role), result)
    }

    @Test
    fun relationshipCollection() {
        val result = subject
            .submit("""
                    MATCH (c:Company)<-[r:WORK]-(p:Person)
                    RETURN collect(r) as rows
                """)
            .map {
                it.unwrap("rows")
            }
            .flatMap(Cursor::asSequence)
            .map {
                it.string("role")
            }
            .toList()

        assertEquals(listOf(role), result)
    }

    @Test
    fun relationshipMap() {
        val result = subject
            .submit("""
                    MATCH (c:Company)<-[r:WORK]-(p:Person)
                    RETURN {person: p, role: r.role} as staff
                """)
            .map {
                it.unwrap("staff")
            }
            .map {
                it.unwrap("person").string("name") to it.string("role")
            }
            .toList()

        assertEquals(listOf(nameB to role), result)
    }

    @Test
    fun isNull() {
        val result = subject
            .submit("""
                    MATCH (c:Company)
                    OPTIONAL MATCH (c)-[:AAA]->(x:BBB)
                    RETURN c,x
                """)
            .map { it.unwrap("x") }
            .map { it.isNull }
            .single()
        assertTrue(result)
    }

    @Test
    fun asMapOrNull() {
        val result = subject
            .submit("""
                    MATCH (c:Company { name: {nameA} })
                    OPTIONAL MATCH (c)<-[r:WORK]-(p:Person { name: {nameB} })
                    RETURN c,p
                """, mapOf("nameA" to nameA, "nameB" to Rand.str))
            .map { it.unwrap("c") to it.unwrap("p") }
            .map {
                val (company, person) = it
                company.asMap() to person.asMapOrNull()
            }
            .single()
        assertEquals(mapOf("name" to nameA), result.first)
        assertNull(result.second)
    }


    @Test
    fun complexParameter() {
        val companyName = Rand.str
        val people = (1..10).map { Person(Rand.str) }
        subject
            .submit("""
                    MERGE (c:Company { name: {companyName} })
                    WITH c
                    UNWIND {people} AS person
                    CREATE (c)<-[:WORK]-(p:Person)
                    SET p=person
                """, mapOf("companyName" to companyName, "people" to people.map(Person::destruct)))

        val result = subject
            .submit(
                "MATCH (c:Company { name:{companyName} } )<-[:WORK]-(p:Person) RETURN p",
                mapOf("companyName" to companyName)
            )
            .map { it.unwrap("p") }
            .map(Cursor::asMap)
            .map { Person(it["name"] as String) }
            .toList()

        assertEquals(people.sortedBy { it.name }, result.sortedBy { it.name })
    }

}