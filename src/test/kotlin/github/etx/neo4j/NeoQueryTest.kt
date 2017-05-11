package github.etx.neo4j

import github.etx.test.Rand
import helper.Gender
import helper.Person
import helper.person
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
    val serializer = DefaultNeoSerializer()
    val subject: NeoQuery = NeoQuery(driver, serializer)


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


    val alice = Rand.person
    val aliceKnowBob = Rand.pInstant
    val bob = Rand.person
    val candy = Rand.person

    val insertQuery = """
            CREATE (p:Person {
            name:{name},
            age:{age},
            gender:{gender}
            })
            RETURN p
            """

    @BeforeEach
    fun reset() {
        driver.session().let {
            it.run("MATCH (n) DETACH DELETE n")
            it.close()
        }


        subject.submit(insertQuery, mapOf("name" to alice.name, "age" to alice.age, "gender" to alice.gender))
        subject.submit(insertQuery, mapOf("name" to bob.name, "age" to bob.age, "gender" to bob.gender))
        subject.submit(insertQuery, mapOf("name" to candy.name, "age" to candy.age, "gender" to candy.gender))


        /**
         * [alice]-KNOW->[bob]
         */
        val fdshipQuery = """
        MATCH (a:Person {name: {nameA} })
        MATCH (b:Person {name: {nameB} })
        CREATE (a)-[r:KNOW]->(b)
        SET r.since = {since}
        """
        subject.submit(fdshipQuery, mapOf("nameA" to alice.name, "nameB" to bob.name, "since" to aliceKnowBob))
    }


    @Test
    fun submit() {
        val p = Rand.person
        val result = subject
                .submit(insertQuery, mapOf("name" to p.name, "age" to p.age, "gender" to p.gender))
                .unwrap("p")
                .let { Person(it.string("name"), it.int("age"), Gender.valueOf(it.string("gender"))) }
        assertEquals(p, result)
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
                    MATCH (a:Person)-[r:KNOW]->(b:Person)
                    RETURN r
                """)
                .map { it.unwrap("r") }
                .map { it.string("since") }
                .map { Instant.parse(it) }
                .toList()

        assertEquals(listOf(aliceKnowBob), result)
    }

    @Test
    fun relationshipCollection() {
        val result = subject
                .submit("""
                    MATCH (a:Person)-[r:KNOW]->(b:Person)
                    RETURN collect(r) as rows
                """)
                .map { it.unwrap("rows") }
                .flatMap(Cursor::asSequence)
                .map { it.string("since") }
                .map { Instant.parse(it) }
                .toList()

        assertEquals(listOf(aliceKnowBob), result)
    }

    @Test
    fun relationshipMap() {
        val result = subject
                .submit("""
                    MATCH (:Person)-[r:KNOW]->(p:Person)
                    RETURN {person: p, since: r.since} as fdship
                """)
                .map { it.unwrap("fdship") }
                .map { it.unwrap("person").string("name") to Instant.parse(it.string("since")) }
                .toList()

        assertEquals(listOf(bob.name to aliceKnowBob), result)
    }

    @Test
    fun isNull() {
        val result = subject
                .submit("""
                    MATCH (p:Person { name:{name} } )
                    OPTIONAL MATCH (c)-[:AAA]->(x:BBB)
                    RETURN p,x
                """, mapOf("name" to alice.name))
                .map { it.unwrap("x") }
                .map { it.isNull }
                .single()
        assertTrue(result)
    }

    @Test
    fun asMapOrNull() {
        val (first, second) = subject
                .submit("""
                    MATCH (a:Person { name: {nameA} })
                    OPTIONAL MATCH (a)-[r:KNOW]->(b:Person { name: {nameB} })
                    RETURN a,b
                """, mapOf("nameA" to alice.name, "nameB" to candy.name))
                .map { it.unwrap("a") to it.unwrap("b") }
                .map { (personA, personB) ->
                    personA.asMap() to personB.asMapOrNull()
                }
                .single()

        assertEquals(3, first.size)
        assertEquals(alice.name, first["name"])
        assertEquals(alice.age.toLong(), first["age"])
        assertEquals(alice.gender.name, first["gender"])
        assertNull(second)
    }


    @Test
    fun complexParameter() {
        val people = (1..10).map { Rand.person }


        subject.submit("""
                    MATCH (a:Person { name: {name} })
                    WITH a
                    UNWIND {people} AS person
                    CREATE (a)-[:KNOW]->(p:Person)
                    SET p=person
                """,
                mapOf("name" to candy.name, "people" to people.destruct()))

        val result = subject
                .submit(
                        """MATCH (a:Person { name:{name} } )-[:KNOW]->(b:Person)
                RETURN b
                ORDER BY b.name
                """,
                        mapOf("name" to candy.name)
                )
                .map { it.unwrap("b") }
                .map { Person(it.string("name"), it.int("age"), Gender.valueOf(it.string("gender"))) }
                .toList()

        assertEquals(people.sortedBy { it.name }, result)
    }

}