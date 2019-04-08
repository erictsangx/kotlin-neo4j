
# kotlin-neo4j

kotlin-neo4j is a wrapper of 'org.neo4j.driver:neo4j-java-driver' and written by Kotlin.
* **Easy to use with Kotlin:** declarative style
* **Kotlin std naming styles:** stringOrNull, int("key")
* **Support Sl4j Logger**

## Gradle
```gradle
implementation "io.github.erictsangx:kotlin-neo4j:0.0.6"
```

## Maven
```maven
<dependency>
  <groupId>io.github.erictsangx</groupId>
  <artifactId>kotlin-neo4j</artifactId>
  <version>0.0.6</version>
</dependency>
```
## Examples

```kotlin
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import github.etx.neo4j.DefaultNeoSerializer
import github.etx.neo4j.NeoLogging
import github.etx.neo4j.NeoQuery
import github.etx.neo4j.destruct
import org.neo4j.driver.v1.AuthTokens
import org.neo4j.driver.v1.Config
import org.neo4j.driver.v1.GraphDatabase
import org.slf4j.LoggerFactory


data class User(val name: String, val age: Int?)

class HelloWorld {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val logger = LoggerFactory.getLogger(HelloWorld::class.java)

            val driver = GraphDatabase.driver(
                "bolt://127.0.0.1",
                AuthTokens.basic("neo4j", "neo4j")
                , Config.build().withLogging(NeoLogging(logger)).toConfig()
            )

            val neo = NeoQuery(driver, DefaultNeoSerializer())

            neo.submit("CREATE (u:User { name: {name}, age: {age} })", User("Alice", 18).destruct())
            neo.submit("CREATE (u:User { name: {name}, age: {age} })", User("Bob", null).destruct())

            val alice = neo.submit("MATCH (u:User { name:{name} }) RETURN u", mapOf("name" to "Alice"))
                .map { it.unwrap("u") }
                .map { User(it.string("name"), it.intOrNull("age")) }
                .singleOrNull()
            println(alice) //User(name=Alice, age=18)

            //or
            val aliceB = neo.submit("MATCH (u:User { name:{name} }) RETURN u", mapOf("name" to "Alice"))
                .unwrap("u")
                .let {
                    User(it.string("name"), it.intOrNull("age"))
                }
            println(aliceB) //User(name=Alice, age=18)


            //You can use with Jackson
            val mapper = ObjectMapper().registerModule(KotlinModule())
            val users = neo.submit("MATCH (u:User) WHERE u.age IS NULL RETURN u")
                .map { it.unwrap("u") }
                .map { mapper.convertValue(it.asMap(), User::class.java) }
                .toList()
            println(users) //[User(name=Bob, age=null)]

            driver.close()
        }
    }
}
```