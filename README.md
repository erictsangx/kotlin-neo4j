# kotlin-neo4j

kotlin-neo4j is a wrapper of 'org.neo4j.driver:neo4j-java-driver' and written by Kotlin.
* **Easy to use with Kotlin:** declarative style
* **Kotlin std naming styles:** stringOrNull, int("key")
* **Support Sl4j Logger**

## Gradle
```gradle
compile "io.github.erictsangx:kotlin-neo4j:0.0.3"
```

## Maven
```maven
<dependency>
  <groupId>io.github.erictsangx</groupId>
  <artifactId>kotlin-neo4j</artifactId>
  <version>0.0.3</version>
</dependency>
```
## Examples

```kotlin
data class User(val name: String, val age: Int?)

fun main(args: Array<String>) {
      val driver: Driver = GraphDatabase.driver(
          "bolt://127.0.0.1",
          AuthTokens.basic("neo4j", "neo4j")
          , Config.build().withLogging(NeoLogging(logger)).toConfig())
  
      val neo = NeoQuery(driver)
  
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
}
```