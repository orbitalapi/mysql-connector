package com.orbitalhq.connectors.jdbc.mysql.query

import com.orbitalhq.connectors.config.jdbc.JdbcUrlAndCredentials
import com.orbitalhq.connectors.config.jdbc.JdbcUrlCredentialsConnectionConfiguration
import com.orbitalhq.connectors.jdbc.HikariJdbcConnectionFactory
import com.orbitalhq.connectors.jdbc.JdbcConnectionFactory
import com.orbitalhq.connectors.jdbc.JdbcConnectorTaxi
import com.orbitalhq.connectors.jdbc.JdbcInvoker
import com.orbitalhq.connectors.jdbc.mysql.MySqlDbSupport
import com.orbitalhq.connectors.jdbc.registry.InMemoryJdbcConnectionRegistry
import com.orbitalhq.models.TypedInstance
import com.orbitalhq.query.VyneQlGrammar
import com.orbitalhq.schema.api.SimpleSchemaProvider
import com.orbitalhq.stubbing.StubService
import com.orbitalhq.testVyne
import com.orbitalhq.typedObjects
import com.zaxxer.hikari.HikariConfig
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import kotlinx.coroutines.runBlocking
import org.junit.BeforeClass
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Configuration
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.config.EnableJpaRepositories
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.LocalDate
import org.testcontainers.containers.MySQLContainer

@Testcontainers
@SpringBootTest(
    classes = [MySqlQueryTestConfig::class],
    properties = [
        "spring.jpa.hibernate.ddl-auto=create",
        "spring.jpa.properties.hibernate.show_sql=true",
        "spring.jpa.properties.hibernate.format_sql=true"
    ]
)
class MySqlQueryTest {
    @Autowired
    lateinit var movieRepository: MovieRepository

    lateinit var connectionRegistry: InMemoryJdbcConnectionRegistry
    lateinit var connectionFactory: JdbcConnectionFactory

    companion object {
        @JvmField
        @Container
        @ServiceConnection
        val mysqlContainer = MySQLContainer("mysql:8.4.0")
    }


    @BeforeEach
    fun setup() {
        MySqlDbSupport.register()

        mysqlContainer.waitingFor(Wait.forListeningPort())
        val connectionDetails = JdbcUrlCredentialsConnectionConfiguration(
            "movies",
            MySqlDbSupport.driverName,
            JdbcUrlAndCredentials(
                mysqlContainer.getJdbcUrl(),
                mysqlContainer.username,
                mysqlContainer.password
            )
        )
        connectionRegistry =
            InMemoryJdbcConnectionRegistry(listOf(connectionDetails))
        connectionFactory = HikariJdbcConnectionFactory(connectionRegistry, HikariConfig())
    }

    @Test
    fun `can use a TaxiQL statement to query a db`(): Unit = runBlocking {
        movieRepository.save(
            Movie("1", "A New Hope")
        )
        val vyne = testVyne(
            listOf(
                JdbcConnectorTaxi.schema,
                VyneQlGrammar.QUERY_TYPE_TAXI,
                """
         ${JdbcConnectorTaxi.Annotations.imports}
         import ${VyneQlGrammar.QUERY_TYPE_NAME}
         type MovieId inherits Int
         type MovieTitle inherits String

         @Table(connection = "movies", table = "movie",  schema = "public")
         model Movie {
            ID : MovieId
            TITLE : MovieTitle
         }

         @DatabaseService( connection = "movies" )
         service MovieDb {
            table movie : Movie[]
         }
      """
            )
        ) { schema -> listOf(JdbcInvoker(connectionFactory, SimpleSchemaProvider(schema))) }
        val result = vyne.query("""find { Movie[]( MovieTitle == "A New Hope" ) } """)
            .typedObjects()
        result.shouldHaveSize(1)
        result.first().toRawObject()
            .shouldBe(mapOf("TITLE" to "A New Hope", "ID" to 1))
    }

    @Test
    fun `can issue a query that starts with jdbc and joins to api`(): Unit = runBlocking {
        movieRepository.save(
            Movie("1", "A New Hope")
        )
        val vyne = testVyne(
            listOf(
                JdbcConnectorTaxi.schema,
                VyneQlGrammar.QUERY_TYPE_TAXI,
                """
         ${JdbcConnectorTaxi.Annotations.imports}
         import ${VyneQlGrammar.QUERY_TYPE_NAME}
         type MovieId inherits Int
         type MovieTitle inherits String
         type AvailableCopyCount inherits Int
         @Table(connection = "movies", table = "movie", schema = "public")
         model Movie {
             ID : MovieId
            TITLE : MovieTitle
         }

         service ApiService {
            operation getAvailableCopies(MovieId):AvailableCopyCount
         }

         @DatabaseService( connection = "movies" )
         service MovieDb {
            table movie : Movie[]
         }
      """
            )
        ) { schema ->

            val stub = StubService(schema = schema)
            stub.addResponse(
                "getAvailableCopies",
                TypedInstance.from(schema.type("AvailableCopyCount"), 150, schema),
                modifyDataSource = true
            )
            listOf(JdbcInvoker(connectionFactory, SimpleSchemaProvider(schema)), stub)
        }
        val result = vyne.query(
            """find { Movie[]( MovieTitle == "A New Hope" ) }
         | as {
         |  title : MovieTitle
         |  availableCopies : AvailableCopyCount
         |}[]
      """.trimMargin()
        )
            .typedObjects()
        result.shouldHaveSize(1)
        result.first().toRawObject()
            .shouldBe(mapOf("title" to "A New Hope", "availableCopies" to 150))
    }

    @Test
    fun `can issue a query that starts with api and joins to jdbc`(): Unit = runBlocking {
        movieRepository.save(
            Movie("1", "A New Hope")
        )
        val vyne = testVyne(
            listOf(
                JdbcConnectorTaxi.schema,
                VyneQlGrammar.QUERY_TYPE_TAXI,
                """
         ${JdbcConnectorTaxi.Annotations.imports}
         import ${VyneQlGrammar.QUERY_TYPE_NAME}
         type MovieId inherits Int
         type MovieTitle inherits String
         type AvailableCopyCount inherits Int
         @Table(connection = "movies", table = "movie", schema = "public")
         model Movie {
            @Id
              ID : MovieId
            TITLE : MovieTitle
         }

         model NewRelease {
            movieId : MovieId
            releaseDate : ReleaseDate inherits Date
         }

         service ApiService {
            operation getNewReleases():NewRelease[]
         }

         @DatabaseService( connection = "movies" )
         service MovieDb {
            table movie : Movie[]
         }
      """
            )
        ) { schema ->

            val stub = StubService(schema = schema)
            stub.addResponse(
                "getNewReleases",
                TypedInstance.from(
                    schema.type("NewRelease"), mapOf(
                        "movieId" to 1,
                        "releaseDate" to LocalDate.parse("1979-05-10")
                    ), schema
                ),
                modifyDataSource = true
            )
            listOf(JdbcInvoker(connectionFactory, SimpleSchemaProvider(schema)), stub)
        }
        val result = vyne.query(
            """find { NewRelease[] }
         | as {
         |  title : MovieTitle
         |  releaseDate : ReleaseDate
         |}[]
      """.trimMargin()
        )
            .typedObjects()
        result.shouldHaveSize(1)
        result.first().toRawObject()
            .shouldBe(mapOf("title" to "A New Hope", "releaseDate" to "1979-05-10"))
    }

    @Test
    fun `can issue a query that starts with api and joins to jdbc for csv model`(): Unit = runBlocking {
        movieRepository.save(
            Movie("1", "A New Hope")
        )
        val vyne = testVyne(
            listOf(
                JdbcConnectorTaxi.schema,
                VyneQlGrammar.QUERY_TYPE_TAXI,
                """
         ${JdbcConnectorTaxi.Annotations.imports}
         import ${VyneQlGrammar.QUERY_TYPE_NAME}
         type MovieId inherits Int
         type MovieTitle inherits String
         type AvailableCopyCount inherits Int
         @Table(connection = "movies", table = "movie", schema = "public")
         model Movie {
            @Id
            ID : MovieId by column("movie id")
            TITLE : MovieTitle by column("movie title")
         }

         model NewRelease {
            movieId : MovieId
            releaseDate : ReleaseDate inherits Date
         }

         service ApiService {
            operation getNewReleases():NewRelease[]
         }

         @DatabaseService( connection = "movies" )
         service MovieDb {
            table movie : Movie[]
         }
      """
            )
        ) { schema ->

            val stub = StubService(schema = schema)
            stub.addResponse(
                "getNewReleases",
                TypedInstance.from(
                    schema.type("NewRelease"), mapOf(
                        "movieId" to 1,
                        "releaseDate" to LocalDate.parse("1979-05-10")
                    ), schema
                ),
                modifyDataSource = true
            )
            listOf(JdbcInvoker(connectionFactory, SimpleSchemaProvider(schema)), stub)
        }
        val result = vyne.query(
            """find { NewRelease[] }
         | as {
         |  title : MovieTitle
         |  releaseDate : ReleaseDate
         |}[]
      """.trimMargin()
        )
            .typedObjects()
        result.shouldHaveSize(1)
        result.first().toRawObject()
            .shouldBe(mapOf("title" to "A New Hope", "releaseDate" to "1979-05-10"))
    }


}

@Entity
data class Movie(
    @Id
    @Column
    val id: String,
    @Column
    val title: String,
)


@Configuration
@EnableAutoConfiguration
@EnableJpaRepositories
class MySqlQueryTestConfig

interface MovieRepository : JpaRepository<Movie, Int>

interface ActorRepository : JpaRepository<Actor, Int>

@Entity
data class Actor(
    @Id val id: Int,
    val firstName: String,
    val lastName: String
)

