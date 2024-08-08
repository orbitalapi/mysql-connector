package com.orbitalhq.connectors.jdbc.mysql.ddl

import com.orbitalhq.connectors.config.jdbc.JdbcUrlAndCredentials
import com.orbitalhq.connectors.config.jdbc.JdbcUrlCredentialsConnectionConfiguration
import com.orbitalhq.connectors.jdbc.DatabaseMetadataService
import com.orbitalhq.connectors.jdbc.JdbcConnectionFactory
import com.orbitalhq.connectors.jdbc.SimpleJdbcConnectionFactory
import com.orbitalhq.connectors.jdbc.drivers.databaseSupport
import com.orbitalhq.connectors.jdbc.mysql.MySqlDbSupport
import com.orbitalhq.connectors.jdbc.sql.ddl.TableGenerator
import com.orbitalhq.schemas.taxi.TaxiSchema
import com.orbitalhq.utils.withoutWhitespace
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Testcontainers
import kotlin.random.Random

@Testcontainers
class MySqlTableGeneratorTest {
    companion object {
        @JvmStatic
        @BeforeClass
        fun setup() {
            MySqlDbSupport.register()
        }
    }


    lateinit var jdbcUrl: String
    lateinit var username: String
    lateinit var password: String
    lateinit var connectionDetails: JdbcUrlCredentialsConnectionConfiguration
    lateinit var connectionFactory: JdbcConnectionFactory

    @Rule
    @JvmField
    val mySqlContainer = MySQLContainer<Nothing>("mysql:8.4.0") as MySQLContainer<*>

    @Before
    fun before() {
        mySqlContainer.start()
        mySqlContainer.waitingFor(Wait.forListeningPort())

        jdbcUrl = mySqlContainer.jdbcUrl
        username = mySqlContainer.username
        password = mySqlContainer.password
        connectionDetails = JdbcUrlCredentialsConnectionConfiguration(
            "mysql",
            MySqlDbSupport.driverName,
            JdbcUrlAndCredentials(jdbcUrl, username, password)
        )
        connectionFactory = SimpleJdbcConnectionFactory()
    }

    @Test
    fun `can create table with auto incrementing numeric id`() {
        val tableName = "Person"
        val schema = TaxiSchema.from(
            """
         @com.orbitalhq.jdbc.Table(schema = "public", table = "$tableName", connection = "postgres")
         model Person {
            @Id @GeneratedId
            id : PersonId inherits Int
            firstName : FirstName inherits String
            lastName : LastName inherits String
         }
      """.trimIndent()
        )
        val ddl = TableGenerator(schema, connectionDetails.databaseSupport)
            .generate(schema.type("Person"), connectionFactory.dsl(connectionDetails))
        ddl.ddlStatement.sql.withoutWhitespace().shouldBe(
            """create table if not exists `Person` (
         `id` int not null auto_increment,
         `firstName` text not null,
         `lastName` text not null,
         constraint `Person-pk` primary key (`id`)
)""".withoutWhitespace()
        )
    }

    @Test
    fun `can create MySql table`() {
        val tableName = "Person_" + Random.nextInt(0, 999999)
        val schema = TaxiSchema.from(
            """
         @com.orbitalhq.jdbc.Table(schema = "public", table = "$tableName", connection = "postgres")
         model Person {
            @Id
            id : PersonId inherits Int
            @Id
            firstName : FirstName inherits String
            @Id
            lastName : LastName inherits String
            favouriteColor : String?
            age : Age inherits Int
            @Index
            fullName : FullName inherits String by concat(this.firstName, ' ', this.lastName)
         }
      """.trimIndent()
        )
        TableGenerator(schema, connectionDetails.databaseSupport).execute(
            schema.type("Person"),
            connectionFactory.dsl(connectionDetails)
        )

        val template = SimpleJdbcConnectionFactory()
            .jdbcTemplate(connectionDetails)
        val metadataService = DatabaseMetadataService(template.jdbcTemplate, connectionDetails)
        val tables = metadataService.listTables()
        val createdTable = tables.firstOrNull { it.tableName == tableName } ?: error("Failed to create $tableName")
        val columns = metadataService.listColumns(createdTable.schemaName, createdTable.tableName)
        columns.shouldHaveSize(6)
        createdTable.constrainedColumns.shouldHaveSize(3)
        columns.single { it.columnName == "favouriteColor" }.nullable.shouldBeTrue()
        columns.single { it.columnName == "firstName" }.nullable.shouldBeFalse()
        columns.single { it.columnName == "age" }.dataType.shouldBe("INT")
        // two indexes one for the primary key another one for fullName through Index annotation.
        createdTable.indexes.shouldHaveSize(2)
        createdTable
            .indexes
            .flatMap { it.columns.map { indexColumn -> indexColumn.columnName } }
            .shouldContainAll("fullName", "id", "firstName", "lastName")
    }

}
