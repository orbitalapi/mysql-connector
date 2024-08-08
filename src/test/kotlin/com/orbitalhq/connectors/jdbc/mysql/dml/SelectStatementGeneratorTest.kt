package com.orbitalhq.connectors.jdbc.mysql.dml

import com.orbitalhq.connectors.config.jdbc.JdbcUrlAndCredentials
import com.orbitalhq.connectors.config.jdbc.JdbcUrlCredentialsConnectionConfiguration
import com.orbitalhq.connectors.jdbc.mysql.MySqlDbSupport
import com.orbitalhq.connectors.jdbc.sql.dml.SelectStatementGenerator
import com.orbitalhq.connectors.jdbc.sql.dml.SqlTemplateParameter
import com.orbitalhq.connectors.jdbc.sqlBuilder
import com.orbitalhq.schemas.taxi.TaxiSchema
import io.kotest.core.spec.style.DescribeSpec
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.shouldBe
import lang.taxi.Compiler
import lang.taxi.TaxiDocument
import lang.taxi.query.TaxiQlQuery
import lang.taxi.types.toQualifiedName

class SelectStatementGeneratorTest : DescribeSpec({
    beforeTest {
        MySqlDbSupport.register()
    }
    describe("select statement generation") {

        val connectionDetails = JdbcUrlCredentialsConnectionConfiguration(
            "mysql",
            MySqlDbSupport.driverName,
            JdbcUrlAndCredentials("jdbc:mysql://localhost:49229/test", "username", "password")
        )

        it("generates simple select from table") {
            val taxi = """
         type MovieId inherits Int
         type MovieTitle inherits String

         model Movie {
            id : MovieId by column("id")
            title : MovieTitle by column("title")
         }""".compiled()
            val query = """find { Movie[] }""".query(taxi)
            val (sql, params) = SelectStatementGenerator(
                taxi,
                { type -> type.qualifiedName.toQualifiedName().typeName }).toSql(
                query,
                connectionDetails.sqlBuilder()
            )
            sql.shouldBe("""select * from `Movie` as `t0`""")
            params.shouldBeEmpty()
        }

        it("generates simple select from table with simple where clause") {
            val taxi = """
         type MovieId inherits Int
         type MovieTitle inherits String

         model Movie {
            id : MovieId
            title : MovieTitle
         }""".compiled()
            val query = """find { Movie[]( MovieTitle == 'Hello' ) }""".query(taxi)
            val selectStatement =
                SelectStatementGenerator(taxi).selectSqlWithNamedParams(query, connectionDetails.sqlBuilder())
            val (sql, params) = selectStatement

            // The odd cast expression here is JOOQ doing it's thing.
            // CAn't work out how to supress it
            val expected = """select * from `Movie` as `t0` where `t0`.`title` = :title0"""
            sql.shouldBe(expected)
            params.shouldBe(listOf(SqlTemplateParameter("title0", "Hello")))
        }

        it("generates simple select from table with simple where clause using int type") {
            val taxi = """
         type MovieId inherits Int
         type MovieTitle inherits String

         @com.orbitalhq.jdbc.Table(table = "movie", connection = "", schema = "")
         model Movie {
            id : MovieId
            title : MovieTitle
         }""".compiled()
            val query = """find { Movie[]( MovieId == 123 ) }""".query(taxi)
            val selectStatement =
                SelectStatementGenerator(taxi).selectSqlWithNamedParams(query, connectionDetails.sqlBuilder())
            val (sql, params) = selectStatement

            // The odd cast expression here is JOOQ doing it's thing.
            // CAn't work out how to supress it
            val expected = """select * from `movie` as `t0` where `t0`.`id` = :id0"""
            sql.shouldBe(expected)
            params.shouldBe(listOf(SqlTemplateParameter("id0", 123)))
        }

        it("generates simple select from table with simple where clause using String type for Id") {
            val taxi = """
         type MovieId inherits String
         type MovieTitle inherits String

         @com.orbitalhq.jdbc.Table(table = "movie", connection = "", schema = "")
         model Movie {
            id : MovieId
            title : MovieTitle
         }""".compiled()
            val query = """find { Movie[]( MovieId == '123' ) }""".query(taxi)
            val selectStatement =
                SelectStatementGenerator(taxi).selectSqlWithNamedParams(query, connectionDetails.sqlBuilder())
            val (sql, params) = selectStatement

            val expected = """select * from `movie` as `t0` where `t0`.`id` = :id0"""
            sql.shouldBe(expected)
            params.shouldBe(listOf(SqlTemplateParameter("id0", "123")))
        }

        describe("where clauses") {
            val schema = TaxiSchema.from(
                """
            model Person {
               age : Age inherits Int
            }
         """.trimIndent()
            )
            val taxi = schema.taxi
            val generator = SelectStatementGenerator(schema)
            val dsl = connectionDetails.sqlBuilder()
            it("generates a select for multiple number params") {
                val query =
                    generator.selectSqlWithNamedParams("find { Person[]( Age >= 21 && Age < 40 ) }".query(taxi), dsl)
                query.shouldBeQueryWithParams(
                    """select * from `Person` as `t0` where (`t0`.`age` >= :age0 and `t0`.`age` < :age1)""",
                    listOf(21, 40)
                )
            }
        }
    }


})


fun Pair<String, List<SqlTemplateParameter>>.shouldBeQueryWithParams(sql: String, params: List<Any>) {
    this.first.shouldBe(sql)
    this.second.map { it.value }.shouldBe(params)
}

fun String.query(taxi: TaxiDocument): TaxiQlQuery {
    return Compiler(this, importSources = listOf(taxi)).queries().first()
}

fun String.compiled(): TaxiDocument {
    val sourceWithImports = """

      $this
   """.trimIndent()
    return Compiler.forStrings(listOf(sourceWithImports)).compile()
}

