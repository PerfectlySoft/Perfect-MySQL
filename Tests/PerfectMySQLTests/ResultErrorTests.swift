import Foundation
import PerfectCRUD
import Testing
@testable import PerfectMySQL

/// A statement can fail after it starts returning rows: here the scalar subquery returns two
/// rows for the second outer row (error 1242). Reading on from that failed result crashed
/// inside the client library (SIGSEGV).
struct ResultErrorTests {
	struct Row: Codable {
		let id: Int
		let x: Int?
	}

	static let failingQuery = "SELECT id, (SELECT x FROM b WHERE b.k = a.id) AS x FROM a ORDER BY id"

	static func database() throws -> CatalogFixtureDatabase {
		let fixture = try CatalogFixtureDatabase()
		let db = fixture.database
		try db.sql("CREATE TABLE a (id INT PRIMARY KEY)")
		try db.sql("CREATE TABLE b (k INT, x INT)")
		try db.sql("INSERT INTO a VALUES (1), (2), (3)")
		try db.sql("INSERT INTO b VALUES (1, 10), (2, 20), (2, 21)")
		return fixture
	}

	@Test(.enabled(if: MySQLTestEnvironment.isFixtureEnabled))
	func aFailingResultThrowsThroughCRUD() throws {
		let fixture = try Self.database()
		let db = fixture.database
		let error = #expect(throws: MySQLCRUDError.self) {
			_ = try db.sql(Self.failingQuery, Row.self)
		}
		#expect(error?.description.contains("more than 1 row") == true)
		#expect(throws: MySQLCRUDError.self) {
			_ = try db.execute(DynamicSQL(sql: Self.failingQuery))
		}
		// The connection is still usable.
		#expect(try db.sql("SELECT id, NULL AS x FROM a", Row.self).count == 3)
	}

	@Test(.enabled(if: MySQLTestEnvironment.isFixtureEnabled))
	func aFailingResultEndsRawIterationWithAnError() throws {
		let fixture = try Self.database()
		let mysql = MySQL()
		try #require(MySQLTestEnvironment.connect(mysql, database: fixture.config.schema))
		let stmt = MySQLStmt(mysql)
		try #require(stmt.prepare(statement: Self.failingQuery))
		#expect(stmt.execute())
		let results = stmt.results()
		var rows = 0
		while results.next() != nil { rows += 1 }
		#expect(results.failed)
		#expect(results.errorCode == 1242)
		#expect(results.errorMessage.contains("more than 1 row"))
		#expect(rows == 0)
	}
}

/// The parameter bind array is freed whenever it exists. clearBinds() (also called from deinit)
/// used to free it only when binds were pending, but reset() clears them, so every
/// parameterized statement that ran leaked its array, as did one prepared twice.
struct ParameterBindTests {
	@Test(.enabled(if: MySQLTestEnvironment.isEnabled))
	func clearBindsFreesTheArrayAfterReset() throws {
		let mysql = MySQL()
		try #require(MySQLTestEnvironment.connect(mysql, database: testAdminDB))
		let stmt = MySQLStmt(mysql)
		try #require(stmt.prepare(statement: "SELECT ? + ?"))
		#expect(stmt.paramBinds != nil)
		stmt.bindParam(1)
		stmt.bindParam(2)
		#expect(stmt.execute())
		_ = stmt.results().next()
		stmt.reset()
		stmt.clearBinds()
		#expect(stmt.paramBinds == nil)
	}

	@Test(.enabled(if: MySQLTestEnvironment.isEnabled))
	func preparingAgainReplacesTheArray() throws {
		let mysql = MySQL()
		try #require(MySQLTestEnvironment.connect(mysql, database: testAdminDB))
		let stmt = MySQLStmt(mysql)
		try #require(stmt.prepare(statement: "SELECT ?"))
		stmt.bindParam(1)
		try #require(stmt.prepare(statement: "SELECT CAST(? + ? + ? AS SIGNED)"))
		stmt.bindParam(1)
		stmt.bindParam(2)
		stmt.bindParam(3)
		#expect(stmt.execute(), "\(stmt.errorMessage())")
		#expect(stmt.results().next()?.first as? Int64 == 6)
	}
}
