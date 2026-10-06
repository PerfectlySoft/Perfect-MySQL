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
		#expect(throws: MySQLCRUDError.self) {
			_ = try db.sql(Self.failingQuery, Row.self)
		}
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
		#expect(stmt.errorCode() == 1242)
		#expect(rows == 0)
	}
}
