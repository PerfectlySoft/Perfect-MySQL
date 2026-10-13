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

	// Blob parameters copy the value into a buffer that reset() never freed, so every
	// [UInt8] / [Int8] / (_:length:) parameter leaked (891 leaks, 37,984 bytes over 300 executions
	// of a 3-blob INSERT under `leaks --atExit`; 0 after). This checks that freeing them on
	// reset and on rebind doesn't disturb the values, including empty blobs and pending binds.
	@Test(.enabled(if: MySQLTestEnvironment.isFixtureEnabled))
	func repeatedBlobBindsWithResetAndRebind() throws {
		let fixture = try CatalogFixtureDatabase()
		try fixture.database.sql("CREATE TABLE blobs (n INT PRIMARY KEY, a BLOB, b BLOB, c BLOB)")
		let mysql = MySQL()
		try #require(MySQLTestEnvironment.connect(mysql, database: fixture.config.schema))
		let stmt = MySQLStmt(mysql)
		try #require(stmt.prepare(statement: "INSERT INTO blobs VALUES (?, ?, ?, ?)"))
		for n in 0..<200 {
			let bytes = [UInt8](repeating: UInt8(n % 256), count: n % 7)
			let signed = [Int8](repeating: -1, count: n % 5)
			if n % 3 == 0 { // binds left pending, then cleared by reset() before the real ones
				stmt.bindParam([UInt8](repeating: 9, count: 100))
				stmt.reset()
			}
			stmt.bindParam(n)
			stmt.bindParam(bytes)
			stmt.bindParam(signed)
			let raw: [Int8] = [1, 2, 3, 4]
			raw.withUnsafeBufferPointer { stmt.bindParam($0.baseAddress!, length: n % 4) }
			#expect(stmt.execute(), "\(n): \(stmt.errorMessage())")
			stmt.reset()
		}
		stmt.bindParam(1000)
		stmt.bindParam([UInt8](repeating: 1, count: 10))
		stmt.clearBinds() // frees pending blob binds and the array

		struct Row: Codable { let n: Int; let a: [UInt8]; let b: [UInt8]; let c: [UInt8] }
		let rows = try fixture.database.sql("SELECT n, a, b, c FROM blobs ORDER BY n", Row.self)
		#expect(rows.count == 200)
		for row in rows {
			#expect(row.a == [UInt8](repeating: UInt8(row.n % 256), count: row.n % 7), "\(row.n)")
			#expect(row.b == [UInt8](repeating: 255, count: row.n % 5), "\(row.n)")
			#expect(row.c == Array([1, 2, 3, 4].prefix(row.n % 4)), "\(row.n)")
		}
	}
}
