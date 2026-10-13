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

	// Reusing a statement (execute, read results, reset, execute again) corrupted the heap:
	// the client library kept pointers into the freed Results' buffers and wrote through them on
	// the next execute. It crashed within ~20 iterations with the system allocator, and on the
	// second under Guard Malloc, with or without parameters.
	@Test(.enabled(if: MySQLTestEnvironment.isEnabled))
	func aStatementCanBeReusedAfterReadingResults() throws {
		let mysql = MySQL()
		try #require(MySQLTestEnvironment.connect(mysql, database: testAdminDB))
		let stmt = MySQLStmt(mysql)
		try #require(stmt.prepare(statement: "SELECT CAST(? AS SIGNED) AS n, ? * 1.5 AS d, CONCAT('v', ?) AS s, LENGTH(?) AS l"))
		for n in 0..<300 {
			stmt.bindParam(n)
			stmt.bindParam(Double(n))
			stmt.bindParam("\(n)")
			stmt.bindParam([UInt8](repeating: 1, count: n % 9))
			try #require(stmt.execute(), "\(n): \(stmt.errorMessage())")
			let row = stmt.results().next()
			#expect(row?[0] as? Int64 == Int64(n))
			#expect(row?[2] as? String == "v\(n)")
			#expect(row?[3] as? Int64 == Int64(n % 9))
			stmt.reset()
		}
	}

	// Replacing a Results with a newer one releases the older one after the newer has bound.
	// The older one's cleanup used to detach the newer one's binds, so it read 0 / "".
	@Test(.enabled(if: MySQLTestEnvironment.isEnabled))
	func reassigningAResultsVariableKeepsTheNewOneBound() throws {
		let mysql = MySQL()
		try #require(MySQLTestEnvironment.connect(mysql, database: testAdminDB))
		let stmt = MySQLStmt(mysql)
		try #require(stmt.prepare(statement: "SELECT CAST(? AS SIGNED) AS n, CONCAT('v', ?) AS s, ? * 2.5 AS d"))
		stmt.bindParam(0)
		stmt.bindParam("0")
		stmt.bindParam(0.0)
		try #require(stmt.execute())
		var results = stmt.results()
		_ = results.next()
		for n in 1...50 {
			stmt.reset()
			stmt.bindParam(n)
			stmt.bindParam("\(n)")
			stmt.bindParam(Double(n))
			try #require(stmt.execute(), "\(stmt.errorMessage())")
			results = stmt.results()
			let row = results.next()
			#expect(row?[0] as? Int64 == Int64(n))
			#expect(row?[1] as? String == "v\(n)")
			#expect(row?[2] as? Double == Double(n) * 2.5)
		}
	}

	// Walking a CALL's result sets with one variable, and keeping an older Results alive while
	// the statement moves to a wider result set or is prepared again with more columns. The
	// older one's cleanup used to bind as many binds as it had columns while the library copied
	// the current column count, reading past the allocation.
	@Test(.enabled(if: MySQLTestEnvironment.isFixtureEnabled))
	func olderResultsDontDisturbLaterResultSets() throws {
		let fixture = try CatalogFixtureDatabase()
		try fixture.database.sql("CREATE PROCEDURE three_sets() BEGIN SELECT 1 AS a, 'x' AS b; SELECT 2 AS a, 'y' AS b, 0 AS c; SELECT 3 AS a, 'z' AS b; END")
		let mysql = MySQL()
		try #require(MySQLTestEnvironment.connect(mysql, database: fixture.config.schema))
		let call = MySQLStmt(mysql)
		try #require(call.prepare(statement: "CALL three_sets()"))
		for _ in 0..<3 {
			try #require(call.execute(), "\(call.errorMessage())")
			var results = call.results()
			var got: [String] = []
			while true {
				while let row = results.next() {
					got.append("\(row[0] as? Int64 ?? -1)/\(row[1] as? String ?? "nil")")
				}
				call.freeResult()
				guard call.nextResult() == 0, call.fieldCount() > 0 else { break }
				results = call.results()
			}
			#expect(got == ["1/x", "2/y", "3/z"])
			while call.nextResult() == 0 {}
			call.reset()
		}

		let stmt = MySQLStmt(mysql)
		try #require(stmt.prepare(statement: "SELECT 1 AS a"))
		try #require(stmt.execute())
		var older: MySQLStmt.Results? = stmt.results()
		_ = older?.next()
		stmt.freeResult()
		try #require(stmt.prepare(statement: "SELECT 1 AS a, 2 AS b, 3 AS c, 4 AS d, 5 AS e, 6 AS f, 7 AS g, 8 AS h"))
		try #require(stmt.execute())
		older = nil
		#expect(older == nil)
		let row = stmt.results().next()
		#expect(row?.count == 8)
		#expect(row?[7] as? Int64 == 8)
	}

	// After reset() the parameter buffers are freed, but the client library still holds the old
	// binds; executing again without rebinding read freed memory. It's now refused.
	@Test(.enabled(if: MySQLTestEnvironment.isEnabled))
	func executingWithoutBindingEveryParameterIsRefused() throws {
		let mysql = MySQL()
		try #require(MySQLTestEnvironment.connect(mysql, database: testAdminDB))
		let stmt = MySQLStmt(mysql)
		try #require(stmt.prepare(statement: "SELECT LENGTH(?) AS l, ? AS s"))
		stmt.bindParam([UInt8](repeating: 7, count: 4096))
		stmt.bindParam("hello")
		try #require(stmt.execute())
		#expect(stmt.results().next()?[0] as? Int64 == 4096)
		stmt.reset()

		#expect(!stmt.execute())
		#expect(stmt.errorCode() == 2031)
		#expect(stmt.errorMessage().contains("No data supplied"))
		stmt.bindParam([UInt8](repeating: 1, count: 3))
		#expect(!stmt.execute(), "one of two parameters bound")
		stmt.reset()

		stmt.bindParam([UInt8](repeating: 1, count: 3))
		stmt.bindParam("again")
		#expect(stmt.execute(), "\(stmt.errorMessage())")
		#expect(stmt.errorCode() == 0)
		#expect(stmt.results().next()?[0] as? Int64 == 3)

		let plain = MySQLStmt(mysql) // no parameters: nothing to bind
		try #require(plain.prepare(statement: "SELECT 1"))
		#expect(plain.execute())
		_ = plain.results().next()
		plain.reset()
		#expect(plain.execute())
	}

	// Binding more values than a statement has parameters wrote past the bind array (SIGSEGV
	// through Perfect-CRUD, even with the system allocator). The extra values are now refused with
	// error 2034, and execute() refuses until the statement is reset and bound correctly.
	@Test(.enabled(if: MySQLTestEnvironment.isFixtureEnabled))
	func bindingTooManyParametersIsRefused() throws {
		struct X: Codable { let x: Int }
		let fixture = try CatalogFixtureDatabase()
		let db = fixture.database
		#expect(throws: (any Error).self) {
			_ = try db.sql("SELECT ? AS x", bindings: [("?", .integer(1)), ("?", .string("two")), ("?", .blob([3]))], X.self)
		}
		#expect(try db.sql("SELECT ? AS x", bindings: [("?", .integer(4))], X.self).map(\.x) == [4])

		let mysql = db.configuration.connection
		let stmt = MySQLStmt(mysql)
		try #require(stmt.prepare(statement: "SELECT ? AS x"))
		stmt.bindParam(1)
		stmt.bindParam("x")
		stmt.bindParam([UInt8](repeating: 1, count: 64))
		stmt.bindParam()
		#expect(stmt.errorCode() == 2034)
		#expect(!stmt.execute())
		#expect(stmt.errorCode() == 2034)
		stmt.reset()
		#expect(stmt.errorCode() == 0, "a 2034 doesn't outlive reset()")
		stmt.bindParam(5)
		#expect(stmt.errorCode() == 0)
		#expect(stmt.execute(), "\(stmt.errorMessage())")
		#expect((stmt.results().next()?[0] as? any BinaryInteger).map { Int64($0) } == 5)
		stmt.reset()

		// A stray value on a statement without parameters doesn't stick after reset().
		let plain = MySQLStmt(mysql)
		try #require(plain.prepare(statement: "SELECT 1"))
		plain.bindParam(1)
		#expect(!plain.execute())
		plain.reset()
		#expect(plain.execute(), "\(plain.errorMessage())")
		_ = plain.results().next()

		// After clearBinds() on a prepared statement there's no array: values are refused, and
		// execute() says so (2034) rather than failing with no error.
		let cleared = MySQLStmt(mysql)
		try #require(cleared.prepare(statement: "SELECT ? AS x"))
		cleared.clearBinds()
		cleared.bindParam(1)
		#expect(!cleared.execute())
		#expect(cleared.errorCode() == 2034, "\(cleared.errorCode()) \(cleared.errorMessage())")

		// After a failed prepare, execute() fails with the library's error, not 2031: 1243
		// ("Unknown prepared statement handler") from libmysqlclient, 2014 ("Commands out of sync")
		// from MariaDB Connector/C.
		let broken = MySQLStmt(mysql)
		try #require(broken.prepare(statement: "SELECT ? AS x"))
		broken.bindParam(1)
		#expect(!broken.prepare(statement: "SELEC nonsense"))
		#expect(!broken.execute())
		#expect(broken.errorCode() != 2031, "\(broken.errorCode()) \(broken.errorMessage())")
	}
}
