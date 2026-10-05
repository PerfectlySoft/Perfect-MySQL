//
//  DirectStatementTests.swift
//  PerfectMySQLTests
//
//  Statements MySQL can't prepare (transaction control, savepoints, LOCK/UNLOCK, XA) must still
//  run through PerfectCRUD; MySQLStmt must keep its connection alive; fieldNames() must work.
//

import XCTest
import PerfectCRUD
@testable import PerfectMySQL

final class DirectStatementTests: XCTestCase {

	struct Item: Codable, TableNameProvider {
		static let tableName = "direct_items"
		let id: Int
	}

	/// No server needed.
	func testFirstKeyword() {
		XCTAssertEqual(MySQLDatabaseConfiguration.firstKeyword("SAVEPOINT crud_sp_2"), "SAVEPOINT")
		XCTAssertEqual(MySQLDatabaseConfiguration.firstKeyword("  \n\tlock tables t write"), "LOCK")
		XCTAssertEqual(MySQLDatabaseConfiguration.firstKeyword("release savepoint x"), "RELEASE")
		XCTAssertEqual(MySQLDatabaseConfiguration.firstKeyword("SELECT 1"), "SELECT")
		XCTAssertEqual(MySQLDatabaseConfiguration.firstKeyword(""), "")
		// DDL is only routed directly by the old case-sensitive prefix check.
		XCTAssertFalse(MySQLDatabaseConfiguration.directStatements.contains(MySQLDatabaseConfiguration.firstKeyword("create table t as select ?")))
		// Whole keyword, not a prefix: these must still be prepared.
		XCTAssertFalse(MySQLDatabaseConfiguration.directStatements.contains(MySQLDatabaseConfiguration.firstKeyword("USERS")))
		XCTAssertFalse(MySQLDatabaseConfiguration.directStatements.contains(MySQLDatabaseConfiguration.firstKeyword("LOCKED_ROWS()")))
	}

	func testNestedTransactionsUseSavepoints() throws {
		try MySQLTestEnvironment.skipUnlessEnabled()
		let db = try getDB()
		try db.create(Item.self, policy: .dropTable)
		let items = db.table(Item.self)
		try db.transaction {
			try items.insert(Item(id: 1))
			do {
				try db.transaction {
					try items.insert(Item(id: 2))
					throw MySQLCRUDError("roll back the inner transaction")
				}
			} catch {}
			_ = try db.transaction {
				try items.insert(Item(id: 3))
			}
		}
		XCTAssertEqual(try items.order(by: \.id).select().map(\.id), [1, 3])
	}

	func testLockAndUnlockTables() throws {
		try MySQLTestEnvironment.skipUnlessEnabled()
		let db = try getDB()
		try db.create(Item.self, policy: .dropTable)
		try db.sql("LOCK TABLES direct_items WRITE")
		try db.table(Item.self).insert(Item(id: 7))
		try db.sql("  unlock tables")
		XCTAssertEqual(try db.table(Item.self).count(), 1)
	}

	func testUnpreparableStatementOutsideTheListFallsBackToDirect() throws {
		try MySQLTestEnvironment.skipUnlessEnabled()
		let db = try getDB()
		// XA isn't in the direct list; the server rejects preparing it with error 1295.
		try db.sql("XA START 'perfect-direct-test'")
		try db.sql("XA END 'perfect-direct-test'")
		try db.sql("XA ROLLBACK 'perfect-direct-test'")
		XCTAssertEqual(try db.sql("SELECT 41 + 1 AS answer", Answer.self).first?.answer, 42)
	}

	func testUnpreparableStatementReturningRowsFailsLoudly() throws {
		try MySQLTestEnvironment.skipUnlessEnabled()
		let db = try getDB()
		// XA RECOVER and CHECK TABLE can't be prepared and return rows that this path can't
		// decode; they must throw rather than quietly return nothing.
		XCTAssertThrowsError(try db.sql("XA RECOVER")) { error in
			XCTAssertTrue("\(error)".contains("returns rows"), "\(error)")
		}
		XCTAssertThrowsError(try db.sql("CHECK TABLE no_such_table", Answer.self))
		// The results were drained, so the connection is still in sync.
		XCTAssertEqual(try db.sql("SELECT 41 + 1 AS answer", Answer.self).first?.answer, 42)
	}

	func testMultiStatementTextIsFullyDrained() throws {
		try MySQLTestEnvironment.skipUnlessEnabled()
		let mysql = MySQL()
		XCTAssertTrue(MySQLTestEnvironment.connect(mysql, database: testAdminDB, flag: 1 << 16), mysql.errorMessage()) // CLIENT_MULTI_STATEMENTS
		let db = Database(configuration: MySQLDatabaseConfiguration(connection: mysql))
		XCTAssertThrowsError(try db.sql("UNLOCK TABLES; SELECT 1"))
		// Without draining every result this fails with "Commands out of sync".
		XCTAssertEqual(try db.sql("SELECT 41 + 1 AS answer", Answer.self).first?.answer, 42)
	}

	func testLowercaseDDLWithBindingsIsStillPrepared() throws {
		try MySQLTestEnvironment.skipUnlessEnabled()
		let db = try getDB()
		try db.sql("drop table if exists direct_ctas")
		try db.sql("create table direct_ctas as select ? as answer", bindings: [("?", .integer(42))])
		XCTAssertEqual(try db.sql("SELECT answer FROM direct_ctas", Answer.self).first?.answer, 42)
	}

	func testFieldNamesForCallAfterExecute() throws {
		try MySQLTestEnvironment.skipUnlessEnabled()
		let mysql = rawMySQL
		XCTAssertTrue(mysql.query(statement: "DROP PROCEDURE IF EXISTS direct_proc"), mysql.errorMessage())
		XCTAssertTrue(mysql.query(statement: "CREATE PROCEDURE direct_proc() SELECT 1 AS one"), mysql.errorMessage())
		let stmt = MySQLStmt(mysql)
		XCTAssertTrue(stmt.prepare(statement: "CALL direct_proc()"), stmt.errorMessage())
		XCTAssertTrue(stmt.execute(), stmt.errorMessage())
		XCTAssertEqual(stmt.fieldNames(), [0: "one"])
	}

	struct Answer: Codable {
		let answer: Int
	}

	func testFieldNamesUsesCachedMetadata() throws {
		try MySQLTestEnvironment.skipUnlessEnabled()
		let mysql = rawMySQL
		let stmt = MySQLStmt(mysql)
		XCTAssertTrue(stmt.prepare(statement: "SELECT 1 AS one, 'x' AS two"), stmt.errorMessage())
		for _ in 0..<3 {
			XCTAssertEqual(stmt.fieldNames(), [0: "one", 1: "two"])
		}
	}

	func testFieldNamesFollowEachResultSet() throws {
		try MySQLTestEnvironment.skipUnlessEnabled()
		let mysql = rawMySQL
		XCTAssertTrue(mysql.query(statement: "DROP PROCEDURE IF EXISTS direct_two_sets"), mysql.errorMessage())
		XCTAssertTrue(mysql.query(statement: "CREATE PROCEDURE direct_two_sets() BEGIN SELECT 1 AS a; SELECT 1 AS b, 2 AS c, 3 AS d; END"), mysql.errorMessage())
		let call = MySQLStmt(mysql)
		XCTAssertTrue(call.prepare(statement: "CALL direct_two_sets()"), call.errorMessage())
		XCTAssertTrue(call.execute(), call.errorMessage())
		XCTAssertEqual(call.fieldNames(), [0: "a"])
		call.freeResult()
		XCTAssertEqual(call.nextResult(), 0)
		// Reading the second set's names through the first set's metadata read out of bounds.
		XCTAssertEqual(call.fieldNames(), [0: "b", 1: "c", 2: "d"])
	}

	func testResultsOfCallWithoutFieldNamesFirst() throws {
		try MySQLTestEnvironment.skipUnlessEnabled()
		let mysql = rawMySQL
		XCTAssertTrue(mysql.query(statement: "DROP PROCEDURE IF EXISTS direct_proc_value"), mysql.errorMessage())
		XCTAssertTrue(mysql.query(statement: "CREATE PROCEDURE direct_proc_value() SELECT 6 * 7 AS v"), mysql.errorMessage())
		let call = MySQLStmt(mysql)
		XCTAssertTrue(call.prepare(statement: "CALL direct_proc_value()"), call.errorMessage())
		XCTAssertTrue(call.execute(), call.errorMessage())
		var value: Any?
		_ = call.results().forEachRow { row in value = row.first ?? nil }
		XCTAssertEqual(value.map { "\($0)" }, "42")
	}

	func testStatementKeepsItsConnectionAlive() throws {
		try MySQLTestEnvironment.skipUnlessEnabled()
		// `rawMySQL` makes a new connection; nothing else holds it once the statement exists.
		let stmt = MySQLStmt(rawMySQL)
		XCTAssertTrue(stmt.prepare(statement: "SELECT 2 + 2"), stmt.errorMessage())
		XCTAssertTrue(stmt.execute(), stmt.errorMessage())
		var value: Any?
		_ = stmt.results().forEachRow { row in value = row.first ?? nil }
		XCTAssertEqual(value as? Int64, 4)
	}
}
