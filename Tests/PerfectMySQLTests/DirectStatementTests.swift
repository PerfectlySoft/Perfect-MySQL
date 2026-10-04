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
	var mysqlTests: Bool { ProcessInfo.processInfo.environment["MYSQL_TESTS"] == "1" }

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
		guard mysqlTests else { return }
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
		guard mysqlTests else { return }
		let db = try getDB()
		try db.create(Item.self, policy: .dropTable)
		try db.sql("LOCK TABLES direct_items WRITE")
		try db.table(Item.self).insert(Item(id: 7))
		try db.sql("  unlock tables")
		XCTAssertEqual(try db.table(Item.self).count(), 1)
	}

	func testUnpreparableStatementOutsideTheListFallsBackToDirect() throws {
		guard mysqlTests else { return }
		let db = try getDB()
		// XA isn't in the direct list; the server rejects preparing it with error 1295.
		try db.sql("XA START 'perfect-direct-test'")
		try db.sql("XA END 'perfect-direct-test'")
		try db.sql("XA ROLLBACK 'perfect-direct-test'")
		XCTAssertEqual(try db.sql("SELECT 41 + 1 AS answer", Answer.self).first?.answer, 42)
	}

	func testUnpreparableStatementReturningRowsFailsLoudly() throws {
		guard mysqlTests else { return }
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
		guard mysqlTests else { return }
		let mysql = MySQL()
		XCTAssertTrue(mysql.connect(host: testHost, user: testUser, password: testPassword, db: testAdminDB,
									port: UInt32(testPort ?? 0), flag: 1 << 16), mysql.errorMessage()) // CLIENT_MULTI_STATEMENTS
		let db = Database(configuration: MySQLDatabaseConfiguration(connection: mysql))
		XCTAssertThrowsError(try db.sql("UNLOCK TABLES; SELECT 1"))
		// Without draining every result this fails with "Commands out of sync".
		XCTAssertEqual(try db.sql("SELECT 41 + 1 AS answer", Answer.self).first?.answer, 42)
	}

	func testLowercaseDDLWithBindingsIsStillPrepared() throws {
		guard mysqlTests else { return }
		let db = try getDB()
		try db.sql("drop table if exists direct_ctas")
		try db.sql("create table direct_ctas as select ? as answer", bindings: [("?", .integer(42))])
		XCTAssertEqual(try db.sql("SELECT answer FROM direct_ctas", Answer.self).first?.answer, 42)
	}

	func testFieldNamesForCallAfterExecute() throws {
		guard mysqlTests else { return }
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
		guard mysqlTests else { return }
		let mysql = rawMySQL
		let stmt = MySQLStmt(mysql)
		XCTAssertTrue(stmt.prepare(statement: "SELECT 1 AS one, 'x' AS two"), stmt.errorMessage())
		for _ in 0..<3 {
			XCTAssertEqual(stmt.fieldNames(), [0: "one", 1: "two"])
		}
	}

	func testStatementKeepsItsConnectionAlive() throws {
		guard mysqlTests else { return }
		// `rawMySQL` makes a new connection; nothing else holds it once the statement exists.
		let stmt = MySQLStmt(rawMySQL)
		XCTAssertTrue(stmt.prepare(statement: "SELECT 2 + 2"), stmt.errorMessage())
		XCTAssertTrue(stmt.execute(), stmt.errorMessage())
		var value: Any?
		_ = stmt.results().forEachRow { row in value = row.first ?? nil }
		XCTAssertEqual(value as? Int64, 4)
	}
}
