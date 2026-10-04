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
		// XA RECOVER returns a result set; it must be discarded so the connection stays usable.
		try db.sql("XA RECOVER")
		XCTAssertEqual(try db.sql("SELECT 41 + 1 AS answer", Answer.self).first?.answer, 42)
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
