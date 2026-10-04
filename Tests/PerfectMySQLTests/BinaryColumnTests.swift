//
//  BinaryColumnTests.swift
//  PerfectMySQLTests
//
//  BINARY / VARBINARY columns report as (VAR_)STRING with the binary charset (63). They must come
//  back as bytes, not as a String decoded from bytes that needn't be UTF-8.
//

import XCTest
import Foundation
import PerfectCRUD
@testable import PerfectMySQL

final class BinaryColumnTests: XCTestCase {
	var mysqlTests: Bool { ProcessInfo.processInfo.environment["MYSQL_TESTS"] == "1" }

	struct BinaryRow: Codable, TableNameProvider {
		static let tableName = "binary_cols"
		let id: Int
		let b: [UInt8]
		let vb: Data
		let vc: String
		let u: UUID
	}

	struct BinaryAsText: Codable, TableNameProvider {
		static let tableName = "binary_cols"
		let id: Int
		let vb: String
	}

	let uuid1 = UUID(uuidString: "07A1B2C3-D4E5-46F7-8899-AABBCCDDEEFF")!
	let uuid2 = UUID(uuidString: "17A1B2C3-D4E5-46F7-8899-AABBCCDDEEFF")!

	private func makeTable() throws -> Database<DBConfiguration> {
		let db = try getDB()
		try db.sql("CREATE TABLE binary_cols (id INT PRIMARY KEY, b BINARY(4), vb VARBINARY(8), vc VARCHAR(8), j JSON, u VARBINARY(36), cb VARCHAR(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)")
		try db.sql("INSERT INTO binary_cols VALUES (1, X'FF00FE01', X'FF', 'abc', '{\"a\": 1}', '\(uuid1)', 'bin'), (2, 'ok', 'hi', 'x', NULL, '\(uuid2)', NULL)")
		return db
	}

	func testStatementReturnsBinaryColumnsAsBytes() throws {
		guard mysqlTests else { return }
		_ = try makeTable()
		let mysql = rawMySQL
		let stmt = MySQLStmt(mysql)
		XCTAssertTrue(stmt.prepare(statement: "SELECT id, b, vb, vc, j, cb FROM binary_cols ORDER BY id"), stmt.errorMessage())
		XCTAssertTrue(stmt.execute(), stmt.errorMessage())
		var rows: [[Any?]] = []
		XCTAssertTrue(stmt.results().forEachRow { rows.append($0) })
		XCTAssertEqual(rows.count, 2)
		guard rows.count == 2 else { return }
		XCTAssertEqual(rows[0][1] as? [UInt8], [0xFF, 0x00, 0xFE, 0x01])
		XCTAssertEqual(rows[0][2] as? [UInt8], [0xFF])
		XCTAssertEqual(rows[0][3] as? String, "abc")
		// JSON also reports the binary charset on MySQL (on MariaDB it's LONGTEXT); still text.
		XCTAssertNotNil(rows[0][4] as? String)
		// A binary *collation* is still text; only the binary charset means bytes.
		XCTAssertEqual(rows[0][5] as? String, "bin")
		// BINARY is right-padded with NULs.
		XCTAssertEqual(rows[1][1] as? [UInt8], [0x6F, 0x6B, 0x00, 0x00])
		XCTAssertEqual(rows[1][2] as? [UInt8], Array("hi".utf8))
	}

	func testCRUDDecodesBinaryColumns() throws {
		guard mysqlTests else { return }
		let db = try makeTable()
		let rows = try db.table(BinaryRow.self).order(by: \.id).select().map { $0 }
		XCTAssertEqual(rows.count, 2)
		guard rows.count == 2 else { return }
		XCTAssertEqual(rows[0].b, [0xFF, 0x00, 0xFE, 0x01])
		XCTAssertEqual(rows[0].vb, Data([0xFF]))
		XCTAssertEqual(rows[0].vc, "abc")
		// Text-typed properties (UUID here) over a VARBINARY column still decode.
		XCTAssertEqual(rows.map(\.u), [uuid1, uuid2])
		// A String property over a VARBINARY column still decodes text stored there.
		let text = try db.table(BinaryAsText.self).where(\BinaryAsText.id == 2).first()
		XCTAssertEqual(text?.vb, "hi")
	}
}
