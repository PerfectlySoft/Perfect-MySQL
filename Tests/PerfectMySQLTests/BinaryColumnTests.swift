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
		try MySQLTestEnvironment.skipUnlessEnabled()
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

	func testQueryResultsExposeBinaryColumnsAsBytes() throws {
		try MySQLTestEnvironment.skipUnlessEnabled()
		_ = try makeTable()
		let mysql = rawMySQL
		XCTAssertTrue(mysql.query(statement: "SELECT id, b, vb, vc, j, cb FROM binary_cols ORDER BY id"), mysql.errorMessage())
		guard let results = mysql.storeResults() else { return XCTFail(mysql.errorMessage()) }
		XCTAssertEqual((0..<results.numFields()).map { results.fieldIsBinary(at: $0) },
					   [false, true, true, false, false, false])
		XCTAssertFalse(results.fieldIsBinary(at: 6))
		var rows: [[[UInt8]?]] = []
		results.forEachRowBytes { rows.append($0) }
		XCTAssertEqual(rows.count, 2)
		guard rows.count == 2 else { return }
		XCTAssertEqual(rows[0][0], Array("1".utf8))
		XCTAssertEqual(rows[0][1], [0xFF, 0x00, 0xFE, 0x01])
		XCTAssertEqual(rows[0][2], [0xFF])
		XCTAssertEqual(rows[0][3], Array("abc".utf8))
		XCTAssertEqual(rows[1][1], [0x6F, 0x6B, 0x00, 0x00])
		XCTAssertNil(rows[1][4])
	}

	func testQueryResultsStringsAreNotTruncatedAtInvalidUTF8() throws {
		try MySQLTestEnvironment.skipUnlessEnabled()
		let mysql = rawMySQL
		XCTAssertTrue(mysql.query(statement: "SELECT X'61FF62' AS v"), mysql.errorMessage())
		guard let results = mysql.storeResults() else { return XCTFail(mysql.errorMessage()) }
		XCTAssertTrue(results.fieldIsBinary(at: 0))
		// Used to stop at the first invalid byte and return "a".
		XCTAssertEqual(results.next()?.first ?? nil, "a\u{FFFD}b")
		XCTAssertTrue(mysql.query(statement: "SELECT X'61FF62' AS v"), mysql.errorMessage())
		XCTAssertEqual(mysql.storeResults()?.nextBytes()?.first ?? nil, [0x61, 0xFF, 0x62])
	}

	func testBitAndGeometryAreBytesOnBothPaths() throws {
		try MySQLTestEnvironment.skipUnlessEnabled()
		let db = try getDB()
		try db.sql("CREATE TABLE bit_geo (bits BIT(8), g GEOMETRY, e VARBINARY(4))")
		try db.sql("INSERT INTO bit_geo VALUES (b'10000001', ST_GeomFromText('POINT(1 2)'), X'')")
		let mysql = rawMySQL
		let stmt = MySQLStmt(mysql)
		XCTAssertTrue(stmt.prepare(statement: "SELECT bits, g, e FROM bit_geo"), stmt.errorMessage())
		XCTAssertTrue(stmt.execute(), stmt.errorMessage())
		var stmtRow: [Any?] = []
		XCTAssertTrue(stmt.results().forEachRow { stmtRow = $0 })
		XCTAssertEqual(stmtRow.count, 3)
		guard stmtRow.count == 3 else { return }
		XCTAssertEqual(stmtRow[0] as? [UInt8], [0x81])
		// 4-byte SRID + 21-byte WKB point.
		XCTAssertEqual((stmtRow[1] as? [UInt8])?.count, 25)
		XCTAssertEqual(stmtRow[2] as? [UInt8], [])

		XCTAssertTrue(mysql.query(statement: "SELECT bits, g, e FROM bit_geo"), mysql.errorMessage())
		guard let results = mysql.storeResults() else { return XCTFail(mysql.errorMessage()) }
		XCTAssertEqual((0..<3).map { results.fieldIsBinary(at: $0) }, [true, true, true])
		let row = results.nextBytes()
		XCTAssertEqual(row?[0] ?? nil, [0x81])
		XCTAssertEqual(row?[1] ?? nil, stmtRow[1] as? [UInt8])
		// Empty, not NULL.
		XCTAssertEqual(row?[2] ?? nil, [])
	}

	func testCRUDDecodesBinaryColumns() throws {
		try MySQLTestEnvironment.skipUnlessEnabled()
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
