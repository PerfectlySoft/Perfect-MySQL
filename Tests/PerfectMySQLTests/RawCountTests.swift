import Foundation
import Testing
@testable import PerfectMySQL

/// MySQL.numberAffectedRows() and lastInsertId() over the C API's unsigned values.
struct RawCountTests {
	/// A connection to a fresh schema, dropped when `body` returns.
	static func withSchema(_ body: (MySQL) throws -> Void) throws {
		let schema = "perfect_mysql_counts_\(UUID().uuidString.replacingOccurrences(of: "-", with: "_"))"
		let mysql = MySQL()
		try #require(MySQLTestEnvironment.connect(mysql, database: testAdminDB), "\(mysql.errorMessage())")
		try #require(mysql.query(statement: "CREATE DATABASE `\(schema)`"), "\(mysql.errorMessage())")
		defer { _ = mysql.query(statement: "DROP DATABASE IF EXISTS `\(schema)`") }
		try #require(mysql.selectDatabase(named: schema), "\(mysql.errorMessage())")
		try body(mysql)
	}

	// Both used Int64(_:) and trapped on (my_ulonglong)-1.
	@Test(.enabled(if: MySQLTestEnvironment.isEnabled))
	func affectedRowsIsMinusOneAfterAnErrorOrAnUnstoredSelect() throws {
		try Self.withSchema { mysql in
			#expect(!mysql.query(statement: "SELECT * FROM no_such_table"))
			#expect(mysql.numberAffectedRows() == -1)

			#expect(mysql.query(statement: "SELECT 1"))
			#expect(mysql.numberAffectedRows() == -1)
			_ = mysql.storeResults()

			#expect(mysql.query(statement: "CREATE TABLE t (id INT PRIMARY KEY)"))
			#expect(mysql.query(statement: "INSERT INTO t VALUES (1), (2), (3)"))
			#expect(mysql.numberAffectedRows() == 3)
		}
	}

	@Test(.enabled(if: MySQLTestEnvironment.isEnabled))
	func insertIdAboveInt64MaxKeepsItsBits() throws {
		try Self.withSchema { mysql in
			let big: UInt64 = UInt64(Int64.max) + 5
			#expect(mysql.query(statement: "CREATE TABLE u (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, v INT) AUTO_INCREMENT = \(big)"))
			#expect(mysql.query(statement: "INSERT INTO u (v) VALUES (1)"), "\(mysql.errorMessage())")
			#expect(UInt64(bitPattern: mysql.lastInsertId()) == big)
			#expect(mysql.query(statement: "CREATE TABLE s (id BIGINT AUTO_INCREMENT PRIMARY KEY, v INT)"))
			#expect(mysql.query(statement: "INSERT INTO s (v) VALUES (1)"))
			#expect(mysql.lastInsertId() == 1)
		}
	}
}
