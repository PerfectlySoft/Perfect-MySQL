import Foundation
import Testing
@testable import PerfectMySQL

/// listTables / listDatabases pass the wildcard to mysql_list_tables / mysql_list_dbs, which
/// build "SHOW ... LIKE '<wild>'" by putting a backslash before `'` and `\` without regard to the
/// connection's character set. Under big5, cp932, gbk, gb18030 or sjis a byte >= 0x80 before the
/// `'` can swallow that backslash as the second byte of a character, so the `'` ends the literal:
/// "中'" (E4 B8 AD 27) became E4 B8 AD 5C 27, read as 中 then AD5C then a closing quote. With
/// CLIENT_MULTI_STATEMENTS the rest of the wildcard ran as SQL.
struct ListingWildcardTests {
	static let charsets = ["big5", "cp932", "gbk", "gb18030", "sjis"]

	static func withSchema(charset: String, _ body: (MySQL, String) throws -> Void) throws {
		let schema = "perfect_mysql_wild_\(UUID().uuidString.replacingOccurrences(of: "-", with: "_"))"
		let admin = MySQL()
		try #require(MySQLTestEnvironment.connect(admin, database: testAdminDB))
		try #require(admin.query(statement: "CREATE DATABASE `\(schema)`"))
		defer { _ = admin.query(statement: "DROP DATABASE IF EXISTS `\(schema)`") }
		try #require(admin.query(statement: "CREATE TABLE `\(schema)`.victim (i INT)"))
		let mysql = MySQL()
		try #require(mysql.setOption(.MYSQL_SET_CHARSET_NAME, charset))
		try #require(MySQLTestEnvironment.connect(mysql, database: schema, flag: 1 << 16), "\(mysql.errorMessage())") // CLIENT_MULTI_STATEMENTS
		try body(mysql, schema)
		try #require(admin.selectDatabase(named: schema))
		#expect(admin.listTables() == ["victim"], "\(charset): the victim table must survive")
	}

	@Test(.enabled(if: MySQLTestEnvironment.isEnabled), arguments: charsets)
	func aMultibyteWildcardCannotEndTheLiteral(charset: String) throws {
		try Self.withSchema(charset: charset) { mysql, schema in
			for payload in ["中'; DROP TABLE victim; -- ", "中\\'; DROP TABLE victim; -- ", "\u{8A}'; DROP TABLE victim; -- "] {
				#expect(mysql.listTables(wildcard: payload).isEmpty, "\(charset)")
				#expect(mysql.listDatabases(wildcard: payload).isEmpty, "\(charset)")
				// The connection is still in sync.
				#expect(mysql.query(statement: "SELECT 1"), "\(charset): \(mysql.errorMessage())")
				_ = mysql.storeResults()
			}
		}
	}

	// MariaDB Connector/C's mysql_list_tables / mysql_list_dbs don't escape the wildcard at
	// all, so on a Connector/C build a plain `'` ended the literal under any charset.
	@Test(.enabled(if: MySQLTestEnvironment.isEnabled), arguments: ["utf8mb4", "latin1", "big5"])
	func anAsciiQuoteCannotEndTheLiteral(charset: String) throws {
		try Self.withSchema(charset: charset) { mysql, schema in
			for payload in ["x'; DROP TABLE victim; -- ", "x\\'; DROP TABLE victim; -- "] {
				#expect(mysql.listTables(wildcard: payload).isEmpty, "\(charset)")
				#expect(mysql.listDatabases(wildcard: payload).isEmpty, "\(charset)")
				#expect(mysql.query(statement: "SELECT 1"), "\(charset): \(mysql.errorMessage())")
				_ = mysql.storeResults()
			}
			// Quotes and backslashes are matched literally.
			let admin = MySQL()
			try #require(MySQLTestEnvironment.connect(admin, database: schema))
			try #require(admin.query(statement: "CREATE TABLE `it's` (i INT)"))
			#expect(mysql.listTables(wildcard: "it's") == ["it's"], "\(charset)")
			#expect(mysql.listTables(wildcard: "it\\'s") == ["it's"], "\(charset)")
			try #require(admin.query(statement: "DROP TABLE `it's`"))
		}
	}

	@Test(.enabled(if: MySQLTestEnvironment.isEnabled))
	func ordinaryWildcardsStillWork() throws {
		try Self.withSchema(charset: "utf8mb4") { mysql, schema in
			#expect(mysql.listTables(wildcard: "vic%") == ["victim"])
			#expect(mysql.listTables(wildcard: "nothing'%").isEmpty)
			#expect(mysql.listTables(wildcard: "中'%").isEmpty) // fine under utf8mb4
			#expect(mysql.listDatabases(wildcard: schema) == [schema])
		}
		try Self.withSchema(charset: "big5") { mysql, schema in
			#expect(mysql.listTables(wildcard: "vic%") == ["victim"])
			#expect(mysql.listTables(wildcard: "v'ictim").isEmpty) // ASCII only: escaped correctly
			#expect(mysql.listDatabases(wildcard: schema) == [schema])
		}
	}

	@Test(.enabled(if: MySQLTestEnvironment.isEnabled))
	func theGuardOnlyRefusesRiskyWildcards() throws {
		let big5 = MySQL()
		try #require(big5.setOption(.MYSQL_SET_CHARSET_NAME, "big5"))
		try #require(MySQLTestEnvironment.connect(big5, database: testAdminDB))
		#expect(big5.listingWildcardIsUnsafe("中'"))
		#expect(big5.listingWildcardIsUnsafe("中\\%"))
		#expect(!big5.listingWildcardIsUnsafe("中%"))
		#expect(!big5.listingWildcardIsUnsafe("o'brien"))
		#expect(!big5.listingWildcardIsUnsafe(nil))
		let utf8 = MySQL()
		try #require(utf8.setOption(.MYSQL_SET_CHARSET_NAME, "utf8mb4"))
		try #require(MySQLTestEnvironment.connect(utf8, database: testAdminDB))
		#expect(!utf8.listingWildcardIsUnsafe("中'"))
	}
}
