import Foundation
import PerfectCRUD
import Testing
@testable import PerfectMySQL

struct DynamicMySQLTests {
	@Test func convertsNativeValues() throws {
		#expect(try mysqlDynamicValue(nil, column: "value") == .null)
		#expect(try mysqlDynamicValue(Int64(-7), column: "value") == .int(-7))
		#expect(try mysqlDynamicValue(UInt64(9), column: "value") == .uint(9))
		#expect(try mysqlDynamicValue(3.5, column: "value") == .double(3.5))
		#expect(try mysqlDynamicValue("Koi", column: "value") == .string("Koi"))
		#expect(
			try mysqlDynamicValue([UInt8](arrayLiteral: 1, 2, 3), column: "value") ==
				.bytes([1, 2, 3])
		)
	}

	@Test func rejectsUnknownNativeValue() {
		struct Unsupported {}
		do {
			_ = try mysqlDynamicValue(Unsupported(), column: "value")
			Issue.record("Expected unsupported dynamic value to throw.")
		} catch is MySQLCRUDError {
			// Expected.
		} catch {
			Issue.record("Unexpected error: \(error)")
		}
	}

	@Test(.enabled(if: ProcessInfo.processInfo.environment["MYSQL_FIXTURE_TESTS"] == "1"))
	func selectsDynamicRowsFromMySQL() throws {
		let fixture = try CatalogFixtureDatabase()
		let database = fixture.database
		try database.sql("DROP TABLE IF EXISTS dynamic_catalog")
		try database.sql("""
		CREATE TABLE dynamic_catalog (
			id BIGINT NOT NULL,
			name VARCHAR(255) NOT NULL,
			price DOUBLE,
			payload BLOB,
			featured VARCHAR(255)
		)
		""")
		try database.sql("""
		INSERT INTO dynamic_catalog (id, name, price, payload, featured)
		VALUES (1, 'Top', 31.95, X'0102', 'koi_sale_top')
		""")

		let result = try database.select(DynamicQuery(
			table: "dynamic_catalog",
			fields: ["id", "name", "price", "payload"],
			predicates: [
				DynamicPredicate(
					field: "featured",
					comparison: .contains,
					value: .string("sale")
				),
			]
		))

		let row = try #require(result.rows.first)
		#expect(row["id"] == .int(1))
		#expect(row["name"] == .string("Top"))
		#expect(row["price"] == .double(31.95))
		#expect(row["payload"] == .bytes([1, 2]))
	}
}
