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

	@Test(.enabled(if: MySQLTestEnvironment.isFixtureEnabled))
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

	// Built against MariaDB Connector/C, affectedRows trapped ("Not enough bits to represent the
	// passed value"): it was read after hasNext() had reset the statement, which sets
	// Connector/C's count to -1.
	@Test(.enabled(if: MySQLTestEnvironment.isFixtureEnabled))
	func mutationsReportAffectedRowsAndInsertedID() throws {
		let fixture = try CatalogFixtureDatabase()
		let database = fixture.database
		try database.sql("DROP TABLE IF EXISTS dynamic_mutation")
		try database.sql("""
		CREATE TABLE dynamic_mutation (
			id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL
		)
		""")
		try database.sql("INSERT INTO dynamic_mutation (name) VALUES ('a'), ('b'), ('b')")

		let insert = try database.mutate(DynamicMutation(
			action: .insert, table: "dynamic_mutation", values: ["name": .string("c")]))
		#expect(insert.affectedRows == 1)
		#expect(insert.insertedID == 4)

		let update = try database.mutate(DynamicMutation(
			action: .update, table: "dynamic_mutation", values: ["name": .string("z")],
			predicates: [.init(field: "name", comparison: .equal, value: .string("b"))]))
		#expect(update.affectedRows == 2)

		let delete = try database.mutate(DynamicMutation(
			action: .delete, table: "dynamic_mutation",
			predicates: [.init(field: "name", comparison: .equal, value: .string("nothing"))]))
		#expect(delete.affectedRows == 0)
	}

	// DynamicRow lowercases its keys and traps on duplicates (Perfect-CRUD 3.0.0), so columns
	// whose names differ only by case crashed the process. Same-named columns kept a random one.
	// The first column of each name now wins, ignoring case.
	@Test(.enabled(if: MySQLTestEnvironment.isFixtureEnabled))
	func duplicateColumnNamesKeepTheFirstColumn() throws {
		let fixture = try CatalogFixtureDatabase()
		let database = fixture.database
		let byCase = try database.execute(DynamicSQL(sql: "SELECT 1 AS a, 2 AS A, 3 AS b"))
		let row = try #require(byCase.rows.first)
		#expect(row["a"] == .int(1))
		#expect(row["b"] == .int(3))
		#expect(row.values.count == 2)

		try database.sql("CREATE TABLE dyn_parent (id BIGINT PRIMARY KEY, Name VARCHAR(20))")
		try database.sql("CREATE TABLE dyn_child (id BIGINT PRIMARY KEY, name VARCHAR(20))")
		try database.sql("INSERT INTO dyn_parent VALUES (1, 'parent')")
		try database.sql("INSERT INTO dyn_child VALUES (2, 'child')")
		for _ in 0..<5 { // the old column map's order varied from run to run
			let joined = try database.execute(DynamicSQL(sql: """
				SELECT p.id, p.Name, c.id, c.name FROM dyn_parent p CROSS JOIN dyn_child c
				"""))
			let joinedRow = try #require(joined.rows.first)
			#expect(joinedRow["id"] == .int(1))
			#expect(joinedRow["name"] == .string("parent"))
		}
	}
}
