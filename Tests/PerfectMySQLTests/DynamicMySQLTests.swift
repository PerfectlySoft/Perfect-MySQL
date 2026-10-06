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

	// A statement that returns rows reports how many it returned and no inserted ID, even right
	// after an INSERT on the same connection (Connector/C's insert ID is the connection's).
	@Test(.enabled(if: MySQLTestEnvironment.isFixtureEnabled))
	func rowReturningStatementsReportRowCountAndNoInsertedID() throws {
		let fixture = try CatalogFixtureDatabase()
		let database = fixture.database
		try database.sql("CREATE TABLE dyn_counts (id BIGINT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(20))")
		let insert = try database.execute(DynamicSQL(
			sql: "INSERT INTO dyn_counts (name) VALUES (?), (?)", bindings: [.string("a"), .string("b")]))
		#expect(insert.affectedRows == 2)
		#expect(insert.insertedID == 1)

		let select = try database.execute(DynamicSQL(sql: "SELECT id, name FROM dyn_counts"))
		#expect(select.rows.count == 2)
		#expect(select.affectedRows == 2)
		#expect(select.insertedID == nil)

		let empty = try database.execute(DynamicSQL(sql: "SELECT id FROM dyn_counts WHERE id > 99"))
		#expect(empty.affectedRows == 0)
		#expect(empty.insertedID == nil)

		let update = try database.execute(DynamicSQL(sql: "UPDATE dyn_counts SET name = 'z' WHERE id = 1"))
		#expect(update.affectedRows == 1)
		#expect(update.insertedID == nil)
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

	@Test(.enabled(if: MySQLTestEnvironment.isFixtureEnabled))
	func executeReturnsRowsAndBindsValues() throws {
		let fixture = try CatalogFixtureDatabase()
		let database = fixture.database
		let result = try database.execute(DynamicSQL(
			sql: "SELECT ? AS n, ? AS s, ? AS b, ? AS z",
			bindings: [.int(-5), .string("Koi"), .bytes([0, 255]), .null]))
		let row = try #require(result.rows.first)
		#expect(result.rows.count == 1)
		#expect(row["n"] == .int(-5))
		#expect(row["s"] == .string("Koi"))
		#expect(row["b"] == .bytes([0, 255]))
		#expect(row["z"] == .null)
	}

	// Unsigned values must bind as unsigned: UInt64.max is not -1.
	@Test(.enabled(if: MySQLTestEnvironment.isFixtureEnabled))
	func unsignedValuesRoundTrip() throws {
		let fixture = try CatalogFixtureDatabase()
		let database = fixture.database
		try database.sql("CREATE TABLE dyn_unsigned (id BIGINT PRIMARY KEY, u BIGINT UNSIGNED)")
		for (id, value) in [(1, UInt64.max), (2, UInt64(Int64.max) + 1), (3, 0)] {
			_ = try database.mutate(DynamicMutation(
				action: .insert, table: "dyn_unsigned",
				values: ["id": .int(Int64(id)), "u": .uint(value)]))
		}
		let selected = try database.select(DynamicQuery(
			table: "dyn_unsigned", fields: ["id", "u"],
			predicates: [.init(field: "u", comparison: .equal, value: .uint(.max))]))
		#expect(selected.rows.map { $0["id"] } == [.int(1)])
		#expect(selected.rows.first?["u"] == .uint(.max))
		let echoed = try database.execute(DynamicSQL(sql: "SELECT ? AS u", bindings: [.uint(.max)]))
		#expect(echoed.rows.first?["u"] == .uint(.max))
	}

	// MySQL can't prepare several statements at once, and a CRUD connection doesn't set
	// CLIENT_MULTI_STATEMENTS, so a batch must fail as a whole rather than run its first part.
	@Test(.enabled(if: MySQLTestEnvironment.isFixtureEnabled))
	func multipleStatementsThrowWithoutRunningAny() throws {
		struct Count: Codable { let c: Int }
		let fixture = try CatalogFixtureDatabase()
		let database = fixture.database
		try database.sql("CREATE TABLE dyn_multi (id BIGINT PRIMARY KEY)")
		for sql in [
			"INSERT INTO dyn_multi VALUES (1); INSERT INTO dyn_multi VALUES (2)",
			"BEGIN; INSERT INTO dyn_multi VALUES (3); COMMIT",
			"SELECT 1; SELECT 2",
		] {
			#expect(throws: (any Error).self, "\(sql)") {
				try database.execute(DynamicSQL(sql: sql, allowsMultipleStatements: true))
			}
			// Without the flag Perfect-CRUD refuses it before reaching the connector.
			#expect(throws: (any Error).self, "\(sql)") {
				try database.execute(DynamicSQL(sql: sql))
			}
		}
		let rows = try database.sql("SELECT COUNT(*) AS c FROM dyn_multi", Count.self)
		#expect(rows.first?.c == 0)
	}
}
