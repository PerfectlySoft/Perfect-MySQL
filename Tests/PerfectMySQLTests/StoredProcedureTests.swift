import Foundation
import PerfectCRUD
import Testing
@testable import PerfectMySQL

/// CALL through Perfect-CRUD: a procedure's rows are read, and its remaining result sets
/// (at least the final status) are drained so the connection stays usable.
struct StoredProcedureTests {
	struct Item: Codable, Equatable {
		let id: Int
		let name: String
	}

	static func procedureDatabase() throws -> CatalogFixtureDatabase {
		let fixture = try CatalogFixtureDatabase()
		let db = fixture.database
		try db.sql("CREATE TABLE Item (id INT PRIMARY KEY, name VARCHAR(20))")
		try db.sql("INSERT INTO Item VALUES (1, 'one'), (2, 'two'), (3, 'three')")
		try db.sql("CREATE PROCEDURE items_from(IN low INT) SELECT id, name FROM Item WHERE id >= low ORDER BY id")
		try db.sql("CREATE PROCEDURE two_sets() BEGIN SELECT 1 AS id, 'a' AS name; SELECT 2 AS other; END")
		return fixture
	}

	@Test(.enabled(if: MySQLTestEnvironment.isFixtureEnabled))
	func callReturnsRowsAndKeepsTheConnectionInSync() throws {
		let fixture = try Self.procedureDatabase()
		let db = fixture.database
		let items = try db.sql("CALL items_from(?)", bindings: [("?", .integer(2))], Item.self)
		#expect(items == [Item(id: 2, name: "two"), Item(id: 3, name: "three")])
		// Used to fail with "Commands out of sync".
		#expect(try db.table(Item.self).count() == 3)

		let dynamic = try db.execute(DynamicSQL(sql: "CALL items_from(?)", bindings: [.int(3)]))
		#expect(dynamic.rows.map { $0["name"] } == [.string("three")])
		#expect(dynamic.insertedID == nil)
		#expect(try db.table(Item.self).count() == 3)
	}

	@Test(.enabled(if: MySQLTestEnvironment.isFixtureEnabled))
	func onlyTheFirstResultSetIsReadAndTheRestAreDiscarded() throws {
		let fixture = try Self.procedureDatabase()
		let db = fixture.database
		// A CALL whose result is only executed, not read as rows.
		try db.sql("CALL two_sets()")
		#expect(try db.table(Item.self).count() == 3)
		let items = try db.sql("CALL two_sets()", Item.self)
		#expect(items == [Item(id: 1, name: "a")])
		#expect(try db.table(Item.self).count() == 3)
		// The same prepared statement can run again.
		try db.transaction { () throws -> () in
			#expect(try db.sql("CALL two_sets()", Item.self).count == 1)
			#expect(try db.sql("CALL two_sets()", Item.self).count == 1)
		}
	}

	// A CALL that fails after returning a result set used to report success with the first
	// set's rows: the error came with a later result, which was discarded.
	@Test(.enabled(if: MySQLTestEnvironment.isFixtureEnabled), arguments: [
		("dup_key", "INSERT INTO Item VALUES (1, 'again')", "Duplicate entry"),
		("subquery", "SELECT (SELECT 1 UNION SELECT 2) AS x", "more than 1 row"),
		("signal", "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'procedure failed'", "procedure failed"),
	])
	func anErrorAfterTheFirstResultSetThrows(name: String, failing: String, message: String) throws {
		let fixture = try Self.procedureDatabase()
		let db = fixture.database
		try db.sql("CREATE PROCEDURE fails_\(name)() BEGIN SELECT 1 AS id, 'a' AS name; \(failing); END")
		let typed = #expect(throws: MySQLCRUDError.self) {
			_ = try db.sql("CALL fails_\(name)()", Item.self)
		}
		#expect(typed?.description.contains(message) == true, "\(String(describing: typed))")
		#expect(try db.table(Item.self).count() == 3)

		let dynamic = #expect(throws: MySQLCRUDError.self) {
			_ = try db.execute(DynamicSQL(sql: "CALL fails_\(name)()"))
		}
		#expect(dynamic?.description.contains(message) == true, "\(String(describing: dynamic))")
		#expect(try db.table(Item.self).count() == 3)

		// Untyped: Perfect-CRUD calls hasNext() once, so the error can't be reported, but the
		// connection must still be usable.
		try db.sql("CALL fails_\(name)()")
		#expect(try db.table(Item.self).count() == 3)
	}
}
