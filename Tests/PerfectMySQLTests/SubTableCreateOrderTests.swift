import Foundation
import Testing
@testable import PerfectMySQL
@testable import PerfectCRUD

// MARK: - create() statement order for sub-tables
//
// `create(Parent.self)` also creates the tables of Parent's `[Child]` properties. A child
// with a @ForeignKey back to the parent can only be created once the parent exists, and a
// parent can only be dropped (`.dropTable`) once nothing references it. MySQL used to
// emit the sub-tables' statements first, so MySQL 8.4 failed with "Failed to open the
// referenced table" and MariaDB 11.8 with errno 150.

private struct OrderParent: Codable {
	let id: Int
	var name: String
	var children: [OrderChild]?
}

private struct OrderChild: Codable {
	let id: Int
	@ForeignKey(OrderParent.self, onDelete: setNull, onUpdate: restrict)
	var parentId: Int?
}

// The reverse dependency: the parent references a sub-table type that doesn't reference
// it back, so that sub-table has to be created first.
private struct OrderOwner: Codable {
	let id: Int
	@ForeignKey(OrderPet.self, onDelete: setNull, onUpdate: restrict)
	var favoritePetId: Int?
	var pets: [OrderPet]?
}

private struct OrderPet: Codable {
	let id: Int
	var ownerId: Int
}

private func statementIndex(_ statements: [String], _ prefix: String) -> Int? {
	statements.firstIndex { $0.hasPrefix(prefix) }
}

@Suite("MySQLGenDelegate sub-table create order")
struct SubTableCreateOrderDDLTests {

	@Test("a sub-table that references its parent is created after the parent")
	func childReferencingParentIsCreatedAfterIt() throws {
		let delegate = MySQLGenDelegate(connection: MySQL())
		let statements = try delegate.getCreateTableSQL(forTable: try OrderParent.CRUDTableStructure(), policy: [])
		let parent = try #require(statementIndex(statements, "CREATE TABLE IF NOT EXISTS `OrderParent`"))
		let child = try #require(statementIndex(statements, "CREATE TABLE IF NOT EXISTS `OrderChild`"))
		#expect(parent < child)
		// The child's FOREIGN KEY clause belongs to the child's table only.
		#expect(!statements[parent].contains("FOREIGN KEY"))
		#expect(statements[child].contains("FOREIGN KEY (`parentId`) REFERENCES `OrderParent`(`id`)"))
	}

	@Test("with .dropTable, sub-tables are dropped before the parent and created after it")
	func dropTableOrder() throws {
		let delegate = MySQLGenDelegate(connection: MySQL())
		let statements = try delegate.getCreateTableSQL(forTable: try OrderParent.CRUDTableStructure(), policy: .dropTable)
		#expect(statements.map { $0.split(separator: "(").first.map(String.init) ?? "" } == [
			"DROP TABLE IF EXISTS `OrderChild`",
			"DROP TABLE IF EXISTS `OrderParent`",
			"CREATE TABLE IF NOT EXISTS `OrderParent` ",
			"CREATE TABLE IF NOT EXISTS `OrderChild` ",
		])
	}

	@Test("a parent that references a sub-table is created after that sub-table")
	func parentReferencingSubTableIsCreatedAfterIt() throws {
		let delegate = MySQLGenDelegate(connection: MySQL())
		let statements = try delegate.getCreateTableSQL(forTable: try OrderOwner.CRUDTableStructure(), policy: .dropTable)
		#expect(statements.map { $0.split(separator: "(").first.map(String.init) ?? "" } == [
			"DROP TABLE IF EXISTS `OrderOwner`",
			"DROP TABLE IF EXISTS `OrderPet`",
			"CREATE TABLE IF NOT EXISTS `OrderPet` ",
			"CREATE TABLE IF NOT EXISTS `OrderOwner` ",
		])
		let owner = try #require(statementIndex(statements, "CREATE TABLE IF NOT EXISTS `OrderOwner`"))
		let pet = try #require(statementIndex(statements, "CREATE TABLE IF NOT EXISTS `OrderPet`"))
		#expect(statements[owner].contains("REFERENCES `OrderPet`(`id`)"))
		#expect(!statements[pet].contains("FOREIGN KEY"))
	}

	@Test("a backtick in an identifier is doubled")
	func quoteEscapesBackticks() throws {
		let delegate = MySQLGenDelegate(connection: MySQL())
		#expect(try delegate.quote(identifier: "plain") == "`plain`")
		#expect(try delegate.quote(identifier: "a`b") == "`a``b`")
		#expect(try delegate.quote(identifier: "x` FROM t; --") == "`x`` FROM t; --`")
	}

	@Test(".shallow creates only the table itself")
	func shallowSkipsSubTables() throws {
		let delegate = MySQLGenDelegate(connection: MySQL())
		let statements = try delegate.getCreateTableSQL(forTable: try OrderParent.CRUDTableStructure(), policy: [.shallow, .dropTable])
		#expect(statements.count == 2)
		#expect(statements[0] == "DROP TABLE IF EXISTS `OrderParent`")
		#expect(statements[1].hasPrefix("CREATE TABLE IF NOT EXISTS `OrderParent`"))
	}
}

@Suite("create() with sub-tables on a live server", .serialized)
struct SubTableCreateOrderLiveTests {
	static let schema = "perfect_subtable_order_test"

	private func freshDatabase() throws -> Database<MySQLDatabaseConfiguration> {
		let admin = Database(configuration: try MySQLTestEnvironment.configuration(database: testAdminDB))
		try admin.sql("DROP DATABASE IF EXISTS `\(Self.schema)`")
		try admin.sql("CREATE DATABASE `\(Self.schema)` DEFAULT CHARACTER SET utf8mb4")
		return Database(configuration: try MySQLTestEnvironment.configuration(database: Self.schema))
	}

	private func dropSchema() throws {
		let admin = Database(configuration: try MySQLTestEnvironment.configuration(database: testAdminDB))
		try admin.sql("DROP DATABASE IF EXISTS `\(Self.schema)`")
	}

	@Test(.enabled(if: MySQLTestEnvironment.isEnabled))
	func createParentWithReferencingChildren() throws {
		let db = try freshDatabase()
		defer { try? dropSchema() }

		try db.create(OrderParent.self)
		try db.table(OrderParent.self).insert(OrderParent(id: 1, name: "a", children: nil))
		try db.table(OrderChild.self).insert(OrderChild(id: 1, parentId: ForeignKey(OrderParent.self, onDelete: setNull, onUpdate: restrict, wrappedValue: 1)))

		// The constraint is real: deleting the parent nulls the child's key.
		try db.table(OrderParent.self).where(\OrderParent.id == 1).delete()
		let child = try #require(try db.table(OrderChild.self).where(\OrderChild.id == 1).first())
		#expect(child.parentId == nil)

		// Creating again over the existing tables, then reconciling them, works too.
		try db.create(OrderParent.self)
		try db.create(OrderParent.self, policy: .reconcileTable)
		#expect(try db.table(OrderChild.self).count() == 1)

		// Dropping and recreating has to drop the child before the parent it references.
		try db.table(OrderParent.self).insert(OrderParent(id: 2, name: "b", children: nil))
		try db.table(OrderChild.self).insert(OrderChild(id: 2, parentId: ForeignKey(OrderParent.self, onDelete: setNull, onUpdate: restrict, wrappedValue: 2)))
		try db.create(OrderParent.self, policy: .dropTable)
		#expect(try db.table(OrderParent.self).count() == 0)
		#expect(try db.table(OrderChild.self).count() == 0)
	}

	@Test(.enabled(if: MySQLTestEnvironment.isEnabled))
	func createParentReferencingASubTable() throws {
		let db = try freshDatabase()
		defer { try? dropSchema() }

		try db.create(OrderOwner.self)
		try db.table(OrderPet.self).insert(OrderPet(id: 1, ownerId: 1))
		try db.table(OrderOwner.self).insert(OrderOwner(id: 1, favoritePetId: ForeignKey(OrderPet.self, onDelete: setNull, onUpdate: restrict, wrappedValue: 1), pets: nil))
		try db.create(OrderOwner.self, policy: .dropTable)
		#expect(try db.table(OrderOwner.self).count() == 0)
	}

	@Test(.enabled(if: MySQLTestEnvironment.isEnabled))
	func reconcileAddsForeignKeyConstraint() throws {
		let db = try freshDatabase()
		defer { try? dropSchema() }

		// OrderChild's table as it was before `parentId` was added to the model.
		try db.create(OrderParent.self, policy: .shallow)
		try db.sql("CREATE TABLE `OrderChild` (`id` bigint PRIMARY KEY)")
		try db.create(OrderParent.self, policy: .reconcileTable)

		// The added column has its FOREIGN KEY constraint: an unknown parent is rejected.
		#expect(throws: (any Error).self) {
			try db.table(OrderChild.self).insert(OrderChild(id: 1, parentId: ForeignKey(OrderParent.self, onDelete: setNull, onUpdate: restrict, wrappedValue: 99)))
		}
		try db.table(OrderParent.self).insert(OrderParent(id: 1, name: "a", children: nil))
		try db.table(OrderChild.self).insert(OrderChild(id: 1, parentId: ForeignKey(OrderParent.self, onDelete: setNull, onUpdate: restrict, wrappedValue: 1)))
		try db.table(OrderParent.self).where(\OrderParent.id == 1).delete()
		let child = try #require(try db.table(OrderChild.self).where(\OrderChild.id == 1).first())
		#expect(child.parentId == nil)
	}

	@Test(.enabled(if: MySQLTestEnvironment.isEnabled))
	func reconcileDropsARemovedForeignKeyColumn() throws {
		let db = try freshDatabase()
		defer { try? dropSchema() }

		try db.create(OrderParent.self, policy: .shallow)
		try db.create(OrderPet.self, policy: .shallow)
		try db.create(LinkedRowV1.self)
		try db.table(OrderParent.self).insert(OrderParent(id: 1, name: "a", children: nil))
		try db.table(OrderPet.self).insert(OrderPet(id: 1, ownerId: 1))
		try db.sql("INSERT INTO `LinkedRow` (`id`, `parentId`, `petId`) VALUES (1, 1, 1)")

		// The model no longer has `parentId`: the column goes, with its constraint.
		try db.create(LinkedRowV2.self, policy: .reconcileTable)
		let columns = try db.sql("SHOW COLUMNS FROM `LinkedRow`", ColumnName.self).map(\.Field)
		#expect(columns == ["id", "petId"])
		let row = try #require(try db.table(LinkedRowV2.self).where(\LinkedRowV2.id == 1).first())
		#expect(row.petId == 1)
		// The parent row is no longer referenced, so it can be deleted.
		try db.table(OrderParent.self).where(\OrderParent.id == 1).delete()
		// `petId` keeps its constraint: an unknown pet is still rejected.
		#expect(throws: (any Error).self) {
			try db.sql("INSERT INTO `LinkedRow` (`id`, `petId`) VALUES (2, 99)")
		}
	}

	@Test(.enabled(if: MySQLTestEnvironment.isEnabled))
	func reconcileDropsColumnsSharingAForeignKey() throws {
		let db = try freshDatabase()
		defer { try? dropSchema() }

		// A hand-made two-column constraint (CRUD only makes single-column ones), both of whose
		// columns the model no longer has: the constraint must be dropped exactly once.
		try db.sql("CREATE TABLE `PairTarget` (`a` bigint, `b` bigint, PRIMARY KEY (`a`, `b`))")
		try db.sql("""
			CREATE TABLE `PairRow` (`id` bigint PRIMARY KEY, `pa` bigint, `pb` bigint,
			CONSTRAINT `pair_fk` FOREIGN KEY (`pa`, `pb`) REFERENCES `PairTarget` (`a`, `b`))
			""")
		try db.sql("INSERT INTO `PairTarget` VALUES (1, 2)")
		try db.sql("INSERT INTO `PairRow` VALUES (1, 1, 2)")
		try db.create(PairRow.self, policy: .reconcileTable)
		let columns = try db.sql("SHOW COLUMNS FROM `PairRow`", ColumnName.self).map(\.Field)
		#expect(columns == ["id"])
		#expect(try db.table(PairRow.self).count() == 1)
	}

	@Test(.enabled(if: MySQLTestEnvironment.isEnabled))
	func reconcileKeepsMixedCaseColumns() throws {
		let db = try freshDatabase()
		defer { try? dropSchema() }

		try db.create(ReconcileRow.self)
		try db.table(ReconcileRow.self).insert(ReconcileRow(id: 1, camelName: "kept"))
		try db.sql("ALTER TABLE `ReconcileRow` ADD COLUMN `OldColumn` bigint")
		try db.create(ReconcileRow.self, policy: .reconcileTable)

		let row = try #require(try db.table(ReconcileRow.self).where(\ReconcileRow.id == 1).first())
		#expect(row.camelName == "kept")
		// A column the model no longer has is still dropped, whatever its case.
		let columns = try db.sql("SHOW COLUMNS FROM `ReconcileRow`", ColumnName.self).map(\.Field)
		#expect(columns == ["id", "camelName"])
	}
}

private struct ReconcileRow: Codable {
	let id: Int
	var camelName: String
}

private struct ColumnName: Codable {
	let Field: String
}

// Two versions of one table: V2 drops the `parentId` foreign key column.
private struct LinkedRowV1: Codable, TableNameProvider {
	static let tableName = "LinkedRow"
	let id: Int
	@ForeignKey(OrderParent.self, onDelete: restrict, onUpdate: restrict)
	var parentId: Int?
	@ForeignKey(OrderPet.self, onDelete: restrict, onUpdate: restrict)
	var petId: Int?
}

private struct LinkedRowV2: Codable, TableNameProvider {
	static let tableName = "LinkedRow"
	let id: Int
	@ForeignKey(OrderPet.self, onDelete: restrict, onUpdate: restrict)
	var petId: Int?
}

private struct PairRow: Codable {
	let id: Int
}
