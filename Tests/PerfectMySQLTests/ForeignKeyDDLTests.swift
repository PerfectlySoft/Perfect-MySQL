import Testing
@testable import PerfectMySQL
@testable import PerfectCRUD

// MARK: - ADR-0001 Phase 4: @ForeignKey DDL generation
//
// No live server needed: MySQLGenDelegate's DDL generation is pure string
// assembly over a MySQL() connection handle that's never actually
// connected (MySQL() just wraps mysql_init(), a local C call). Real
// connect/query behavior is exercised elsewhere in this suite behind
// MYSQL_TESTS gating; this only proves the SQL PerfectCRUD would send.

private struct FKParentModel: Codable {
	var id: Int
	var name: String
}

private struct FKChildModel: Codable {
	var id: Int
	@ForeignKey(FKParentModel.self, onDelete: cascade, onUpdate: restrict)
	var parentId: Int
}

@Suite("MySQLGenDelegate FOREIGN KEY DDL")
struct ForeignKeyDDLTests {

	@Test("a @ForeignKey column produces a real FOREIGN KEY clause, not silence")
	func foreignKeyProducesConstraint() throws {
		let delegate = MySQLGenDelegate(connection: MySQL())
		let structure = try FKChildModel.CRUDTableStructure()

		let statements = try delegate.getCreateTableSQL(forTable: structure, policy: .shallow)
		let sql = statements.joined(separator: "\n")

		// This is the actual bug ADR-0001 Phase 4 fixes: before this fix,
		// getColumnDefinition only ever branched on .primaryKey, so a
		// @ForeignKey column compiled and ran without error but silently
		// produced no constraint at all on MySQL/MariaDB.
		#expect(sql.contains("FOREIGN KEY"))
		#expect(sql.contains("REFERENCES"))
		#expect(sql.contains("`FKParentModel`"))
		#expect(sql.contains("ON DELETE CASCADE"))
		#expect(sql.contains("ON UPDATE RESTRICT"))
	}

	@Test("a plain primary-key-only column is unaffected by the fix")
	func primaryKeyStillWorksUnaffected() throws {
		let delegate = MySQLGenDelegate(connection: MySQL())
		let structure = try FKParentModel.CRUDTableStructure(primaryKey: \FKParentModel.id)

		let statements = try delegate.getCreateTableSQL(forTable: structure, policy: .shallow)
		let sql = statements.joined(separator: "\n")

		#expect(sql.contains("PRIMARY KEY"))
		#expect(!sql.contains("FOREIGN KEY"))
	}
}
