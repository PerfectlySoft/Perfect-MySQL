import Foundation
import Testing
import PerfectCRUD
@testable import PerfectMySQL

// MARK: - ADR-0001 Phase 4: live FOREIGN KEY behavior
//
// ForeignKeyDDLTests.swift (in this same target) proves PerfectCRUD emits
// the right SQL. This proves a real MySQL server actually HONORS it --
// ON DELETE CASCADE really removes the child row, not just that the DDL
// text contains the words "ON DELETE CASCADE". Uses the same disposable-
// schema fixture pattern as GenericCatalogFixtureTests.swift
// (MySQLFixtureConfig.fromEnvironment(), a fresh perfect_mysql_fixture_*
// schema per run, dropped on teardown), gated the same way behind
// MYSQL_FIXTURE_TESTS=1 with a create/drop-capable non-root account.

private struct FKLiveParent: Codable {
	var id: Int
	var name: String
}

private struct FKLiveChild: Codable {
	var id: Int
	@ForeignKey(FKLiveParent.self, onDelete: cascade, onUpdate: restrict)
	var parentId: Int
}

private final class ForeignKeyFixtureDatabase {
	let config: MySQLFixtureConfig
	let database: Database<MySQLDatabaseConfiguration>

	init(config: MySQLFixtureConfig = .fromEnvironment()) throws {
		self.config = config
		let admin = try Database(configuration: MySQLDatabaseConfiguration(
			database: config.adminDatabase, host: config.host, port: config.port,
			username: config.username, password: config.password
		))
		try admin.sql("DROP DATABASE IF EXISTS `\(config.schema)`")
		try admin.sql("CREATE DATABASE `\(config.schema)` DEFAULT CHARACTER SET utf8mb4")

		database = try Database(configuration: MySQLDatabaseConfiguration(
			database: config.schema, host: config.host, port: config.port,
			username: config.username, password: config.password
		))
		// Parent must exist before the child's FOREIGN KEY constraint can
		// reference it.
		try database.create(FKLiveParent.self, primaryKey: \FKLiveParent.id, policy: .shallow)
		try database.create(FKLiveChild.self, primaryKey: \FKLiveChild.id, policy: .shallow)
	}

	deinit {
		do {
			let admin = try Database(configuration: MySQLDatabaseConfiguration(
				database: config.adminDatabase, host: config.host, port: config.port,
				username: config.username, password: config.password
			))
			try admin.sql("DROP DATABASE IF EXISTS `\(config.schema)`")
		} catch {
			Issue.record("Could not drop fixture schema \(config.schema): \(error)")
		}
	}
}

struct ForeignKeyLiveIntegrationTests {

	@Test(.enabled(if: ProcessInfo.processInfo.environment["MYSQL_FIXTURE_TESTS"] == "1"))
	func onDeleteCascadeActuallyRemovesTheChildRow() throws {
		let fixture = try ForeignKeyFixtureDatabase()
		let db = fixture.database

		try db.table(FKLiveParent.self).insert(FKLiveParent(id: 1, name: "Acme"))
		try db.table(FKLiveChild.self).insert(FKLiveChild(id: 1, parentId: ForeignKey(FKLiveParent.self, onDelete: cascade, onUpdate: restrict, wrappedValue: 1)))

		let beforeDelete = try db.table(FKLiveChild.self).where(\FKLiveChild.id == 1).select().map { $0 }
		#expect(beforeDelete.count == 1)

		try db.table(FKLiveParent.self).where(\FKLiveParent.id == 1).delete()

		// The real assertion: MySQL's own InnoDB engine removed the child
		// row as a side effect of the parent delete, via ON DELETE CASCADE
		// -- PerfectCRUD never issued a DELETE against catalog_child itself.
		let afterDelete = try db.table(FKLiveChild.self).where(\FKLiveChild.id == 1).select().map { $0 }
		#expect(afterDelete.isEmpty)

		let parentGone = try db.table(FKLiveParent.self).where(\FKLiveParent.id == 1).select().map { $0 }
		#expect(parentGone.isEmpty)
	}

	@Test(.enabled(if: ProcessInfo.processInfo.environment["MYSQL_FIXTURE_TESTS"] == "1"))
	func insertingAChildWithAnUnknownParentIsRejected() throws {
		let fixture = try ForeignKeyFixtureDatabase()
		let db = fixture.database

		// No parent row with id 999 exists -- the FOREIGN KEY constraint
		// itself (not application code) must reject this insert.
		#expect(throws: (any Error).self) {
			try db.table(FKLiveChild.self).insert(FKLiveChild(id: 1, parentId: ForeignKey(FKLiveParent.self, onDelete: cascade, onUpdate: restrict, wrappedValue: 999)))
		}
	}
}
