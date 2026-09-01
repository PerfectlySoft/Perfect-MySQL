import Foundation
import PerfectCRUD
import Testing
@testable import PerfectMySQL

private struct FixtureCount: Codable {
	let count: Int

	enum CodingKeys: String, CodingKey {
		case count = "count"
	}
}

struct MySQLFixtureConfig {
	let host: String
	let port: Int?
	let adminDatabase: String
	let username: String
	let password: String
	let schema: String

	static func fromEnvironment(schemaSuffix: String = UUID().uuidString.replacingOccurrences(of: "-", with: "_")) -> Self {
		let env = ProcessInfo.processInfo.environment
		return MySQLFixtureConfig(
			host: env["MYSQL_TEST_HOST"] ?? testHost,
			port: env["MYSQL_TEST_PORT"].flatMap(Int.init) ?? testPort,
			adminDatabase: env["MYSQL_TEST_ADMIN_DATABASE"] ?? testAdminDB,
			username: env["MYSQL_TEST_USER"] ?? testUser,
			password: env["MYSQL_TEST_PASSWORD"] ?? testPassword,
			schema: "\(env["MYSQL_TEST_DATABASE"] ?? "perfect_mysql_fixture")_\(schemaSuffix)"
		)
	}
}

final class CatalogFixtureDatabase {
	let config: MySQLFixtureConfig
	let database: Database<MySQLDatabaseConfiguration>

	init(config: MySQLFixtureConfig = .fromEnvironment()) throws {
		self.config = config
		let admin = try Database(configuration: MySQLDatabaseConfiguration(
			database: config.adminDatabase,
			host: config.host,
			port: config.port,
			username: config.username,
			password: config.password
		))
		try admin.sql("DROP DATABASE IF EXISTS `\(config.schema)`")
		try admin.sql("CREATE DATABASE `\(config.schema)` DEFAULT CHARACTER SET utf8mb4")

		database = try Database(configuration: MySQLDatabaseConfiguration(
			database: config.schema,
			host: config.host,
			port: config.port,
			username: config.username,
			password: config.password
		))
		try loadDump(into: database)
	}

	deinit {
		do {
			let admin = try Database(configuration: MySQLDatabaseConfiguration(
				database: config.adminDatabase,
				host: config.host,
				port: config.port,
				username: config.username,
				password: config.password
			))
			try admin.sql("DROP DATABASE IF EXISTS `\(config.schema)`")
		} catch {
			Issue.record("Could not drop fixture schema \(config.schema): \(error)")
		}
	}

	private func loadDump(into database: Database<MySQLDatabaseConfiguration>) throws {
		let fixtureURL = try #require(Bundle.module.url(
			forResource: "sample_catalog_cart",
			withExtension: "sql"
		))
		let sql = try String(contentsOf: fixtureURL, encoding: .utf8)
		for statement in sql.fixtureStatements {
			try database.sql(statement)
		}
	}
}

private extension String {
	var fixtureStatements: [String] {
		split(separator: "\n")
			.map(String.init)
			.filter { line in
				let trimmed = line.trimmingCharacters(in: .whitespaces)
				return trimmed.isEmpty == false && trimmed.hasPrefix("--") == false
			}
			.joined(separator: "\n")
			.split(separator: ";")
			.map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
			.filter { $0.isEmpty == false }
	}
}

struct GenericCatalogFixtureTests {
	@Test(.enabled(if: ProcessInfo.processInfo.environment["MYSQL_FIXTURE_TESTS"] == "1"))
	func loadsGenericCatalogCartFixtureAndReadsDynamicRows() throws {
		let fixture = try CatalogFixtureDatabase()
		let database = fixture.database

		let productCount = try database.sql(
			"SELECT COUNT(*) AS `count` FROM catalog_products",
			FixtureCount.self
		)
		let variantCount = try database.sql(
			"SELECT COUNT(*) AS `count` FROM catalog_variants",
			FixtureCount.self
		)
		let cartItemCount = try database.sql(
			"SELECT COUNT(*) AS `count` FROM catalog_cart_items",
			FixtureCount.self
		)
		#expect(try #require(productCount.first).count == 120)
		#expect(try #require(variantCount.first).count == 360)
		#expect(try #require(cartItemCount.first).count == 240)

		let result = try database.select(DynamicQuery(
			table: "catalog_products",
			fields: ["id", "sku", "name", "active"],
			predicates: [
				DynamicPredicate(field: "active", comparison: .equal, value: .int(1)),
				DynamicPredicate(field: "price", comparison: .greaterThan, value: .double(100)),
			],
			orderings: [
				DynamicOrdering(field: "price", descending: true),
			],
			limit: 5
		))

		#expect(result.rows.count == 5)
		let firstProduct = try #require(result.rows.first)
		#expect(firstProduct["id"] == DynamicValue.int(120))
		#expect(firstProduct["sku"] == DynamicValue.string("PROD-0120"))
		#expect(firstProduct["active"] == DynamicValue.int(1))
	}
}
