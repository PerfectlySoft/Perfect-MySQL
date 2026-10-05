//
//  MySQLTestEnvironment.swift
//  PerfectMySQLTests
//
//  Live-server settings shared by every test that connects to MySQL:
//
//    MYSQL_TESTS=1                 enable the XCTest integration tests and the live Swift Testing suites
//    MYSQL_FIXTURE_TESTS=1         enable the disposable-schema fixture tests
//    MYSQL_TEST_PORT               required, 1...65535 -- there is no default
//    MYSQL_TEST_HOST               default 127.0.0.1 (host.docker.internal on Linux); "localhost" and "" are refused
//    MYSQL_TEST_USER               default root
//    MYSQL_TEST_PASSWORD           default empty
//    MYSQL_TEST_DATABASE           default test; for the fixture tests, a schema prefix (default perfect_mysql_fixture)
//    MYSQL_TEST_ADMIN_DATABASE     default mysql
//    MYSQL_TEST_NOTLS_PORT/HOST    optional second server without TLS, for SSLModeTests
//
//  Live tests are skipped unless their gate is set, MYSQL_TEST_PORT is a valid
//  port and the host isn't "localhost" or empty. These tests drop and recreate
//  databases. Falling back to the client's default port (3306, which libmysql
//  also uses for port 0) would point them at whatever MySQL server happens to
//  be running locally, and libmysql reaches "localhost" (and an empty host)
//  through the Unix socket, ignoring the port entirely. As a backstop, every
//  connection is also forced to use TCP.
//

import Foundation
import XCTest
@testable import PerfectMySQL

enum MySQLTestEnvironment {
	private static let env = ProcessInfo.processInfo.environment

#if os(macOS)
	static let defaultHost = "127.0.0.1"
#else
	static let defaultHost = "host.docker.internal"
#endif
	static let host = env["MYSQL_TEST_HOST"] ?? defaultHost
	static let port = validPort(env["MYSQL_TEST_PORT"])
	static let user = env["MYSQL_TEST_USER"] ?? "root"
	static let password = env["MYSQL_TEST_PASSWORD"] ?? ""
	static let database = env["MYSQL_TEST_DATABASE"] ?? "test"
	static let adminDatabase = env["MYSQL_TEST_ADMIN_DATABASE"] ?? "mysql"

	/// A server without TLS, for the SSL mode tests. Not checked unless MYSQL_TEST_NOTLS_PORT is set.
	static let noTLSHost = env["MYSQL_TEST_NOTLS_HOST"] ?? host
	static let noTLSPort = validPort(env["MYSQL_TEST_NOTLS_PORT"])
	static let hasNoTLSServer = env["MYSQL_TEST_NOTLS_PORT"] != nil

	private static func validPort(_ value: String?) -> Int? {
		value.flatMap { Int($0) }.flatMap { (1...65535).contains($0) ? $0 : nil }
	}

	/// Why `host` and `port` can't be used, or nil when they can.
	private static func serverProblem(host: String, port: Int?, portVariable: String) -> String? {
		if port == nil { return "set \(portVariable) to a test server's port, 1...65535 (3306 is never assumed)" }
		let trimmed = host.trimmingCharacters(in: .whitespaces).lowercased()
		if trimmed.isEmpty || trimmed == "localhost" {
			return "a host of localhost or an empty host uses the Unix socket and ignores the port; use 127.0.0.1"
		}
		return nil
	}

	private static var serverProblem: String? {
		serverProblem(host: host, port: port, portVariable: "MYSQL_TEST_PORT")
	}

	/// Why the MYSQL_TESTS tests are skipped, or nil when they are enabled.
	static var skipReason: String? {
		if env["MYSQL_TESTS"] != "1" { return "set MYSQL_TESTS=1 to enable live MySQL tests" }
		return serverProblem
	}

	/// Why the MYSQL_FIXTURE_TESTS tests are skipped, or nil when they are enabled.
	static var fixtureSkipReason: String? {
		if env["MYSQL_FIXTURE_TESTS"] != "1" { return "set MYSQL_FIXTURE_TESTS=1 to enable live MySQL fixture tests" }
		return serverProblem
	}

	/// Why the plaintext-server SSL tests are skipped, or nil when they are enabled.
	static var noTLSSkipReason: String? {
		if let reason = skipReason { return reason }
		if !hasNoTLSServer { return "set MYSQL_TEST_NOTLS_PORT to a server without TLS" }
		return serverProblem(host: noTLSHost, port: noTLSPort, portVariable: "MYSQL_TEST_NOTLS_PORT")
	}

	/// True only when MYSQL_TESTS=1, MYSQL_TEST_PORT is a valid port and the host isn't "localhost".
	static var isEnabled: Bool { skipReason == nil }

	/// True only when MYSQL_FIXTURE_TESTS=1, MYSQL_TEST_PORT is a valid port and the host isn't "localhost".
	static var isFixtureEnabled: Bool { fixtureSkipReason == nil }

	/// For XCTest: skips (rather than passes) the test unless live tests are enabled.
	static func skipUnlessEnabled() throws {
		if let reason = skipReason { throw XCTSkip(reason) }
	}

	private static func requireUsable(host: String, port: Int?) {
		precondition(isEnabled || isFixtureEnabled, "live MySQL test ran while disabled: \(skipReason ?? "")")
		precondition(serverProblem(host: host, port: port, portVariable: "port") == nil,
					 "live MySQL test would connect to \(host):\(port.map(String.init) ?? "default port")")
	}

	/// A connection to `database` on the configured test server.
	/// Call only when enabled; otherwise this could reach a server on 3306.
	static func configuration(database: String = database) throws -> MySQLDatabaseConfiguration {
		let mysql = MySQL()
		_ = mysql.setOption(.MYSQL_SET_CHARSET_NAME, "utf8mb4")
		guard connect(mysql, database: database) else {
			throw MySQLCRUDError("Could not connect. \(mysql.errorMessage())")
		}
		return MySQLDatabaseConfiguration(connection: mysql)
	}

	/// Connects `mysql` over TCP to the test server (or the given one). Call only when enabled.
	@discardableResult
	static func connect(_ mysql: MySQL, host: String = host, port: Int? = port, database: String? = nil, flag: UInt = 0) -> Bool {
		requireUsable(host: host, port: port)
		// MYSQL_PROTOCOL_TCP: never the Unix socket, whatever the host string.
		precondition(mysql.setOption(.MYSQL_OPT_PROTOCOL, 1), "couldn't force TCP: \(mysql.errorMessage())")
		return mysql.connect(host: host, user: user, password: password, db: database,
							 port: UInt32(port!), flag: flag)
	}
}

let testHost = MySQLTestEnvironment.host
let testAdminDB = MySQLTestEnvironment.adminDatabase
let testUser = MySQLTestEnvironment.user
let testPassword = MySQLTestEnvironment.password
let testDB = MySQLTestEnvironment.database
