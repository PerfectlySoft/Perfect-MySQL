//
//  SSLModeTests.swift
//  PerfectMySQLTests
//
//  MYSQL_OPT_SSL_MODE must work with both client libraries. libmariadb has no such option, so
//  MySQL.setOption maps the modes onto MYSQL_OPT_SSL_ENFORCE / MYSQL_OPT_SSL_VERIFY_SERVER_CERT.
//
//  The server under test must have TLS enabled (MySQL 8.4 and MariaDB 11.4+ do by default, with a
//  self-signed certificate). Set MYSQL_TEST_NOTLS_PORT (and MYSQL_TEST_NOTLS_HOST if it differs) to
//  a server started without TLS (e.g. MariaDB with --skip-ssl) to also check that modes requiring
//  TLS refuse it.
//

import XCTest
import Foundation
@testable import PerfectMySQL

final class SSLModeTests: XCTestCase {
	var mysqlTests: Bool { ProcessInfo.processInfo.environment["MYSQL_TESTS"] == "1" }
	var noTLSPort: Int? { ProcessInfo.processInfo.environment["MYSQL_TEST_NOTLS_PORT"].flatMap(Int.init) }
	var noTLSHost: String { ProcessInfo.processInfo.environment["MYSQL_TEST_NOTLS_HOST"] ?? testHost }

	// The values of MySQL's enum mysql_ssl_mode (SSL_MODE_DISABLED ... SSL_MODE_VERIFY_IDENTITY).
	enum Mode: Int, CaseIterable {
		case disabled = 1, preferred, required, verifyCA, verifyIdentity
	}

	var unrelatedCA: String {
		get throws { try XCTUnwrap(Bundle.module.url(forResource: "unrelated-ca", withExtension: "pem")).path }
	}

	private func connect(_ mode: Mode, host: String = testHost, port: Int? = testPort, ca: String? = nil, reconnect: Bool = false) -> MySQL {
		let mysql = MySQL()
		mysql.setOption(.MYSQL_OPT_CONNECT_TIMEOUT, 5)
		if reconnect {
			XCTAssertTrue(mysql.setOption(.MYSQL_OPT_RECONNECT, true))
		}
		if let ca {
			XCTAssertTrue(mysql.setOption(.MYSQL_OPT_SSL_CA, ca))
		}
		XCTAssertTrue(mysql.setOption(.MYSQL_OPT_SSL_MODE, mode.rawValue), "setOption(MYSQL_OPT_SSL_MODE, \(mode)) failed: \(mysql.errorMessage())")
		_ = mysql.connect(host: host, user: testUser, password: testPassword, db: testAdminDB, port: UInt32(port ?? 0))
		return mysql
	}

	private func cipher(_ mysql: MySQL) -> String? {
		guard mysql.query(statement: "SHOW SESSION STATUS LIKE 'Ssl_cipher'"),
			  let results = mysql.storeResults(),
			  let row = results.next() else {
			XCTFail("Ssl_cipher query failed: \(mysql.errorMessage())")
			return nil
		}
		return row[1] ?? ""
	}

	func testEncryptedModesUseTLS() throws {
		guard mysqlTests else { return }
		for mode in [Mode.preferred, .required] {
			let mysql = connect(mode)
			XCTAssertEqual(mysql.errorCode(), 0, "\(mode): \(mysql.errorMessage())")
			XCTAssertNotEqual(cipher(mysql), "", "\(mode) connected without TLS")
		}
	}

	func testDisabledModeUsesPlaintext() throws {
		guard mysqlTests else { return }
		let mysql = connect(.disabled)
		XCTAssertEqual(mysql.errorCode(), 0, mysql.errorMessage())
		XCTAssertEqual(cipher(mysql), "")
	}

	func testVerifyingModesRejectAnUntrustedServer() throws {
		guard mysqlTests else { return }
		for mode in [Mode.verifyCA, .verifyIdentity] {
			let mysql = connect(mode, ca: try unrelatedCA)
			XCTAssertNotEqual(mysql.errorCode(), 0, "\(mode) accepted a certificate the CA didn't sign")
			XCTAssertFalse(mysql.ping())
		}
	}

	func testVerifyingModesRejectASelfSignedServerWithoutCA() throws {
		// Connector/C 3.4 deliberately skips verification on loopback connections without a CA.
		guard mysqlTests, !["127.0.0.1", "::1", "localhost"].contains(testHost) else { return }
		for mode in [Mode.verifyCA, .verifyIdentity] {
			let mysql = connect(mode)
			XCTAssertNotEqual(mysql.errorCode(), 0, "\(mode) accepted a self-signed certificate")
			XCTAssertFalse(mysql.ping())
		}
	}

	func testRequiredModeTurnsOffReconnectOnMariaDBConnector() throws {
		guard mysqlTests, MySQL.usesMariaDBConnector else { return }
		let mysql = connect(.required, reconnect: true)
		XCTAssertEqual(mysql.errorCode(), 0, mysql.errorMessage())
		XCTAssertTrue(mysql.query(statement: "SELECT CONNECTION_ID()"), mysql.errorMessage())
		let id = try XCTUnwrap(mysql.storeResults()?.next()?[0] ?? nil)
		let killer = rawMySQL
		XCTAssertTrue(killer.query(statement: "KILL \(id)"), killer.errorMessage())
		XCTAssertFalse(mysql.ping(), "reconnected; a reconnect isn't checked for TLS")
	}

	func testUnknownModeIsRejected() {
		let mysql = MySQL()
		for value in [0, 6, -1, Int(UInt32.max) + 3] {
			XCTAssertFalse(mysql.setOption(.MYSQL_OPT_SSL_MODE, value), "accepted SSL mode \(value)")
		}
	}

	func testModesRequiringTLSRefuseAPlaintextServer() throws {
		guard mysqlTests, let noTLSPort else { return }
		for mode in [Mode.required, .verifyCA, .verifyIdentity] {
			let mysql = connect(mode, host: noTLSHost, port: noTLSPort)
			XCTAssertEqual(mysql.errorCode(), 2026 /* CR_SSL_CONNECTION_ERROR */, "\(mode): \(mysql.errorMessage())")
			XCTAssertFalse(mysql.errorMessage().isEmpty)
			XCTAssertFalse(mysql.ping(), "\(mode) left a plaintext connection open")
		}
		// A retry on the same handle is refused too: a failed connect mustn't drop the SSL mode.
		for mode in [Mode.required, .verifyIdentity] {
			let mysql = connect(mode, host: noTLSHost, port: noTLSPort)
			XCTAssertEqual(mysql.errorCode(), 2026)
			XCTAssertFalse(mysql.connect(host: noTLSHost, user: testUser, password: testPassword, port: UInt32(noTLSPort)), "\(mode) retry connected")
			XCTAssertEqual(mysql.errorCode(), 2026, "\(mode) retry: \(mysql.errorMessage())")
			XCTAssertFalse(mysql.ping())
		}
		for mode in [Mode.disabled, .preferred] {
			let mysql = connect(mode, host: noTLSHost, port: noTLSPort)
			XCTAssertEqual(mysql.errorCode(), 0, "\(mode): \(mysql.errorMessage())")
			XCTAssertEqual(cipher(mysql), "")
		}
	}
}
