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
	// The values of MySQL's enum mysql_ssl_mode (SSL_MODE_DISABLED ... SSL_MODE_VERIFY_IDENTITY).
	enum Mode: Int, CaseIterable {
		case disabled = 1, preferred, required, verifyCA, verifyIdentity
	}

	var unrelatedCA: String {
		get throws { try XCTUnwrap(Bundle.module.url(forResource: "unrelated-ca", withExtension: "pem")).path }
	}

	private func connect(_ mode: Mode, host: String = testHost, port: Int? = MySQLTestEnvironment.port, ca: String? = nil, reconnect: Bool = false) -> MySQL {
		let mysql = MySQL()
		mysql.setOption(.MYSQL_OPT_CONNECT_TIMEOUT, 5)
		if reconnect {
			XCTAssertTrue(mysql.setOption(.MYSQL_OPT_RECONNECT, true))
		}
		if let ca {
			XCTAssertTrue(mysql.setOption(.MYSQL_OPT_SSL_CA, ca))
		}
		XCTAssertTrue(mysql.setOption(.MYSQL_OPT_SSL_MODE, mode.rawValue), "setOption(MYSQL_OPT_SSL_MODE, \(mode)) failed: \(mysql.errorMessage())")
		MySQLTestEnvironment.connect(mysql, host: host, port: port, database: testAdminDB)
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
		try MySQLTestEnvironment.skipUnlessEnabled()
		for mode in [Mode.preferred, .required] {
			let mysql = connect(mode)
			XCTAssertEqual(mysql.errorCode(), 0, "\(mode): \(mysql.errorMessage())")
			XCTAssertNotEqual(cipher(mysql), "", "\(mode) connected without TLS")
		}
	}

	func testDisabledModeUsesPlaintext() throws {
		try MySQLTestEnvironment.skipUnlessEnabled()
		let mysql = connect(.disabled)
		XCTAssertEqual(mysql.errorCode(), 0, mysql.errorMessage())
		XCTAssertEqual(cipher(mysql), "")
	}

	func testVerifyingModesRejectAnUntrustedServer() throws {
		try MySQLTestEnvironment.skipUnlessEnabled()
		for mode in [Mode.verifyCA, .verifyIdentity] {
			let mysql = connect(mode, ca: try unrelatedCA)
			XCTAssertNotEqual(mysql.errorCode(), 0, "\(mode) accepted a certificate the CA didn't sign")
			XCTAssertFalse(mysql.ping())
		}
	}

	func testVerifyingModesRejectASelfSignedServerWithoutCA() throws {
		// Connector/C 3.4 deliberately skips verification on loopback connections without a CA.
		try MySQLTestEnvironment.skipUnlessEnabled()
		guard !["127.0.0.1", "::1", "localhost"].contains(testHost) else { throw XCTSkip("needs a non-loopback MYSQL_TEST_HOST") }
		for mode in [Mode.verifyCA, .verifyIdentity] {
			let mysql = connect(mode)
			XCTAssertNotEqual(mysql.errorCode(), 0, "\(mode) accepted a self-signed certificate")
			XCTAssertFalse(mysql.ping())
		}
	}

	func testRequiredModeTurnsOffReconnectOnMariaDBConnector() throws {
		try MySQLTestEnvironment.skipUnlessEnabled()
		guard MySQL.usesMariaDBConnector else { throw XCTSkip("only applies to MariaDB Connector/C") }
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
		if let reason = MySQLTestEnvironment.noTLSSkipReason { throw XCTSkip(reason) }
		let noTLSHost = MySQLTestEnvironment.noTLSHost, noTLSPort = MySQLTestEnvironment.noTLSPort
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
			XCTAssertFalse(MySQLTestEnvironment.connect(mysql, host: noTLSHost, port: noTLSPort), "\(mode) retry connected")
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
