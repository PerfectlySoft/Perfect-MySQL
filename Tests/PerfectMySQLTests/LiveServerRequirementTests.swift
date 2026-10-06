import Foundation
import Testing
@testable import PerfectMySQL

/// Live tests skip when they have no server, so a CI misconfiguration would pass silently.
/// With MYSQL_REQUIRE_LIVE=1 (set in CI wherever a server runs) this fails instead.
struct LiveServerRequirementTests {
	static let required = ProcessInfo.processInfo.environment["MYSQL_REQUIRE_LIVE"] == "1"

	@Test(.enabled(if: required))
	func liveServerIsReachableWhenRequired() throws {
		#expect(MySQLTestEnvironment.skipReason == nil, "\(MySQLTestEnvironment.skipReason ?? "")")
		#expect(MySQLTestEnvironment.fixtureSkipReason == nil, "\(MySQLTestEnvironment.fixtureSkipReason ?? "")")
		try #require(MySQLTestEnvironment.isEnabled)
		let mysql = MySQL()
		#expect(MySQLTestEnvironment.connect(mysql), "\(mysql.errorMessage())")
	}
}
