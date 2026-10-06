import Foundation
import PerfectCRUD
import Testing
@testable import PerfectMySQL

/// Dates are written as UTC text and must be read back as UTC, whatever the process's time zone.
/// `TimeZone.current` can't be changed in-process on macOS (`NSTimeZone.default` doesn't affect
/// it), so these tests only catch a regression when the suite runs with a non-UTC `TZ`; CI runs
/// it under America/Denver and Asia/Kolkata. Before the fix, Denver lost the spring-forward row.
struct DateTests {
	static func inEachZone(_ body: (String) throws -> Void) rethrows {
		try body(TimeZone.current.identifier)
	}

	static func utc(_ y: Int, _ mo: Int, _ d: Int, _ h: Int = 0, _ mi: Int = 0, _ s: Int = 0) -> Date {
		var calendar = Calendar(identifier: .gregorian)
		calendar.timeZone = TimeZone(identifier: "UTC")!
		return calendar.date(from: DateComponents(year: y, month: mo, day: d, hour: h, minute: mi, second: s))!
	}

	@Test func parsesMySQLTextAsUTC() {
		Self.inEachZone { zone in
			#expect(Date(fromMysqlFormatted: "2023-11-14 22:13:20") == Self.utc(2023, 11, 14, 22, 13, 20), "\(zone)")
			// In America/Denver's spring-forward gap; parsing as local time returned nil.
			#expect(Date(fromMysqlFormatted: "2026-03-08 02:30:00") == Self.utc(2026, 3, 8, 2, 30), "\(zone)")
			#expect(Date(fromMysqlFormatted: "2024-02-29") == Self.utc(2024, 2, 29), "\(zone)")
			let fractional = Date(fromMysqlFormatted: "2023-11-14 22:13:20.250000")
			#expect(fractional == Self.utc(2023, 11, 14, 22, 13, 20).addingTimeInterval(0.25), "\(zone)")
			#expect(Date(fromMysqlFormatted: "2023-11-14 22:13:20.5") == Self.utc(2023, 11, 14, 22, 13, 20).addingTimeInterval(0.5))
			#expect(Date(fromMysqlFormatted: "2023-11-14T22:13:20") == Self.utc(2023, 11, 14, 22, 13, 20))
			for bad in ["0000-00-00", "0000-00-00 00:00:00", "2023-02-30", "2023-13-01", "2023-11-14 24:00:00",
						"2023-11-14 22:13", "2023-11-14 22:13:20.", "2023-11-14 22:13:20.1234567", "22:13:20", "", "abcd-ef-gh"] {
				#expect(Date(fromMysqlFormatted: bad) == nil, "\(bad) in \(zone)")
			}
			#expect(Self.utc(2026, 3, 8, 2, 30).mysqlFormatted() == "2026-03-08 02:30:00", "\(zone)")
		}
	}

	struct DateRow: Codable, TableNameProvider {
		static let tableName = "date_row"
		let id: Int
		let d: Date
	}

	struct OptionalDateRow: Codable, TableNameProvider {
		static let tableName = "date_row"
		let id: Int
		let d: Date?
	}

	@Test(.enabled(if: MySQLTestEnvironment.isFixtureEnabled))
	func datesRoundTripInEveryZone() throws {
		let fixture = try CatalogFixtureDatabase()
		let db = fixture.database
		let written = [
			DateRow(id: 1, d: Self.utc(2023, 11, 14, 22, 13, 20)),
			DateRow(id: 2, d: Self.utc(2026, 3, 8, 2, 30)), // Denver DST gap, in UTC wall time
		]
		try Self.inEachZone { zone in
			try db.sql("DROP TABLE IF EXISTS date_row")
			try db.create(DateRow.self, policy: .dropTable)
			try db.table(DateRow.self).insert(written)
			let read = try db.table(DateRow.self).order(by: \.id).select().map { $0 }
			#expect(read.map(\.id) == [1, 2], "\(zone): every row comes back")
			#expect(read.map(\.d) == written.map(\.d), "\(zone)")
		}
	}

	@Test(.enabled(if: MySQLTestEnvironment.isFixtureEnabled))
	func readsFractionalDateOnlyAndZeroDates() throws {
		let fixture = try CatalogFixtureDatabase()
		let db = fixture.database
		try Self.inEachZone { zone in
			try db.sql("DROP TABLE IF EXISTS date_row")
			try db.sql("CREATE TABLE date_row (id INT PRIMARY KEY, d DATETIME(6) NULL)")
			try db.sql("INSERT INTO date_row VALUES (1, '2023-11-14 22:13:20.250000'), (2, NULL)")
			let fractional = try db.table(OptionalDateRow.self).order(by: \.id).select().map { $0 }
			#expect(fractional.map(\.id) == [1, 2], "\(zone)")
			#expect(fractional.first?.d == Self.utc(2023, 11, 14, 22, 13, 20).addingTimeInterval(0.25), "\(zone)")
			#expect(fractional.last?.d == nil, "\(zone)")

			try db.sql("DROP TABLE date_row")
			try db.sql("CREATE TABLE date_row (id INT PRIMARY KEY, d DATE NULL)")
			try db.sql("INSERT INTO date_row VALUES (1, '2024-02-29')")
			#expect(try db.table(DateRow.self).first()?.d == Self.utc(2024, 2, 29), "\(zone)")

			// The zero date: nil for an optional property, an error (so no row) for a non-optional one.
			try db.sql("DROP TABLE date_row")
			try db.sql("CREATE TABLE date_row (id INT PRIMARY KEY, d DATETIME NULL)")
			try db.sql("SET SESSION sql_mode = ''")
			try db.sql("INSERT INTO date_row VALUES (1, '0000-00-00 00:00:00')")
			let optional = try db.table(OptionalDateRow.self).select().map { $0 }
			#expect(optional.count == 1 && optional.first?.d == nil, "\(zone)")
			#expect(try db.table(DateRow.self).select().map { $0 }.isEmpty, "\(zone)")
			try db.sql("SET SESSION sql_mode = DEFAULT")
		}
	}
}
