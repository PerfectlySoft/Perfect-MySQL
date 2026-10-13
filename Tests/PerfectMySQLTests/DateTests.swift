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

	@Test func parsesMySQLTextAsUTC() throws {
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
		}
		#expect(try Self.utc(2026, 3, 8, 2, 30).mysqlFormatted() == "2026-03-08 02:30:00")
	}

	// Fractions are carried as whole microseconds and truncated when writing: a Double sum used
	// to round '9999-12-31 23:59:59.999999' into year 10000, which the writer then emitted.
	@Test func fractionsStayInsideTheirSecond() throws {
		let lastSecond = Date(fromMysqlFormatted: "9999-12-31 23:59:59.999999")
		#expect(lastSecond.map { $0 < Self.utc(9999, 12, 31, 23, 59, 59).addingTimeInterval(1) } == true)
		#expect(try lastSecond?.mysqlFormatted() == "9999-12-31 23:59:59")
		let beforeEpoch = Date(fromMysqlFormatted: "1969-12-31 23:59:59.999999")
		#expect(try beforeEpoch?.mysqlFormatted() == "1969-12-31 23:59:59")
		#expect(beforeEpoch.map { abs($0.timeIntervalSince1970 + 0.000001) < 0.0000005 } == true)
		#expect(try Date(fromMysqlFormatted: "2023-11-14 22:13:20.000001")?.mysqlFormatted() == "2023-11-14 22:13:20")
		#expect(try Date(timeIntervalSince1970: -0.5).mysqlFormatted() == "1969-12-31 23:59:59")
		#expect(try Date(timeIntervalSinceReferenceDate: 721_692_800.9999).mysqlFormatted() == "2023-11-14 22:13:20")
	}

	// MySQL's calendar is proleptic Gregorian. Foundation's .gregorian switches to Julian before
	// 1582-10-15: 1582-10-05...14 didn't parse and earlier dates were 10+ days off the server's.
	@Test func datesUseTheProlepticGregorianCalendar() throws {
		let cases: [(String, TimeInterval)] = [ // Unix times from Python's proleptic datetime
			("0001-01-01", -62_135_596_800),
			("1000-03-01", -30_605_126_400),
			("1582-10-04", -12_220_243_200),
			("1582-10-10", -12_219_724_800),
			("1582-10-15", -12_219_292_800),
			("1970-01-01", 0),
			("2000-02-29", 951_782_400),
			("9999-12-31", 253_402_214_400),
		]
		for (text, unix) in cases {
			let date = Date(fromMysqlFormatted: text)
			#expect(date?.timeIntervalSince1970 == unix, "\(text)")
			#expect(try date?.mysqlFormatted() == text + " 00:00:00", "\(text)")
		}
		#expect(Date(fromMysqlFormatted: "1900-02-29") == nil)
		#expect(Date(fromMysqlFormatted: "1600-02-29") != nil)
		#expect(try Date(fromMysqlFormatted: "1000-03-01 12:34:56")?.mysqlFormatted() == "1000-03-01 12:34:56")
		// Every day of a 400-year cycle round-trips.
		var day = Date(fromMysqlFormatted: "1600-01-01")!
		for _ in 0..<146_097 {
			let text = try day.mysqlFormatted()
			#expect(Date(fromMysqlFormatted: text) == day, "\(text)")
			if Date(fromMysqlFormatted: text) != day { break }
			day = day.addingTimeInterval(86_400)
		}
		#expect(try day.mysqlFormatted() == "2000-01-01 00:00:00")
	}

	// The server agrees: dates written through Perfect-CRUD land on the same calendar day.
	@Test(.enabled(if: MySQLTestEnvironment.isFixtureEnabled))
	func oldDatesMatchTheServersCalendar() throws {
		struct Day: Codable { let d: String }
		let fixture = try CatalogFixtureDatabase()
		let db = fixture.database
		try db.sql("DROP TABLE IF EXISTS date_row")
		try db.create(DateRow.self, policy: .dropTable)
		let texts = ["1000-03-01 00:00:00", "1582-10-10 00:00:00", "1582-10-04 12:00:00"]
		try db.table(DateRow.self).insert(texts.enumerated().map { DateRow(id: $0, d: Date(fromMysqlFormatted: $1)!) })
		let server = try db.sql("SELECT DATE_FORMAT(d, '%Y-%m-%d %H:%i:%s') AS d FROM date_row ORDER BY id", Day.self)
		#expect(server.map(\.d) == texts)
		let read = try db.table(DateRow.self).order(by: \.id).select().map { try $0.d.mysqlFormatted() }
		#expect(read == texts)
		#expect(try db.sql("SELECT CAST(DATEDIFF('1582-10-15', '1582-10-04') AS CHAR) AS d", Day.self).first?.d == "11")
	}

	// Date.distantPast is 0000-12-30 in the proleptic calendar. The writer emitted it and MySQL
	// stored it, but the reader rejected year 0, and Perfect-CRUD's select() stops at a row that
	// fails to decode, so every later row vanished.
	@Test func sentinelDatesRoundTrip() throws {
		#expect(try Date.distantPast.mysqlFormatted() == "0000-12-30 00:00:00")
		#expect(Date(fromMysqlFormatted: "0000-12-30 00:00:00") == .distantPast)
		#expect(try Date.distantFuture.mysqlFormatted() == "4001-01-01 00:00:00")
		#expect(Date(fromMysqlFormatted: "4001-01-01 00:00:00") == .distantFuture)
		#expect(Date(fromMysqlFormatted: "0000-01-01") != nil)
		#expect(Date(fromMysqlFormatted: "0000-02-29") != nil) // year 0 is a leap year
		#expect(Date(fromMysqlFormatted: "0000-00-00") == nil)
		#expect(Date(fromMysqlFormatted: "0000-01-00") == nil)
		// Not representable: thrown, not clamped and not a trap.
		for bad in [Date(timeIntervalSinceReferenceDate: .infinity), Date(timeIntervalSinceReferenceDate: -.infinity),
					Date(timeIntervalSinceReferenceDate: .nan), Date(timeIntervalSinceReferenceDate: 1e300),
					Date(fromMysqlFormatted: "0000-01-01")!.addingTimeInterval(-1),
					Date(fromMysqlFormatted: "9999-12-31 23:59:59")!.addingTimeInterval(1)] {
			#expect(throws: MySQLCRUDError.self) { try bad.mysqlFormatted() }
		}
	}

	@Test(.enabled(if: MySQLTestEnvironment.isFixtureEnabled))
	func sentinelDatesDontEndASelect() throws {
		let fixture = try CatalogFixtureDatabase()
		let db = fixture.database
		try db.sql("DROP TABLE IF EXISTS date_row")
		try db.create(DateRow.self, policy: .dropTable)
		let written: [DateRow] = [
			DateRow(id: 0, d: Date(timeIntervalSince1970: 0)), DateRow(id: 1, d: .distantPast),
			DateRow(id: 2, d: Self.utc(1, 1, 1)), DateRow(id: 3, d: .distantFuture),
			DateRow(id: 4, d: Date(timeIntervalSince1970: -0.000001)), DateRow(id: 5, d: Self.utc(9999, 12, 31, 23, 59, 59)),
		]
		try db.table(DateRow.self).insert(written)
		let read = try db.table(DateRow.self).order(by: \.id).select().map { $0 }
		#expect(read.map(\.id) == [0, 1, 2, 3, 4, 5])
		#expect(read.map(\.d) == written.map { Date(timeIntervalSinceReferenceDate: $0.d.timeIntervalSinceReferenceDate.rounded(.down)) })
		let infinite = DateRow(id: 6, d: Date(timeIntervalSinceReferenceDate: .infinity))
		#expect(throws: (any Error).self) { try db.table(DateRow.self).insert(infinite) }
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
