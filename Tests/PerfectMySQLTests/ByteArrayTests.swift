import Foundation
import PerfectCRUD
import Testing
@testable import PerfectMySQL

/// [Int8] properties are stored as their bytes; reading them back used Int8(_:), which trapped
/// on any byte >= 0x80, so a negative value crashed the read. The rows are written with SQL:
/// Perfect-CRUD 3.0.0's encoder traps the same way writing a negative [Int8] (fixed in 3.0.1).
struct ByteArrayTests {
	struct Signed: Codable, TableNameProvider {
		static let tableName = "signed_bytes"
		let id: Int
		let bytes: [Int8]
		let plain: [UInt8]
	}

	@Test(.enabled(if: MySQLTestEnvironment.isFixtureEnabled))
	func negativeInt8ValuesRoundTrip() throws {
		let fixture = try CatalogFixtureDatabase()
		let db = fixture.database
		try db.create(Signed.self, policy: .dropTable)
		let written = [
			Signed(id: 1, bytes: [-1, -128, 0, 127, 1], plain: [255, 128, 0]),
			Signed(id: 2, bytes: [], plain: []),
			Signed(id: 3, bytes: Array(Int8.min...Int8.max), plain: Array(UInt8.min...UInt8.max)),
		]
		func hex<T: FixedWidthInteger>(_ values: [T]) -> String {
			"X'" + values.map { String(format: "%02X", UInt8(truncatingIfNeeded: $0)) }.joined() + "'"
		}
		for row in written {
			try db.sql("INSERT INTO signed_bytes (id, bytes, plain) VALUES (\(row.id), \(hex(row.bytes)), \(hex(row.plain)))")
		}
		let read = try db.table(Signed.self).order(by: \.id).select().map { $0 }
		#expect(read.map(\.id) == [1, 2, 3])
		#expect(read.map(\.bytes) == written.map(\.bytes))
		#expect(read.map(\.plain) == written.map(\.plain))
		let filtered = try db.table(Signed.self).where(\Signed.bytes == [-1, -128, 0, 127, 1]).select().map(\.id)
		#expect(filtered == [1])
	}
}
