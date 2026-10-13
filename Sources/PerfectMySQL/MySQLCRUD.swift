//
//  MySQLCRUD.swift
//  PerfectMySQL
//
//  Created by Kyle Jessup on 2018-03-06.
//

import Foundation
import PerfectCRUD

public struct MySQLCRUDError: Error, CustomStringConvertible, Sendable {
	public let description: String
	public init(_ msg: String) {
		description = msg
		CRUDLogging.log(.error, msg)
	}
}

// maps column name to position which must be computed once before row reading action
typealias MySQLCRUDColumnMap = [String:Int]

class MySQLCRUDRowReader<K : CodingKey>: KeyedDecodingContainerProtocol, @unchecked Sendable {
	typealias Key = K
	var codingPath: [CodingKey] = []
	var allKeys: [Key] = []
	let database: MySQL
	let statement: MySQLStmt
	let columns: MySQLCRUDColumnMap
	let row: MySQLStmt.Results.Element
	init(_ db: MySQL,
		 stat: MySQLStmt,
		 columns cols: MySQLCRUDColumnMap,
		 row r: MySQLStmt.Results.Element) {
		database = db
		statement = stat
		columns = cols
		row = r
	}
	func column(_ key: Key) -> Any? {
		guard let idx = columns[key.stringValue],
			idx >= 0,
			idx < row.count else {
				return nil
		}
		return row[idx]
	}
	/// A text value; BINARY / VARBINARY / BLOB columns arrive as bytes and are read as UTF-8.
	func text(_ val: Any?) -> String? {
		switch val {
		case let s as String:
			return s
		case let bytes as [UInt8]:
			return String(decoding: bytes, as: UTF8.self)
		default:
			return nil
		}
	}
	func contains(_ key: Key) -> Bool {
		return nil != columns[key.stringValue]
	}
	func decodeNil(forKey key: Key) throws -> Bool {
		guard let value = column(key) else {
			return true
		}
		// MySQL's zero date ('0000-00-00', '0000-00-00 00:00:00') isn't a date. An optional
		// property over a date column reads it as nil; a non-optional Date throws.
		if let text = value as? String, text.hasPrefix("0000-00-00"),
		   let index = columns[key.stringValue], statement.fieldInfo(index: index)?.type == .date {
			return true
		}
		return false
	}
	func decode(_ type: Bool.Type, forKey key: Key) throws -> Bool {
		return ((column(key) as? Int8) ?? 0) != 0
	}
	func decode(_ type: Int.Type, forKey key: Key) throws -> Int {
		let a = column(key)
		switch a {
		case let i as Int64:
			return Int(i)
		case let i as Int32:
			return Int(i)
		case let i as Int16:
			return Int(i)
		case let i as Int8:
			return Int(i)
		case let i as Int:
			return i
		default:
			throw MySQLCRUDError("Could not convert \(String(describing: a)) into an Int for key: \(key.stringValue)")
		}
	}
	func decode(_ type: Int8.Type, forKey key: Key) throws -> Int8 {
		return (column(key) as? Int8) ?? 0
	}
	func decode(_ type: Int16.Type, forKey key: Key) throws -> Int16 {
		return (column(key) as? Int16) ?? 0
	}
	func decode(_ type: Int32.Type, forKey key: Key) throws -> Int32 {
		return (column(key) as? Int32) ?? 0
	}
	func decode(_ type: Int64.Type, forKey key: Key) throws -> Int64 {
		return (column(key) as? Int64) ?? 0
	}
	func decode(_ type: UInt.Type, forKey key: Key) throws -> UInt {
		let a = column(key)
		switch a {
		case let i as UInt64:
			return UInt(i)
		case let i as UInt32:
			return UInt(i)
		case let i as UInt16:
			return UInt(i)
		case let i as UInt8:
			return UInt(i)
		case let i as UInt:
			return i
		default:
			throw MySQLCRUDError("Could not convert \(String(describing: a)) into an UInt for key: \(key.stringValue)")
		}
	}
	func decode(_ type: UInt8.Type, forKey key: Key) throws -> UInt8 {
		return (column(key) as? UInt8) ?? 0
	}
	func decode(_ type: UInt16.Type, forKey key: Key) throws -> UInt16 {
		return (column(key) as? UInt16) ?? 0
	}
	func decode(_ type: UInt32.Type, forKey key: Key) throws -> UInt32 {
		return (column(key) as? UInt32) ?? 0
	}
	func decode(_ type: UInt64.Type, forKey key: Key) throws -> UInt64 {
		return (column(key) as? UInt64) ?? 0
	}
	func decode(_ type: Float.Type, forKey key: Key) throws -> Float {
		switch column(key) {
		case let f as Float:
			return f
		case let d as Double:
			return Float(d)
		case let text as String: // DECIMAL arrives as text
			guard let f = Float(text) else {
				throw MySQLCRUDError("Could not convert \(text) into a Float for key: \(key.stringValue)")
			}
			return f
		default:
			return 0
		}
	}
	func decode(_ type: Double.Type, forKey key: Key) throws -> Double {
		switch column(key) {
		case let d as Double:
			return d
		case let f as Float:
			return Double(f)
		case let text as String: // DECIMAL arrives as text
			guard let d = Double(text) else {
				throw MySQLCRUDError("Could not convert \(text) into a Double for key: \(key.stringValue)")
			}
			return d
		default:
			return 0
		}
	}
	func decode(_ type: String.Type, forKey key: Key) throws -> String {
		return text(column(key)) ?? ""
	}
	func decode<T>(_ type: T.Type, forKey key: Key) throws -> T where T : Decodable {
		guard let special = SpecialType(type) else {
			throw CRUDDecoderError("Unsupported type: \(type) for key: \(key.stringValue)")
		}
		let val = column(key)
		switch special {
		case .uint8Array:
			let ret: [UInt8] = (val as? [UInt8]) ?? []
			return ret as! T
		case .int8Array:
			let ret: [Int8] = ((val as? [UInt8]) ?? []).map { Int8(bitPattern: $0) }
			return ret as! T
		case .data:
			let bytes: [UInt8] = (val as? [UInt8]) ?? []
			return Data(bytes) as! T
		case .uuid:
			guard let str = text(val), let uuid = UUID(uuidString: str) else {
				throw CRUDDecoderError("Invalid UUID string \(String(describing: val)).")
			}
			return uuid as! T
		case .date:
			guard let str = text(val), let date = Date(fromMysqlFormatted: str) else {
				if let str = text(val), str.hasPrefix("0000-00-00") {
					throw CRUDDecoderError("Zero date \(str) for key \(key.stringValue) can't be a Date; use an optional property.")
				}
				throw CRUDDecoderError("Invalid Date string \(String(describing: val)).")
			}
			return date as! T
		case .url:
			guard let str = text(val), let url = URL(string: str) else {
				throw CRUDDecoderError("Invalid URL string \(String(describing: val)).")
			}
			return url as! T
		case .codable:
			guard let data = text(val)?.data(using: .utf8) else {
				throw CRUDDecoderError("Unsupported type: \(type) for key: \(key.stringValue)")
			}
			return try JSONDecoder().decode(type, from: data)
		case .wrapped:
			// Property-wrapper-backed columns (e.g. @ForeignKey) were never
			// actually decodable on this connector -- unconditionally
			// threw here instead of delegating to the wrapper's own
			// init(from:), unlike Perfect-SQLite's SQLiteCRUDRowReader
			// (ADR-0001 Phase 4: found live, via a real @ForeignKey
			// round-trip test against a running MySQL server, not by
			// inspection -- the write path worked, but any subsequent
			// SELECT on that same row silently looked empty, since
			// SelectIterator.next() swallows decode errors).
			let decoder = CRUDColumnValueDecoder(source: KeyedDecodingContainer(self), key: key)
			return try T(from: decoder)
		}
	}
	func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type, forKey key: Key) throws -> KeyedDecodingContainer<NestedKey> where NestedKey : CodingKey {
		throw CRUDDecoderError("Unimplimented nestedContainer")
	}
	func nestedUnkeyedContainer(forKey key: Key) throws -> UnkeyedDecodingContainer {
		throw CRUDDecoderError("Unimplimented nestedUnkeyedContainer")
	}
	func superDecoder() throws -> Decoder {
		throw CRUDDecoderError("Unimplimented superDecoder")
	}
	func superDecoder(forKey key: Key) throws -> Decoder {
		throw CRUDDecoderError("Unimplimented superDecoder")
	}
}

struct MySQLColumnInfo: Codable, Sendable {
	enum CodingKeys: String, CodingKey {
		case field = "Field", type = "Type", null = "Null", key = "Key"
	}
	let field: String
	let type: String
	private let null: String
	private let key: String
	
	var isNull: Bool {
		return null == "YES"
	}
	var isPrimaryKey: Bool {
		return key == "PRI"
	}
}

class MySQLGenDelegate: SQLGenDelegate, @unchecked Sendable {
	let database: MySQL
	var parentTableStack: [TableStructure] = []
	var bindings: Bindings = []
	// FOREIGN KEY clauses accumulated per-column by getColumnDefinition(_:),
	// emitted alongside the column list in getCreateTableSQL. Mirrors
	// Perfect-SQLite's SQLiteGenDelegate.extraCreate -- this connector's own
	// getColumnDefinition only ever branched on .primaryKey, never
	// .foreignKey (ADR-0001 Phase 4), so @ForeignKey silently produced no
	// constraint at all on MySQL/MariaDB while SQLite/Postgres emitted a
	// real one for the exact same model.
	var extraCreate: [String] = []

	init(connection db: MySQL) {
		database = db
	}
	
	func getEmptyInsertSnippet() -> String {
		return "() VALUES ()"
	}
	
	func getBinding(for expr: CRUDExpression) throws -> String {
		bindings.append(("?", expr))
		return "?"
	}
	
	// A backtick inside the name is doubled, so a name (such as a Dynamic API table or field
	// name, or a constraint name read from the server) can't end the quoted identifier early.
	//
	// Escaped per Unicode scalar, not with replacingOccurrences: that matches whole
	// Characters, so a backtick followed by a combining mark (U+0301) would not match and
	// would reach the SQL unescaped.
	//
	// This assumes a utf8mb4 connection (what the database:host: initializers set). Under a
	// legacy multibyte charset such as gbk or big5, the server can read a UTF-8 byte and the
	// first backtick as one character, so a caller-supplied connection must not use one.
	func quote(identifier: String) throws -> String {
		var escaped = String.UnicodeScalarView()
		for scalar in identifier.unicodeScalars {
			if scalar == "`" {
				escaped.append(scalar)
			}
			escaped.append(scalar)
		}
		return "`\(String(escaped))`"
	}
	
	// A table's FOREIGN KEY constraint needs its target table to exist, and a table can't be
	// dropped while another table's constraint references it. So the sub-tables aren't simply
	// emitted before (or after) the table itself: all the tables are put in an order where
	// each one comes after the tables it references (the parent before a child that
	// references it, a sub-table before a parent that references it), then they're dropped
	// in the reverse of that order (`.dropTable`), then created or reconciled in it. A table
	// referencing itself is fine either way. Two tables referencing each other can't be
	// created by `create()` at all, so they keep the order they were given in.
	func getCreateTableSQL(forTable: TableStructure, policy: TableCreatePolicy) throws -> [String] {
		var tables: [TableStructure] = []
		func collect(_ table: TableStructure) {
			guard !tables.contains(where: { $0.tableName == table.tableName }) else {
				return
			}
			tables.append(table)
			if !policy.contains(.shallow) {
				table.subTables.forEach(collect)
			}
		}
		collect(forTable)
		var ordered: [TableStructure] = []
		var remaining = tables
		while !remaining.isEmpty {
			let pending = Set(remaining.map(\.tableName))
			let next = remaining.firstIndex { table in
				!table.columns.contains { column in
					column.properties.contains {
						if case .foreignKey(let target, _, _, _) = $0 {
							return target != table.tableName && pending.contains(target)
						}
						return false
					}
				}
			} ?? remaining.startIndex
			ordered.append(remaining.remove(at: next))
		}
		var sql: [String] = []
		if policy.contains(.dropTable) {
			sql += try ordered.reversed().map { "DROP TABLE IF EXISTS \(try quote(identifier: $0.tableName))" }
		}
		for table in ordered {
			parentTableStack.append(table)
			defer {
				parentTableStack.removeLast()
			}
			sql += try getCreateOrReconcileSQL(forTable: table, policy: policy)
		}
		return sql
	}

	private func getCreateOrReconcileSQL(forTable: TableStructure, policy: TableCreatePolicy) throws -> [String] {
		// getColumnDefinition(_:) collects the FOREIGN KEY clauses of the columns it's given,
		// so start each table with none, or one table's clauses end up in the next table's
		// CREATE.
		extraCreate = []
		if !policy.contains(.dropTable),
			policy.contains(.reconcileTable),
			let existingColumns = getExistingColumnData(forTable: forTable.tableName) {
			// MySQL column names are case-insensitive, so both sides are keyed by the lowercased
			// name. Only the new columns were, so every column with an uppercase letter looked
			// removed and added: it was dropped, losing its data, and added back empty.
			let existingColumnMap: [String:MySQLColumnInfo] = .init(uniqueKeysWithValues: existingColumns.map { ($0.field.lowercased(), $0) })
			let newColumnMap: [String:TableStructure.Column] = .init(uniqueKeysWithValues: forTable.columns.map { ($0.name.lowercased(), $0) })
			
			let addColumns = newColumnMap.keys.filter { existingColumnMap[$0] == nil }
			let removeColumns: [String] = existingColumnMap.filter { newColumnMap[$0.key] == nil }.map { $0.value.field }
			
			// A column can't be dropped while a FOREIGN KEY constraint on it exists (error 1828),
			// so a removed @ForeignKey column's constraints are dropped in the same statement.
			// If they can't be read, the columns are dropped as before (and one with a constraint
			// fails as before), rather than failing a reconcile that has no constraint to drop. A
			// constraint over several removed columns is dropped with the first of them only.
			let foreignKeys = removeColumns.isEmpty ? [] : (try? getExistingForeignKeys(forTable: forTable.tableName)) ?? []
			var droppedForeignKeys: Set<String> = []
			var sub: [String] = try removeColumns.map { column in
				let dropForeignKeys = try foreignKeys
					.filter { $0.columnName.lowercased() == column.lowercased() && droppedForeignKeys.insert($0.constraintName).inserted }
					.map { "DROP FOREIGN KEY \(try quote(identifier: $0.constraintName)), " }
				return """
				ALTER TABLE \(try quote(identifier: forTable.tableName)) \(dropForeignKeys.joined())DROP COLUMN \(try quote(identifier: column))
				"""
			}
			sub += try addColumns.compactMap { newColumnMap[$0] }.map {
				// A new @ForeignKey column gets its constraint in the same statement.
				extraCreate = []
				let nameType = try getColumnDefinition($0)
				return """
				ALTER TABLE \(try quote(identifier: forTable.tableName)) ADD COLUMN \(nameType)\(extraCreate.map { ", ADD \($0)" }.joined())
				"""
			}
			return sub
		}
		// getColumnDefinition(_:) populates `extraCreate` (FOREIGN KEY
		// clauses) as a side effect while mapping columns -- must be read
		// after the map, not before.
		let columnDefs = try forTable.columns.map { try getColumnDefinition($0) }
		return [
			"""
			CREATE TABLE IF NOT EXISTS \(try quote(identifier: forTable.tableName)) (
			\((columnDefs + extraCreate).joined(separator: ",\n\t"))
			)
			"""]
	}
	
	func getCreateIndexSQL(forTable name: String, on columns: [String], unique: Bool) throws -> [String] {
		let stat =
		"""
		CREATE \(unique ? "UNIQUE " : "")INDEX \(try quote(identifier: "index_\(columns.joined(separator: "_"))"))
		ON \(try quote(identifier: name)) (\(try columns.map{try quote(identifier: $0)}.joined(separator: ",")))
		"""
		return [stat]
	}
	
	struct ForeignKeyInfo: Codable {
		let columnName: String
		let constraintName: String
	}

	// The FOREIGN KEY constraints of a table in the current database, one row per column.
	func getExistingForeignKeys(forTable: String) throws -> [ForeignKeyInfo] {
		let statement = """
			SELECT COLUMN_NAME AS columnName, CONSTRAINT_NAME AS constraintName
			FROM information_schema.KEY_COLUMN_USAGE
			WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND REFERENCED_TABLE_NAME IS NOT NULL
			"""
		let stat = MySQLStmt(database)
		guard stat.prepare(statement: statement) else {
			throw MySQLCRUDError("Error preparing statement. \(stat.errorMessage())")
		}
		stat.bindParam(forTable)
		let exeDelegate = MySQLStmtExeDelegate(connection: database, stat: stat)
		var ret: [ForeignKeyInfo] = []
		while try exeDelegate.hasNext() {
			let rowDecoder: CRUDRowDecoder<ColumnKey> = CRUDRowDecoder(delegate: exeDelegate)
			ret.append(try ForeignKeyInfo(from: rowDecoder))
		}
		return ret
	}

	func getExistingColumnData(forTable: String) -> [MySQLColumnInfo]? {
		do {
			let statement = "SHOW COLUMNS FROM \(try quote(identifier: forTable))"
			let stat = MySQLStmt(database)
			guard stat.prepare(statement: statement) else {
				return nil
			}
			let exeDelegate = MySQLStmtExeDelegate(connection: database, stat: stat)
			var ret: [MySQLColumnInfo] = []
			while try exeDelegate.hasNext() {
				let rowDecoder: CRUDRowDecoder<ColumnKey> = CRUDRowDecoder(delegate: exeDelegate)
				ret.append(try MySQLColumnInfo(from: rowDecoder))
			}
			guard !ret.isEmpty else {
				return nil
			}
			return ret
		} catch {
			return nil
		}
	}
	
	func getColumnDefinition(_ column: TableStructure.Column) throws -> String {
		let name = column.name
		let type = column.type
		let typeName: String
		switch type {
		case is Int.Type:
			typeName = "bigint"
		case is Int8.Type:
			typeName = "tinyint"
		case is Int16.Type:
			typeName = "smallint"
		case is Int32.Type:
			typeName = "int"
		case is Int64.Type:
			typeName = "bigint"
		case is UInt.Type:
			typeName = "bigint unsigned"
		case is UInt8.Type:
			typeName = "tinyint unsigned"
		case is UInt16.Type:
			typeName = "smallint unsigned"
		case is UInt32.Type:
			typeName = "int unsigned"
		case is UInt64.Type:
			typeName = "bigint unsigned"
		case is Double.Type:
			typeName = "double"
		case is Float.Type:
			typeName = "float"
		case is Bool.Type:
			typeName = "tinyint"
		case is String.Type:
			// MySQL cannot use LONGTEXT as a primary key without a prefix length.
			// Use VARCHAR(255) for primary key columns; longtext for everything else.
			typeName = column.properties.contains(.primaryKey) ? "varchar(255)" : "longtext"
		default:
			guard let special = SpecialType(type) else {
				throw MySQLCRUDError("Unsupported SQL column type \(type)")
			}
			switch special {
			case .uint8Array:
				typeName = "longblob"
			case .int8Array:
				typeName = "longblob"
			case .data:
				typeName = "longblob"
			case .uuid:
				typeName = "varchar(36)"
			case .date:
				typeName = "datetime"
			case .url:
				typeName = "longtext"
			case .codable:
				typeName = "json"
			case .wrapped:
				throw MySQLCRUDError("Unsupported SQL column type \(type)")
			}
		}
		var addendum = ""
		for prop in column.properties {
			switch prop {
			case .primaryKey:
				addendum += " PRIMARY KEY"
			case .foreignKey(let table, let column, let onDelete, let onUpdate):
				var str = "FOREIGN KEY (\(try quote(identifier: name))) REFERENCES \(try quote(identifier: table))(\(try quote(identifier: column)))"
				let scenarios = [(" ON DELETE ", onDelete), (" ON UPDATE ", onUpdate)]
				for (scenario, action) in scenarios {
					str += scenario
					switch action {
					case .ignore:
						str += "NO ACTION"
					case .restrict:
						str += "RESTRICT"
					case .setNull:
						str += "SET NULL"
					case .setDefault:
						str += "SET DEFAULT"
					case .cascade:
						str += "CASCADE"
					}
				}
				extraCreate.append(str)
			}
		}
		if !column.properties.contains(.primaryKey) && !column.optional {
			addendum += " NOT NULL"
		}
		return "\(try quote(identifier: name)) \(typeName)\(addendum)"
	}
}

typealias MySQLColumnMap = [String:Int]

struct MySQLDirectExeDelegate: SQLExeDelegate, Sendable {
	let connection: MySQL
	let sql: String
	func bind(_ bindings: Bindings, skip: Int) throws {
		guard bindings.isEmpty else {
			throw MySQLCRUDError("Binds are not permitted for this type of statement.")
		}
	}
	func hasNext() throws -> Bool {
		guard connection.query(statement: sql) else {
			throw MySQLCRUDError("Error executing statement. \(connection.errorMessage())")
		}
		// Read every result (multi-statement text can produce several) so the connection is
		// never left "out of sync", and report errors from any of them.
		var returnedRows = false
		while true {
			if connection.storeResults() != nil {
				returnedRows = true
			} else if connection.errorCode() != 0 {
				throw MySQLCRUDError("Error reading results. \(connection.errorMessage())")
			}
			let next = connection.nextResult()
			if next > 0 {
				throw MySQLCRUDError("Error executing statement. \(connection.errorMessage())")
			}
			if next < 0 {
				break
			}
		}
		// This path can't decode text-protocol rows into Codable values. Failing beats silently
		// returning nothing, e.g. for CHECK TABLE, which reports problems as rows.
		if returnedRows {
			throw MySQLCRUDError("This statement returns rows but MySQL can't prepare it, so PerfectCRUD can't read them. Run it with MySQL.query(statement:) and MySQL.storeResults().")
		}
		return false
	}
	func next<A>() throws -> KeyedDecodingContainer<A>? where A : CodingKey {
		return nil
	}
}

class MySQLStmtExeDelegate: SQLExeDelegate, @unchecked Sendable {
	let connection: MySQL
	let statement: MySQLStmt
	var results: MySQLStmt.Results?
	/// Built after each execute(): a CALL has no result metadata until then.
	private(set) var columnMap = MySQLColumnMap()
	/// The columns of a dynamic row, in result order, one per name ignoring case.
	private(set) var dynamicColumns: [(name: String, index: Int)] = []
	// Read right after execute(): hasNext() resets the statement once its rows run out, and
	// MariaDB Connector/C's mysql_stmt_reset sets the affected-row count to -1.
	private var affectedRows: UInt64 = 0
	private var insertID: UInt64 = 0
	init(connection c: MySQL, stat: MySQLStmt) {
		connection = c
		statement = stat
	}

	deinit {
		// Callers can stop before the rows run out (Database.sql without a type, first(), a
		// decoding error). Leave the connection ready for the next statement anyway.
		if results != nil {
			results = nil
			// Nothing can throw from here (Database.sql without a type calls hasNext() once), so
			// an error from a later result is only logged.
			if let later = statement.discardRemainingResults() {
				CRUDLogging.log(.error, "Error executing statement. \(later.message)")
			}
			statement.reset()
		}
	}

	private func buildColumnMaps() {
		// When several columns share a name (`a.id, b.id`), the first one wins. This used to
		// depend on dictionary order. DynamicRow also lowercases its keys and, in Perfect-CRUD
		// 3.0.0, traps on duplicates, so dynamic rows keep one column per name ignoring case.
		var m = MySQLColumnMap()
		var dynamic: [(name: String, index: Int)] = []
		var dynamicNames = Set<String>()
		for (pos, name) in statement.fieldNames().sorted(by: { $0.key < $1.key }) {
			if m[name] == nil {
				m[name] = pos
			}
			if dynamicNames.insert(name.lowercased()).inserted {
				dynamic.append((name, pos))
			}
		}
		columnMap = m
		dynamicColumns = dynamic
	}
	
	func bind(_ bindings: Bindings, skip: Int) throws {
		try bindings[skip...].forEach {
			let (_, expr) = $0
			try bindOne(expr: expr)
		}
	}
	
	func hasNext() throws -> Bool {
		if nil == results {
			guard statement.execute() else {
				throw MySQLCRUDError("Error executing statement. \(statement.errorMessage())")
			}
			let returnsRows = statement.fieldCount() > 0
			if !returnsRows {
				affectedRows = UInt64(statement.affectedRows())
				insertID = UInt64(statement.insertId())
			}
			results = statement.results()
			if returnsRows {
				// What libmysqlclient reported for a SELECT: the number of rows it returned.
				affectedRows = UInt64(statement.numRows())
				insertID = 0
				buildColumnMaps()
			}
		}
		if results?.fetchRow() ?? false {
			return true
		}
		let failed = results?.failed ?? false
		let error = failed ? "Error reading results. \(results?.errorMessage ?? "")" : ""
		results = nil
		// Read past any further result sets (a CALL ends with a status result), then reset.
		// A CALL can fail after its first result set; report that too.
		let later = statement.discardRemainingResults()
		statement.reset()
		if failed {
			throw MySQLCRUDError(error)
		}
		if let later {
			throw MySQLCRUDError("Error executing statement. \(later.message)")
		}
		return false
	}
	
	func next<A>() throws -> KeyedDecodingContainer<A>? where A : CodingKey {
		guard let row = results?.currentRow() else {
			return nil
		}
		return KeyedDecodingContainer(MySQLCRUDRowReader<A>(connection,
															stat: statement,
															columns: columnMap,
															row: row))
	}

	func nextDynamicRow() throws -> DynamicRow? {
		guard let row = results?.currentRow() else {
			return nil
		}
		var values: [String: DynamicValue] = [:]
		for (name, index) in dynamicColumns {
			guard row.indices.contains(index) else {
				throw MySQLCRUDError("Missing dynamic row value for column \(name).")
			}
			values[name] = try mysqlDynamicValue(row[index], column: name)
		}
		return DynamicRow(values)
	}

	func affectedRowCount() -> Int {
		// (my_ulonglong)-1 means "no count" (an error, or a SELECT before its rows are stored).
		Int(exactly: affectedRows) ?? 0
	}

	func lastInsertedID() -> Int64? {
		insertID == 0 ? nil : Int64(exactly: insertID)
	}

	private func bindOne(expr: CRUDExpression) throws {
		switch expr {
		case .lazy(let e):
			try bindOne(expr: e())
		case .integer(let i):
			statement.bindParam(i)
		case .decimal(let d):
			statement.bindParam(d)
		case .string(let s):
			statement.bindParam(s)
		case .blob(let b):
			statement.bindParam(b)
		case .bool(let b):
			statement.bindParam(b ? 1 : 0)
		case .date(let d):
			statement.bindParam(d.mysqlFormatted())
		case .url(let u):
			statement.bindParam(u.absoluteString)
		case .uuid(let u):
			statement.bindParam(u.uuidString)
		case .null:
			statement.bindParam()
		case .column(_), .and(_, _), .or(_, _),
			 .equality(_, _), .inequality(_, _),
			 .not(_), .lessThan(_, _), .lessThanEqual(_, _),
			 .greaterThan(_, _), .greaterThanEqual(_, _),
			 .keyPath(_), .in(_, _), .like(_, _, _, _):
			throw MySQLCRUDError("Asked to bind unsupported expression type: \(expr)")
		case .uinteger(let i):
			statement.bindParam(i)
		case .integer64(let i):
			statement.bindParam(i)
		case .uinteger64(let i):
			statement.bindParam(i)
		case .integer32(let i):
			statement.bindParam(i)
		case .uinteger32(let i):
			statement.bindParam(i)
		case .integer16(let i):
			statement.bindParam(i)
		case .uinteger16(let i):
			statement.bindParam(i)
		case .integer8(let i):
			statement.bindParam(i)
		case .uinteger8(let i):
			statement.bindParam(i)
		case .float(let d):
			statement.bindParam(d)
		case .sblob(let b):
			statement.bindParam(b)
		}
	}
}

func mysqlDynamicValue(_ value: Any?, column: String) throws -> DynamicValue {
	switch value {
	case nil:
		return .null
	case let value as Bool:
		return .bool(value)
	case let value as Int:
		return .int(Int64(value))
	case let value as Int8:
		return .int(Int64(value))
	case let value as Int16:
		return .int(Int64(value))
	case let value as Int32:
		return .int(Int64(value))
	case let value as Int64:
		return .int(value)
	case let value as UInt:
		return .uint(UInt64(value))
	case let value as UInt8:
		return .uint(UInt64(value))
	case let value as UInt16:
		return .uint(UInt64(value))
	case let value as UInt32:
		return .uint(UInt64(value))
	case let value as UInt64:
		return .uint(value)
	case let value as Float:
		return .double(Double(value))
	case let value as Double:
		return .double(value)
	case let value as String:
		return .string(value)
	case let value as [UInt8]:
		return .bytes(value)
	case let value as [Int8]:
		return .bytes(value.map(UInt8.init(bitPattern:)))
	case let value as Date:
		return .date(value)
	default:
		throw MySQLCRUDError(
			"Unsupported dynamic value type \(type(of: value)) for column \(column)."
		)
	}
}

public struct MySQLDatabaseConfiguration: DatabaseConfigurationProtocol, @unchecked Sendable {
	let connection: MySQL
	
	public init(url: String?,
				name: String?,
				host: String?,
				port: Int?,
				user: String?,
				pass: String?) throws {
		guard let database = name, let host = host else {
			throw MySQLCRUDError("Database name and host must be provided.")
		}
		try self.init(database: database, host: host, port: port, username: user, password: pass)
	}
	
	public init(database: String,
				host: String,
				port: Int? = nil,
				username: String? = nil,
				password: String? = nil) throws {
		connection = MySQL()
		_ = connection.setOption(.MYSQL_SET_CHARSET_NAME, "utf8mb4")
		guard connection.connect(host: host, user: username, password: password, db: database, port: UInt32(port ?? 0), socket: nil, flag: 0) else {
			throw MySQLCRUDError("Could not connect. \(connection.errorMessage())")
		}
	}
	public init(connection c: MySQL) {
		connection = c
	}
	public var sqlGenDelegate: SQLGenDelegate {
		return MySQLGenDelegate(connection: connection)
	}
	
	/// Statements run without preparing, matched on their first keyword regardless of case.
	/// MySQL can't prepare BEGIN/START TRANSACTION, SAVEPOINT, RELEASE SAVEPOINT, ROLLBACK TO
	/// SAVEPOINT, LOCK/UNLOCK TABLES or USE (see "SQL Syntax Permitted in Prepared Statements"
	/// in the MySQL manual); Perfect-CRUD's nested `transaction {}` sends the savepoint ones.
	/// COMMIT/ROLLBACK can be prepared but take no parameters, so running them directly is fine.
	/// Anything else MySQL refuses to prepare falls back to direct execution (error 1295).
	static let directStatements: Set<String> = [
		"BEGIN", "START", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE",
		"LOCK", "UNLOCK", "USE",
	]

	/// DDL prefixes that were always run directly, matched case-sensitively as before. MySQL can
	/// prepare most DDL, so lowercase DDL (which may carry `?` bindings) stays prepared.
	static let directDDLPrefixes = ["CREATE", "DROP", "ALTER"]

	/// ER_UNSUPPORTED_PS: "This command is not supported in the prepared statement protocol yet".
	static let unsupportedPreparedStatementError: UInt32 = 1295

	/// The statement's first keyword, uppercased, ignoring leading whitespace.
	static func firstKeyword(_ sql: String) -> String {
		sql.drop(while: { $0.isWhitespace }).prefix(while: { $0.isLetter }).uppercased()
	}

	public func sqlExeDelegate(forSQL: String) throws -> SQLExeDelegate {
		if Self.directStatements.contains(Self.firstKeyword(forSQL))
			|| Self.directDDLPrefixes.contains(where: { forSQL.hasPrefix($0) }) {
			return MySQLDirectExeDelegate(connection: connection, sql: forSQL)
		}
		let stat = MySQLStmt(connection)
		guard stat.prepare(statement: forSQL) else {
			// Anything else the server refuses to prepare still runs, directly.
			if stat.errorCode() == Self.unsupportedPreparedStatementError {
				return MySQLDirectExeDelegate(connection: connection, sql: forSQL)
			}
			throw MySQLCRUDError("Could not prepare statement. \(stat.errorMessage())")
		}
		return MySQLStmtExeDelegate(connection: connection, stat: stat)
	}
}

// Perfect-CRUD writes dates as UTC wall-clock text, so they're read back as UTC too. Neither
// direction depends on the process's time zone.
//
// Calendar arithmetic is proleptic Gregorian, as MySQL's is. Foundation's .gregorian calendar
// switches to the Julian calendar before 1582-10-15, so it rejected 1582-10-05...14 and put
// earlier dates 10 or more days away from the server's. These are Howard Hinnant's
// days_from_civil / civil_from_days.

/// Days since 1970-01-01 of a proleptic Gregorian date.
func mysqlDaysFromCivil(year: Int, month: Int, day: Int) -> Int {
	let y = month <= 2 ? year - 1 : year
	let era = (y >= 0 ? y : y - 399) / 400
	let yearOfEra = y - era * 400
	let dayOfYear = (153 * (month > 2 ? month - 3 : month + 9) + 2) / 5 + day - 1
	let dayOfEra = yearOfEra * 365 + yearOfEra / 4 - yearOfEra / 100 + dayOfYear
	return era * 146097 + dayOfEra - 719468
}

/// The proleptic Gregorian date `days` after 1970-01-01.
func mysqlCivilFromDays(_ days: Int) -> (year: Int, month: Int, day: Int) {
	let z = days + 719468
	let era = (z >= 0 ? z : z - 146096) / 146097
	let dayOfEra = z - era * 146097
	let yearOfEra = (dayOfEra - dayOfEra / 1460 + dayOfEra / 36524 - dayOfEra / 146096) / 365
	let dayOfYear = dayOfEra - (365 * yearOfEra + yearOfEra / 4 - yearOfEra / 100)
	let mp = (5 * dayOfYear + 2) / 153
	let day = dayOfYear - (153 * mp + 2) / 5 + 1
	let month = mp < 10 ? mp + 3 : mp - 9
	return (yearOfEra + era * 400 + (month <= 2 ? 1 : 0), month, day)
}

private func mysqlDaysInMonth(year: Int, month: Int) -> Int {
	switch month {
	case 2:
		return (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 ? 29 : 28
	case 4, 6, 9, 11:
		return 30
	default:
		return 31
	}
}

/// Seconds between 1970-01-01 and 2001-01-01, Date's reference date.
private let mysqlReferenceOffset = 978_307_200

extension Date {
	/// `yyyy-MM-dd HH:mm:ss` in UTC, in whole seconds truncated toward the past (rounding could
	/// carry 9999-12-31 23:59:59.9999995 into a year-10000 string the server rejects).
	func mysqlFormatted() -> String {
		let seconds = Int(timeIntervalSinceReferenceDate.rounded(.down)) + mysqlReferenceOffset
		let days = seconds >= 0 ? seconds / 86400 : (seconds - 86399) / 86400
		let secondOfDay = seconds - days * 86400
		let (year, month, day) = mysqlCivilFromDays(days)
		func pad(_ value: Int, _ width: Int) -> String {
			let digits = String(value)
			return String(repeating: "0", count: max(0, width - digits.count)) + digits
		}
		return "\(pad(year, 4))-\(pad(month, 2))-\(pad(day, 2)) "
			+ "\(pad(secondOfDay / 3600, 2)):\(pad(secondOfDay / 60 % 60, 2)):\(pad(secondOfDay % 60, 2))"
	}
	
	/// Parses MySQL's DATETIME / TIMESTAMP / DATE text as UTC: `YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS`,
	/// optionally with up to six fractional digits. Returns nil for anything else, including the
	/// zero date and out-of-range parts such as February 30.
	init?(fromMysqlFormatted string: String) {
		let chars = Array(string.utf8)
		func number(_ range: Range<Int>) -> Int? {
			guard !range.isEmpty, range.upperBound <= chars.count else {
				return nil
			}
			var value = 0
			for i in range {
				guard chars[i] >= 48, chars[i] <= 57 else {
					return nil
				}
				value = value * 10 + Int(chars[i] - 48)
			}
			return value
		}
		guard chars.count == 10 || (19...26).contains(chars.count),
			  chars[4] == UInt8(ascii: "-"), chars[7] == UInt8(ascii: "-"),
			  let year = number(0..<4), let month = number(5..<7), let day = number(8..<10),
			  year >= 1, (1...12).contains(month),
			  (1...mysqlDaysInMonth(year: year, month: month)).contains(day) else {
			return nil
		}
		var secondOfDay = 0
		var microseconds = 0
		if chars.count >= 19 {
			guard chars[10] == UInt8(ascii: " ") || chars[10] == UInt8(ascii: "T"),
				  chars[13] == UInt8(ascii: ":"), chars[16] == UInt8(ascii: ":"),
				  let hour = number(11..<13), let minute = number(14..<16), let second = number(17..<19),
				  hour < 24, minute < 60, second < 60 else {
				return nil
			}
			secondOfDay = hour * 3600 + minute * 60 + second
			if chars.count > 19 {
				guard chars[19] == UInt8(ascii: "."), let digits = number(20..<chars.count) else {
					return nil
				}
				microseconds = digits
				for _ in chars.count..<26 {
					microseconds *= 10
				}
			}
		}
		let seconds = mysqlDaysFromCivil(year: year, month: month, day: day) * 86400 + secondOfDay
		self.init(mysqlSeconds: TimeInterval(seconds - mysqlReferenceOffset), microseconds: microseconds)
	}

	/// A whole second plus a fraction kept inside that second. A Date is a Double count of seconds
	/// since 2001, which near year 9999 can't hold six fractional digits: plain addition rounded
	/// 9999-12-31 23:59:59.999999 up to year 10000. Such values become the largest Date still
	/// inside the second.
	init(mysqlSeconds seconds: TimeInterval, microseconds: Int) {
		var interval = seconds + Double(microseconds) / 1_000_000
		if interval >= seconds + 1 {
			interval = (seconds + 1).nextDown
		}
		self.init(timeIntervalSinceReferenceDate: interval)
	}
}

public extension Insert {
	func lastInsertId() throws -> UInt64? {
		let exeDelegate = try databaseConfiguration.sqlExeDelegate(forSQL: "SELECT LAST_INSERT_ID()")
		guard try exeDelegate.hasNext(), let next: KeyedDecodingContainer<ColumnKey> = try exeDelegate.next() else {
			throw CRUDSQLGenError("Did not get return value from statement \"SELECT LAST_INSERT_ID()\".")
		}
		let value = try next.decode(UInt64.self, forKey: ColumnKey(stringValue: "LAST_INSERT_ID()")!)
		return value
	}
}
