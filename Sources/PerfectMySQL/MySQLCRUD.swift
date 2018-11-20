//
//  MySQLCRUD.swift
//  PerfectMySQL
//
//  Created by Kyle Jessup on 2018-03-06.
//

import Foundation
import PerfectCRUD

public struct MySQLCRUDError: Error, CustomStringConvertible {
	public let description: String
	public init(_ msg: String) {
		description = msg
		CRUDLogging.log(.error, msg)
	}
}

// maps column name to position which must be computed once before row reading action
typealias MySQLCRUDColumnMap = [String:Int]

class MySQLCRUDRowReader<K : CodingKey>: KeyedDecodingContainerProtocol {
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
	func contains(_ key: Key) -> Bool {
		return nil != columns[key.stringValue]
	}
	func decodeNil(forKey key: Key) throws -> Bool {
		return nil == column(key)
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
		case let i as Int:
			return i
		default:
			throw MySQLCRUDError("Could not convert \(String(describing: a)) into an Int.")
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
		case let i as UInt:
			return i
		default:
			throw MySQLCRUDError("Could not convert \(String(describing: a)) into an UInt.")
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
		return (column(key) as? Float) ?? 0
	}
	func decode(_ type: Double.Type, forKey key: Key) throws -> Double {
		return (column(key) as? Double) ?? 0
	}
	func decode(_ type: String.Type, forKey key: Key) throws -> String {
		return (column(key) as? String) ?? ""
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
			let ret: [Int8] = ((val as? [UInt8]) ?? []).map { Int8($0) }
			return ret as! T
		case .data:
			let bytes: [UInt8] = (val as? [UInt8]) ?? []
			return Data(bytes: bytes) as! T
		case .uuid:
			guard let str = val as? String, let uuid = UUID(uuidString: str) else {
				throw CRUDDecoderError("Invalid UUID string \(String(describing: val)).")
			}
			return uuid as! T
		case .date:
			guard let str = val as? String, let date = Date(fromMysqlFormatted: str) else {
				throw CRUDDecoderError("Invalid Date string \(String(describing: val)).")
			}
			return date as! T
		case .url:
			guard let str = val as? String, let url = URL(string: str) else {
				throw CRUDDecoderError("Invalid URL string \(String(describing: val)).")
			}
			return url as! T
		case .codable:
			guard let data = (val as? String)?.data(using: .utf8) else {
				throw CRUDDecoderError("Unsupported type: \(type) for key: \(key.stringValue)")
			}
			return try JSONDecoder().decode(type, from: data)
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

struct MySQLColumnInfo: Codable {
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

class MySQLGenDelegate: SQLGenDelegate {
	let database: MySQL
	var parentTableStack: [TableStructure] = []
	var bindings: Bindings = []
	
	init(connection db: MySQL) {
		database = db
	}
	
	func getBinding(for expr: Expression) throws -> String {
		bindings.append(("?", expr))
		return "?"
	}
	
	func quote(identifier: String) throws -> String {
		return "`\(identifier)`"
	}
	
	func getCreateTableSQL(forTable: TableStructure, policy: TableCreatePolicy) throws -> [String] {
		parentTableStack.append(forTable)
		defer {
			parentTableStack.removeLast()
		}
		var sub: [String]
		if !policy.contains(.shallow) {
			sub = try forTable.subTables.flatMap { try getCreateTableSQL(forTable: $0, policy: policy) }
		} else {
			sub = []
		}
		if policy.contains(.dropTable) {
			sub += ["DROP TABLE IF EXISTS \(try quote(identifier: forTable.tableName))"]
		}
		if !policy.contains(.dropTable),
			policy.contains(.reconcileTable),
			let existingColumns = getExistingColumnData(forTable: forTable.tableName) {
			let existingColumnMap: [String:MySQLColumnInfo] = .init(uniqueKeysWithValues: existingColumns.map { ($0.field, $0) })
			let newColumnMap: [String:TableStructure.Column] = .init(uniqueKeysWithValues: forTable.columns.map { ($0.name.lowercased(), $0) })
			
			let addColumns = newColumnMap.keys.filter { existingColumnMap[$0] == nil }
			let removeColumns: [String] = existingColumnMap.keys.filter { newColumnMap[$0] == nil }
			
			sub += try removeColumns.map {
				return """
				ALTER TABLE \(try quote(identifier: forTable.tableName)) DROP COLUMN \(try quote(identifier: $0))
				"""
			}
			sub += try addColumns.compactMap { newColumnMap[$0] }.map {
				let nameType = try getColumnDefinition($0)
				return """
				ALTER TABLE \(try quote(identifier: forTable.tableName)) ADD COLUMN \(nameType)
				"""
			}
			return sub
		} else {
			sub += [
				"""
				CREATE TABLE IF NOT EXISTS \(try quote(identifier: forTable.tableName)) (
				\(try forTable.columns.map { try getColumnDefinition($0) }.joined(separator: ",\n\t"))
				)
				"""]
		}
		return sub
	}
	
	func getCreateIndexSQL(forTable name: String, on columns: [String], unique: Bool) throws -> [String] {
		let stat =
		"""
		CREATE \(unique ? "UNIQUE " : "")INDEX \(try quote(identifier: "index_\(columns.joined(separator: "_"))"))
		ON \(try quote(identifier: name)) (\(try columns.map{try quote(identifier: $0)}.joined(separator: ",")))
		"""
		return [stat]
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
			typeName = "longtext"
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
			}
		}
		let addendum: String
		if column.properties.contains(.primaryKey) {
			addendum = " PRIMARY KEY"
		} else if !column.optional {
			addendum = " NOT NULL"
		} else {
			addendum = ""
		}
		return "\(try quote(identifier: name)) \(typeName)\(addendum)"
	}
}

typealias MySQLColumnMap = [String:Int]

struct MySQLDirectExeDelegate: SQLExeDelegate {
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
		return false
	}
	func next<A>() throws -> KeyedDecodingContainer<A>? where A : CodingKey {
		return nil
	}
}

class MySQLStmtExeDelegate: SQLExeDelegate {
	let connection: MySQL
	let statement: MySQLStmt
	var results: MySQLStmt.Results?
	let columnMap: MySQLColumnMap
	init(connection c: MySQL, stat: MySQLStmt) {
		connection = c
		statement = stat
		var m = MySQLColumnMap()
		let inv = stat.fieldNames()
		inv.forEach {
			let (pos, name) = $0
			m[name] = pos
		}
		columnMap = m
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
			results = statement.results()
		}
		if results?.fetchRow() ?? false {
			return true
		}
		statement.reset()
		results = nil
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

public struct MySQLDatabaseConfiguration: DatabaseConfigurationProtocol {
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
	
	public func sqlExeDelegate(forSQL: String) throws -> SQLExeDelegate {
		let noPrepCommands = ["CREATE", "DROP", "ALTER", "BEGIN", "COMMIT", "ROLLBACK"]
		if nil != noPrepCommands.first(where: { forSQL.hasPrefix($0) }) {
			return MySQLDirectExeDelegate(connection: connection, sql: forSQL)
		}
		let stat = MySQLStmt(connection)
		guard stat.prepare(statement: forSQL) else {
			throw MySQLCRUDError("Could not prepare statement. \(stat.errorMessage())")
		}
		return MySQLStmtExeDelegate(connection: connection, stat: stat)
	}
}

extension Date {
	func mysqlFormatted() -> String {
		let dateFormatter = DateFormatter()
		dateFormatter.locale = Locale(identifier: "en_US_POSIX")
		dateFormatter.timeZone = TimeZone(abbreviation: "GMT")
		dateFormatter.dateFormat = "yyyy-MM-dd HH:mm:ss"
		let ret = dateFormatter.string(from: self)
		return ret
	}
	
	init?(fromMysqlFormatted string: String) {
		let dateFormatter = DateFormatter()
		dateFormatter.locale = Locale(identifier: "en_US_POSIX")
		dateFormatter.timeZone = TimeZone.current
		dateFormatter.dateFormat = "yyyy-MM-dd HH:mm:ss"
		if let d = dateFormatter.date(from: string) {
			self = d
			return
		}
		return nil
	}
}



