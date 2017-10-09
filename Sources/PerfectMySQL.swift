//
//  MySQL.swift
//  MySQL
//
//  Created by Kyle Jessup on 2015-10-01.
//	Copyright (C) 2015 PerfectlySoft, Inc.
//
//===----------------------------------------------------------------------===//
//
// This source file is part of the Perfect.org open source project
//
// Copyright (c) 2015 - 2016 PerfectlySoft Inc. and the Perfect project authors
// Licensed under Apache License v2.0
//
// See http://perfect.org/licensing.html for license information
//
//===----------------------------------------------------------------------===//
//

#if os(Linux)
	import SwiftGlibc
#else
	import Darwin
#endif
import mysqlclient

/// This class permits an UnsafeMutablePointer to be used as a IteratorProtocol
struct GenerateFromPointer<T> : IteratorProtocol {
	
	typealias Element = T
	
	var count = 0
	var pos = 0
	var from: UnsafeMutablePointer<T>
	
	/// Initialize given an UnsafeMutablePointer and the number of elements pointed to.
	init(from: UnsafeMutablePointer<T>, count: Int) {
		self.from = from
		self.count = count
	}
	
	/// Return the next element or nil if the sequence has been exhausted.
	mutating func next() -> Element? {
		guard count > 0 else {
			return nil
		}
		self.count -= 1
		let result = self.from[self.pos]
		self.pos += 1
		return result
	}
}

/// A generalized wrapper around the Unicode codec operations.
struct Encoding {
	/// Return a String given a character generator.
	static func encode<D : UnicodeCodec, G : IteratorProtocol>(codec inCodec: D, generator: G) -> String where G.Element == D.CodeUnit {
		var encodedString = ""
		var finished: Bool = false
		var mutableDecoder = inCodec
		var mutableGenerator = generator
		repeat {
			let decodingResult = mutableDecoder.decode(&mutableGenerator)
			switch decodingResult {
			case .scalarValue(let char):
				encodedString.append(String(char))
			case .emptyInput:
				finished = true
				/* ignore errors and unexpected values */
			case .error:
				finished = true
			}
		} while !finished
		return encodedString
	}
}

/// Utility wrapper permitting a UTF-8 character generator to encode a String. Also permits a String to be converted into a UTF-8 byte array.
struct UTF8Encoding {
	/// Use a character generator to create a String.
	static func encode<G : IteratorProtocol>(generator gen: G) -> String where G.Element == UTF8.CodeUnit {
		return Encoding.encode(codec: UTF8(), generator: gen)
	}
	
	/// Use a character sequence to create a String.
	static func encode<S : Sequence>(bytes byts: S) -> String where S.Iterator.Element == UTF8.CodeUnit {
		return encode(generator: byts.makeIterator())
	}
	
	/// Decode a String into an array of UInt8.
	static func decode(string str: String) -> Array<UInt8> {
		return [UInt8](str.utf8)
	}
}

/// enum for mysql options
public enum MySQLOpt {
	case MYSQL_OPT_CONNECT_TIMEOUT, MYSQL_OPT_COMPRESS, MYSQL_OPT_NAMED_PIPE,
		MYSQL_INIT_COMMAND, MYSQL_READ_DEFAULT_FILE, MYSQL_READ_DEFAULT_GROUP,
		MYSQL_SET_CHARSET_DIR, MYSQL_SET_CHARSET_NAME, MYSQL_OPT_LOCAL_INFILE,
		MYSQL_OPT_PROTOCOL, MYSQL_SHARED_MEMORY_BASE_NAME, MYSQL_OPT_READ_TIMEOUT,
		MYSQL_OPT_WRITE_TIMEOUT, MYSQL_OPT_USE_RESULT,
		MYSQL_OPT_USE_REMOTE_CONNECTION, MYSQL_OPT_USE_EMBEDDED_CONNECTION,
		MYSQL_OPT_GUESS_CONNECTION, MYSQL_SET_CLIENT_IP, MYSQL_SECURE_AUTH,
		MYSQL_REPORT_DATA_TRUNCATION, MYSQL_OPT_RECONNECT,
		MYSQL_OPT_SSL_VERIFY_SERVER_CERT, MYSQL_PLUGIN_DIR, MYSQL_DEFAULT_AUTH,
		MYSQL_OPT_BIND,
		MYSQL_OPT_SSL_KEY, MYSQL_OPT_SSL_CERT,
		MYSQL_OPT_SSL_CA, MYSQL_OPT_SSL_CAPATH, MYSQL_OPT_SSL_CIPHER,
		MYSQL_OPT_SSL_CRL, MYSQL_OPT_SSL_CRLPATH,
		MYSQL_OPT_CONNECT_ATTR_RESET, MYSQL_OPT_CONNECT_ATTR_ADD,
		MYSQL_OPT_CONNECT_ATTR_DELETE,
		MYSQL_SERVER_PUBLIC_KEY,
		MYSQL_ENABLE_CLEARTEXT_PLUGIN,
		MYSQL_OPT_CAN_HANDLE_EXPIRED_PASSWORDS
}

/// Provide access to MySQL connector functions
public final class MySQL {
	
	static private var dispatchOnce = pthread_once_t()
	
	var ptr: UnsafeMutablePointer<MYSQL>?
	
    /// Returns client info from mysql_get_client_info
	public static func clientInfo() -> String {
		return String(validatingUTF8: mysql_get_client_info()) ?? ""
	}
	
    private static var initOnce: Bool = {
        mysql_server_init(0, nil, nil)
        return true
    }()
    
    /// Create mysql server connection and set ptr
    public init() {
        _ = MySQL.initOnce
        self.ptr = mysql_init(nil)
    }
	
	deinit {
		self.close()
	}
	
    /// Close connection and set ptr to nil
	public func close() {
		if self.ptr != nil {
			mysql_close(self.ptr!)
			self.ptr = nil
		}
	}
	/// Return mysql error number
	public func errorCode() -> UInt32 {
		return mysql_errno(self.ptr!)
	}
	/// Return mysql error message
	public func errorMessage() -> String {
		return String(validatingUTF8: mysql_error(self.ptr!)) ?? ""
	}
	
    /// Return mysql server version
	public func serverVersion() -> Int {
		return Int(mysql_get_server_version(self.ptr!))
	}
	
	/// returns an allocated buffer holding the string's contents and the full size in bytes which was allocated
	/// An empty (but not nil) string would have a count of 1
	static func convertString(_ s: String?) -> (UnsafeMutablePointer<Int8>?, Int) {
        // this can be cleaned up with Swift 2.2 support is no longer required
		var ret: (UnsafeMutablePointer<Int8>?, Int) = (UnsafeMutablePointer<Int8>(nil as OpaquePointer?), 0)
		guard let notNilString = s else {
			return convertString("")
		}
		notNilString.withCString { p in
			var c = 0
			while p[c] != 0 {
				c += 1
			}
			c += 1
			let alloced = UnsafeMutablePointer<Int8>.allocate(capacity: c)
			alloced.initialize(to: 0)
			for i in 0..<c {
				alloced[i] = p[i]
			}
			alloced[c-1] = 0
			ret = (alloced, c)
		}
		return ret
	}
	
	func cleanConvertedString(_ pair: (UnsafeMutablePointer<Int8>?, Int)) {
		if let p0 = pair.0 , pair.1 > 0 {
			p0.deinitialize(count: pair.1)
			p0.deallocate(capacity: pair.1)
		}
	}
	
    /// Connects to a MySQL server
	public func connect(host hst: String? = nil, user: String? = nil, password: String? = nil, db: String? = nil, port: UInt32 = 0, socket: String? = nil, flag: UInt = 0) -> Bool {
		if self.ptr == nil {
			self.ptr = mysql_init(nil)
		}
		
		let hostOrBlank = MySQL.convertString(hst)
		let userOrBlank = MySQL.convertString(user)
		let passwordOrBlank = MySQL.convertString(password)
		let dbOrBlank = MySQL.convertString(db)
		let socketOrBlank = MySQL.convertString(socket)

		defer {
			self.cleanConvertedString(hostOrBlank)
			self.cleanConvertedString(userOrBlank)
			self.cleanConvertedString(passwordOrBlank)
			self.cleanConvertedString(dbOrBlank)
			self.cleanConvertedString(socketOrBlank)
		}
		
		let check = mysql_real_connect(self.ptr!, hostOrBlank.0!, userOrBlank.0!, passwordOrBlank.0!, dbOrBlank.0!, port, socketOrBlank.0!, flag)
		return check != nil && check == self.ptr
	}
	
    /// Selects a database
	public func selectDatabase(named namd: String) -> Bool {
		let r = mysql_select_db(self.ptr!, namd)
		return r == 0
	}
	
    /// Returns table names matching an optional simple regular expression in an array of Strings
	public func listTables(wildcard wild: String? = nil) -> [String] {
		var result = [String]()
		let res = (wild == nil ? mysql_list_tables(self.ptr!, nil) : mysql_list_tables(self.ptr!, wild!))
		if res != nil {
			var row = mysql_fetch_row(res)
			while row != nil {
			
			#if swift(>=3.0)
				if let tabPtr = row![0] {
					result.append(String(validatingUTF8: tabPtr) ?? "")
				}
			#else
				let tabPtr = row[0]
				if nil != tabPtr {
					result.append(String(validatingUTF8: tabPtr) ?? "")
				}
			#endif
				row = mysql_fetch_row(res)
			}
			mysql_free_result(res)
		}
		return result
	}
	
    /// Returns database names matching an optional simple regular expression in an array of Strings
	public func listDatabases(wildcard wild: String? = nil) -> [String] {
		var result = [String]()
		let res = wild == nil ? mysql_list_dbs(self.ptr!, nil) : mysql_list_dbs(self.ptr!, wild!)
		if res != nil {
			var row = mysql_fetch_row(res)
			while row != nil {
				
			#if swift(>=3.0)
				if let tabPtr = row![0] {
				result.append(String(validatingUTF8: tabPtr) ?? "")
				}
			#else
				let tabPtr = row[0]
				if nil != tabPtr {
					result.append(String(validatingUTF8: tabPtr) ?? "")
				}
			#endif
				row = mysql_fetch_row(res)
			}
			mysql_free_result(res)
		}
		return result
	}
	
    /// Commits the transaction
	public func commit() -> Bool {
		let r = mysql_commit(self.ptr!)
		return r == 1
	}
	
    /// Rolls back the transaction
	public func rollback() -> Bool {
		let r = mysql_rollback(self.ptr!)
		return r == 1
	}
	
    /// Checks whether any more results exist
	public func moreResults() -> Bool {
		let r = mysql_more_results(self.ptr!)
		return r == 1
	}
	
    /// Returns/initiates the next result in multiple-result executions
	public func nextResult() -> Int {
		let r = mysql_next_result(self.ptr!)
		return Int(r)
	}
	
    /// Executes an SQL query using the specified string
	public func query(statement stmt: String) -> Bool {
		let r = mysql_real_query(self.ptr!, stmt, UInt(stmt.utf8.count))
		return r == 0
	}
	
    /// Retrieves a complete result set to the client
    public func storeResults() -> MySQL.Results? {
	#if swift(>=3.0)
		guard let ret = mysql_store_result(self.ptr) else {
            return nil
        }
	#else
		let ret = mysql_store_result(self.ptr!)
		guard nil != ret else {
			return nil
		}
	#endif
		return MySQL.Results(ret)
	}
	
	func exposedOptionToMySQLOption(_ o: MySQLOpt) -> mysql_option {
		switch o {
		case MySQLOpt.MYSQL_OPT_CONNECT_TIMEOUT:
			return MYSQL_OPT_CONNECT_TIMEOUT
		case MySQLOpt.MYSQL_OPT_COMPRESS:
			return MYSQL_OPT_COMPRESS
		case MySQLOpt.MYSQL_OPT_NAMED_PIPE:
			return MYSQL_OPT_NAMED_PIPE
		case MySQLOpt.MYSQL_INIT_COMMAND:
			return MYSQL_INIT_COMMAND
		case MySQLOpt.MYSQL_READ_DEFAULT_FILE:
			return MYSQL_READ_DEFAULT_FILE
		case MySQLOpt.MYSQL_READ_DEFAULT_GROUP:
			return MYSQL_READ_DEFAULT_GROUP
		case MySQLOpt.MYSQL_SET_CHARSET_DIR:
			return MYSQL_SET_CHARSET_DIR
		case MySQLOpt.MYSQL_SET_CHARSET_NAME:
			return MYSQL_SET_CHARSET_NAME
		case MySQLOpt.MYSQL_OPT_LOCAL_INFILE:
			return MYSQL_OPT_LOCAL_INFILE
		case MySQLOpt.MYSQL_OPT_PROTOCOL:
			return MYSQL_OPT_PROTOCOL
		case MySQLOpt.MYSQL_SHARED_MEMORY_BASE_NAME:
			return MYSQL_SHARED_MEMORY_BASE_NAME
		case MySQLOpt.MYSQL_OPT_READ_TIMEOUT:
			return MYSQL_OPT_READ_TIMEOUT
		case MySQLOpt.MYSQL_OPT_WRITE_TIMEOUT:
			return MYSQL_OPT_WRITE_TIMEOUT
		case MySQLOpt.MYSQL_OPT_USE_RESULT:
			return MYSQL_OPT_USE_RESULT
		case MySQLOpt.MYSQL_OPT_USE_REMOTE_CONNECTION:
			return MYSQL_OPT_USE_REMOTE_CONNECTION
		case MySQLOpt.MYSQL_OPT_USE_EMBEDDED_CONNECTION:
			return MYSQL_OPT_USE_EMBEDDED_CONNECTION
		case MySQLOpt.MYSQL_OPT_GUESS_CONNECTION:
			return MYSQL_OPT_GUESS_CONNECTION
		case MySQLOpt.MYSQL_SET_CLIENT_IP:
			return MYSQL_SET_CLIENT_IP
		case MySQLOpt.MYSQL_SECURE_AUTH:
			return MYSQL_SECURE_AUTH
		case MySQLOpt.MYSQL_REPORT_DATA_TRUNCATION:
			return MYSQL_REPORT_DATA_TRUNCATION
		case MySQLOpt.MYSQL_OPT_RECONNECT:
			return MYSQL_OPT_RECONNECT
		case MySQLOpt.MYSQL_OPT_SSL_VERIFY_SERVER_CERT:
			return MYSQL_OPT_SSL_VERIFY_SERVER_CERT
		case MySQLOpt.MYSQL_PLUGIN_DIR:
			return MYSQL_PLUGIN_DIR
		case MySQLOpt.MYSQL_DEFAULT_AUTH:
			return MYSQL_DEFAULT_AUTH
		case MySQLOpt.MYSQL_OPT_BIND:
			return MYSQL_OPT_BIND
		case MySQLOpt.MYSQL_OPT_SSL_KEY:
			return MYSQL_OPT_SSL_KEY
		case MySQLOpt.MYSQL_OPT_SSL_CERT:
			return MYSQL_OPT_SSL_CERT
		case MySQLOpt.MYSQL_OPT_SSL_CA:
			return MYSQL_OPT_SSL_CA
		case MySQLOpt.MYSQL_OPT_SSL_CAPATH:
			return MYSQL_OPT_SSL_CAPATH
		case MySQLOpt.MYSQL_OPT_SSL_CIPHER:
			return MYSQL_OPT_SSL_CIPHER
		case MySQLOpt.MYSQL_OPT_SSL_CRL:
			return MYSQL_OPT_SSL_CRL
		case MySQLOpt.MYSQL_OPT_SSL_CRLPATH:
			return MYSQL_OPT_SSL_CRLPATH
		case MySQLOpt.MYSQL_OPT_CONNECT_ATTR_RESET:
			return MYSQL_OPT_CONNECT_ATTR_RESET
		case MySQLOpt.MYSQL_OPT_CONNECT_ATTR_ADD:
			return MYSQL_OPT_CONNECT_ATTR_ADD
		case MySQLOpt.MYSQL_OPT_CONNECT_ATTR_DELETE:
			return MYSQL_OPT_CONNECT_ATTR_DELETE
		case MySQLOpt.MYSQL_SERVER_PUBLIC_KEY:
			return MYSQL_SERVER_PUBLIC_KEY
		case MySQLOpt.MYSQL_ENABLE_CLEARTEXT_PLUGIN:
			return MYSQL_ENABLE_CLEARTEXT_PLUGIN
		case MySQLOpt.MYSQL_OPT_CAN_HANDLE_EXPIRED_PASSWORDS:
			return MYSQL_OPT_CAN_HANDLE_EXPIRED_PASSWORDS
		}
	}
	
    /// Sets connect options for connect()
	public func setOption(_ option: MySQLOpt) -> Bool {
		return mysql_options(self.ptr!, exposedOptionToMySQLOption(option), nil) == 0
	}
	
    /// Sets connect options for connect() with boolean option argument
	public func setOption(_ option: MySQLOpt, _ b: Bool) -> Bool {
		var myB = my_bool(b ? 1 : 0)
		return mysql_options(self.ptr!, exposedOptionToMySQLOption(option), &myB) == 0
	}
	
    /// Sets connect options for connect() with integer option argument
	public func setOption(_ option: MySQLOpt, _ i: Int) -> Bool {
		var myI = UInt32(i)
		return mysql_options(self.ptr!, exposedOptionToMySQLOption(option), &myI) == 0
	}
	
    /// Sets connect options for connect() with string option argument
	public func setOption(_ option: MySQLOpt, _ s: String) -> Bool {
		var b = false
		s.withCString { p in
			b = mysql_options(self.ptr!, exposedOptionToMySQLOption(option), p) == 0
		}
		return b
	}
	
    /// Class used to manage and interact with result sets
	public final class Results: IteratorProtocol {
		var ptr: UnsafeMutablePointer<MYSQL_RES>?
		
		public typealias Element = [String?]
		
		init(_ ptr: UnsafeMutablePointer<MYSQL_RES>) {
			self.ptr = ptr
		}
		
		deinit {
			self.close()
		}
		
        /// close result set by releasing the results
		public func close() {
			if self.ptr != nil {
				mysql_free_result(self.ptr!)
				self.ptr = nil
			}
		}
		
        /// Seeks to an arbitrary row number in a query result set
		public func dataSeek(_ offset: UInt) {
			mysql_data_seek(self.ptr!, my_ulonglong(offset))
		}
		
        /// Returns the number of rows in a result set
		public func numRows() -> Int {
			return Int(mysql_num_rows(self.ptr!))
		}
		
        /// Returns the number of columns in a result set
        /// Returns: Int
		public func numFields() -> Int {
			return Int(mysql_num_fields(self.ptr!))
		}
		
        /// Fetches the next row from the result set
        ///     returning a String array of column names if row available
        /// Returns: optional Element
		public func next() -> Element? {
			guard let row = mysql_fetch_row(self.ptr), let lengths = mysql_fetch_lengths(self.ptr) else {
				return nil
			}
			
			var ret = [String?]()
			for fieldIdx in 0..<self.numFields() {
				let length = lengths[fieldIdx]
				let rowVal = row[fieldIdx]
				let len = Int(length)
				if let raw = rowVal {
					let s = raw.withMemoryRebound(to: UInt8.self, capacity: len) { UTF8Encoding.encode(generator: GenerateFromPointer(from: $0, count: len)) }
					ret.append(s)
                } else {
                    ret.append(nil)
                }
			}
			return ret
		}
		
        /// passes a string array of the column names to the callback provided
		public func forEachRow(callback: (Element) -> ()) {
			while let element = self.next() {
				callback(element)
			}
		}
	}
}

/// handles mysql prepared statements
public final class MySQLStmt {
	private var ptr: UnsafeMutablePointer<MYSQL_STMT>?
	private var paramBinds = UnsafeMutablePointer<MYSQL_BIND>(nil as OpaquePointer?)
	private var paramBindsOffset = 0
	var meta: UnsafeMutablePointer<MYSQL_RES>?
	
	public func fieldNames() -> [Int: String] {

		var fieldDictionary = [Int: String]()
		let fields = mysql_fetch_fields(mysql_stmt_result_metadata(self.ptr!))
		let columnCount = Int(self.fieldCount())

		var i = 0

		while i != columnCount {
			fieldDictionary[i] = String(cString: fields![i].name)
			i += 1
		}

		return fieldDictionary

	}
	
	public enum FieldType {
		case integer,
		double,
		bytes,
		string,
		date,
		null
	}
	
	public struct FieldInfo {
		public let name: String
		public let type: FieldType
	}
	
	public func fieldInfo(index: Int) -> FieldInfo? {
		let fieldCount = Int(self.fieldCount())
		guard index < fieldCount else {
			return nil
		}
		guard let field = mysql_fetch_field_direct(meta, UInt32(index)) else {
			return nil
		}
		let f: MYSQL_FIELD = field.pointee
		return FieldInfo(name: String(validatingUTF8: f.name) ?? "invalid field name", type: mysqlTypeToFieldType(field))
	}
	
	func mysqlTypeToFieldType(_ field: UnsafeMutablePointer<MYSQL_FIELD>) -> FieldType {
		switch field.pointee.type {
		case MYSQL_TYPE_NULL:
			return .null
		case MYSQL_TYPE_FLOAT,
		     MYSQL_TYPE_DOUBLE:
			return .double
		case MYSQL_TYPE_TINY,
		     MYSQL_TYPE_SHORT,
		     MYSQL_TYPE_LONG,
		     MYSQL_TYPE_INT24,
		     MYSQL_TYPE_LONGLONG:
			return .integer
		case MYSQL_TYPE_TIMESTAMP,
		     MYSQL_TYPE_DATE,
		     MYSQL_TYPE_TIME,
		     MYSQL_TYPE_DATETIME,
		     MYSQL_TYPE_YEAR,
		     MYSQL_TYPE_NEWDATE:
			return .date
		case MYSQL_TYPE_DECIMAL,
		     MYSQL_TYPE_NEWDECIMAL:
			return .string
		case MYSQL_TYPE_TINY_BLOB,
		     MYSQL_TYPE_MEDIUM_BLOB,
		     MYSQL_TYPE_LONG_BLOB,
		     MYSQL_TYPE_BLOB:
			if ( (field.pointee.flags & UInt32(BINARY_FLAG)) != 0)
			{
				return .bytes
			}
			fallthrough
		default:
			return .string
		}
	}
	
    /// Possible status for fetch results
	public enum FetchResult {
		case OK, Error, NoData, DataTruncated
	}
	
    /// initialize mysql statement structure
	public init(_ mysql: MySQL) {
		self.ptr = mysql_stmt_init(mysql.ptr!)
	}
	
	deinit {
		self.close()
	}
	
    /// close and free mysql statement structure pointer
	public func close() {
		clearBinds()
		if let meta = self.meta {
			mysql_free_result(meta)
			self.meta = nil
		}
		if self.ptr != nil {
			mysql_stmt_close(self.ptr!)
			self.ptr = nil
		}
	}
	
    /// Resets the statement buffers in the server
	public func reset() {
		clearBinds()
		mysql_stmt_reset(self.ptr!)
	}
	
	func clearBinds() {
		let count = self.paramBindsOffset
		if let paramBinds = self.paramBinds , count > 0 {
			for i in 0..<count {
				
				let bind = paramBinds[i]
			
				switch bind.buffer_type.rawValue {
				case MYSQL_TYPE_DOUBLE.rawValue:
					bind.buffer.assumingMemoryBound(to: Double.self).deallocate(capacity: 1)
				case MYSQL_TYPE_LONGLONG.rawValue:
					if bind.is_unsigned == 1 {
						bind.buffer.assumingMemoryBound(to: UInt64.self).deallocate(capacity: 1)
					} else {
						bind.buffer.assumingMemoryBound(to: Int64.self).deallocate(capacity: 1)
					}
				case MYSQL_TYPE_VAR_STRING.rawValue,
					MYSQL_TYPE_DATE.rawValue,
					MYSQL_TYPE_DATETIME.rawValue:
					bind.buffer.assumingMemoryBound(to: Int8.self).deallocate(capacity: Int(bind.buffer_length))
				case MYSQL_TYPE_LONG_BLOB.rawValue,
				     MYSQL_TYPE_NULL.rawValue:
					()
				default:
					assertionFailure("Unhandled MySQL type \(bind.buffer_type)")
				}
				
				if bind.length != nil {
					bind.length.deallocate(capacity: 1)
				}
			}
			paramBinds.deinitialize(count: count)
			paramBinds.deallocate(capacity: count)
			self.paramBinds = nil
			self.paramBindsOffset = 0
		}
	}
	
    /// Free the resources allocated to a statement handle
	public func freeResult() {
		mysql_stmt_free_result(self.ptr!)
	}
	
    /// Returns the error number for the last statement execution
	public func errorCode() -> UInt32 {
		return mysql_stmt_errno(self.ptr!)
	}
	
    /// Returns the error message for the last statement execution
	public func errorMessage() -> String {
		return String(validatingUTF8: mysql_stmt_error(self.ptr!)) ?? ""
	}
	
    /// Prepares an SQL statement string for execution
	public func prepare(statement query: String) -> Bool {
		let utf8Chars = query.utf8
		let r = mysql_stmt_prepare(self.ptr!, query, UInt(utf8Chars.count))
		guard r == 0 else {
			return false
		}
		if let meta = self.meta {
			mysql_free_result(meta)
		}
		self.meta = mysql_stmt_result_metadata(ptr!)
		let count = self.paramCount()
		if count > 0 {
			self.paramBinds = UnsafeMutablePointer<MYSQL_BIND>.allocate(capacity: count)
			let initBind = MYSQL_BIND()
			for i in 0..<count {
				self.paramBinds?.advanced(by: i).initialize(to: initBind)
			}
		}
		return true
	}
	
    /// Executes a prepared statement, binding parameters if needed
	public func execute() -> Bool {
        
        guard let ptr = self.ptr else {
            return false
        }
        
		if self.paramBindsOffset > 0 {
            
            guard let paramBinds = self.paramBinds else {
                return false
            }
            
			guard 0 == mysql_stmt_bind_param(ptr, paramBinds) else {
				return false
			}
		}
		let r = mysql_stmt_execute(self.ptr!)
		return r == 0
	}
	
    /// returns current results
	public func results() -> MySQLStmt.Results {
		return Results(self)
	}
	
    /// Fetches the next row of data from a result set and returns status
	public func fetch() -> FetchResult {
		let r = mysql_stmt_fetch(self.ptr!)
		switch r {
		case 0:
			return .OK
		case 1:
			return .Error
		case MYSQL_NO_DATA:
			return .NoData
		case MYSQL_DATA_TRUNCATED:
			return .DataTruncated
		default:
			return .Error
		}
	}
	
    /// Returns the row count from a buffered statement result set
	public func numRows() -> UInt {
		return UInt(mysql_stmt_num_rows(self.ptr!))
	}
	
    /// Returns the number of rows changed, deleted, or inserted by prepared UPDATE, DELETE, or INSERT statement
	public func affectedRows() -> UInt {
		return UInt(mysql_stmt_affected_rows(self.ptr!))
	}
	
    /// Returns the ID generated for an AUTO_INCREMENT column by a prepared statement
	public func insertId() -> UInt {
		return UInt(mysql_stmt_insert_id(self.ptr!))
	}
	
    /// Returns the number of result columns for the most recent statement
	public func fieldCount() -> UInt {
		return UInt(mysql_stmt_field_count(self.ptr!))
	}
	
    /// Returns/initiates the next result in a multiple-result execution
	public func nextResult() -> Int {
		let r = mysql_stmt_next_result(self.ptr!)
		return Int(r)
	}
	
    /// Seeks to an arbitrary row number in a statement result set
	public func dataSeek(offset: Int) {
		mysql_stmt_data_seek(self.ptr!, my_ulonglong(offset))
	}
	
    /// Returns the number of parameters in a prepared statement
	public func paramCount() -> Int {
		let r = mysql_stmt_param_count(self.ptr!)
		return Int(r)
	}
	
	func bindParam(_ s: String, type: enum_field_types) {
		let convertedTup = MySQL.convertString(s)
		self.paramBinds?[self.paramBindsOffset].buffer_type = type
		self.paramBinds?[self.paramBindsOffset].buffer_length = UInt(convertedTup.1-1)
		self.paramBinds?[self.paramBindsOffset].length = UnsafeMutablePointer<UInt>.allocate(capacity: 1)
		self.paramBinds?[self.paramBindsOffset].length.initialize(to: UInt(convertedTup.1-1))
		self.paramBinds?[self.paramBindsOffset].buffer = UnsafeMutableRawPointer(convertedTup.0!)
		
		self.paramBindsOffset += 1
	}
	
    /// create Double parameter binding
	public func bindParam(_ d: Double) {
		self.paramBinds?[self.paramBindsOffset].buffer_type = MYSQL_TYPE_DOUBLE
		self.paramBinds?[self.paramBindsOffset].buffer_length = UInt(MemoryLayout<Double>.size)
		let a = UnsafeMutablePointer<Double>.allocate(capacity: 1)
		a.initialize(to: d)
		self.paramBinds?[self.paramBindsOffset].buffer = UnsafeMutableRawPointer(a)
		
		self.paramBindsOffset += 1
    }
    
    /// create Int parameter binding
    public func bindParam(_ i: Int) {
        self.paramBinds?[self.paramBindsOffset].buffer_type = MYSQL_TYPE_LONGLONG
        self.paramBinds?[self.paramBindsOffset].buffer_length = UInt(MemoryLayout<Int64>.size)
        let a = UnsafeMutablePointer<Int64>.allocate(capacity: 1)
        a.initialize(to: Int64(i))
        self.paramBinds?[self.paramBindsOffset].buffer = UnsafeMutableRawPointer(a)
        
        self.paramBindsOffset += 1
    }
    
    /// create UInt64 parameter binding
    public func bindParam(_ i: UInt64) {
        self.paramBinds?[self.paramBindsOffset].buffer_type = MYSQL_TYPE_LONGLONG
        self.paramBinds?[self.paramBindsOffset].buffer_length = UInt(MemoryLayout<UInt64>.size)
        let a = UnsafeMutablePointer<UInt64>.allocate(capacity: 1)
        a.initialize(to: UInt64(i))
        self.paramBinds?[self.paramBindsOffset].is_unsigned = 1
        self.paramBinds?[self.paramBindsOffset].buffer = UnsafeMutableRawPointer(a)
        
        self.paramBindsOffset += 1
    }
	
    /// create  String parameter binding
	public func bindParam(_ s: String) {
		let convertedTup = MySQL.convertString(s)
		self.paramBinds?[self.paramBindsOffset].buffer_type = MYSQL_TYPE_VAR_STRING
		self.paramBinds?[self.paramBindsOffset].buffer_length = UInt(convertedTup.1-1)
		self.paramBinds?[self.paramBindsOffset].length = UnsafeMutablePointer<UInt>.allocate(capacity: 1)
		self.paramBinds?[self.paramBindsOffset].length.initialize(to: UInt(convertedTup.1-1))
		self.paramBinds?[self.paramBindsOffset].buffer = UnsafeMutableRawPointer(convertedTup.0!)
		
		self.paramBindsOffset += 1
	}
	
    /// create Blob parameter binding
	public func bindParam(_ b: UnsafePointer<Int8>, length: Int) {
		self.paramBinds?[self.paramBindsOffset].buffer_type = MYSQL_TYPE_LONG_BLOB
		self.paramBinds?[self.paramBindsOffset].buffer_length = UInt(length)
		self.paramBinds?[self.paramBindsOffset].length = UnsafeMutablePointer<UInt>.allocate(capacity: 1)
		self.paramBinds?[self.paramBindsOffset].length.initialize(to: UInt(length))
		self.paramBinds?[self.paramBindsOffset].buffer = UnsafeMutableRawPointer(UnsafeMutablePointer(mutating: b))
		
		self.paramBindsOffset += 1
	}
	
    /// create binary blob parameter binding
//	public func bindParam(_ b: [UInt8]) {
//		self.paramBinds?[self.paramBindsOffset].buffer_type = MYSQL_TYPE_LONG_BLOB
//		self.paramBinds?[self.paramBindsOffset].buffer_length = UInt(b.count)
//		self.paramBinds?[self.paramBindsOffset].length = UnsafeMutablePointer<UInt>.allocate(capacity: 1)
//		self.paramBinds?[self.paramBindsOffset].length.initialize(to: UInt(b.count))
//		self.paramBinds?[self.paramBindsOffset].buffer = UnsafeMutableRawPointer(UnsafeMutablePointer<UInt8>(b))
//		
//		self.paramBindsOffset += 1
//	}
	
	/// create null parameter binding
	public func bindParam() {
		self.paramBinds?[self.paramBindsOffset].buffer_type = MYSQL_TYPE_NULL
		self.paramBinds?[self.paramBindsOffset].length = UnsafeMutablePointer<UInt>.allocate(capacity: 1)
		self.paramBindsOffset += 1
	}
	
    
    //commented out Iterator Protocol until next() function can be implemented tt 04272016
	//public final class Results: IteratorProtocol {
    
    /// manage results sets for MysqlStmt
    public final class Results {
        let _UNSIGNED_FLAG = UInt32(UNSIGNED_FLAG)
		public typealias Element = [Any?]
		
		let stmt: MySQLStmt
        
        /// Field count for result set
		public let numFields: Int
		
		var meta: UnsafeMutablePointer<MYSQL_RES>? { return stmt.meta }
		let binds: UnsafeMutablePointer<MYSQL_BIND>
		
		let lengthBuffers: UnsafeMutablePointer<UInt>
		let isNullBuffers: UnsafeMutablePointer<my_bool>
		private var closed = false
		
		init(_ stmt: MySQLStmt) {
			self.stmt = stmt
			numFields = Int(stmt.fieldCount())
			binds = UnsafeMutablePointer<MYSQL_BIND>.allocate(capacity: numFields)
			lengthBuffers = UnsafeMutablePointer<UInt>.allocate(capacity: numFields)
			isNullBuffers = UnsafeMutablePointer<my_bool>.allocate(capacity: numFields)
			mysql_stmt_store_result(self.stmt.ptr!)
		}
		
		deinit {
			self.close()
		}
		
        /// Release results set
		public func close() {
			guard !closed else {
				return
			}
			closed = true
			binds.deallocate(capacity: numFields)
			lengthBuffers.deallocate(capacity: numFields)
			isNullBuffers.deallocate(capacity: numFields)
		}
		
        /// Row count for current set
		public var numRows: Int {
			return Int(self.stmt.numRows())
		}
		
		enum GeneralType {
			case integer(enum_field_types),
				double(enum_field_types),
				bytes(enum_field_types),
				string(enum_field_types),
				date(enum_field_types),
				null
		}
		
		func mysqlTypeToGeneralType(_ field: UnsafeMutablePointer<MYSQL_FIELD>) -> GeneralType {
			let type = field.pointee.type
			switch type {
			case MYSQL_TYPE_NULL:
				return .null
			case MYSQL_TYPE_FLOAT,
				MYSQL_TYPE_DOUBLE:
				return .double(type)
			case MYSQL_TYPE_TINY,
				MYSQL_TYPE_SHORT,
				MYSQL_TYPE_LONG,
				MYSQL_TYPE_INT24,
				MYSQL_TYPE_LONGLONG:
				return .integer(type)
			case MYSQL_TYPE_TIMESTAMP,
				MYSQL_TYPE_DATE,
				MYSQL_TYPE_TIME,
				MYSQL_TYPE_DATETIME,
				MYSQL_TYPE_YEAR,
				MYSQL_TYPE_NEWDATE:
				return .date(type)
			case MYSQL_TYPE_DECIMAL,
				MYSQL_TYPE_NEWDECIMAL:
				return .string(type)
			case MYSQL_TYPE_TINY_BLOB,
				MYSQL_TYPE_MEDIUM_BLOB,
				MYSQL_TYPE_LONG_BLOB,
				MYSQL_TYPE_BLOB:
				if ( (field.pointee.flags & UInt32(BINARY_FLAG)) != 0)
				{
					return .bytes(type)
				}
				fallthrough
			default:
				return .string(type)
			}
		}
		
		func mysqlTypeToGeneralType(_ type: enum_field_types) -> GeneralType {
			switch type {
			case MYSQL_TYPE_NULL:
				return .null
			case MYSQL_TYPE_FLOAT,
				MYSQL_TYPE_DOUBLE:
				return .double(type)
			case MYSQL_TYPE_TINY,
				MYSQL_TYPE_SHORT,
				MYSQL_TYPE_LONG,
				MYSQL_TYPE_INT24,
				MYSQL_TYPE_LONGLONG:
				return .integer(type)
			case MYSQL_TYPE_TIMESTAMP,
				MYSQL_TYPE_DATE,
				MYSQL_TYPE_TIME,
				MYSQL_TYPE_DATETIME,
				MYSQL_TYPE_YEAR,
				MYSQL_TYPE_NEWDATE:
				return .date(type)
			case MYSQL_TYPE_DECIMAL,
				MYSQL_TYPE_NEWDECIMAL:
				return .string(type)
			case MYSQL_TYPE_TINY_BLOB,
				MYSQL_TYPE_MEDIUM_BLOB,
				MYSQL_TYPE_LONG_BLOB,
				MYSQL_TYPE_BLOB:
				return .bytes(type)
			default:
				return .string(type)
			}
		}
		
		func bindField(_ field: UnsafeMutablePointer<MYSQL_FIELD>) -> MYSQL_BIND {
			let generalType = mysqlTypeToGeneralType(field)
			let bind = bindToType(generalType)
			return bind
		}
		
		func bindBuffer<T>(_ sourceBind: MYSQL_BIND, type: T) -> MYSQL_BIND {
			var bind = sourceBind
			bind.buffer = UnsafeMutableRawPointer(UnsafeMutablePointer<T>.allocate(capacity: 1))
			bind.buffer_length = UInt(MemoryLayout<T>.size)
			return bind
		}
        
        /// Retrieve and process each row with the provided callback
		public func forEachRow(callback: (Element) -> ()) -> Bool {
			
			let scratch = UnsafeMutableRawPointer(UnsafeMutablePointer<Int8>.allocate(capacity: 0))
			
			for i in 0..<numFields {
				guard let field = mysql_fetch_field_direct(meta, UInt32(i)) else {
					continue
				}
                let f: MYSQL_FIELD = field.pointee
				var bind = bindField(field)
				bind.length = lengthBuffers.advanced(by: i)
				bind.length.initialize(to: 0)
				bind.is_null = isNullBuffers.advanced(by: i)
				bind.is_null.initialize(to: 0)
				
				let genType = mysqlTypeToGeneralType(field)
				switch genType {
				case .double:
                    switch bind.buffer_type {
                    case MYSQL_TYPE_FLOAT:
                        bind = bindBuffer(bind, type: Float.self);
                    case MYSQL_TYPE_DOUBLE:
                        bind = bindBuffer(bind, type: Double.self);
                    default: break
                    }
                case .integer:
                    if (f.flags & _UNSIGNED_FLAG) == _UNSIGNED_FLAG {
                        bind.is_unsigned = 1
                        switch bind.buffer_type {
                        case MYSQL_TYPE_LONGLONG:
                            bind = bindBuffer(bind, type: CUnsignedLongLong.self);
                        case MYSQL_TYPE_LONG, MYSQL_TYPE_INT24:
                            bind = bindBuffer(bind, type: CUnsignedInt.self);
                        case MYSQL_TYPE_SHORT:
                            bind = bindBuffer(bind, type: CUnsignedShort.self);
                        case MYSQL_TYPE_TINY:
                            bind = bindBuffer(bind, type: CUnsignedChar.self);
                        default: break
                        }
                    } else {
                        switch bind.buffer_type {
                        case MYSQL_TYPE_LONGLONG:
                            bind = bindBuffer(bind, type: CLongLong.self);
                        case MYSQL_TYPE_LONG, MYSQL_TYPE_INT24:
                            bind = bindBuffer(bind, type: CInt.self);
                        case MYSQL_TYPE_SHORT:
                            bind = bindBuffer(bind, type: CShort.self);
                        case MYSQL_TYPE_TINY:
                            bind = bindBuffer(bind, type: CChar.self);
                        default: break
                        }
                    }
				case .bytes, .string, .date, .null:
					bind.buffer = scratch
					bind.buffer_length = 0
				}
				
				binds.advanced(by: i).initialize(to: bind)
			}
			
			defer {
				for i in 0..<numFields {
					let bind = binds[i]
					let genType = mysqlTypeToGeneralType(bind.buffer_type)
					switch genType {
					case .double:
                        switch bind.buffer_type {
                        case MYSQL_TYPE_FLOAT:
                            bind.buffer.assumingMemoryBound(to: Float.self).deallocate(capacity: 1)
                        case MYSQL_TYPE_DOUBLE:
                            bind.buffer.assumingMemoryBound(to: Double.self).deallocate(capacity: 1)
                        default: break
                        }
					case .integer:
                        if bind.is_unsigned == 1 {
                            switch bind.buffer_type {
                            case MYSQL_TYPE_LONGLONG:
                                bind.buffer.assumingMemoryBound(to: CUnsignedLongLong.self).deallocate(capacity: 1)
                            case MYSQL_TYPE_LONG, MYSQL_TYPE_INT24:
                                bind.buffer.assumingMemoryBound(to: CUnsignedInt.self).deallocate(capacity: 1)
                            case MYSQL_TYPE_SHORT:
                                bind.buffer.assumingMemoryBound(to: CUnsignedShort.self).deallocate(capacity: 1)
                            case MYSQL_TYPE_TINY:
                                bind.buffer.assumingMemoryBound(to: CUnsignedChar.self).deallocate(capacity: 1)
                            default: break
                            }
                        } else {
                            switch bind.buffer_type {
                            case MYSQL_TYPE_LONGLONG:
                                bind.buffer.assumingMemoryBound(to: CLongLong.self).deallocate(capacity: 1)
                            case MYSQL_TYPE_LONG, MYSQL_TYPE_INT24:
                                bind.buffer.assumingMemoryBound(to: CInt.self).deallocate(capacity: 1)
                            case MYSQL_TYPE_SHORT:
                                bind.buffer.assumingMemoryBound(to: CShort.self).deallocate(capacity: 1)
                            case MYSQL_TYPE_TINY:
                                bind.buffer.assumingMemoryBound(to: CChar.self).deallocate(capacity: 1)
                            default: break
                            }
                        }
					case .bytes, .string, .date, .null:
						() // do nothing. these were cleaned right after use or not allocated at all
					}
				}
			}
			
			guard 0 == mysql_stmt_bind_result(self.stmt.ptr!, binds) else {
				return false
			}
			
			while true {
				
				let fetchRes = mysql_stmt_fetch(self.stmt.ptr!)
				if fetchRes == MYSQL_NO_DATA {
					return true
				}
				if fetchRes == 1 {
					return false
				}
				
				var row = Element()
				
				for i in 0..<numFields {
					var bind = binds[i]
					let genType = mysqlTypeToGeneralType(bind.buffer_type)
					let length = Int(bind.length.pointee)
					let isNull = bind.is_null.pointee
					
					if isNull != 0 {
						row.append(nil)
					} else {
						
						switch genType {
						case .double:
                            switch bind.buffer_type {
                            case MYSQL_TYPE_FLOAT:
                                let f = bind.buffer.assumingMemoryBound(to: Float.self).pointee
                                row.append(f)
                            case MYSQL_TYPE_DOUBLE:
                                let f = bind.buffer.assumingMemoryBound(to: Double.self).pointee
                                row.append(f)
                            default: break
                            }
						case .integer:
                            if bind.is_unsigned == 1 {
                                switch bind.buffer_type {
                                case MYSQL_TYPE_LONGLONG:
                                    let i = bind.buffer.assumingMemoryBound(to: CUnsignedLongLong.self).pointee
                                    row.append(i)
                                case MYSQL_TYPE_LONG, MYSQL_TYPE_INT24:
                                    let i = bind.buffer.assumingMemoryBound(to: CUnsignedInt.self).pointee
                                    row.append(i)
                                case MYSQL_TYPE_SHORT:
                                    let i = bind.buffer.assumingMemoryBound(to: CUnsignedShort.self).pointee
                                    row.append(i)
                                case MYSQL_TYPE_TINY:
                                    let i = bind.buffer.assumingMemoryBound(to: CUnsignedChar.self).pointee
                                    row.append(i)
                                default: break
                                }
                            } else {
                                switch bind.buffer_type {
                                case MYSQL_TYPE_LONGLONG:
                                    let i = bind.buffer.assumingMemoryBound(to: CLongLong.self).pointee
                                    row.append(i)
                                case MYSQL_TYPE_LONG, MYSQL_TYPE_INT24:
                                    let i = bind.buffer.assumingMemoryBound(to: CInt.self).pointee
                                    row.append(i)
                                case MYSQL_TYPE_SHORT:
                                    let i = bind.buffer.assumingMemoryBound(to: CShort.self).pointee
                                    row.append(i)
                                case MYSQL_TYPE_TINY:
                                    let i = bind.buffer.assumingMemoryBound(to: CChar.self).pointee
                                    row.append(i)
                                default: break
                                }
                            }
						case .bytes:
							
							let raw = UnsafeMutablePointer<UInt8>.allocate(capacity: length)
							defer {
								raw.deallocate(capacity: length)
							}
							bind.buffer = UnsafeMutableRawPointer(raw)
							bind.buffer_length = UInt(length)
							
							let res = mysql_stmt_fetch_column(self.stmt.ptr!, &bind, UInt32(i), 0)
							guard res == 0 else {
								return false
							}
							
							var a = [UInt8]()
							var gen = GenerateFromPointer(from: raw, count: length)
							while let c = gen.next() {
								a.append(c)
							}
							row.append(a)
							
						case .string, .date:
							
							let raw = UnsafeMutablePointer<UInt8>.allocate(capacity: length)
							defer {
								raw.deallocate(capacity: length)
							}
							bind.buffer = UnsafeMutableRawPointer(raw)
							bind.buffer_length = UInt(length)
							
							let res = mysql_stmt_fetch_column(self.stmt.ptr!, &bind, UInt32(i), 0)
							guard res == 0 else {
								return false
							}
							
							let s = UTF8Encoding.encode(generator: GenerateFromPointer(from: raw, count: length))
							row.append(s)
							
						case .null:
							row.append(nil)
						}
					}
				}
				
				callback(row)
			}
			// @unreachable
		}
		
		func bindToType(_ type: GeneralType) -> MYSQL_BIND {
			switch type {
			case .double(let s):
				return bindToIntegral(s)
			case .integer(let s):
				return bindToIntegral(s)
			case .bytes:
				return bindToBlob()
			case .string, .date:
				return bindToString()
			case .null:
				return bindToNull()
			}
		}
		
		func bindToBlob() -> MYSQL_BIND {
			var bind = MYSQL_BIND()
			bind.buffer_type = MYSQL_TYPE_LONG_BLOB
			return bind
		}
		
		func bindToString() -> MYSQL_BIND {
			var bind = MYSQL_BIND()
			bind.buffer_type = MYSQL_TYPE_VAR_STRING
			return bind
		}
		
		func bindToNull() -> MYSQL_BIND {
			var bind = MYSQL_BIND()
			bind.buffer_type = MYSQL_TYPE_NULL
			return bind
		}
		
		func bindToIntegral(_ type: enum_field_types) -> MYSQL_BIND {
			var bind = MYSQL_BIND()
			bind.buffer_type = type
			return bind
		}
	}
}
