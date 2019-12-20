//
//  MySQL.swift
//  PerfectMySQL
//
//  Created by Kyle Jessup on 2018-03-07.
//

#if os(Linux)
	import SwiftGlibc
#else
	import Darwin
#endif
import mysqlclient

/// Provide access to MySQL connector functions
public final class MySQL {
	private static var initOnce: Bool = {
		mysql_server_init(0, nil, nil)
		return true
	}()
	
	var mysqlPtr: UnsafeMutablePointer<MYSQL>
	/// Create mysql server connection and set ptr
	public init() {
		_ = MySQL.initOnce
		mysqlPtr = mysql_init(nil)
	}
	
	deinit {
		mysql_close(mysqlPtr)
	}
	
	/// Returns client info from mysql_get_client_info
	public static func clientInfo() -> String {
		return String(validatingUTF8: mysql_get_client_info()) ?? ""
	}
	
	public func ping() -> Bool {
		return 0 == mysql_ping(mysqlPtr)
	}
	
	@available(*, deprecated)
	public func close() {}
	
	/// Return mysql error number
	public func errorCode() -> UInt32 {
		return mysql_errno(mysqlPtr)
	}
	/// Return mysql error message
	public func errorMessage() -> String {
		return String(validatingUTF8: mysql_error(mysqlPtr)) ?? ""
	}
	
	/// Return mysql server version
	public func serverVersion() -> Int {
		return Int(mysql_get_server_version(mysqlPtr))
	}
	
	/// Connects to a MySQL server
	public func connect(host: String? = nil, user: String? = nil, password: String? = nil, db: String? = nil, port: UInt32 = 0, socket: String? = nil, flag: UInt = 0) -> Bool {
		let check = mysql_real_connect(mysqlPtr,
									   host, user, password,
									   db, port,
									   socket, flag)
		return check != nil && check == mysqlPtr
	}
	
	/// Selects a database
	public func selectDatabase(named: String) -> Bool {
		return 0 == mysql_select_db(mysqlPtr, named)
	}
	
	/// Returns table names matching an optional simple regular expression as an array of Strings
	public func listTables(wildcard wild: String? = nil) -> [String] {
		var result = [String]()
		if let res = mysql_list_tables(mysqlPtr, wild) {
			while let row = mysql_fetch_row(res) {
				if let tabPtr = row[0] {
					result.append(String(validatingUTF8: tabPtr) ?? "")
				}
			}
			mysql_free_result(res)
		}
		return result
	}
	
	/// Returns database names matching an optional simple regular expression in an array of Strings
	public func listDatabases(wildcard wild: String? = nil) -> [String] {
		var result = [String]()
		if let res = mysql_list_dbs(mysqlPtr, wild) {
			while let row = mysql_fetch_row(res) {
				if let tabPtr = row[0] {
					result.append(String(validatingUTF8: tabPtr) ?? "")
				}
			}
			mysql_free_result(res)
		}
		return result
	}

	/// If false it starts transaction which can be rolled back
	public func autocommit(_ autocommit: Bool) -> Bool {
		return 1 == mysql_autocommit(mysqlPtr, autocommit ? 1 : 0)
	}

	/// Commits the transaction
	public func commit() -> Bool {
		var res = mysql_commit(mysqlPtr)
    	var FALSE = 0
    	return memcmp(&res, &FALSE, MemoryLayout.size(ofValue: res)) != 0
	}
	
	/// Rolls back the transaction
	public func rollback() -> Bool {
		var res = mysql_rollback(mysqlPtr)
    	var FALSE = 0
    	return memcmp(&res, &FALSE, MemoryLayout.size(ofValue: res)) != 0
	}
	
	/// Checks whether any more results exist
	public func moreResults() -> Bool {
		var res = mysql_more_results(mysqlPtr)
    	var FALSE = 0
    	return memcmp(&res, &FALSE, MemoryLayout.size(ofValue: res)) != 0
	}
	
	/// Returns/initiates the next result in multiple-result executions
	public func nextResult() -> Int {
		return Int(mysql_next_result(mysqlPtr))
	}
	
	/// Executes an SQL query using the specified string
	public func query(statement stmt: String) -> Bool {
		return 0 == mysql_real_query(mysqlPtr, stmt, UInt(stmt.utf8.count))
	}
	
	/// Retrieves a complete result set to the client
	public func storeResults() -> MySQL.Results? {
		guard let ret = mysql_store_result(mysqlPtr) else {
			return nil
		}
		return MySQL.Results(ret)
	}
 
    public func lastInsertId() -> Int64 {
        return Int64(mysql_insert_id(mysqlPtr))
    }
    
    public func numberAffectedRows() -> Int64 {
        return Int64(mysql_affected_rows(mysqlPtr))
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
		/*
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
		*/
		case MySQLOpt.MYSQL_REPORT_DATA_TRUNCATION:
			return MYSQL_REPORT_DATA_TRUNCATION
		case MySQLOpt.MYSQL_OPT_RECONNECT:
			return MYSQL_OPT_RECONNECT
		//case MySQLOpt.MYSQL_OPT_SSL_VERIFY_SERVER_CERT:
			//return MYSQL_OPT_SSL_VERIFY_SERVER_CERT
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
        case .MYSQL_OPT_SSL_MODE:
            return MYSQL_OPT_SSL_MODE
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

	func exposedOptionToMySQLServerOption(_ o: MySQLServerOpt) -> enum_mysql_set_option {
		switch o {
		case MySQLServerOpt.MYSQL_OPTION_MULTI_STATEMENTS_ON:
			return MYSQL_OPTION_MULTI_STATEMENTS_ON
		case MySQLServerOpt.MYSQL_OPTION_MULTI_STATEMENTS_OFF:
			return MYSQL_OPTION_MULTI_STATEMENTS_OFF
		}
	}

	/// Sets connect options for connect()
	@discardableResult
	public func setOption(_ option: MySQLOpt) -> Bool {
		return mysql_options(mysqlPtr, exposedOptionToMySQLOption(option), nil) == 0
	}
	
	/// Sets connect options for connect() with boolean option argument
	@discardableResult
	public func setOption(_ option: MySQLOpt, _ b: Bool) -> Bool {
		var myB = my_bool(b ? 1 : 0)
		return mysql_options(mysqlPtr, exposedOptionToMySQLOption(option), &myB) == 0
	}
	
	/// Sets connect options for connect() with integer option argument
	@discardableResult
	public func setOption(_ option: MySQLOpt, _ i: Int) -> Bool {
		var myI = UInt32(i)
		return mysql_options(mysqlPtr, exposedOptionToMySQLOption(option), &myI) == 0
	}
	
	/// Sets connect options for connect() with string option argument
	@discardableResult
	public func setOption(_ option: MySQLOpt, _ s: String) -> Bool {
		var b = false
		s.withCString { p in
			b = mysql_options(mysqlPtr, exposedOptionToMySQLOption(option), p) == 0
		}
		return b
	}
	
	/// Sets server option (must be set after connect() is called)
	@discardableResult
	public func setServerOption(_ option: MySQLServerOpt) -> Bool {
		return mysql_set_server_option(mysqlPtr, exposedOptionToMySQLServerOption(option)) == 0
	}
	
	/// Class used to manage and interact with result sets
	public final class Results: IteratorProtocol {
		var ptr: UnsafeMutablePointer<MYSQL_RES>
		public typealias Element = [String?]
		init(_ ptr: UnsafeMutablePointer<MYSQL_RES>) {
			self.ptr = ptr
		}
		deinit {
			mysql_free_result(ptr)
		}
		
		@available(*, deprecated)
		public func close() {}
		
		/// Seeks to an arbitrary row number in a query result set
		public func dataSeek(_ offset: UInt) {
			mysql_data_seek(ptr, my_ulonglong(offset))
		}
		
		/// Returns the number of rows in a result set
		public func numRows() -> Int {
			return Int(mysql_num_rows(ptr))
		}
		
		/// Returns the number of columns in a result set
		/// Returns: Int
		public func numFields() -> Int {
			return Int(mysql_num_fields(ptr))
		}
		
		/// Fetches the next row from the result set
		///     returning a String array of column names if row available
		/// Returns: optional Element
		public func next() -> Element? {
			guard let row = mysql_fetch_row(ptr),
				let lengths = mysql_fetch_lengths(ptr) else {
					return nil
			}
			var ret: [String?] = []
			for fieldIdx in 0..<numFields() {
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
			while let element = next() {
				callback(element)
			}
		}
	}
}

#if swift(>=4.1)
#else
// Added for Swift 4.0/4.1 compat
extension UnsafeMutableRawBufferPointer {
	static func allocate(byteCount: Int, alignment: Int) -> UnsafeMutableRawBufferPointer {
		return allocate(count: byteCount)
	}
}
extension UnsafeMutablePointer {
	func deallocate() {
		deallocate(capacity: 0)
	}
}
extension Collection {
	func compactMap<ElementOfResult>(_ transform: (Element) throws -> ElementOfResult?) rethrows -> [ElementOfResult] {
		return try flatMap(transform)
	}
}
#endif
