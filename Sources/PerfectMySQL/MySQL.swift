//
//  MySQL.swift
//  PerfectMySQL
//
//  Created by Kyle Jessup on 2018-03-07.
//

import mysqlclient
#if canImport(Glibc)
import Glibc
#elseif canImport(Darwin)
import Darwin
#endif

/// Provide access to MySQL connector functions
public final class MySQL: @unchecked Sendable {
	private static let initOnce: Bool = {
		mysql_server_init(0, nil, nil)
		return true
	}()
	
	var mysqlPtr: UnsafeMutablePointer<MYSQL>
	/// Set when MYSQL_OPT_SSL_MODE asks for TLS on libmariadb, which doesn't enforce it itself.
	var sslModeRequiresTLS = false
	/// An error from connect() that libmysqlclient doesn't know about.
	var connectError: String?
	/// The options set so far, to set again on a fresh handle after connect() refuses a connection.
	var appliedOptions: [(MySQLOpt, OptionValue)] = []
	enum OptionValue {
		case none, bool(Bool), int(UInt32), string(String)
	}
	/// Create mysql server connection and set ptr
	public init() {
		_ = MySQL.initOnce
		mysqlPtr = mysql_init(nil)
	}
	
	deinit {
		mysql_close(mysqlPtr)
	}
	
	/// Whether this build uses MariaDB Connector/C rather than libmysqlclient.
	static var usesMariaDBConnector: Bool { PERFECT_MYSQL_LIBMARIADB != 0 }
	
	/// Returns client info from mysql_get_client_info
	public static func clientInfo() -> String {
		return String(validatingCString: mysql_get_client_info()) ?? ""
	}
	
	public func ping() -> Bool {
		return 0 == mysql_ping(mysqlPtr)
	}
	
	@available(*, deprecated)
	public func close() {}
	
	/// Return mysql error number
	public func errorCode() -> UInt32 {
		let code = mysql_errno(mysqlPtr)
		if code == 0 && connectError != nil {
			return 2026 // CR_SSL_CONNECTION_ERROR
		}
		return code
	}
	/// Return mysql error message
	public func errorMessage() -> String {
		if mysql_errno(mysqlPtr) == 0, let connectError {
			return connectError
		}
		return String(validatingCString: mysql_error(mysqlPtr)) ?? ""
	}
	
	/// Return mysql server version
	public func serverVersion() -> Int {
		return Int(mysql_get_server_version(mysqlPtr))
	}
	
	/// Connects to a MySQL server
	public func connect(host: String? = nil, user: String? = nil, password: String? = nil, db: String? = nil, port: UInt32 = 0, socket: String? = nil, flag: UInt = 0) -> Bool {
		connectError = nil
		if sslModeRequiresTLS {
			// libmariadb's automatic reconnect would skip the check below.
			var off = my_bool(0)
			mysql_options(mysqlPtr, MYSQL_OPT_RECONNECT, &off)
		}
		// CLIENT_REMEMBER_OPTIONS: both libraries otherwise reset the options when a connection
		// fails, so a retry would quietly drop MYSQL_OPT_SSL_MODE (and everything else).
		let check = mysql_real_connect(mysqlPtr,
									   host, user, password,
									   db, port,
									   socket, flag | (1 << 31))
		guard check != nil && check == mysqlPtr else {
			return false
		}
		// libmariadb quietly falls back to plaintext when the server has no TLS. Refuse that
		// connection, and start a fresh handle with the same options so connect() can be retried.
		if sslModeRequiresTLS && mysql_get_ssl_cipher(mysqlPtr) == nil {
			mysql_close(mysqlPtr)
			mysqlPtr = mysql_init(nil)
			for (option, value) in appliedOptions {
				apply(option, value)
			}
			connectError = "SSL connection error: SSL is required, but the server does not support it"
			return false
		}
		return true
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
					result.append(String(validatingCString: tabPtr) ?? "")
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
					result.append(String(validatingCString: tabPtr) ?? "")
				}
			}
			mysql_free_result(res)
		}
		return result
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
		return record(option, .none)
	}
	
	/// Sets connect options for connect() with boolean option argument
	@discardableResult
	public func setOption(_ option: MySQLOpt, _ b: Bool) -> Bool {
		return record(option, .bool(b))
	}
	
	/// Sets connect options for connect() with integer option argument.
	///
	/// MYSQL_OPT_SSL_MODE takes one of MySQL's SSL_MODE_* values (1 = DISABLED ... 5 = VERIFY_IDENTITY).
	/// With MariaDB Connector/C, which has no such option, it's mapped onto MYSQL_OPT_SSL_ENFORCE and
	/// MYSQL_OPT_SSL_VERIFY_SERVER_CERT, with these differences:
	/// - REQUIRED: libmariadb doesn't refuse a server without TLS, so connect() does, but only after
	///   authenticating in plaintext. Someone able to tamper with the connection can capture the
	///   authentication exchange (or the password, if the server asks for mysql_clear_password).
	///   Use VERIFY_IDENTITY, which fails before authenticating. REQUIRED also turns off
	///   MYSQL_OPT_RECONNECT, since a reconnect could fall back to plaintext.
	/// - VERIFY_CA also checks the server's host name, except that Connector/C 3.4 checks neither the
	///   host name nor (without MYSQL_OPT_SSL_CA) the CA on local connections.
	/// - DISABLED still uses TLS if MYSQL_OPT_SSL_CA, _CERT, _KEY, _CAPATH or _CIPHER is set.
	@discardableResult
	public func setOption(_ option: MySQLOpt, _ i: Int) -> Bool {
		guard let myI = UInt32(exactly: i) else {
			return false
		}
		return record(option, .int(myI))
	}
	
	/// Sets connect options for connect() with string option argument
	@discardableResult
	public func setOption(_ option: MySQLOpt, _ s: String) -> Bool {
		return record(option, .string(s))
	}
	
	private func record(_ option: MySQLOpt, _ value: OptionValue) -> Bool {
		guard apply(option, value) else {
			return false
		}
		appliedOptions.append((option, value))
		return true
	}
	
	@discardableResult
	private func apply(_ option: MySQLOpt, _ value: OptionValue) -> Bool {
		let mysqlOption = exposedOptionToMySQLOption(option)
		switch value {
		case .none:
			return mysql_options(mysqlPtr, mysqlOption, nil) == 0
		case .bool(let b):
			var myB = my_bool(b ? 1 : 0)
			return mysql_options(mysqlPtr, mysqlOption, &myB) == 0
		case .int(var myI):
			if option == .MYSQL_OPT_SSL_MODE {
				guard perfect_mysql_set_ssl_mode(mysqlPtr, myI) == 0 else {
					return false
				}
				sslModeRequiresTLS = PERFECT_MYSQL_LIBMARIADB != 0 && myI >= SSL_MODE_REQUIRED.rawValue
				return true
			}
			return mysql_options(mysqlPtr, mysqlOption, &myI) == 0
		case .string(let s):
			return s.withCString { mysql_options(mysqlPtr, mysqlOption, $0) == 0 }
		}
	}
	
	/// Sets server option (must be set after connect() is called)
	@discardableResult
	public func setServerOption(_ option: MySQLServerOpt) -> Bool {
		return mysql_set_server_option(mysqlPtr, exposedOptionToMySQLServerOption(option)) == 0
	}
	
	/// Class used to manage and interact with result sets
	public final class Results: IteratorProtocol, @unchecked Sendable {
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
		/// Invalid UTF-8 is replaced with U+FFFD. Use `nextBytes()` for binary columns
		/// (see `fieldIsBinary(at:)`).
		/// Returns: optional Element
		public func next() -> Element? {
			return nextRow { raw, len in
				raw.withMemoryRebound(to: UInt8.self, capacity: len) { UTF8Encoding.encode($0, count: len) }
			}
		}
		
		/// Fetches the next row from the result set as the exact bytes of each column
		/// (nil for NULL). Advances the same cursor as `next()`.
		public func nextBytes() -> [[UInt8]?]? {
			return nextRow { raw, len in
				raw.withMemoryRebound(to: UInt8.self, capacity: len) { Array(UnsafeBufferPointer(start: $0, count: len)) }
			}
		}
		
		private func nextRow<T>(_ convert: (UnsafeMutablePointer<CChar>, Int) -> T) -> [T?]? {
			guard let row = mysql_fetch_row(ptr),
				let lengths = mysql_fetch_lengths(ptr) else {
					return nil
			}
			var ret: [T?] = []
			for fieldIdx in 0..<numFields() {
				if let raw = row[fieldIdx] {
					ret.append(convert(raw, Int(lengths[fieldIdx])))
				} else {
					ret.append(nil)
				}
			}
			return ret
		}
		
		/// True if the column's values are raw bytes: BINARY, VARBINARY, BLOB and other string types in
		/// the binary character set, plus BIT and GEOMETRY. These are the columns `MySQLStmt` returns as
		/// `[UInt8]`; read them here with `nextBytes()`, since `next()` can't represent them as text.
		public func fieldIsBinary(at index: Int) -> Bool {
			guard index >= 0, index < numFields(),
				let field = mysql_fetch_field_direct(ptr, UInt32(index)) else {
				return false
			}
			return mysqlFieldIsBinary(field)
		}
		
		/// passes a string array of the column names to the callback provided
		public func forEachRow(callback: (Element) -> ()) {
			while let element = next() {
				callback(element)
			}
		}
		
		/// passes each remaining row's exact column bytes to the callback provided
		public func forEachRowBytes(callback: ([[UInt8]?]) -> ()) {
			while let element = nextBytes() {
				callback(element)
			}
		}
	}
}
