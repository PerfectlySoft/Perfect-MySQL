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

import mysqlclient

/// Converts between UTF-8 bytes and String. Invalid UTF-8 is replaced with U+FFFD rather than truncating.
struct UTF8Encoding {
	/// Use a character sequence to create a String. Invalid UTF-8 is replaced with U+FFFD.
	static func encode<S : Sequence>(bytes byts: S) -> String where S.Iterator.Element == UTF8.CodeUnit {
		return String(decoding: Array(byts), as: UTF8.self)
	}
	
	/// Create a String from a buffer. Invalid UTF-8 is replaced with U+FFFD.
	static func encode(_ ptr: UnsafePointer<UInt8>, count: Int) -> String {
		return String(decoding: UnsafeBufferPointer(start: ptr, count: count), as: UTF8.self)
	}
	
	/// Decode a String into an array of UInt8.
	static func decode(string str: String) -> Array<UInt8> {
		return [UInt8](str.utf8)
	}
}

/// True for columns whose values are raw bytes: string and BLOB types in the binary character set
/// (BINARY, VARBINARY, BLOB, CAST(... AS BINARY), ...), plus BIT and GEOMETRY. Numeric and temporal
/// types also report the binary character set but aren't bytes.
func mysqlFieldIsBinary(_ field: UnsafeMutablePointer<MYSQL_FIELD>) -> Bool {
	switch field.pointee.type {
	case MYSQL_TYPE_BIT, MYSQL_TYPE_GEOMETRY:
		return true
	case MYSQL_TYPE_TINY_BLOB, MYSQL_TYPE_MEDIUM_BLOB, MYSQL_TYPE_LONG_BLOB, MYSQL_TYPE_BLOB,
		 MYSQL_TYPE_STRING, MYSQL_TYPE_VAR_STRING, MYSQL_TYPE_VARCHAR:
		return field.pointee.charsetnr == 63 /* binary */
	default:
		return false
	}
}

/// enum for mysql options
public enum MySQLOpt: Sendable {
	case MYSQL_OPT_CONNECT_TIMEOUT, MYSQL_OPT_COMPRESS, MYSQL_OPT_NAMED_PIPE,
	MYSQL_INIT_COMMAND, MYSQL_READ_DEFAULT_FILE, MYSQL_READ_DEFAULT_GROUP,
	MYSQL_SET_CHARSET_DIR, MYSQL_SET_CHARSET_NAME, MYSQL_OPT_LOCAL_INFILE,
	MYSQL_OPT_PROTOCOL, MYSQL_SHARED_MEMORY_BASE_NAME, MYSQL_OPT_READ_TIMEOUT,
	MYSQL_OPT_WRITE_TIMEOUT, MYSQL_OPT_USE_RESULT,
	//MYSQL_OPT_USE_REMOTE_CONNECTION, MYSQL_OPT_USE_EMBEDDED_CONNECTION,
	//MYSQL_OPT_GUESS_CONNECTION, MYSQL_SET_CLIENT_IP, MYSQL_SECURE_AUTH,
	MYSQL_REPORT_DATA_TRUNCATION, MYSQL_OPT_RECONNECT,
	//MYSQL_OPT_SSL_VERIFY_SERVER_CERT,
 	MYSQL_PLUGIN_DIR, MYSQL_DEFAULT_AUTH,
	MYSQL_OPT_BIND,
    MYSQL_OPT_SSL_MODE,
	MYSQL_OPT_SSL_KEY, MYSQL_OPT_SSL_CERT,
	MYSQL_OPT_SSL_CA, MYSQL_OPT_SSL_CAPATH, MYSQL_OPT_SSL_CIPHER,
	MYSQL_OPT_SSL_CRL, MYSQL_OPT_SSL_CRLPATH,
	MYSQL_OPT_CONNECT_ATTR_RESET, MYSQL_OPT_CONNECT_ATTR_ADD,
	MYSQL_OPT_CONNECT_ATTR_DELETE,
	MYSQL_SERVER_PUBLIC_KEY,
	MYSQL_ENABLE_CLEARTEXT_PLUGIN,
	MYSQL_OPT_CAN_HANDLE_EXPIRED_PASSWORDS
}

/// enum for mysql server options
public enum MySQLServerOpt: Sendable {
    case MYSQL_OPTION_MULTI_STATEMENTS_ON, MYSQL_OPTION_MULTI_STATEMENTS_OFF
}

