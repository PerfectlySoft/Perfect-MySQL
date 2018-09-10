//
//  MySQLTests.swift
//  MySQLTests
//
//  Created by Kyle Jessup on 2015-10-20.
//  Copyright © 2015 PerfectlySoft. All rights reserved.
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

import XCTest
@testable import PerfectMySQL
import PerfectCRUD

let testDBRowCount = 5
#if os(macOS)
let testHost = "127.0.0.1"
#else
let testHost = "host.docker.internal"
#endif
let testUser = "root"
let testPassword = ""
let testDB = "test"
typealias DBConfiguration = MySQLDatabaseConfiguration
func getDB(reset: Bool = true) throws -> Database<DBConfiguration> {
	if reset {
		let db = Database(configuration: try DBConfiguration(database: "mysql",
															 host: testHost,
															 username: testUser,
															 password: testPassword))
		try db.sql("DROP DATABASE \(testDB)")
		try db.sql("CREATE DATABASE \(testDB) DEFAULT CHARACTER SET utf8mb4")
	}
	return Database(configuration: try DBConfiguration(database: testDB,
													   host: testHost,
													   username: testUser,
													   password: testPassword))
}

var rawMySQL: MySQL {
	let mysql = MySQL()
	mysql.setOption(.MYSQL_OPT_CONNECT_TIMEOUT, 5)
	mysql.setOption(.MYSQL_SET_CHARSET_NAME, "utf8mb4")
	_ = mysql.connect(host: testHost, user: testUser, password: testPassword, db: "mysql")
	_ = mysql.query(statement: "CREATE DATABASE \(testDB) DEFAULT CHARACTER SET utf8mb4")
	_ = mysql.selectDatabase(named: testDB)
	return mysql
}

class PerfectMySQLTests: XCTestCase {
	override func setUp() {
		super.setUp()
	}
	override func tearDown() {
		CRUDLogging.flush()
		super.tearDown()
	}
	
	func testConnect() {
		
		let mysql = MySQL()
		
		XCTAssert(mysql.setOption(.MYSQL_OPT_RECONNECT, true) == true)
		XCTAssert(mysql.setOption(.MYSQL_OPT_LOCAL_INFILE) == true)
		XCTAssert(mysql.setOption(.MYSQL_OPT_CONNECT_TIMEOUT, 5) == true)
		
		let res = mysql.connect(host: testHost, user: testUser, password: testPassword)
		
		XCTAssert(res)
		
		if !res {
			print(mysql.errorMessage())
			return
		}
		
		var sres = mysql.selectDatabase(named: testDB)
		if sres == false {
			sres = mysql.query(statement: "CREATE DATABASE `\(testDB)` DEFAULT CHARACTER SET utf8mb4 ;")
		}
		
		XCTAssert(sres == true)
		
		if !sres {
			print(mysql.errorMessage())
		}
	}
	
	func testListDbs1() {
		let mysql = rawMySQL
		let list = mysql.listDatabases()
		XCTAssert(list.count > 0)
	}
	
	func testListDbs2() {
		let mysql = rawMySQL
		let list = mysql.listDatabases(wildcard: "information_%")
		XCTAssert(list.count > 0)
	}
	
	func testListTables1() {
		let mysql = rawMySQL
		let sres = mysql.selectDatabase(named: "information_schema")
		XCTAssert(sres == true)
		let list = mysql.listTables()
		XCTAssert(list.count > 0)
	}
	
	func testListTables2() {
		let mysql = rawMySQL
		let sres = mysql.selectDatabase(named: "information_schema")
		XCTAssert(sres == true)
		let list = mysql.listTables(wildcard: "INNODB_%")
		XCTAssert(list.count > 0)
	}
	
	func testQuery1() {
		let mysql = rawMySQL
		XCTAssert(mysql.query(statement: "DROP TABLE IF EXISTS test"), mysql.errorMessage())
		let qres = mysql.query(statement: "CREATE TABLE test (id INT, d DOUBLE, s VARCHAR(1024))")
		XCTAssert(qres == true, mysql.errorMessage())
		let list = mysql.listTables(wildcard: "test")
		XCTAssert(list.count > 0)
		for i in 1...10 {
			let ires = mysql.query(statement: "INSERT INTO test (id,d,s) VALUES (\(i),42.9,\"Row \(i)\")")
			XCTAssert(ires == true, mysql.errorMessage())
		}
		let sres2 = mysql.query(statement: "SELECT id,d,s FROM test")
		XCTAssert(sres2 == true, mysql.errorMessage())
		guard let results = mysql.storeResults() else {
			XCTAssert(false, "mysql.storeResults() failed")
			return
		}
		XCTAssert(results.numRows() == 10)
		
		var count = 0
		while let _ = results.next() {
			count += 1
		}
		XCTAssert(count == 10)
		
		let qres2 = mysql.query(statement: "DROP TABLE test")
		XCTAssert(qres2 == true, mysql.errorMessage())
		
		let list2 = mysql.listTables(wildcard: "test")
		XCTAssert(list2.count == 0)
	}
	
	func testQuery2() {
		let mysql = rawMySQL
		XCTAssert(mysql.query(statement: "DROP TABLE IF EXISTS test"))
		
		let qres = mysql.query(statement: "CREATE TABLE test (id INT, d DOUBLE, s VARCHAR(1024))")
		XCTAssert(qres == true, mysql.errorMessage())
		
		let list = mysql.listTables(wildcard: "test")
		XCTAssert(list.count > 0)
		
		for i in 1...10 {
			let ires = mysql.query(statement: "INSERT INTO test (id,d,s) VALUES (\(i),42.9,\"Row \(i)\")")
			XCTAssert(ires == true, mysql.errorMessage())
		}
		
		let sres2 = mysql.query(statement: "SELECT id,d,s FROM test")
		XCTAssert(sres2 == true, mysql.errorMessage())
		
		guard let results = mysql.storeResults() else {
			XCTAssert(false, "mysql.storeResults() failed")
			return
		}
		XCTAssert(results.numRows() == 10)
		
		var count = 0
		results.forEachRow { a in
			count += 1
		}
		XCTAssert(count == 10)
		
		let qres2 = mysql.query(statement: "DROP TABLE test")
		XCTAssert(qres2 == true, mysql.errorMessage())
		
		let list2 = mysql.listTables(wildcard: "test")
		XCTAssert(list2.count == 0)
	}
	
	func testInsertNull() {
		let mysql = rawMySQL
		XCTAssert(mysql.query(statement: "DROP TABLE IF EXISTS test"))
		
		let qres = mysql.query(statement: "CREATE TABLE test (id INT, d DOUBLE, s VARCHAR(1024))")
		XCTAssert(qres == true, mysql.errorMessage())
		
		let list = mysql.listTables(wildcard: "test")
		XCTAssert(list.count > 0)
		
		let ires = mysql.query(statement: "INSERT INTO test (id,d,s) VALUES (1,NULL,\"Row 1\")")
		XCTAssert(ires == true, mysql.errorMessage())
		
		let sres2 = mysql.query(statement: "SELECT id,d,s FROM test")
		XCTAssert(sres2 == true, mysql.errorMessage())
		
		guard let results = mysql.storeResults() else {
			XCTAssert(false, "mysql.storeResults() failed")
			return
		}
		XCTAssert(results.numRows() == 1)
		XCTAssert(results.numFields() == 3)
		
		results.forEachRow { row in
			XCTAssert(row.count == 3)
			XCTAssertEqual(row[0], "1")
			XCTAssertNil(row[1])
			XCTAssertEqual(row[2], "Row 1")
		}
		
		let qres2 = mysql.query(statement: "DROP TABLE test")
		XCTAssert(qres2 == true, mysql.errorMessage())
		
		let list2 = mysql.listTables(wildcard: "test")
		XCTAssert(list2.count == 0)
	}
	
	func testQueryStmt1() {
		let mysql = rawMySQL
		XCTAssert(mysql.query(statement: "DROP TABLE IF EXISTS all_data_types"))
		
		let qres = mysql.query(statement: "CREATE TABLE `all_data_types` (`varchar` VARCHAR( 20 ),\n`tinyint` TINYINT,\n`text` TEXT,\n`date` DATE,\n`smallint` SMALLINT,\n`mediumint` MEDIUMINT,\n`int` INT,\n`bigint` BIGINT,\n`float` FLOAT( 10, 2 ),\n`double` DOUBLE,\n`decimal` DECIMAL( 10, 2 ),\n`datetime` DATETIME,\n`timestamp` TIMESTAMP,\n`time` TIME,\n`year` YEAR,\n`char` CHAR( 10 ),\n`tinyblob` TINYBLOB,\n`tinytext` TINYTEXT,\n`blob` BLOB,\n`mediumblob` MEDIUMBLOB,\n`mediumtext` MEDIUMTEXT,\n`longblob` LONGBLOB,\n`longtext` LONGTEXT,\n`enum` ENUM( '1', '2', '3' ),\n`set` SET( '1', '2', '3' ),\n`bool` BOOL,\n`binary` BINARY( 20 ),\n`varbinary` VARBINARY( 20 ) ) ENGINE = MYISAM")
		XCTAssert(qres == true, mysql.errorMessage())
		
		let stmt1 = MySQLStmt(mysql)
		let prepRes = stmt1.prepare(statement: "INSERT INTO all_data_types VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
		XCTAssert(prepRes, stmt1.errorMessage())
		XCTAssert(stmt1.paramCount() == 28)
		
		stmt1.bindParam("varchar 20 string")
		stmt1.bindParam(1)
		stmt1.bindParam("text string")
		stmt1.bindParam("2015-10-21")
		stmt1.bindParam(1)
		stmt1.bindParam(1)
		stmt1.bindParam(1)
		stmt1.bindParam(1)
		stmt1.bindParam(1.1)
		stmt1.bindParam(1.1)
		stmt1.bindParam(1.1)
		stmt1.bindParam("2015-10-21 12:00:00")
		stmt1.bindParam("2015-10-21 12:00:00")
		stmt1.bindParam("03:14:07")
		stmt1.bindParam("2015")
		stmt1.bindParam("K")
		
		"BLOB DATA".withCString { p in
			stmt1.bindParam(p, length: 9)
			
			stmt1.bindParam("tiny text string")
			
			stmt1.bindParam(p, length: 9)
			stmt1.bindParam(p, length: 9)
			
			stmt1.bindParam("medium text string")
			
			stmt1.bindParam(p, length: 9)
			
			stmt1.bindParam("long text string")
			stmt1.bindParam("1")
			stmt1.bindParam("2")
			stmt1.bindParam(1)
			stmt1.bindParam(0)
			stmt1.bindParam(1)
			
			let execRes = stmt1.execute()
			XCTAssert(execRes, "\(stmt1.errorCode()) \(stmt1.errorMessage()) - \(mysql.errorCode()) \(mysql.errorMessage())")
		}
	}
	
	func testQueryStmt2() {
		let mysql = rawMySQL
		XCTAssert(mysql.query(statement: "DROP TABLE IF EXISTS all_data_types"))
		
		let qres = mysql.query(statement: "CREATE TABLE `all_data_types` (`varchar` VARCHAR( 22 ),\n`tinyint` TINYINT,\n`text` TEXT,\n`date` DATE,\n`smallint` SMALLINT,\n`mediumint` MEDIUMINT,\n`int` INT,\n`bigint` BIGINT,\n`ubigint` BIGINT UNSIGNED,\n`float` FLOAT( 10, 2 ),\n`double` DOUBLE,\n`decimal` DECIMAL( 10, 2 ),\n`datetime` DATETIME,\n`timestamp` TIMESTAMP,\n`time` TIME,\n`year` YEAR,\n`char` CHAR( 10 ),\n`tinyblob` TINYBLOB,\n`tinytext` TINYTEXT,\n`blob` BLOB,\n`mediumblob` MEDIUMBLOB,\n`mediumtext` MEDIUMTEXT,\n`longblob` LONGBLOB,\n`longtext` LONGTEXT,\n`enum` ENUM( '1', '2', '3' ),\n`set` SET( '1', '2', '3' ),\n`bool` BOOL,\n`binary` BINARY( 20 ),\n`varbinary` VARBINARY( 20 ) ) ENGINE = MYISAM")
		XCTAssert(qres == true, mysql.errorMessage())
		
		for _ in 1...2 {
			let stmt1 = MySQLStmt(mysql)
			let prepRes = stmt1.prepare(statement: "INSERT INTO all_data_types VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
			XCTAssert(prepRes, stmt1.errorMessage())
			XCTAssert(stmt1.paramCount() == 29)
			
			stmt1.bindParam("varchar ’22’ string 👻")
			stmt1.bindParam(1)
			stmt1.bindParam("text string")
			stmt1.bindParam("2015-10-21")
			stmt1.bindParam(32767)
			stmt1.bindParam(8388607)
			stmt1.bindParam(2147483647)
			stmt1.bindParam(9223372036854775807)
			stmt1.bindParam(18446744073709551615 as UInt64)
			stmt1.bindParam(1.1)
			stmt1.bindParam(1.1)
			stmt1.bindParam(1.1)
			stmt1.bindParam("2015-10-21 12:00:00")
			stmt1.bindParam("2015-10-21 12:00:00")
			stmt1.bindParam("03:14:07")
			stmt1.bindParam("2015")
			stmt1.bindParam("K")
			
			"BLOB DATA".withCString { p in
				stmt1.bindParam(p, length: 9)
				stmt1.bindParam("tiny text string")
				stmt1.bindParam(p, length: 9)
				stmt1.bindParam(p, length: 9)
				stmt1.bindParam("medium text string")
				stmt1.bindParam(p, length: 9)
				stmt1.bindParam("long text string")
				stmt1.bindParam("1")
				stmt1.bindParam("2")
				stmt1.bindParam(1)
				stmt1.bindParam(1)
				stmt1.bindParam(1)
				
				let execRes = stmt1.execute()
				XCTAssert(execRes, "\(stmt1.errorCode()) \(stmt1.errorMessage()) - \(mysql.errorCode()) \(mysql.errorMessage())")
			}
		}
		
		do {
			let stmt1 = MySQLStmt(mysql)
			
			let prepRes = stmt1.prepare(statement: "SELECT * FROM all_data_types")
			XCTAssert(prepRes, stmt1.errorMessage())
			
			let execRes = stmt1.execute()
			XCTAssert(execRes, stmt1.errorMessage())
			
			let results = stmt1.results()
			
			let ok = results.forEachRow {
				e in
				
				XCTAssertEqual(e[0] as? String, "varchar ’22’ string 👻")
				XCTAssertEqual(e[1] as? Int8, 1)
				XCTAssertEqual(e[2] as? String, "text string")
				XCTAssertEqual(e[3] as? String, "2015-10-21")
				XCTAssertEqual(e[4] as? Int16, 32767)
				XCTAssertEqual(e[5] as? Int32, 8388607)
				XCTAssertEqual(e[6] as? Int32, 2147483647)
				XCTAssertEqual(e[7] as? Int64, 9223372036854775807)
				XCTAssertEqual(e[8] as? UInt64, 18446744073709551615 as UInt64)
				XCTAssertEqual(e[9] as? Float, 1.1)
				XCTAssertEqual(e[10] as? Double, 1.1)
				XCTAssertEqual(e[11] as? String, "1.10")
				XCTAssertEqual(e[12] as? String, "2015-10-21 12:00:00")
				XCTAssertEqual(e[13] as? String, "2015-10-21 12:00:00")
				XCTAssertEqual(e[14] as? String, "03:14:07")
				XCTAssertEqual(e[15] as? String, "2015")
				XCTAssertEqual(e[16] as? String, "K")
				XCTAssertEqual(UTF8Encoding.encode(bytes: e[17] as! [UInt8]), "BLOB DATA")
				XCTAssertEqual(e[18] as? String, "tiny text string")
				XCTAssertEqual(UTF8Encoding.encode(bytes: e[19] as! [UInt8]), "BLOB DATA")
				XCTAssertEqual(UTF8Encoding.encode(bytes: e[20] as! [UInt8]), "BLOB DATA")
				XCTAssertEqual(e[21] as? String, "medium text string")
				XCTAssertEqual(UTF8Encoding.encode(bytes: e[22] as! [UInt8]), "BLOB DATA")
				XCTAssertEqual(e[23] as? String, "long text string")
				XCTAssertEqual(e[24] as? String, "1")
				XCTAssertEqual(e[25] as? String, "2")
				XCTAssertEqual(e[26] as? Int8, 1)
				XCTAssertEqual(e[27] as? String, "1\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0")
				XCTAssertEqual(e[28] as? String, "1")
			}
			XCTAssert(ok, stmt1.errorMessage())
		}
	}
	
	func testServerVersion() {
		let mysql = rawMySQL
		let vers = mysql.serverVersion()
		XCTAssert(vers >= 50627) // YMMV
	}
	
	func testQueryInt() {
		let mysql = rawMySQL
		XCTAssert(mysql.query(statement: "DROP TABLE IF EXISTS int_test"), mysql.errorMessage())
		XCTAssert(mysql.query(statement: "CREATE TABLE int_test (a TINYINT, au TINYINT UNSIGNED, b SMALLINT, bu SMALLINT UNSIGNED, c MEDIUMINT, cu MEDIUMINT UNSIGNED, d INT, du INT UNSIGNED, e BIGINT, eu BIGINT UNSIGNED)"), mysql.errorMessage())
		
		var qres = mysql.query(statement: "INSERT INTO int_test (a, au, b, bu, c, cu, d, du, e, eu) VALUES "
			+ "(-1, 1, -2, 2, -3, 3, -4, 4, -5, 5)")
		XCTAssert(qres == true, mysql.errorMessage())
		
		qres =  mysql.query(statement: "SELECT * FROM int_test")
		XCTAssert(qres == true, mysql.errorMessage())
		
		let results = mysql.storeResults()
		if let results = results {
			while let row = results.next() {
				XCTAssertEqual(row[0], "-1")
				XCTAssertEqual(row[1], "1")
				XCTAssertEqual(row[2], "-2")
				XCTAssertEqual(row[3], "2")
				XCTAssertEqual(row[4], "-3")
				XCTAssertEqual(row[5], "3")
				XCTAssertEqual(row[6], "-4")
				XCTAssertEqual(row[7], "4")
				XCTAssertEqual(row[8], "-5")
				XCTAssertEqual(row[9], "5")
			}
		}
	}
	
	func testQueryIntMin() {
		let mysql = rawMySQL
		XCTAssert(mysql.query(statement: "DROP TABLE IF EXISTS int_test"), mysql.errorMessage())
		XCTAssert(mysql.query(statement: "CREATE TABLE int_test (a TINYINT, au TINYINT UNSIGNED, b SMALLINT, bu SMALLINT UNSIGNED, c MEDIUMINT, cu MEDIUMINT UNSIGNED, d INT, du INT UNSIGNED, e BIGINT, eu BIGINT UNSIGNED)"), mysql.errorMessage())
		
		var qres = mysql.query(statement: "INSERT INTO int_test (a, au, b, bu, c, cu, d, du, e, eu) VALUES "
			+ "(-128, 0, -32768, 0, -8388608, 0, -2147483648, 0, -9223372036854775808, 0)")
		XCTAssert(qres == true, mysql.errorMessage())
		
		qres =  mysql.query(statement: "SELECT * FROM int_test")
		XCTAssert(qres == true, mysql.errorMessage())
		
		let results = mysql.storeResults()
		if let results = results {
			while let row = results.next() {
				XCTAssertEqual(row[0], "-128")
				XCTAssertEqual(row[1], "0")
				XCTAssertEqual(row[2], "-32768")
				XCTAssertEqual(row[3], "0")
				XCTAssertEqual(row[4], "-8388608")
				XCTAssertEqual(row[5], "0")
				XCTAssertEqual(row[6], "-2147483648")
				XCTAssertEqual(row[7], "0")
				XCTAssertEqual(row[8], "-9223372036854775808")
				XCTAssertEqual(row[9], "0")
			}
		}
	}
	
	func testQueryIntMax() {
		let mysql = rawMySQL
		XCTAssert(mysql.query(statement: "DROP TABLE IF EXISTS int_test"), mysql.errorMessage())
		XCTAssert(mysql.query(statement: "CREATE TABLE int_test (a TINYINT, au TINYINT UNSIGNED, b SMALLINT, bu SMALLINT UNSIGNED, c MEDIUMINT, cu MEDIUMINT UNSIGNED, d INT, du INT UNSIGNED, e BIGINT, eu BIGINT UNSIGNED)"), mysql.errorMessage())
		
		var qres = mysql.query(statement: "INSERT INTO int_test (a, au, b, bu, c, cu, d, du, e, eu) VALUES "
			+ "(127, 255, 32767, 65535, 8388607, 16777215, 2147483647, 4294967295, 9223372036854775807, 18446744073709551615)")
		XCTAssert(qres == true, mysql.errorMessage())
		
		qres =  mysql.query(statement: "SELECT * FROM int_test")
		XCTAssert(qres == true, mysql.errorMessage())
		
		let results = mysql.storeResults()
		if let results = results {
			while let row = results.next() {
				XCTAssertEqual(row[0], "127")
				XCTAssertEqual(row[1], "255")
				XCTAssertEqual(row[2], "32767")
				XCTAssertEqual(row[3], "65535")
				XCTAssertEqual(row[4], "8388607")
				XCTAssertEqual(row[5], "16777215")
				XCTAssertEqual(row[6], "2147483647")
				XCTAssertEqual(row[7], "4294967295")
				XCTAssertEqual(row[8], "9223372036854775807")
				XCTAssertEqual(row[9], "18446744073709551615")
			}
		}
	}
	
	func testQueryDecimal() {
		let mysql = rawMySQL
		XCTAssert(mysql.query(statement: "DROP TABLE IF EXISTS decimal_test"), mysql.errorMessage())
		XCTAssert(mysql.query(statement: "CREATE TABLE decimal_test (f FLOAT, fm FLOAT, d DOUBLE, dm DOUBLE, de DECIMAL(2,1), dem DECIMAL(2,1))"), mysql.errorMessage())
		
		var qres = mysql.query(statement: "INSERT INTO decimal_test (f, fm, d, dm, de, dem) VALUES "
			+ "(1.1, -1.1, 2.2, -2.2, 3.3, -3.3)")
		XCTAssert(qres == true, mysql.errorMessage())
		
		qres =  mysql.query(statement: "SELECT * FROM decimal_test")
		XCTAssert(qres == true, mysql.errorMessage())
		
		let results = mysql.storeResults()
		if let results = results {
			while let row = results.next() {
				XCTAssertEqual(row[0], "1.1")
				XCTAssertEqual(row[1], "-1.1")
				XCTAssertEqual(row[2], "2.2")
				XCTAssertEqual(row[3], "-2.2")
				XCTAssertEqual(row[4], "3.3")
				XCTAssertEqual(row[5], "-3.3")
			}
		}
	}
	
	func testStmtInt() {
		let mysql = rawMySQL
		XCTAssert(mysql.query(statement: "DROP TABLE IF EXISTS int_test"), mysql.errorMessage())
		XCTAssert(mysql.query(statement: "CREATE TABLE int_test (a TINYINT, au TINYINT UNSIGNED, b SMALLINT, bu SMALLINT UNSIGNED, c MEDIUMINT, cu MEDIUMINT UNSIGNED, d INT, du INT UNSIGNED, e BIGINT, eu BIGINT UNSIGNED)"), mysql.errorMessage())
		
		let stmt = MySQLStmt(mysql)
		var res = stmt.prepare(statement: "INSERT INTO int_test (a, au, b, bu, c, cu, d, du, e, eu) VALUES "
			+ "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		XCTAssert(res == true, stmt.errorMessage())
		
		stmt.bindParam(-1)
		stmt.bindParam(1)
		stmt.bindParam(-2)
		stmt.bindParam(2)
		stmt.bindParam(-3)
		stmt.bindParam(3)
		stmt.bindParam(-4)
		stmt.bindParam(4)
		stmt.bindParam(-5)
		stmt.bindParam(5)
		
		res = stmt.execute()
		XCTAssert(res == true, stmt.errorMessage())
		
		stmt.reset()
		res = stmt.prepare(statement: "SELECT * FROM int_test")
		XCTAssert(res == true, stmt.errorMessage())
		
		res = stmt.execute()
		XCTAssert(res == true, stmt.errorMessage())
		
		let results = stmt.results()
		XCTAssert(results.numRows == 1)
		XCTAssert(results.forEachRow { row in
			XCTAssertEqual(row[0] as? Int8, -1)
			XCTAssertEqual(row[1] as? UInt8, 1)
			XCTAssertEqual(row[2] as? Int16, -2)
			XCTAssertEqual(row[3] as? UInt16, 2)
			XCTAssertEqual(row[4] as? Int32, -3)
			XCTAssertEqual(row[5] as? UInt32, 3)
			XCTAssertEqual(row[6] as? Int32, -4)
			XCTAssertEqual(row[7] as? UInt32, 4)
			XCTAssertEqual(row[8] as? Int64, -5)
			XCTAssertEqual(row[9] as? UInt64, 5)
		})
	}
	
	func testStmtIntMin() {
		let mysql = rawMySQL
		XCTAssert(mysql.query(statement: "DROP TABLE IF EXISTS int_test"), mysql.errorMessage())
		XCTAssert(mysql.query(statement: "CREATE TABLE int_test (a TINYINT, au TINYINT UNSIGNED, b SMALLINT, bu SMALLINT UNSIGNED, c MEDIUMINT, cu MEDIUMINT UNSIGNED, d INT, du INT UNSIGNED, e BIGINT, eu BIGINT UNSIGNED)"), mysql.errorMessage())
		
		let stmt = MySQLStmt(mysql)
		var res = stmt.prepare(statement: "INSERT INTO int_test (a, au, b, bu, c, cu, d, du, e, eu) VALUES "
			+ "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		XCTAssert(res == true, stmt.errorMessage())
		
		stmt.bindParam(-128)
		stmt.bindParam(0)
		stmt.bindParam(-32768)
		stmt.bindParam(0)
		stmt.bindParam(-8388608)
		stmt.bindParam(0)
		stmt.bindParam(-2147483648)
		stmt.bindParam(0)
		stmt.bindParam(-9223372036854775808)
		stmt.bindParam(0)
		
		res = stmt.execute()
		XCTAssert(res == true, stmt.errorMessage())
		
		stmt.reset()
		res = stmt.prepare(statement: "SELECT * FROM int_test")
		XCTAssert(res == true, stmt.errorMessage())
		
		res = stmt.execute()
		XCTAssert(res == true, stmt.errorMessage())
		
		let results = stmt.results()
		XCTAssert(results.forEachRow { row in
			XCTAssertEqual(row[0] as? Int8, -128)
			XCTAssertEqual(row[1] as? UInt8, 0)
			XCTAssertEqual(row[2] as? Int16, -32768)
			XCTAssertEqual(row[3] as? UInt16, 0)
			XCTAssertEqual(row[4] as? Int32, -8388608)
			XCTAssertEqual(row[5] as? UInt32, 0)
			XCTAssertEqual(row[6] as? Int32, -2147483648)
			XCTAssertEqual(row[7] as? UInt32, 0)
			XCTAssertEqual(row[8] as? Int64, -9223372036854775808)
			XCTAssertEqual(row[9] as? UInt64, 0)
		})
	}
	
	func testStmtIntMax() {
		let mysql = rawMySQL
		XCTAssert(mysql.query(statement: "DROP TABLE IF EXISTS int_test"), mysql.errorMessage())
		XCTAssert(mysql.query(statement: "CREATE TABLE int_test (a TINYINT, au TINYINT UNSIGNED, b SMALLINT, bu SMALLINT UNSIGNED, c MEDIUMINT, cu MEDIUMINT UNSIGNED, d INT, du INT UNSIGNED, e BIGINT, eu BIGINT UNSIGNED)"), mysql.errorMessage())
		
		let stmt = MySQLStmt(mysql)
		var res = stmt.prepare(statement: "INSERT INTO int_test (a, au, b, bu, c, cu, d, du, e, eu) VALUES "
			+ "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		XCTAssert(res == true, stmt.errorMessage())
		
		stmt.bindParam(127)
		stmt.bindParam(255)
		stmt.bindParam(32767)
		stmt.bindParam(65535)
		stmt.bindParam(8388607)
		stmt.bindParam(16777215)
		stmt.bindParam(2147483647)
		stmt.bindParam(4294967295)
		stmt.bindParam(9223372036854775807)
		stmt.bindParam(18446744073709551615 as UInt64)
		
		res = stmt.execute()
		XCTAssert(res == true, stmt.errorMessage())
		
		stmt.reset()
		res = stmt.prepare(statement: "SELECT * FROM int_test")
		XCTAssert(res == true, stmt.errorMessage())
		
		res = stmt.execute()
		XCTAssert(res == true, stmt.errorMessage())
		
		let results = stmt.results()
		XCTAssert(results.forEachRow { row in
			XCTAssertEqual(row[0] as? Int8, 127)
			XCTAssertEqual(row[1] as? UInt8, 255)
			XCTAssertEqual(row[2] as? Int16, 32767)
			XCTAssertEqual(row[3] as? UInt16, 65535)
			XCTAssertEqual(row[4] as? Int32, 8388607)
			XCTAssertEqual(row[5] as? UInt32, 16777215)
			XCTAssertEqual(row[6] as? Int32, 2147483647)
			XCTAssertEqual(row[7] as? UInt32, 4294967295)
			XCTAssertEqual(row[8] as? Int64, 9223372036854775807)
			XCTAssertEqual(row[9] as? UInt64, 18446744073709551615)
		})
	}
	
	func testStmtDecimal() {
		let mysql = rawMySQL
		XCTAssert(mysql.query(statement: "DROP TABLE IF EXISTS decimal_test"), mysql.errorMessage())
		XCTAssert(mysql.query(statement: "CREATE TABLE decimal_test (f FLOAT, fm FLOAT, d DOUBLE, dm DOUBLE, de DECIMAL(2,1), dem DECIMAL(2,1))"), mysql.errorMessage())
		
		let stmt = MySQLStmt(mysql)
		var res = stmt.prepare(statement: "INSERT INTO decimal_test (f, fm, d, dm, de, dem) VALUES "
			+ "(?, ?, ?, ?, ?, ?)")
		XCTAssert(res == true, stmt.errorMessage())
		
		stmt.bindParam(1.1)
		stmt.bindParam(-1.1)
		stmt.bindParam(2.2)
		stmt.bindParam(-2.2)
		stmt.bindParam(3.3)
		stmt.bindParam(-3.3)
		
		res = stmt.execute()
		XCTAssert(res == true, stmt.errorMessage())
		
		stmt.reset()
		res = stmt.prepare(statement: "SELECT * FROM decimal_test")
		XCTAssert(res == true, stmt.errorMessage())
		
		res = stmt.execute()
		XCTAssert(res == true, stmt.errorMessage())
		
		let results = stmt.results()
		XCTAssert(results.forEachRow { row in
			XCTAssertEqual(row[0] as? Float, 1.1)
			XCTAssertEqual(row[1] as? Float, -1.1)
			XCTAssertEqual(row[2] as? Double, 2.2)
			XCTAssertEqual(row[3] as? Double, -2.2)
			XCTAssertEqual(row[4] as? String, "3.3")
			XCTAssertEqual(row[5] as? String, "-3.3")
		})
	}
	
	func testStmtNull() {
		let mysql = rawMySQL
		XCTAssert(mysql.query(statement: "DROP TABLE IF EXISTS null_test"), mysql.errorMessage())
		XCTAssert(mysql.query(statement: "CREATE TABLE null_test (a TINYINT, au TINYINT UNSIGNED, b SMALLINT, bu SMALLINT UNSIGNED, c MEDIUMINT, cu MEDIUMINT UNSIGNED, d INT, du INT UNSIGNED, e BIGINT, eu BIGINT UNSIGNED)"), mysql.errorMessage())
		
		let stmt = MySQLStmt(mysql)
		var res = stmt.prepare(statement: "INSERT INTO null_test (a, au, b, bu, c, cu, d, du, e, eu) VALUES "
			+ "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		XCTAssert(res == true, stmt.errorMessage())
		
		stmt.bindParam()
		stmt.bindParam()
		stmt.bindParam()
		stmt.bindParam()
		stmt.bindParam()
		stmt.bindParam()
		stmt.bindParam()
		stmt.bindParam()
		stmt.bindParam()
		stmt.bindParam()
		
		res = stmt.execute()
		XCTAssert(res == true, stmt.errorMessage())
		
		stmt.reset()
		res = stmt.prepare(statement: "SELECT * FROM null_test")
		XCTAssert(res == true, stmt.errorMessage())
		
		res = stmt.execute()
		XCTAssert(res == true, stmt.errorMessage())
		
		let results = stmt.results()
		XCTAssert(results.numRows == 1)
		XCTAssert(results.forEachRow { row in
			XCTAssertNil(row[0])
			XCTAssertNil(row[1])
			XCTAssertNil(row[2])
			XCTAssertNil(row[3])
			XCTAssertNil(row[4])
			XCTAssertNil(row[5])
			XCTAssertNil(row[6])
			XCTAssertNil(row[7])
			XCTAssertNil(row[8])
			XCTAssertNil(row[9])
		})
	}
	
	func testFieldInfo() {
		let mysql = rawMySQL
		XCTAssert(mysql.query(statement: "DROP TABLE IF EXISTS testdb"), mysql.errorMessage())
		XCTAssert(mysql.query(statement: "CREATE TABLE testdb (a VARCHAR( 20 ),\nb TINYINT,\nc TEXT,\nd DATE,\ne SMALLINT,\nf MEDIUMINT,\ng INT,\nh BIGINT,\ni BIGINT UNSIGNED,\nj FLOAT( 10, 2 ))"), mysql.errorMessage())
		let stmt = MySQLStmt(mysql)
		_ = stmt.prepare(statement: "SELECT * FROM testdb WHERE 0=1")
		for index in 0..<Int(stmt.fieldCount()) {
			guard let _ = stmt.fieldInfo(index: index) else {
				XCTAssert(false)
				continue
			}
		}
	}
	
	// copy + paste from here into other CRUD driver projects
	struct TestTable1: Codable, TableNameProvider {
		enum CodingKeys: String, CodingKey {
			case id, name, integer = "int", double = "doub", blob, subTables
		}
		static let tableName = "test_table_1"
		let id: Int
		let name: String?
		let integer: Int?
		let double: Double?
		let blob: [UInt8]?
		let subTables: [TestTable2]?
		init(id: Int,
			 name: String? = nil,
			 integer: Int? = nil,
			 double: Double? = nil,
			 blob: [UInt8]? = nil,
			 subTables: [TestTable2]? = nil) {
			self.id = id
			self.name = name
			self.integer = integer
			self.double = double
			self.blob = blob
			self.subTables = subTables
		}
	}
	
	struct TestTable2: Codable {
		let id: UUID
		let parentId: Int
		let date: Date
		let name: String?
		let int: Int?
		let doub: Double?
		let blob: [UInt8]?
		init(id: UUID,
			 parentId: Int,
			 date: Date,
			 name: String? = nil,
			 int: Int? = nil,
			 doub: Double? = nil,
			 blob: [UInt8]? = nil) {
			self.id = id
			self.parentId = parentId
			self.date = date
			self.name = name
			self.int = int
			self.doub = doub
			self.blob = blob
		}
	}
	
	func testCreate1() {
		do {
			let db = try getDB()
			try db.create(TestTable1.self, policy: .dropTable)
			do {
				let t2 = db.table(TestTable2.self)
				try t2.index(\.parentId)
			}
			let t1 = db.table(TestTable1.self)
			let subId = UUID()
			try db.transaction {
				let newOne = TestTable1(id: 2000, name: "New One", integer: 40)
				try t1.insert(newOne)
				let newSub1 = TestTable2(id: subId, parentId: 2000, date: Date(), name: "Me")
				let newSub2 = TestTable2(id: UUID(), parentId: 2000, date: Date(), name: "Not Me")
				let t2 = db.table(TestTable2.self)
				try t2.insert([newSub1, newSub2])
			}
			let j21 = try t1.join(\.subTables, on: \.id, equals: \.parentId)
			let j2 = j21.where(\TestTable1.id == 2000 && \TestTable2.name == "Me")
			let j3 = j21.where(\TestTable1.id > 20 &&
				!(\TestTable1.name == "Me" || \TestTable1.name == "You"))
			XCTAssertEqual(try j3.count(), 1)
			try db.transaction {
				let j2a = try j2.select().map { $0 }
				XCTAssertEqual(try j2.count(), 1)
				XCTAssertEqual(j2a.count, 1)
				guard j2a.count == 1 else {
					return
				}
				let obj = j2a[0]
				XCTAssertEqual(obj.id, 2000)
				XCTAssertNotNil(obj.subTables)
				let subTables = obj.subTables!
				XCTAssertEqual(subTables.count, 1)
				let obj2 = subTables[0]
				XCTAssertEqual(obj2.id, subId)
			}
			try db.create(TestTable1.self)
			do {
				let j2a = try j2.select().map { $0 }
				XCTAssertEqual(try j2.count(), 1)
				XCTAssertEqual(j2a[0].id, 2000)
			}
			try db.create(TestTable1.self, policy: .dropTable)
			do {
				let j2b = try j2.select().map { $0 }
				XCTAssertEqual(j2b.count, 0)
			}
		} catch {
			XCTFail("\(error)")
		}
	}
	
	func testCreate2() {
		do {
			let db = try getTestDB()
			try db.create(TestTable1.self, primaryKey: \.id, policy: .dropTable)
			do {
				let t2 = db.table(TestTable2.self)
				try t2.index(\.parentId, \.date)
			}
			let t1 = db.table(TestTable1.self)
			do {
				let newOne = TestTable1(id: 2000, name: "New One", integer: 40)
				try t1.insert(newOne)
			}
			let j2 = try t1.where(\TestTable1.id == 2000).select()
			do {
				let j2a = j2.map { $0 }
				XCTAssertEqual(j2a.count, 1)
				XCTAssertEqual(j2a[0].id, 2000)
			}
			try db.create(TestTable1.self)
			do {
				let j2a = j2.map { $0 }
				XCTAssertEqual(j2a.count, 1)
				XCTAssertEqual(j2a[0].id, 2000)
			}
			try db.create(TestTable1.self, policy: .dropTable)
			do {
				let j2b = j2.map { $0 }
				XCTAssertEqual(j2b.count, 0)
			}
		} catch {
			XCTFail("\(error)")
		}
	}
	
	func testCreate3() {
		struct FakeTestTable1: Codable, TableNameProvider {
			enum CodingKeys: String, CodingKey {
				case id, name, double = "doub", double2 = "doub2", blob, subTables
			}
			static let tableName = "test_table_1"
			let id: Int
			let name: String?
			let double2: Double?
			let double: Double?
			let blob: [UInt8]?
			let subTables: [TestTable2]?
		}
		do {
			let db = try getTestDB()
			try db.create(TestTable1.self, policy: [.dropTable, .shallow])
			
			do {
				let t1 = db.table(TestTable1.self)
				let newOne = TestTable1(id: 2000, name: "New One", integer: 40)
				try t1.insert(newOne)
			}
			do {
				try db.create(FakeTestTable1.self, policy: [.reconcileTable, .shallow])
				let t1 = db.table(FakeTestTable1.self)
				let j2 = try t1.where(\FakeTestTable1.id == 2000).select()
				do {
					let j2a = j2.map { $0 }
					XCTAssertEqual(j2a.count, 1)
					XCTAssertEqual(j2a[0].id, 2000)
				}
			}
		} catch {
			XCTFail("\(error)")
		}
	}
	
	func getTestDB() throws -> Database<DBConfiguration> {
		do {
			let db = try getDB()
			try db.create(TestTable1.self, policy: .dropTable)
			try db.transaction {
				() -> () in
				try db.table(TestTable1.self)
					.insert((1...testDBRowCount).map {
						num -> TestTable1 in
						let n = UInt8(num)
						let blob: [UInt8]? = (num % 2 != 0) ? nil : [UInt8](arrayLiteral: n+1, n+2, n+3, n+4, n+5)
						return TestTable1(id: num,
										  name: "This is name bind \(num)",
							integer: num,
							double: Double(num),
							blob: blob)
					})
			}
			try db.transaction {
				() -> () in
				try db.table(TestTable2.self)
					.insert((1...testDBRowCount).flatMap {
						parentId -> [TestTable2] in
						return (1...testDBRowCount).map {
							num -> TestTable2 in
							let n = UInt8(num)
							let blob: [UInt8]? = [UInt8](arrayLiteral: n+1, n+2, n+3, n+4, n+5)
							return TestTable2(id: UUID(),
											  parentId: parentId,
											  date: Date(),
											  name: num % 2 == 0 ? "This is name bind \(num)" : "me",
											  int: num,
											  doub: Double(num),
											  blob: blob)
						}
					})
			}
		} catch {
			XCTFail("\(error)")
		}
		return try getDB(reset: false)
	}
	
	func testSelectAll() {
		do {
			let db = try getTestDB()
			let j2 = db.table(TestTable1.self)
			for row in try j2.select() {
				XCTAssertNil(row.subTables)
			}
		} catch {
			XCTFail("\(error)")
		}
	}
	
	func testSelectIn() {
		do {
			let db = try getTestDB()
			let table = db.table(TestTable1.self)
			XCTAssertEqual(2, try table.where(\TestTable1.id ~ [2, 4]).count())
			XCTAssertEqual(3, try table.where(\TestTable1.id !~ [2, 4]).count())
		} catch {
			XCTFail("\(error)")
		}
	}
	
	func testSelectLikeString() {
		do {
			let db = try getTestDB()
			let table = db.table(TestTable2.self)
			XCTAssertEqual(25, try table.where(\TestTable2.name %=% "me").count())
			XCTAssertEqual(15, try table.where(\TestTable2.name =% "me").count())
			XCTAssertEqual(15, try table.where(\TestTable2.name %= "me").count())
			XCTAssertEqual( 0, try table.where(\TestTable2.name %!=% "me").count())
			XCTAssertEqual(10, try table.where(\TestTable2.name !=% "me").count())
			XCTAssertEqual(10, try table.where(\TestTable2.name %!= "me").count())
		} catch {
			XCTFail("\(error)")
		}
	}
	
	func testSelectJoin() {
		do {
			let db = try getTestDB()
			let j2 = try db.table(TestTable1.self)
				.order(by: \TestTable1.name)
				.join(\.subTables, on: \.id, equals: \.parentId)
				.order(by: \.id)
				.where(\TestTable2.name == "me")
			
			let j2c = try j2.count()
			let j2a = try j2.select().map{$0}
			let j2ac = j2a.count
			XCTAssertNotEqual(j2c, 0)
			XCTAssertEqual(j2c, j2ac)
			j2a.forEach { row in
				XCTAssertFalse(row.subTables?.isEmpty ?? true)
			}
		} catch {
			XCTFail("\(error)")
		}
	}
	
	func testInsert1() {
		do {
			let db = try getTestDB()
			let t1 = db.table(TestTable1.self)
			let newOne = TestTable1(id: 2000, name: "New ` One", integer: 40)
			try t1.insert(newOne)
			let j1 = t1.where(\TestTable1.id == newOne.id)
			let j2 = try j1.select().map {$0}
			XCTAssertEqual(try j1.count(), 1)
			XCTAssertEqual(j2[0].id, 2000)
			XCTAssertEqual(j2[0].name, "New ` One")
		} catch {
			XCTFail("\(error)")
		}
	}
	
	func testInsert2() {
		do {
			let db = try getTestDB()
			let t1 = db.table(TestTable1.self)
			let newOne = TestTable1(id: 2000, name: "New One", integer: 40)
			try t1.insert(newOne, ignoreKeys: \TestTable1.integer)
			let j1 = t1.where(\TestTable1.id == newOne.id)
			let j2 = try j1.select().map {$0}
			XCTAssertEqual(try j1.count(), 1)
			XCTAssertEqual(j2[0].id, 2000)
			XCTAssertNil(j2[0].integer)
		} catch {
			XCTFail("\(error)")
		}
	}
	
	func testInsert3() {
		do {
			let db = try getTestDB()
			let t1 = db.table(TestTable1.self)
			let newOne = TestTable1(id: 2000, name: "New One", integer: 40)
			let newTwo = TestTable1(id: 2001, name: "New One", integer: 40)
			try t1.insert([newOne, newTwo], setKeys: \TestTable1.id, \TestTable1.integer)
			let j1 = t1.where(\TestTable1.id == newOne.id)
			let j2 = try j1.select().map {$0}
			XCTAssertEqual(try j1.count(), 1)
			XCTAssertEqual(j2[0].id, 2000)
			XCTAssertEqual(j2[0].integer, 40)
			XCTAssertNil(j2[0].name)
		} catch {
			XCTFail("\(error)")
		}
	}
	
	func testUpdate() {
		do {
			let db = try getTestDB()
			let newOne = TestTable1(id: 2000, name: "New One", integer: 40)
			let newId: Int = try db.transaction {
				try db.table(TestTable1.self).insert(newOne)
				let newOne2 = TestTable1(id: 2000, name: "New👻One Updated", integer: 41)
				try db.table(TestTable1.self)
					.where(\TestTable1.id == newOne.id)
					.update(newOne2, setKeys: \.name)
				return newOne2.id
			}
			let j2 = try db.table(TestTable1.self)
				.where(\TestTable1.id == newId)
				.select().map { $0 }
			XCTAssertEqual(1, j2.count)
			XCTAssertEqual(2000, j2[0].id)
			XCTAssertEqual("New👻One Updated", j2[0].name)
			XCTAssertEqual(40, j2[0].integer)
		} catch {
			XCTFail("\(error)")
		}
	}
	
	func testDelete() {
		do {
			let db = try getTestDB()
			let t1 = db.table(TestTable1.self)
			let newOne = TestTable1(id: 2000, name: "New One", integer: 40)
			try t1.insert(newOne)
			let query = t1.where(\TestTable1.id == newOne.id)
			let j1 = try query.select().map { $0 }
			XCTAssertEqual(j1.count, 1)
			try query.delete()
			let j2 = try query.select().map { $0 }
			XCTAssertEqual(j2.count, 0)
		} catch {
			XCTFail("\(error)")
		}
	}
	
	func testSelectLimit() {
		do {
			let db = try getTestDB()
			let j2 = db.table(TestTable1.self).limit(3, skip: 2)
			XCTAssertEqual(try j2.count(), 3)
		} catch {
			XCTFail("\(error)")
		}
	}
	
	func testSelectLimitWhere() {
		do {
			let db = try getTestDB()
			let j2 = db.table(TestTable1.self).limit(3).where(\TestTable1.id > 3)
			XCTAssertEqual(try j2.count(), 2)
			XCTAssertEqual(try j2.select().map{$0}.count, 2)
		} catch {
			XCTFail("\(error)")
		}
	}
	
	func testSelectOrderLimitWhere() {
		do {
			let db = try getTestDB()
			let j2 = db.table(TestTable1.self).order(by: \TestTable1.id).limit(3).where(\TestTable1.id > 3)
			XCTAssertEqual(try j2.count(), 2)
			XCTAssertEqual(try j2.select().map{$0}.count, 2)
		} catch {
			XCTFail("\(error)")
		}
	}
	
	func testSelectWhereNULL() {
		do {
			let db = try getTestDB()
			let t1 = db.table(TestTable1.self)
			let j1 = t1.where(\TestTable1.blob == nil)
			XCTAssert(try j1.count() > 0)
			let j2 = t1.where(\TestTable1.blob != nil)
			XCTAssert(try j2.count() > 0)
			CRUDLogging.flush()
		} catch {
			XCTFail("\(error)")
		}
	}
	
	// this is the general-overview example used in the readme
	func testPersonThing() {
		do {
			// CRUD can work with most Codable types.
			struct PhoneNumber: Codable {
				let personId: UUID
				let planetCode: Int
				let number: String
			}
			struct Person: Codable {
				let id: UUID
				let firstName: String
				let lastName: String
				let phoneNumbers: [PhoneNumber]?
			}
			
			// CRUD usage begins by creating a database connection.
			// The inputs for connecting to a database will differ depending on your client library.
			// Create a `Database` object by providing a configuration.
			// All code would be identical regardless of the datasource type.
			let db = try getTestDB()
			
			// Create the table if it hasn't been done already.
			// Table creates are recursive by default, so "PhoneNumber" is also created here.
			try db.create(Person.self, policy: .reconcileTable)
			
			// Get a reference to the tables we will be inserting data into.
			let personTable = db.table(Person.self)
			let numbersTable = db.table(PhoneNumber.self)
			
			// Add an index for personId, if it does not already exist.
			try numbersTable.index(\.personId)
			
			// Insert some sample data.
			do {
				// Insert some sample data.
				let owen = Person(id: UUID(), firstName: "Owen", lastName: "Lars", phoneNumbers: nil)
				let beru = Person(id: UUID(), firstName: "Beru", lastName: "Lars", phoneNumbers: nil)
				
				// Insert the people
				try personTable.insert([owen, beru])
				
				// Give them some phone numbers
				try numbersTable.insert([
					PhoneNumber(personId: owen.id, planetCode: 12, number: "555-555-1212"),
					PhoneNumber(personId: owen.id, planetCode: 15, number: "555-555-2222"),
					PhoneNumber(personId: beru.id, planetCode: 12, number: "555-555-1212")])
			}
			
			// Perform a query.
			// Let's find all people with the last name of Lars which have a phone number on planet 12.
			let query = try personTable
				.order(by: \.lastName, \.firstName)
				.join(\.phoneNumbers, on: \.id, equals: \.personId)
				.order(descending: \.planetCode)
				.where(\Person.lastName == "Lars" && \PhoneNumber.planetCode == 12)
				.select()
			
			// Loop through the results and print the names.
			for user in query {
				// We joined PhoneNumbers, so we should have values here.
				guard let numbers = user.phoneNumbers else {
					continue
				}
				for number in numbers {
					print(number.number)
				}
			}
			CRUDLogging.flush()
		} catch {
			XCTFail("\(error)")
		}
	}
	
	func testStandardJoin() {
		do {
			let db = try getTestDB()
			struct Parent: Codable {
				let id: Int
				let children: [Child]?
				init(id i: Int) {
					id = i
					children = nil
				}
			}
			struct Child: Codable {
				let id: Int
				let parentId: Int
			}
			try db.transaction {
				try db.create(Parent.self, policy: [.shallow, .dropTable]).insert(
					Parent(id: 1))
				try db.create(Child.self, policy: [.shallow, .dropTable]).insert(
					[Child(id: 1, parentId: 1),
					 Child(id: 2, parentId: 1),
					 Child(id: 3, parentId: 1)])
			}
			let join = try db.table(Parent.self)
				.join(\.children,
					  on: \.id,
					  equals: \.parentId)
				.where(\Parent.id == 1)
			
			guard let parent = try join.first() else {
				return XCTFail("Failed to find parent id: 1")
			}
			guard let children = parent.children else {
				return XCTFail("Parent had no children")
			}
			XCTAssertEqual(3, children.count)
			for child in children {
				XCTAssertEqual(child.parentId, parent.id)
			}
			CRUDLogging.flush()
		} catch {
			XCTFail("\(error)")
		}
	}
	
	func testJunctionJoin() {
		do {
			struct Student: Codable {
				let id: Int
				let classes: [Class]?
				init(id i: Int) {
					id = i
					classes = nil
				}
			}
			struct Class: Codable {
				let id: Int
				let students: [Student]?
				init(id i: Int) {
					id = i
					students = nil
				}
			}
			struct StudentClasses: Codable {
				let studentId: Int
				let classId: Int
			}
			let db = try getTestDB()
			try db.transaction {
				try db.create(Student.self, policy: [.dropTable, .shallow]).insert(
					Student(id: 1))
				try db.create(Class.self, policy: [.dropTable, .shallow]).insert([
					Class(id: 1),
					Class(id: 2),
					Class(id: 3)])
				try db.create(StudentClasses.self, policy: [.dropTable, .shallow]).insert([
					StudentClasses(studentId: 1, classId: 1),
					StudentClasses(studentId: 1, classId: 2),
					StudentClasses(studentId: 1, classId: 3)])
			}
			let join = try db.table(Student.self)
				.join(\.classes,
					  with: StudentClasses.self,
					  on: \.id,
					  equals: \.studentId,
					  and: \.id,
					  is: \.classId)
				.where(\Student.id == 1)
			guard let student = try join.first() else {
				return XCTFail("Failed to find student id: 1")
			}
			guard let classes = student.classes else {
				return XCTFail("Student had no classes")
			}
			XCTAssertEqual(3, classes.count)
			for aClass in classes {
				let join = try db.table(Class.self)
					.join(\.students,
						  with: StudentClasses.self,
						  on: \.id,
						  equals: \.classId,
						  and: \.id,
						  is: \.studentId)
					.where(\Class.id == aClass.id)
				guard let found = try join.first() else {
					XCTFail("Class with no students")
					continue
				}
				guard nil != found.students?.first(where: { $0.id == student.id }) else {
					XCTFail("Student not found in class")
					continue
				}
			}
			CRUDLogging.flush()
		} catch {
			XCTFail("\(error)")
		}
	}
	
	func testSelfJoin() {
		do {
			struct Me: Codable {
				let id: Int
				let parentId: Int
				let mes: [Me]?
				init(id i: Int, parentId p: Int) {
					id = i
					parentId = p
					mes = nil
				}
			}
			let db = try getTestDB()
			try db.transaction {
				() -> () in
				try db.create(Me.self, policy: .dropTable).insert([
					Me(id: 1, parentId: 0),
					Me(id: 2, parentId: 1),
					Me(id: 3, parentId: 1),
					Me(id: 4, parentId: 1),
					Me(id: 5, parentId: 1)
					])
			}
			let join = try db.table(Me.self)
				.join(\.mes, on: \.id, equals: \.parentId)
				.where(\Me.id == 1)
			guard let me = try join.first() else {
				return XCTFail("Unable to find me.")
			}
			guard let mes = me.mes else {
				return XCTFail("Unable to find meesa.")
			}
			XCTAssertEqual(mes.count, 4)
		} catch {
			XCTFail("\(error)")
		}
	}
	
	func testSelfJunctionJoin() {
		do {
			struct Me: Codable {
				let id: Int
				let us: [Me]?
				init(id i: Int) {
					id = i
					us = nil
				}
			}
			struct Us: Codable {
				let you: Int
				let them: Int
			}
			let db = try getTestDB()
			try db.transaction {
				() -> () in
				try db.create(Me.self, policy: .dropTable)
					.insert((1...5).map { .init(id: $0) })
				try db.create(Us.self, policy: .dropTable)
					.insert((2...5).map { .init(you: 1, them: $0) })
			}
			let join = try db.table(Me.self)
				.join(\.us,
					  with: Us.self,
					  on: \.id,
					  equals: \.you,
					  and: \.id,
					  is: \.them)
				.where(\Me.id == 1)
			guard let me = try join.first() else {
				return XCTFail("Unable to find me.")
			}
			guard let us = me.us else {
				return XCTFail("Unable to find us.")
			}
			XCTAssertEqual(us.count, 4)
		} catch {
			XCTFail("\(error)")
		}
	}
	
	func testCodableProperty() {
		do {
			struct Sub: Codable {
				let id: Int
			}
			struct Top: Codable {
				let id: Int
				let sub: Sub?
			}
			let db = try getTestDB()
			try db.create(Sub.self)
			try db.create(Top.self)
			let t1 = Top(id: 1, sub: Sub(id: 1))
			try db.table(Top.self).insert(t1)
			guard let top = try db.table(Top.self).where(\Top.id == 1).first() else {
				return XCTFail("Unable to find top.")
			}
			XCTAssertEqual(top.sub?.id, t1.sub?.id)
		} catch {
			XCTFail("\(error)")
		}
	}
	
	func testBadDecoding() {
		do {
			struct Top: Codable, TableNameProvider {
				static var tableName = "Top"
				let id: Int
			}
			struct NTop: Codable, TableNameProvider {
				static var tableName = "Top"
				let nid: Int
			}
			let db = try getTestDB()
			try db.create(Top.self, policy: .dropTable)
			let t1 = Top(id: 1)
			try db.table(Top.self).insert(t1)
			_ = try db.table(NTop.self).first()
			XCTFail("Should not have a valid object.")
		} catch {}
	}
	
	func testAllPrimTypes1() {
		struct AllTypes: Codable {
			let int: Int
			let uint: UInt
			let int64: Int64
			let uint64: UInt64
			let int32: Int32?
			let uint32: UInt32?
			let int16: Int16
			let uint16: UInt16
			let int8: Int8?
			let uint8: UInt8?
			let double: Double
			let float: Float
			let string: String
			let bytes: [Int8]
			let ubytes: [UInt8]?
			let b: Bool
		}
		do {
			let db = try getTestDB()
			try db.create(AllTypes.self, policy: .dropTable)
			let model = AllTypes(int: 1, uint: 2, int64: 3, uint64: 4, int32: 5, uint32: 6, int16: 7, uint16: 8, int8: 9, uint8: 10, double: 11, float: 12, string: "13", bytes: [1, 4], ubytes: [1, 4], b: true)
			try db.table(AllTypes.self).insert(model)
			
			guard let f = try db.table(AllTypes.self).where(\AllTypes.int == 1).first() else {
				return XCTFail("Nil result.")
			}
			XCTAssertEqual(model.int, f.int)
			XCTAssertEqual(model.uint, f.uint)
			XCTAssertEqual(model.int64, f.int64)
			XCTAssertEqual(model.uint64, f.uint64)
			XCTAssertEqual(model.int32, f.int32)
			XCTAssertEqual(model.uint32, f.uint32)
			XCTAssertEqual(model.int16, f.int16)
			XCTAssertEqual(model.uint16, f.uint16)
			XCTAssertEqual(model.int8, f.int8)
			XCTAssertEqual(model.uint8, f.uint8)
			XCTAssertEqual(model.double, f.double)
			XCTAssertEqual(model.float, f.float)
			XCTAssertEqual(model.string, f.string)
			XCTAssertEqual(model.bytes, f.bytes)
			XCTAssertEqual(model.ubytes!, f.ubytes!)
			XCTAssertEqual(model.b, f.b)
		} catch {
			XCTFail("\(error)")
		}
		do {
			let db = try getTestDB()
			try db.create(AllTypes.self, policy: .dropTable)
			let model = AllTypes(int: 1, uint: 2, int64: -3, uint64: 4, int32: nil, uint32: nil, int16: -7, uint16: 8, int8: nil, uint8: nil, double: -11, float: -12, string: "13", bytes: [1, 4], ubytes: nil, b: true)
			try db.table(AllTypes.self).insert(model)
			
			guard let f = try db.table(AllTypes.self)
				.where(\AllTypes.int == 1).first() else {
					return XCTFail("Nil result.")
			}
			XCTAssertEqual(model.int, f.int)
			XCTAssertEqual(model.uint, f.uint)
			XCTAssertEqual(model.int64, f.int64)
			XCTAssertEqual(model.uint64, f.uint64)
			XCTAssertEqual(model.int32, f.int32)
			XCTAssertEqual(model.uint32, f.uint32)
			XCTAssertEqual(model.int16, f.int16)
			XCTAssertEqual(model.uint16, f.uint16)
			XCTAssertEqual(model.int8, f.int8)
			XCTAssertEqual(model.uint8, f.uint8)
			XCTAssertEqual(model.double, f.double)
			XCTAssertEqual(model.float, f.float)
			XCTAssertEqual(model.string, f.string)
			XCTAssertEqual(model.bytes, f.bytes)
			XCTAssertNil(f.ubytes)
			XCTAssertEqual(model.b, f.b)
		} catch {
			XCTFail("\(error)")
		}
	}
	
	func testAllPrimTypes2() {
		struct AllTypes2: Codable {
			func equals(rhs: AllTypes2) -> Bool {
				guard int == rhs.int && uint == rhs.uint &&
					int64 == rhs.int64 && uint64 == rhs.uint64 &&
					int32 == rhs.int32 && uint32 == rhs.uint32 &&
					int16 == rhs.int16 && uint16 == rhs.uint16 &&
					int8 == rhs.int8 && uint8 == rhs.uint8 else {
						return false
				}
				guard double == rhs.double && float == rhs.float &&
					string == rhs.string &&
					b == rhs.b else {
						return false
				}
				guard (bytes == nil && rhs.bytes == nil) || (bytes != nil && rhs.bytes != nil) else {
					return false
				}
				guard (ubytes == nil && rhs.ubytes == nil) || (ubytes != nil && rhs.ubytes != nil) else {
					return false
				}
				if let lhsb = bytes {
					guard lhsb == rhs.bytes! else {
						return false
					}
				}
				if let lhsb = ubytes {
					guard lhsb == rhs.ubytes! else {
						return false
					}
				}
				return true
			}
			let int: Int?
			let uint: UInt?
			let int64: Int64?
			let uint64: UInt64?
			let int32: Int32?
			let uint32: UInt32?
			let int16: Int16?
			let uint16: UInt16?
			let int8: Int8?
			let uint8: UInt8?
			let double: Double?
			let float: Float?
			let string: String?
			let bytes: [Int8]?
			let ubytes: [UInt8]?
			let b: Bool?
		}
		
		do {
			let db = try getTestDB()
			try db.create(AllTypes2.self, policy: .dropTable)
			let model = AllTypes2(int: 1, uint: 2, int64: -3, uint64: 4, int32: 5, uint32: 6,
								  int16: 7, uint16: 8, int8: 9, uint8: 10,
								  double: 11.2, float: 12.3, string: "13",
								  bytes: [1, 4], ubytes: [1, 4], b: true)
			try db.table(AllTypes2.self).insert(model)
			do {
				guard let f = try db.table(AllTypes2.self)
					.where(\AllTypes2.int == 1 &&
						\AllTypes2.uint == 2 &&
						\AllTypes2.int64 == -3).first() else {
							return XCTFail("Nil result.")
				}
				XCTAssert(model.equals(rhs: f), "\(model) != \(f)")
				XCTAssertEqual(try db.table(AllTypes2.self)
					.where(\AllTypes2.int != 1 &&
						\AllTypes2.uint != 2 &&
						\AllTypes2.int64 != -3).count(), 0)
			}
			do {
				guard let f = try db.table(AllTypes2.self)
					.where(\AllTypes2.uint64 == 4 &&
						\AllTypes2.int32 == 5 &&
						\AllTypes2.uint32 == 6).first() else {
							return XCTFail("Nil result.")
				}
				XCTAssert(model.equals(rhs: f), "\(model) != \(f)")
				XCTAssertEqual(try db.table(AllTypes2.self)
					.where(\AllTypes2.uint64 != 4 &&
						\AllTypes2.int32 != 5 &&
						\AllTypes2.uint32 != 6).count(), 0)
			}
			do {
				guard let f = try db.table(AllTypes2.self)
					.where(\AllTypes2.int16 == 7 &&
						\AllTypes2.uint16 == 8 &&
						\AllTypes2.int8 == 9 &&
						\AllTypes2.uint8 == 10).first() else {
							return XCTFail("Nil result.")
				}
				XCTAssert(model.equals(rhs: f), "\(model) != \(f)")
				XCTAssertEqual(try db.table(AllTypes2.self)
					.where(\AllTypes2.int16 != 7 &&
						\AllTypes2.uint16 != 8 &&
						\AllTypes2.int8 != 9 &&
						\AllTypes2.uint8 != 10).count(), 0)
			}
			do {
				guard let f = try db.table(AllTypes2.self)
					.where(\AllTypes2.double == 11.2 &&
						\AllTypes2.float == Float(12.3) &&
						\AllTypes2.string == "13").first() else {
							return XCTFail("Nil result.")
				}
				XCTAssert(model.equals(rhs: f), "\(model) != \(f)")
				XCTAssertEqual(try db.table(AllTypes2.self)
					.where(\AllTypes2.double != 11.2 &&
						\AllTypes2.float != Float(12.3) &&
						\AllTypes2.string != "13").count(), 0)
			}
			do {
				guard let f = try db.table(AllTypes2.self)
					.where(\AllTypes2.bytes == [1, 4] as [Int8] &&
						\AllTypes2.ubytes == [1, 4] as [UInt8] &&
						\AllTypes2.b == true).first() else {
							return XCTFail("Nil result.")
				}
				XCTAssert(model.equals(rhs: f), "\(model) != \(f)")
				XCTAssertEqual(try db.table(AllTypes2.self)
					.where(\AllTypes2.bytes != [1, 4] as [Int8] &&
						\AllTypes2.ubytes != [1, 4] as [UInt8] &&
						\AllTypes2.b != true).count(), 0)
			}
		} catch {
			XCTFail("\(error)")
		}
	}
	
	func testBespokeSQL() {
		do {
			let db = try getTestDB()
			do {
				let r = try db.sql("SELECT * FROM \(TestTable1.CRUDTableName) WHERE id = 2", TestTable1.self)
				XCTAssertEqual(r.count, 1)
			}
			do {
				let r = try db.sql("SELECT * FROM \(TestTable1.CRUDTableName)", TestTable1.self)
				XCTAssertEqual(r.count, 5)
			}
		} catch {
			XCTFail("\(error)")
		}
	}
	
	func testURL() {
		do {
			let db = try getTestDB()
			struct TableWithURL: Codable {
				let id: Int
				let url: URL
			}
			try db.create(TableWithURL.self)
			let t1 = db.table(TableWithURL.self)
			let newOne = TableWithURL(id: 2000, url: URL(string: "http://localhost/")!)
			try t1.insert(newOne)
			let j1 = t1.where(\TableWithURL.id == newOne.id)
			let j2 = try j1.select().map {$0}
			XCTAssertEqual(try j1.count(), 1)
			XCTAssertEqual(j2[0].id, 2000)
			XCTAssertEqual(j2[0].url.absoluteString, "http://localhost/")
		} catch {
			XCTFail("\(error)")
		}
	}
	
	static var allTests = [
		("testConnect", testConnect),
		("testListDbs1", testListDbs1),
		("testListDbs2", testListDbs2),
		("testListTables1", testListTables1),
		("testListTables2", testListTables2),
		("testQuery1", testQuery1),
		("testQuery2", testQuery2),
		("testInsertNull", testInsertNull),
		("testQueryStmt1", testQueryStmt1),
		("testQueryStmt2", testQueryStmt2),
		("testServerVersion", testServerVersion),
		("testQueryInt", testQueryInt),
		("testQueryIntMin", testQueryIntMin),
		("testQueryIntMax", testQueryIntMax),
		("testQueryDecimal", testQueryDecimal),
		("testStmtInt", testStmtInt),
		("testStmtIntMin", testStmtIntMin),
		("testStmtIntMax", testStmtIntMax),
		("testStmtDecimal", testStmtDecimal),
		("testFieldInfo", testFieldInfo),
		
		("testCreate1", testCreate1),
		("testCreate2", testCreate2),
		("testCreate3", testCreate3),
		("testSelectAll", testSelectAll),
		("testSelectIn", testSelectIn),
		("testSelectLikeString", testSelectLikeString),
		("testSelectJoin", testSelectJoin),
		("testInsert1", testInsert1),
		("testInsert2", testInsert2),
		("testInsert3", testInsert3),
		("testUpdate", testUpdate),
		("testDelete", testDelete),
		("testSelectLimit", testSelectLimit),
		("testSelectLimitWhere", testSelectLimitWhere),
		("testSelectOrderLimitWhere", testSelectOrderLimitWhere),
		("testSelectWhereNULL", testSelectWhereNULL),
		("testPersonThing", testPersonThing),
		("testStandardJoin", testStandardJoin),
		("testJunctionJoin", testJunctionJoin),
		("testSelfJoin", testSelfJoin),
		("testSelfJunctionJoin", testSelfJunctionJoin),
		("testCodableProperty", testCodableProperty),
		("testBadDecoding", testBadDecoding),
		("testAllPrimTypes1", testAllPrimTypes1),
		("testAllPrimTypes2", testAllPrimTypes2),
		("testBespokeSQL", testBespokeSQL),
		("testURL", testURL)
	]
}


