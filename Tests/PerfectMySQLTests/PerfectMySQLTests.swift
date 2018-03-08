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
let testHost = "127.0.0.1"
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
		try db.sql("CREATE DATABASE \(testDB)")
	}
	return Database(configuration: try DBConfiguration(database: testDB,
													   host: testHost,
													   username: testUser,
													   password: testPassword))
}

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

class PerfectMySQLTests: XCTestCase {
	override func setUp() {
		super.setUp()
	}
	override func tearDown() {
		CRUDLogging.flush()
		super.tearDown()
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
			let newOne = TestTable1(id: 2000, name: "New One", integer: 40)
			try t1.insert(newOne)
			let j1 = t1.where(\TestTable1.id == newOne.id)
			let j2 = try j1.select().map {$0}
			XCTAssertEqual(try j1.count(), 1)
			XCTAssertEqual(j2[0].id, 2000)
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
				let newOne2 = TestTable1(id: 2000, name: "New One Updated", integer: 41)
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
			XCTAssertEqual("New One Updated", j2[0].name)
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
				let id: UUID
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
			// CRUD usage begins by creating a database connection. The inputs for connecting to a database will differ depending on your client library.
			// Create a `Database` object by providing a configuration. These examples will use SQLite for demonstration purposes.
			let db = try getTestDB()
			// Create the table if it hasn't been done already.
			// Table creates are recursive by default, so "PhoneNumber" is also created here.
			try db.create(Person.self, policy: .reconcileTable)
			// Get a reference to the tables we will be inserting data into.
			let personTable = db.table(Person.self)
			let numbersTable = db.table(PhoneNumber.self)
			// Add an index for personId, if it does not already exist.
			try numbersTable.index(\.personId)
			do {
				// Insert some sample data.
				let personId1 = UUID()
				let personId2 = UUID()
				try personTable.insert([
					Person(id: personId1, firstName: "Owen", lastName: "Lars", phoneNumbers: nil),
					Person(id: personId2, firstName: "Beru", lastName: "Lars", phoneNumbers: nil)])
				try numbersTable.insert([
					PhoneNumber(id: UUID(), personId: personId1, planetCode: 12, number: "555-555-1212"),
					PhoneNumber(id: UUID(), personId: personId1, planetCode: 15, number: "555-555-2222"),
					PhoneNumber(id: UUID(), personId: personId2, planetCode: 12, number: "555-555-1212")
					])
			}
			// Let's find all people with the last name of Lars which have a phone number on planet 12.
			let query = try personTable
				.order(by: \.lastName, \.firstName)
				.join(\.phoneNumbers, on: \.id, equals: \.personId)
				.order(descending: \.planetCode)
				.where(\Person.lastName == "Lars" && \PhoneNumber.planetCode == 12)
				.select()
			// Loop through them and print the names.
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
	
	static var allTests = [
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
		("testSelectWhereNULL", testSelectWhereNULL),
		("testPersonThing", testPersonThing),
		("testStandardJoin", testStandardJoin),
		("testJunctionJoin", testJunctionJoin),
		("testSelfJoin", testSelfJoin),
		("testSelfJunctionJoin", testSelfJunctionJoin),
		("testCodableProperty", testCodableProperty)
	]
}


