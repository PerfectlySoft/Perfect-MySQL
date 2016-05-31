import XCTest

import MySQLTestSuite

var tests = [XCTestCaseEntry]()
tests += MySQLTestSuite.allTests()
XCTMain(tests)
