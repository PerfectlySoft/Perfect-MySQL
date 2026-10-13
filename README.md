# Perfect-MySQL

<p align="center">
    <img src="https://img.shields.io/badge/Swift-6.2-orange.svg?style=flat" alt="Swift 6.2">
    <img src="https://img.shields.io/badge/Platforms-macOS%2012%2B%20%7C%20Linux-lightgray.svg?style=flat" alt="Platforms macOS 12+ | Linux">
    <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache%202.0-lightgrey.svg?style=flat" alt="License Apache 2.0"></a>
</p>

A Swift 6 wrapper around the MySQL client library (libmysqlclient), providing both a raw MySQL API and a PerfectCRUD integration layer.

## Status

Perfect-MySQL is core, actively-used infrastructure, depended on directly by Perfect-NIO,
Perfect-Session, and PerfectTemplate — not a standalone/experimental library.

An `mysql-nio`-based async rewrite was considered and deliberately deferred — see [`Documentation/mysql-nio-integration-plan.md`](Documentation/mysql-nio-integration-plan.md) for the tradeoffs. This package remains the synchronous, blocking libmysqlclient wrapper described below.

The pre-Swift-6 version of this package (3.x, last released as 3.5.1) is preserved on the [`legacy`](https://github.com/PerfectlySoft/Perfect-MySQL/tree/legacy) branch. Stay on 3.5.1 if you can't move to Swift 6.2 yet.

## Requirements

- Swift 6.2 or newer (`swift-tools-version: 6.2`).
- macOS 12+ (`platforms: [.macOS(.v12)]`) or Linux. CI covers macOS 26 with Xcode 26.0.1 and 26.6, and Ubuntu 24.04 with Swift 6.2 and 6.4. There is no iOS support: the client library isn't available there.
- The MySQL client library, found through `pkg-config` as `mysqlclient`: Oracle's libmysqlclient 8.0 or newer (Homebrew `mysql-client` on macOS, `libmysqlclient-dev` on Linux), or MariaDB Connector/C through Debian's `libmariadb-dev-compat`. On macOS, Homebrew's bottle is built for a recent macOS, which then sets the real minimum for your binary; check `brew info mysql-client`.
- [Perfect-CRUD](https://github.com/PerfectlySoft/Perfect-CRUD) 3.0.1 or newer, resolved by SwiftPM.

## macOS Setup

MySQL client is installed via Homebrew. It is **keg-only** (not linked into `/opt/homebrew`) so you also need `pkg-config` installed so SPM can locate the headers and libraries.

```bash
brew install mysql-client pkg-config
```

Then set `PKG_CONFIG_PATH` when building so SPM finds the `mysqlclient.pc` file:

```bash
export PKG_CONFIG_PATH="/opt/homebrew/opt/mysql-client/lib/pkgconfig:$PKG_CONFIG_PATH"
swift build
```

To make this permanent, add the export to your shell profile (`~/.zshrc` or `~/.bash_profile`).

> **Apple Silicon vs Intel:** Homebrew installs to `/opt/homebrew` on Apple Silicon and `/usr/local` on Intel. The path above is for Apple Silicon; substitute `/usr/local` if you're on an Intel Mac.

## Linux Setup

```bash
sudo apt-get install libmysqlclient-dev pkg-config
```

Ubuntu 20.04 and later ship a libmysqlclient 8.0 `libmysqlclient-dev`. To build against MariaDB Connector/C instead, install `libmariadb-dev-compat`, which provides the same `mysqlclient` pkg-config name (see the TLS section for how SSL modes differ there).

## Package.swift

```swift
.package(url: "https://github.com/PerfectlySoft/Perfect-MySQL.git", from: "4.0.0"),
```

```swift
.target(
    name: "MyTarget",
    dependencies: [
        .product(name: "PerfectMySQL", package: "Perfect-MySQL"),
    ]
)
```

## Usage

### Raw MySQL API

```swift
import PerfectMySQL

let mysql = MySQL()
guard mysql.connect(host: "127.0.0.1", user: "root", password: "secret", db: "mydb") else {
    print(mysql.errorMessage())
    exit(1)
}

guard mysql.query(statement: "SELECT id, name FROM users") else {
    print(mysql.errorMessage())
    exit(1)
}

if let results = mysql.storeResults() {
    results.forEachRow { row in
        print(row[0] ?? "nil", row[1] ?? "nil")
    }
}
```

### TLS

Set `MYSQL_OPT_SSL_MODE` before connecting, using MySQL's `SSL_MODE_*` values: 1 = DISABLED, 2 = PREFERRED, 3 = REQUIRED, 4 = VERIFY_CA, 5 = VERIFY_IDENTITY. Add `MYSQL_OPT_SSL_CA` to verify the server's certificate.

```swift
let mysql = MySQL()
mysql.setOption(.MYSQL_OPT_SSL_CA, "/path/to/ca.pem")
guard mysql.setOption(.MYSQL_OPT_SSL_MODE, 5) else { fatalError("SSL mode not supported") }
```

This also works when the package is built against MariaDB Connector/C (Debian's `libmariadb-dev-compat`), which has no `MYSQL_OPT_SSL_MODE`; the modes are mapped onto its own options, with these differences:

- **REQUIRED** is checked only after authenticating: Connector/C doesn't refuse a server without TLS, so `connect()` closes the plaintext connection and fails with error 2026 afterwards. Someone able to tamper with the connection can capture the authentication exchange (or the password, if the server asks for `mysql_clear_password`). Use VERIFY_IDENTITY with `MYSQL_OPT_SSL_CA`, which fails before authenticating. REQUIRED also turns off `MYSQL_OPT_RECONNECT`, since a reconnect could fall back to plaintext.
- **VERIFY_CA** also checks the host name. Connector/C 3.4 checks neither the host name nor, without `MYSQL_OPT_SSL_CA`, the CA on local (loopback or socket) connections.
- **DISABLED** still uses TLS if any `MYSQL_OPT_SSL_*` file or cipher option is set.

### PerfectCRUD Integration

`MySQLDatabaseConfiguration` conforms to `DatabaseConfigurationProtocol` and `Sendable`, so it works directly with PerfectCRUD's `Database` and with PerfectNIO's `Routes.db()` helper.

```swift
import PerfectCRUD
import PerfectMySQL

struct User: Codable {
    let id: Int
    var name: String
    var email: String
}

let config = try MySQLDatabaseConfiguration(
    database: "mydb",
    host: "127.0.0.1",
    username: "root",
    password: "secret"
)

let db = Database(configuration: config)
try db.create(User.self, policy: .reconcileTable)

let users = try db.table(User.self).where(\User.name == "Alice").select().map { $0 }
```

### Foreign Keys

PerfectCRUD's `@ForeignKey` property wrapper generates a real `FOREIGN KEY ... REFERENCES ... ON DELETE ... ON UPDATE ...` constraint when creating a table, and this connector round-trips the wrapped value correctly on decode:

```swift
struct Author: Codable {
    var id: Int
    var name: String
}

struct Book: Codable {
    var id: Int
    @ForeignKey(Author.self, onDelete: cascade, onUpdate: restrict)
    var authorId: Int
}

try db.create(Author.self, policy: .shallow)
try db.create(Book.self, policy: .shallow) // DDL includes the FOREIGN KEY constraint
```

Available actions (each a plain global constant, not an enum case): `cascade`, `restrict`, `setNull`, `setDefault`, `ignore`. Verified against a real server: `ON DELETE CASCADE` actually removes the child row via InnoDB itself, not just correct DDL text, and inserting a child row with an unknown parent id is rejected by the constraint.

### Dynamic PerfectCRUD Rows

Perfect-MySQL also supports PerfectCRUD's dynamic read API. This is useful for
runtime-driven callers such as template engines, admin tools, and query builders
where the table, selected fields, predicates, and ordering are not known at
compile time.

```swift
let result = try db.select(DynamicQuery(
    table: "products",
    fields: ["id", "sku", "name"],
    predicates: [
        DynamicPredicate(
            field: "active",
            comparison: .equal,
            value: .int(1)
        ),
    ],
    limit: 25
))

for row in result.rows {
    print(row["sku"] ?? .null)
}
```

The connector converts MySQL statement rows into `DynamicRow` values while still
using PerfectCRUD's identifier quoting, bound values, and statement execution.

### With PerfectNIO Routes

```swift
import PerfectNIO
import PerfectNIOCRUD
import PerfectMySQL

let routes = Routes()
    .db(try MySQLDatabaseConfiguration(database: "mydb", host: "127.0.0.1")) { req, db in
        try db.table(User.self).select().map { $0 }
    }
```

## Running Tests

The default test suite is safe to run without a live MySQL server:

```bash
# Run tests with PKG_CONFIG_PATH set
PKG_CONFIG_PATH=/opt/homebrew/opt/mysql-client/lib/pkgconfig swift test
```

Live MySQL tests are opt-in. Configure a disposable test account and schema
prefix with environment variables instead of relying on a passwordless root
installation:

```bash
MYSQL_FIXTURE_TESTS=1 \
MYSQL_TEST_HOST=127.0.0.1 \
MYSQL_TEST_PORT=3307 \
MYSQL_TEST_DATABASE=perfect_mysql_fixture \
MYSQL_TEST_USER=perfect_test \
MYSQL_TEST_PASSWORD='...' \
PKG_CONFIG_PATH=/opt/homebrew/opt/mysql-client/lib/pkgconfig \
swift test
```

`MYSQL_TEST_DATABASE` is treated as a prefix. Fixture tests append a unique
suffix so Swift Testing can run live database tests in parallel, create the
schema, load `Tests/PerfectMySQLTests/Fixtures/sample_catalog_cart.sql`, query
it, and drop it afterward. The configured user should have privileges to create
and drop schemas matching that prefix, for example `perfect_mysql_fixture_%`.

The older XCTest integration tests still use `MYSQL_TESTS=1` and the same
`MYSQL_TEST_*` variables when you explicitly want to run the broader legacy
connector suite.

These tests drop and recreate databases, so they never assume a server.
`MYSQL_TEST_PORT` is required (1–65535; there is no fallback to 3306), and
`MYSQL_TEST_HOST` may not be `localhost` or empty, which libmysql reaches
through the Unix socket regardless of the port. Without both, the live tests are
reported as skipped. The tests can't tell whether a valid host and port lead to
a server you care about, so point them at a disposable one, for example a
container:

```bash
container run -d --rm --name perfect-mysql-test --publish 127.0.0.1:3307:3306 \
  --env MYSQL_ALLOW_EMPTY_PASSWORD=yes docker.io/library/mysql:8.4
MYSQL_TESTS=1 MYSQL_TEST_PORT=3307 \
PKG_CONFIG_PATH=/opt/homebrew/opt/mysql-client/lib/pkgconfig \
swift test
```

## Notes on client library versions

MySQL 8.0 removed the `my_bool` typedef that earlier versions used for nullable bool fields. This package's inline `mysqlclient` system library target defines it (`typedef signed char my_bool`) for libmysqlclient 8.0 and newer, and leaves MariaDB Connector/C's own definition alone, so the source compiles against both. CI builds against Ubuntu 24.04's libmysqlclient 8.0 and Homebrew's current `mysql-client`; CI also builds and tests against MariaDB Connector/C (Ubuntu 24.04's `libmariadb-dev-compat`).

## Migrating from 3.x

4.0.0 is the Swift 6 rewrite; the public API of `MySQL`, `MySQLStmt` and `MySQLDatabaseConfiguration` is largely unchanged, apart from `Sendable` conformances and the additions below. When upgrading:

- **Toolchain and platforms:** Swift 6.2+ and macOS 12+. The client library is no longer a separate `Perfect-mysqlclient` package; install it as described above and make sure `pkg-config` can find `mysqlclient` (on macOS, set `PKG_CONFIG_PATH` for the keg-only Homebrew formula).
- **Perfect-CRUD 3:** this release requires Perfect-CRUD 3.0.1. Perfect-CRUD 3 has breaking changes of its own; see its release notes.
- **Binary values are bytes:** `MySQLStmt` results for BINARY, VARBINARY, BIT and GEOMETRY columns, and binary expressions such as `CAST(x AS BINARY)`, `UNHEX()` or `x'..'` literals, are now `[UInt8]` instead of `String` (BLOBs already were). Use `String(decoding: bytes, as: UTF8.self)`, or `CAST(… AS CHAR)` in SQL, where you want text. Through Perfect-CRUD, `String`, `UUID`, `Date` and `URL` properties over binary columns still decode as text.
- **No more truncated text:** `MySQL.Results` (from `query(statement:)`) and `MySQLStmt` used to stop at the first invalid UTF-8 byte; they now return the whole value with U+FFFD for invalid bytes. For exact bytes from `MySQL.Results`, use the new `nextBytes()` / `forEachRowBytes(callback:)` and `fieldIsBinary(at:)`.
- **Nested transactions work:** Perfect-CRUD's nested `transaction {}` sends `SAVEPOINT` statements, which MySQL can't prepare. These, and `BEGIN`, `START TRANSACTION`, `LOCK`/`UNLOCK TABLES` and `USE`, now run directly, as does anything else the server refuses to prepare (error 1295). An unpreparable statement that returns rows (`CHECK TABLE`, `XA RECOVER`) now throws instead of silently returning nothing.
- **`setOption` is stricter:** `MYSQL_OPT_SSL_MODE` accepts only 1...5, and an integer option that doesn't fit a `UInt32` (such as -1) returns `false` instead of trapping. `connect()` always passes `CLIENT_REMEMBER_OPTIONS`, so options survive a failed attempt and a retry uses them again.
- **Table creation and `.reconcileTable`:** a parent's table is created before sub-tables that reference it (and `.dropTable` drops them in reverse); reconcile no longer drops and re-adds columns whose names contain capitals (which lost their data), adds the FOREIGN KEY of a `@ForeignKey` column it adds (so the ALTER fails if existing rows violate it), and drops the constraint of a `@ForeignKey` column it removes.
- **Dates are read as UTC:** Perfect-CRUD has always written `Date` values as UTC text, but 3.x parsed them in the process's local time zone, so outside UTC they came back shifted by the UTC offset, and a row whose UTC time fell in a local daylight-saving gap couldn't be decoded. Dates are now parsed as UTC, and `DATETIME(6)` fractions and `DATE` values are read too. Rows written through Perfect-CRUD on or after 1582-10-15 read back correctly with no migration. What does shift: apps that compensated for the old offset by hand; local-time values written outside Perfect-CRUD, such as `NOW()` / `CURRENT_TIMESTAMP` on a server whose time zone isn't UTC, or `TIMESTAMP` columns read in a non-UTC session `time_zone` (if you need those to line up, run the connection in UTC, for example `SET time_zone = '+00:00'` after connecting); and dates before 1582-10-15 that 3.x wrote. Dates now use the proleptic Gregorian calendar, as MySQL does, while 3.x used Foundation's Julian-before-1582 calendar for both writing and reading, so those older values (including `Date.distantPast`, which 3.x stored as `0001-01-01`) read back 2 or more days different. Year 0 is accepted, so `Date.distantPast` (`0000-12-30`) round-trips. Fractional seconds are read but not written (whole seconds, truncated), and `9999-12-31 23:59:59.999999` no longer turns into year 10000. A `Date` that MySQL can't store now throws when bound: only non-finite dates used to crash, while years outside 0–9999 were written and then rejected by the server. The error comes from `insert`, `update`, `count()` and `sql`; in a `select()` whose `where` compares against such a `Date`, Perfect-CRUD catches it, logs it and returns no rows (`[]`). The zero date (`0000-00-00`) reads as nil for an optional `Date`; for a non-optional one it's a decoding error. Note that Perfect-CRUD's `select()` stops at a row it can't decode and silently drops it and every later row, so any such error loses the rest of the result.
- **Duplicate column names:** when a result has several columns with the same name (`p.id, c.id`), a Codable model or a Dynamic row now reads the first one (before, which one was arbitrary). Dynamic rows also treat names that differ only by case as the same name, which used to crash with Perfect-CRUD 3.0.0. Alias the columns (`c.id AS child_id`) when you need both.
- **Stored procedures:** `CALL` through Perfect-CRUD now returns the procedure's first result set (it used to read every column as missing), and the remaining result sets are discarded so the connection stays usable. An error the procedure raises after its first result set (a failing INSERT, a `SIGNAL`) now throws from typed `db.sql(_:_:)` and Dynamic calls; it used to be silently dropped. Untyped `db.sql("CALL …")` still can't report it, because Perfect-CRUD reads only the first result there; the error is logged and the connection stays usable.
- **Counts and IDs:** `MySQL.numberAffectedRows()` returns -1 after an error, as the C API does, and `lastInsertId()` returns an ID above `Int64.max` as its bit pattern (`UInt64(bitPattern:)`); both used to trap. A Dynamic statement that returns rows reports their count as `affectedRows` and no `insertedID`.
- **Errors while reading rows:** a statement that fails partway through its rows (such as a subquery returning more than one row) now throws through Perfect-CRUD instead of returning an empty result, and `MySQLStmt.Results` sets `failed`.
- **DECIMAL columns** now decode into `Double` and `Float` properties; they used to read as 0.
- **`listTables` / `listDatabases` wildcards** can no longer inject SQL. The wildcard could end the string literal, and with `CLIENT_MULTI_STATEMENTS` the rest of it ran as SQL: with any `'` on MariaDB Connector/C (which doesn't escape it at all); under big5, cp932, gbk, gb18030 or sjis with libmysqlclient; and, with either library, after a stored routine changed `sql_mode`, which leaves the client's idea of `NO_BACKSLASH_ESCAPES` wrong. The query is now built so that it doesn't depend on that mode: a wildcard with `'` has each `'` doubled and nothing else changed; any other wildcard is escaped by the client library; and the result is `[]` (refused) for a wildcard that contains both `'` and `\`, or that contains a non-ASCII byte and a `'` or `\` while the connection charset (as set with `MYSQL_SET_CHARSET_NAME`) is one of those five. Ordinary wildcards work in both `NO_BACKSLASH_ESCAPES` modes.
- **Statements can be reused:** executing a `MySQLStmt` again after reading its results corrupted the heap; it now works, including when a results variable is reassigned or a `CALL`'s result sets are walked one after another, and re-executing a prepared `CALL` on MariaDB Connector/C. `execute()` now returns false (error 2031, `CR_PARAMS_NOT_BOUND`) unless every parameter has been bound since the last `reset()`; re-executing with the old binds read freed memory. Binding more values than the statement has parameters, which wrote past the bind array and crashed (also through `db.sql(_:bindings:)`), is refused: `bindParam` sets `errorCode()` to 2034 (`CR_INVALID_PARAMETER_NO`) and doesn't store the value, and `execute()` returns false with 2034 until `reset()`. Blob parameters (`[UInt8]`, `[Int8]`, `bindParam(_:length:)`) and the parameter array no longer leak.
- **`[Int8]` properties** holding negative values read back correctly; they crashed the read. Writing them works through Perfect-CRUD 3.0.1, which this release requires.
- **Identifiers:** `quote(identifier:)` doubles backticks inside names, so Perfect-CRUD's Dynamic API can't be used to inject SQL through a table or field name. It assumes a utf8mb4 connection, which the `database:host:` initializers set.

## License

Apache 2.0 — see [LICENSE](LICENSE).
