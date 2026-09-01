# Perfect-MySQL

<p align="center">
    <img src="https://img.shields.io/badge/Swift-6.2-orange.svg?style=flat" alt="Swift 6.2">
    <img src="https://img.shields.io/badge/Platforms-macOS%2012%2B-lightgray.svg?style=flat" alt="Platforms macOS 12+">
    <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache%202.0-lightgrey.svg?style=flat" alt="License Apache 2.0"></a>
</p>

A Swift 6 wrapper around the MySQL client library (libmysqlclient), providing both a raw MySQL API and a PerfectCRUD integration layer.

## Status

Perfect-MySQL is core, actively-used infrastructure, depended on directly by Perfect-NIO,
Perfect-Session, and PerfectTemplate — not a standalone/experimental library.

An `mysql-nio`-based async rewrite was considered and deliberately deferred — see [`Documentation/mysql-nio-integration-plan.md`](Documentation/mysql-nio-integration-plan.md) for the tradeoffs. This package remains the synchronous, blocking libmysqlclient wrapper described below.

The pre-Swift-6 version of this package is preserved on the [`legacy`](../../tree/legacy) branch.

## Requirements

- Swift 6.2+ (`Package.swift` declares `platforms: [.macOS(.v12)]`)
- macOS: the Homebrew `mysql-client` formula is keg-only and bottled for a specific macOS floor that moves as Homebrew rotates supported OS versions — check `brew info mysql-client` for the current bottle tag before assuming compatibility (at time of writing, `sonoma`/14.0+) — or Linux with `libmysqlclient-dev` (no specific minimum distro version is enforced by `Package.swift`; Ubuntu 20.04+ is a reasonable practical floor)
- MySQL 8.0+ client library (libmysqlclient)
- `Package.swift` depends on [Perfect-CRUD](https://github.com/PerfectlySoft/Perfect-CRUD) via `.package(url:, branch: "main")` — resolved by SwiftPM automatically, no sibling checkout needed

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

MySQL 8.0+ is required. On Ubuntu 20.04 and later the default `libmysqlclient-dev` package satisfies this.

## Package.swift

```swift
// No tagged releases exist yet, so pin a branch rather than a version:
.package(url: "https://github.com/PerfectlySoft/Perfect-MySQL.git", branch: "main"),
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
MYSQL_TEST_HOST=localhost \
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

## Notes on MySQL 8.0

MySQL 8.0 removed the `my_bool` typedef that earlier versions used for nullable bool fields. This package's inline `mysqlclient` system library target provides a compatibility shim (`typedef signed char my_bool`) so the source compiles against both old and new client versions.

## License

Apache 2.0 — see [LICENSE](LICENSE).
