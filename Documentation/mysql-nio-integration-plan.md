> Received 2026-07-14 from a separate Claude Desktop agent session
> (`local-agent-mode-sessions/.../Perfect-MySQL-NIO-Integration-Plan.md`),
> saved here for the record.

## Decision (2026-07-14, corrected 2026-07-16): Not now — deferred to an explicit trigger, not flatly declined

**Correction**: an earlier version of this note cited a since-superseded
bridge (`Sources/LassoPerfectServer/AsyncBridge.swift`, deleted) as the
reason MySQL doesn't need this. That's stale. The actual, authoritative
answer to this exact question already exists, produced independently
(by Tim + a separate Claude Code session) the same day this plan
arrived, with far more rigor than either that stale note or this plan's
own analysis:

**`Documentation/async-mysql-research-findings.md` in
`/Users/timtaplin/Perfect-Lasso/`** (the Lasso Adapter project — moved/
renamed from `Documents/Perfect resurrection` since this plan was first
filed here). Read that document in full before acting on this one; the
summary below is not a substitute for it.

What actually happened in that project, for context: Phase 1 converted
its render pipeline to native `async throws` end-to-end (no bridge
needed anymore, for any backend); Phase 2 put `PerfectCRUDLassoExecutor`'s
blocking MySQL calls on `@concurrent` (SE-0461) to keep them off the
cooperative pool; Phase 3 is the findings doc above, which asked this
plan's exact question — "should the MySQL side genuinely go async
instead?" — and answered with real evidence, not general reasoning:

- The libmysqlclient build this ecosystem actually links (Oracle
  9.6.0, verified via `otool -L`) has a nonblocking API, but **it has no
  nonblocking prepared-statement calls at all** — and Perfect-MySQL's
  entire CRUD path is prepared statements. Dead end confirmed by
  grepping the actual linked header, not assumed.
- mysql-nio/MySQLKit **cannot back Perfect-CRUD's existing typed
  connector protocol** (`SQLExeDelegate` is synchronous pull-based,
  `Select: Sequence` can't `await`) without a breaking redesign of
  Perfect-CRUD itself — a materially larger and more invasive change
  than this plan's "additive DatabaseService layer" framing suggests.
  It *could* back the Lasso Adapter's narrower `DynamicQuery`/
  `DynamicMutation`/`DynamicSQL` handler seam directly, bypassing
  Perfect-CRUD for that one consumer — a much smaller, additive change
  than this plan's full 8-week Perfect-MySQL replacement.
- At this project's actual scale (one legacy site, no connection
  pooling, not hundreds-to-thousands of concurrent queries), the
  research concludes **connection pooling is the larger, more certain
  latency win** — and Phase 2's thread-offload approach can deliver that
  without adopting any new client.

**Ranked disposition** (from the findings doc, this plan's real answer):
1. Thread-pool offload + pooled persistent connections — the actual
   plan going forward, already adopted in the Lasso Adapter project.
2. mysql-nio/MySQLKit behind the Lasso Adapter's handler seam
   specifically (not a Perfect-MySQL replacement) — kept as an
   explicit escape hatch, revisited only if real, *measured* cooperative-
   pool starvation is observed under real load. Not ruled out forever;
   just not justified by "nicer async style" alone, and not scoped as
   "replace Perfect-MySQL for every consumer" the way this plan proposes.
3. Hand-rolling the C-library nonblocking API (MariaDB's connector has
   one; Oracle's doesn't cover prepared statements) — worst cost/benefit
   of the three, not recommended.

This plan (the 8-week Perfect-MySQL → mysql-nio replacement below) is
**not being pursued** on that basis: it targets a bigger blast radius
(Perfect-MySQL itself, used by PerfectCRUD/Perfect-Session/PerfectTemplate/
the Lasso Adapter) than the real problem needs, for a benefit
(async-native style) the actual evidence says isn't the binding
constraint at this project's scale. Perfect-MySQL stays as-is. Kept here
for reference, not deleted — revisit only under the specific trigger
above (measured cooperative-pool starvation), not speculatively.

---

# Perfect Swift + mysql-nio Integration Plan

## Executive Summary

Migrate Perfect's MySQL connectivity from the aging libmysqlclient wrapper (Perfect-MySQL) to **mysql-nio**, a modern, pure-Swift async MySQL driver. This enables true async/nonblocking prepared statements while maintaining Perfect's lightweight philosophy and eliminating C API dependencies.

## Current State Analysis

### Perfect-MySQL (Current Solution)
- **Foundation**: Wraps Oracle's libmysqlclient library
- **Async Support**: Limited; nonblocking API does NOT support prepared statements
- **Problem**: Binary choice between prepared statements (sync only) or async queries (no prepared statements)
- **Maintenance**: Community-maintained, minimal recent updates
- **Dependencies**: Requires libmysqlclient system library installation

### The Core Constraint
As documented in Oracle's libmysqlclient 9.6.0:
- Nonblocking API functions: `mysql_real_connect_nonblocking()`, `mysql_real_query_nonblocking()`, `mysql_fetch_row_nonblocking()`, etc.
- **No nonblocking variants for**: `mysql_stmt_prepare()`, `mysql_stmt_execute()`, `mysql_stmt_bind_param()`
- **Workaround in libmysqlclient**: Use multiple connections with polling (inefficient, complex state management)

## Proposed Solution: mysql-nio

### What is mysql-nio?
- **Type**: Pure Swift async MySQL client library
- **Owner**: Vapor organization (GitHub: vapor/mysql-nio)
- **License**: Open source
- **Dependencies**: Swift NIO (Apple), swift-nio-ssl, swift-log, swift-crypto — NO Vapor framework required
- **Maturity**: Production-ready (stable releases, active maintenance)
- **Architecture**: Event-driven nonblocking I/O via Swift NIO

### Why mysql-nio Solves This
✅ True async/await prepared statements (no libmysqlclient limitations)
✅ Pure Swift implementation (no C FFI complexity)
✅ Built on Swift NIO (event-driven, efficient resource usage)
✅ Zero Vapor framework dependencies
✅ Seamless Perfect integration (both are Swift-native)
✅ Connection pooling support built-in
✅ Parameter binding for SQL injection prevention

### Functional Parity
| Feature | Perfect-MySQL | mysql-nio | Notes |
|---------|---------------|-----------|-------|
| Text queries | ✅ | ✅ | Basic SELECT/INSERT/UPDATE/DELETE |
| Prepared statements | ✅ | ✅ | Both support parameter binding |
| Async queries | ✅ | ✅ | EventLoop-based concurrency |
| Async prepared statements | ❌ | ✅ | **mysql-nio advantage** |
| Connection pooling | Limited | ✅ | Built-in connection management |
| TLS/SSL | ✅ | ✅ | Encryption support |
| Transaction support | ✅ | ✅ | Transaction control |

## Implementation Architecture

### High-Level Design
```
Perfect HTTP Handler
        ↓
[Perfect Router/Request Handler]
        ↓
[Database Service Layer - NEW]
        ↓
[mysql-nio Connection Pool]
        ↓
MySQL Server
```

### Layer Responsibilities

**Database Service Layer** (NEW abstraction)
- Manages mysql-nio connection pool
- Exposes async query/prepared statement methods
- Handles connection lifecycle and error handling
- Provides transaction management
- Acts as bridge between Perfect handlers and mysql-nio

**Perfect Handler Updates**
- Replace Perfect-MySQL imports with Database Service Layer
- Use `async/await` instead of `.wait()` where possible
- Minimal changes to existing logic (service layer handles compatibility)

## Implementation Plan

### Phase 1: Foundation (Weeks 1-2)

**1.1 Create Database Service Layer**
- New module: `Sources/DatabaseService/`
- File: `MySQLConnection.swift`
  - Wraps mysql-nio connection
  - Exposes: `query()`, `preparedQuery()`, `execute()`, `transaction()`
  - All methods are `async throws`
- File: `MySQLPool.swift`
  - Connection pool management using mysql-nio
  - Configurable pool size, timeout, idle connection handling
- File: `DatabaseConfig.swift`
  - Configuration struct (host, port, user, password, database)
  - Load from environment or config file

**1.2 Add mysql-nio Dependency**
- Update `Package.swift`:
  ```swift
  .package(url: "https://github.com/vapor/mysql-nio.git", from: "1.7.0")
  ```
- No libmysqlclient system dependency required

**1.3 Integration Tests**
- Test basic connection pool creation
- Test single query execution
- Test prepared statement with parameters
- Test connection timeout/recovery

### Phase 2: Core Functionality (Weeks 3-4)

**2.1 Query Execution Methods**
- Implement `async func query(_ sql: String) -> [[String: Any]]`
- Implement `async func preparedQuery(_ sql: String, params: [MySQLData]) -> [[String: Any]]`
- Add result row parsing (convert MySQLRow to dictionary/Codable)

**2.2 Prepared Statement Support**
- Parameter binding with type safety
- Support for common types: Int, String, Double, Bool, Date, nil
- Example usage:
  ```swift
  let results = try await db.preparedQuery(
      "SELECT * FROM users WHERE id = ? AND status = ?",
      params: [MySQLData(int: 42), MySQLData(string: "active")]
  )
  ```

**2.3 Transaction Support**
- Implement `async func transaction<T>(_ block: @escaping () async throws -> T) -> T`
- Handle COMMIT/ROLLBACK
- Ensure connection state consistency

### Phase 3: Migration (Weeks 5-6)

**3.1 Identify Perfect-MySQL Usage**
- Audit codebase for all Perfect-MySQL imports
- Document current usage patterns
- Categorize: text queries, prepared statements, connection handling

**3.2 Incremental Migration**
- Start with one route/handler at a time
- Replace Perfect-MySQL calls with DatabaseService Layer
- Run tests after each module update
- Maintain feature parity during migration

**3.3 Error Handling Alignment**
- Map mysql-nio errors to application error types
- Maintain existing error messages for backwards compatibility
- Add logging for debugging

### Phase 4: Optimization & Cleanup (Weeks 7-8)

**4.1 Connection Pool Tuning**
- Benchmark connection pool size vs throughput
- Configure idle connection timeout
- Add metrics/monitoring

**4.2 Remove Perfect-MySQL**
- Remove dependency from Package.swift
- Remove old wrapper code
- Clean up any libmysqlclient build configuration

**4.3 Documentation**
- Document DatabaseService Layer API
- Update Perfect deployment guide (no more libmysqlclient requirement)
- Add examples for common queries

## Code Examples

### Current Approach (Perfect-MySQL) - Problematic
```swift
import PerfectMySQL

let mysql = MySQL()
mysql.setOption(.MYSQL_OPT_RECONNECT, value: true)
mysql.connect(host: "localhost", user: "root", password: "password", db: "mydb")

// Prepared statements must be synchronous
if mysql.queryStatement("SELECT * FROM users WHERE id = ?") {
    // Can't use async here...
}
mysql.close()
```

### New Approach (mysql-nio) - Async Throughout
```swift
import MySQLNIO

class DatabaseService {
    let pool: MySQLConnectionPool

    init(config: DatabaseConfig) {
        self.pool = MySQLConnectionPool(
            host: config.host,
            port: config.port,
            username: config.username,
            password: config.password,
            database: config.database,
            on: eventLoopGroup
        )
    }

    // Async prepared statement - fully nonblocking
    func getUserById(_ id: Int) async throws -> [String: Any]? {
        let rows = try await pool.execute(
            "SELECT * FROM users WHERE id = ?",
            [MySQLData(int: id)]
        )
        return rows.first
    }

    // Transaction support
    func transferFunds(from: Int, to: Int, amount: Decimal) async throws {
        try await pool.transaction { conn in
            try await conn.execute(
                "UPDATE accounts SET balance = balance - ? WHERE id = ?",
                [MySQLData(decimal: amount), MySQLData(int: from)]
            )
            try await conn.execute(
                "UPDATE accounts SET balance = balance + ? WHERE id = ?",
                [MySQLData(decimal: amount), MySQLData(int: to)]
            )
        }
    }
}

// Usage in Perfect handler
func handleUserRequest(_ req: HTTPRequest) async throws -> HTTPResponse {
    let db = try getDatabase() // Inject or retrieve from app state
    let user = try await db.getUserById(42)

    return HTTPResponse(status: .ok, body: encodeJSON(user))
}
```

## Technical Considerations

### EventLoop Management
- mysql-nio requires a Swift NIO EventLoopGroup
- Perfect can create/manage this centrally in app initialization
- One EventLoopGroup per app, shared across all connections

### Type Conversion
- MySQLData handles type marshaling (Swift type ↔ MySQL wire format)
- Build type-safe wrapper for common conversions
- Support: Int, Int64, String, Double, Bool, Date, Decimal, UUID, JSON, nil

### Connection Pooling Strategy
- Recommended pool size: number of CPU cores × 2-4
- Idle timeout: 30-60 seconds
- Max connections: 20-100 depending on load
- Use metrics to tune (via swift-log integration)

### Error Handling Strategy
```swift
enum DatabaseError: Error {
    case connectionFailed(String)
    case queryFailed(String)
    case parameterBindingFailed(String)
    case transactionFailed(String)
}
```

### Thread Safety
- mysql-nio connections are NOT thread-safe
- Connection pool ensures 1 connection per async task
- No manual lock management needed (EventLoop handles it)

## Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| Breaking existing code | Use adapter layer; migrate incrementally |
| Connection pool exhaustion | Add monitoring, configure timeouts |
| MySQL version compatibility | Test against MySQL 5.7, 8.0, 8.4 |
| Performance regression | Benchmark before/after; optimize pool settings |
| Concurrency bugs in transition | Comprehensive test suite for each phase |

## Testing Strategy

### Unit Tests
- Mock DatabaseService for handler testing
- Test parameter binding type conversion
- Test error handling paths
- Test transaction rollback on failure

### Integration Tests
- Real MySQL database (Docker recommended for CI)
- Test connection pool behavior under load
- Test prepared statement execution
- Test transaction isolation
- Test connection recovery after network failure

### Load Testing
- Compare performance: Perfect-MySQL vs mysql-nio
- Measure throughput (queries/second)
- Monitor memory usage under sustained load
- Test connection pool saturation handling

## Deployment Implications

### System Dependencies
**Before (Perfect-MySQL)**
- Requires libmysqlclient installed on server
- Version conflicts possible
- Platform-specific installation complexity

**After (mysql-nio)**
- Zero system dependencies (pure Swift)
- Simpler deployment (Swift binary + config)
- Easier containerization (no C library compilation)

### Configuration Changes
- Environment variables: `MYSQL_HOST`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE`
- Optional: `MYSQL_PORT`, `MYSQL_POOL_SIZE`, `MYSQL_IDLE_TIMEOUT`
- Backward compatible with existing Perfect environment setup

### Performance Expectations
- **Prepared statements**: 10-30% faster than text queries (SQL parsing avoidance)
- **Async overhead**: Minimal (event-loop based, not thread-based)
- **Memory**: Slightly lower (no libmysqlclient overhead)
- **Throughput**: Same or better (efficient connection pooling)

## Success Criteria

✅ All text queries execute with async/await
✅ All prepared statements execute with async/await
✅ No prepared statement query blocked on sync calls
✅ Connection pool functions under sustained load (>100 concurrent requests)
✅ Error handling consistent with existing Perfect patterns
✅ Zero breaking changes to Perfect API (backwards compatible layer if needed)
✅ Deployment guide updated, no libmysqlclient required
✅ Test coverage >85% for database layer

## Timeline Summary

| Phase | Duration | Deliverable |
|-------|----------|-------------|
| Phase 1: Foundation | 2 weeks | DatabaseService abstraction, tests |
| Phase 2: Core Features | 2 weeks | Query execution, prepared statements, transactions |
| Phase 3: Migration | 2 weeks | Codebase fully migrated, old code removed |
| Phase 4: Optimization | 2 weeks | Performance tuned, documentation complete |
| **Total** | **8 weeks** | Production-ready mysql-nio integration |

## Next Steps

1. **Review with Claude Code agents** ← You are here
2. Validate architecture and API design
3. Create initial DatabaseService skeleton
4. Set up integration test database (Docker MySQL)
5. Begin Phase 1 implementation
6. Establish acceptance criteria per phase

## References

- [mysql-nio GitHub](https://github.com/vapor/mysql-nio)
- [Swift NIO Documentation](https://apple.github.io/swift-nio/docs/current/NIO/index.html)
- [Perfect Swift Server Framework](https://perfect.org/)
- [MySQL Binary Protocol Documentation](https://dev.mysql.com/doc/internals/en/client-server-protocol.html)
- [Swift Concurrency: async/await](https://developer.apple.com/videos/play/wwdc2021/10132/)

---

**Document Version**: 1.0
**Date**: July 2026
**Status**: Ready for Architecture Review
