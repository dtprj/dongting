## Build and Test Commands

### Build
- Full build: `mvn clean package -DskipUTs`
- Compile with protobuf: `mvn clean compile test-compile`

### Unit Testing
- Run all unit tests: `mvn clean test -Dtick=5`
- Run single test class: `mvn -pl module -am test -Dtest=ClassName -Dtick=5 -Dsurefire.failIfNoSpecifiedTests=false`
- Run single test method: `mvn -pl module -am test -Dtest=ClassName#method -Dtick=5 -Dsurefire.failIfNoSpecifiedTests=false`
- We suggest using `-Dtick=5` to increase test stability

### Integration Testing
- Run all integration tests: `mvn verify -DskipUTs -Dtick=5`
- Run single integration test: `mvn -pl it-test -am verify -DskipUTs=true -Dit.test=ClassName -Dtick=5`
- package should be done before run any integration tests

## Codebase Overview
- **Multi-module Maven project** with Java 11 (Java 8 for client module)
- **High-performance, zero-dependency** raft/kv/mq engine
- **Fiber-based concurrency** framework (coroutines)
- **Project name**: Always use "dongting" (English), never Chinese name
- **Quality Standard**: Code as a top-tier expert; prioritize performance and strive for excellence

### File Headers
All Java files must include Apache 2.0 license header (17 lines)

### Field Access Pattern
- Internal usage: Access public/package-private fields directly
- External API: Use getter/setter methods when additional logic is needed
- Encapsulation: Some classes use single-child pattern for hiding internals (e.g., `DtChannel` -> `DtChannelImpl`, `RaftGroup` -> `RaftGroupImpl`)

### Class Inheritance Pattern
For encapsulation purposes, some classes have a single implementation:
- `DtChannel` → `DtChannelImpl`
- `RaftGroupConfig` → `RaftGroupConfigEx`
- `RaftNode` → `RaftNodeEx`
- `RaftGroup` → `RaftGroupImpl`
- `RaftStatus` → `RaftStatusImpl`

**Rule**: When you have a parent class instance but need child class methods, cast directly to the child class—this is safe and intentional.

### Logging
- Logger: `com.github.dtprj.dongting.log.DtLog`
- Factory: `com.github.dtprj.dongting.log.DtLogs`
- Declaration: `private static final DtLog log = DtLogs.getLogger(ClassName.class);`

### Error Handling
- Use `BugLog` for unexpected errors (safer than assert): `BugLog.log(exception)`
- Search logs with `grep BugLog` to find unexpected issues
- Fiber errors: `Fiber.fatal()` for unrecoverable errors

### Comments
- Java source code and comments: **English only**
- All code must be in English
- Comments only when necessary (avoid unless adding value)

### Fiber/Coroutines
- Each raft group runs in a fiber group (single-threaded)
- See com.github.dtprj.dongting.fiber package for more details

### Testing
- Framework: JUnit 6 (org.junit.jupiter)
- Use `Tick.tick(millis)` to scale timeouts based on `-Dtick=N`
- Use `WaitUtil.waitUtil()` for polling conditions

### Zero Dependency Principle
- Main code implements custom protobuf encoding/decoding
- Protobuf files (`dt_packet.proto`, `dt_kv.proto`, `dt_raft_server.proto`) in test directories
- Generated protobuf code used only in tests
- No external dependencies in production code

### Performance
- Always prioritize performance
- Use object pools (e.g., `ByteBufferPool`)
- Minimize allocations in hot paths
- Use direct buffers for I/O
- Profile with performance constants in `PerfConsts`

### Module Structure
- **client**: Java 8 compatible, has module-info.java, multiple compile profiles
- **client-ex**: Development only, copied into client module during package
- **server**: Java 11+, depends on client
- **test-support**: Shared test utilities
- **dist**: Java 11+, packaging and scripts only
- **it-test**: Integration tests
- **demos**: Example applications
