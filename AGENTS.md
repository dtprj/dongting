## Build and Test Commands

### Build
- Full build: `mvn clean package -DskipITs -DskipUTs`
- Compile with protobuf: `mvn clean compile test-compile`

### Testing
- Run all tests: `mvn clean test -Dtick=5`
- Run single test class: `mvn test -Dtest=ClassName -Dtick=5`
- Run single test method: `mvn test -Dtest=ClassName#methodName -Dtick=5`
- The `-Dtick=N` parameter multiplies test timing (default: 1)
- Run integration tests: `mvn verify -DskipUTs`

## Codebase Overview
- **Multi-module Maven project** with Java 11 (Java 8 for client module)
- **High-performance, zero-dependency** raft/kv/mq engine
- **Fiber-based concurrency** framework (coroutines)
- **Project name**: Always use "dongting" (English), never Chinese name

## Code Style Guidelines

### File Headers
All Java files must include Apache 2.0 license header (17 lines)

### Imports
- Organize logically: std lib, third-party, internal project
- No wildcard imports
- Internal imports: `com.github.dtprj.dongting.*`

### Naming Conventions
- Classes: PascalCase (e.g., `FiberGroup`, `LogFileQueue`)
- Methods: camelCase (e.g., `startGroup`, `checkPositive`)
- Constants: UPPER_SNAKE_CASE (e.g., `MAX_WRITE_BUFFER_SIZE`, `RPC_MAJOR_VER`)
- Private fields: camelCase (e.g., `fiberGroup`, `logAppender`)
- Package-private classes: PascalCase but not public

### Field Access Pattern
- Internal usage: Access public/package-private fields directly
- External API: Use getter/setter methods when additional logic is needed
- Encapsulation: Some classes use single-child pattern for hiding internals (e.g., `DtChannel` -> `DtChannelImpl`, `RaftGroup` -> `RaftGroupImpl`)

### Logging
- Logger: `com.github.dtprj.dongting.log.DtLog`
- Factory: `com.github.dtprj.dongting.log.DtLogs`
- Declaration: `private static final DtLog log = DtLogs.getLogger(ClassName.class);`
- Levels: debug(), info(), warn(), error()

### Error Handling
- Use `BugLog` for unexpected errors (safer than assert): `BugLog.log(exception)`
- Checkers: `DtUtil.checkPositive()`, `DtUtil.checkNotNull()`
- Close resources with `DtUtil.close(AutoCloseable)`
- Fiber errors: `Fiber.fatal()` for critical failures

### Types and Generics
- Explicit generic types: `FiberFrame<Void>`, `FrameCallResult`
- Use `@SuppressWarnings` where necessary
- Avoid raw types when possible

### Comments
- Java source code and comments: **English only**
- All code must be in English
- Comments only when necessary (avoid unless adding value)

### Fiber/Coroutines
- Each raft group runs in a fiber group (single-threaded by default)
- Use `FiberFrame` for async operations
- Return `FrameCallResult` from fiber methods
- Use `Fiber.sleep()`, `Fiber.call()`, `Fiber.resume()`

### Testing
- Framework: JUnit 5 (org.junit.jupiter)
- Use `@BeforeEach` and `@AfterEach` for setup/teardown
- Extend abstract test bases (e.g., `AbstractFiberTest`)
- Use `Tick.tick(millis)` to scale timeouts based on `-Dtick=N`
- Use `WaitUtil.waitUtil()` for polling conditions
- Test classes end with `Test`: `FiberLifeCycleTest`

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

### Method Patterns
- Static factory methods: `public static FiberFuture<Void> join()`
- Builder pattern: `new Fiber(name, group, frame, daemon, signals)`
- Check-then-act: Use atomic operations or fiber-safe patterns
- Async callbacks: Lambda expressions with resume points

### Constants
- Define class-level constants in `private static final`
- Use descriptive names with units where applicable
- Magic numbers should be named constants
