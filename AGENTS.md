## Build and Test

- Before running compile/package/test commands or writing tests, see [build-and-test](.agents/skills/build-and-test/SKILL.md) skill for important details.
- If ```IntelliJ-IDEA``` MCP is available, use it instead of Java LSP. Usage skill: [idea-mcp](.agents/skills/idea-mcp/SKILL.md)

## Project Overview
- **Multi-module Maven project** with Java 11 (Java 8 for client module)
- **High-performance, zero-dependency** raft/kv/mq engine
- **Fiber-based concurrency** framework (coroutines)
- **Project name**: Always use "dongting" (English), never Chinese name
- **Quality Standard**: Code as a top-tier expert; prioritize performance and strive for excellence

### Module Structure
- **client**: Java 8 compatible, has module-info.java, multiple compile profiles
- **client-ex**: Development only, copied into client module during package
- **server**: Java 11+, depends on client
- **test-support**: Shared test utilities
- **dist**: Java 11+, packaging and scripts only
- **it-test**: Integration tests
- **demos**: Example applications

## Architecture

### Performance
- Always prioritize performance
- Use object pools (e.g., `ByteBufferPool`)
- Minimize allocations in hot paths
- Use direct buffers for I/O
- Profile with performance constants in `PerfConsts`

### Zero Dependency Principle
- Main code implements custom protobuf encoding/decoding
- Protobuf files (`dt_packet.proto`, `dt_kv.proto`, `dt_raft_server.proto`) in test directories
- Generated protobuf code used only in tests
- No external dependencies in production code

### ID Constraints
- Both **groupId** and **nodeId** must be positive integers (> 0). Zero and negative values are rejected.

### Fiber/Coroutines
- Each raft group runs in a fiber group (single-threaded)
- See com.github.dtprj.dongting.fiber package for more details

### Class Inheritance Pattern
For encapsulation purposes, some classes have a single implementation:
- `DtChannel` → `DtChannelImpl`
- `RaftGroupConfig` → `RaftGroupConfigEx`
- `RaftNode` → `RaftNodeEx`
- `RaftGroup` → `RaftGroupImpl`
- `RaftStatus` → `RaftStatusImpl`

**Rule**: When you have a parent class instance but need child class methods, cast directly to the child class—this is safe and intentional.

### Error Handling
- Use `BugLog` for unexpected errors (safer than assert): `BugLog.log(exception)`
- Search logs with `grep BugLog` to find unexpected issues
- Fiber errors: `Fiber.fatal()` for unrecoverable errors

### Testing
- Framework: JUnit 6 (org.junit.jupiter)
- Use `Tick.tick(millis)` to scale timeouts based on `-Dtick=N`
- Use `WaitUtil.waitUtil()` for polling conditions

## Coding Conventions

### File Headers
All Java files must include Apache 2.0 license header (17 lines)

### Comments
- Java source code and comments: **English only**
- All code must be in English
- Add comments only when necessary
- Don't delete existing comments unless they are no longer relevant

### Field Access Pattern
- Internal usage: Access public/package-private fields directly
- External API: Use getter/setter methods when additional logic is needed
- Encapsulation: Some classes use single-child pattern for hiding internals (e.g., `DtChannel` -> `DtChannelImpl`, `RaftGroup` -> `RaftGroupImpl`)

### Logging
- Logger: `com.github.dtprj.dongting.log.DtLog`
- Factory: `com.github.dtprj.dongting.log.DtLogs`
- Declaration: `private static final DtLog log = DtLogs.getLogger(ClassName.class);`
