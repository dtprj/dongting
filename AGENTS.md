## Build and Test

- Before running compile/package/test commands or writing tests, see `build-and-test` skill for details.
- If `IntelliJ-IDEA` MCP is available, prioritize using the tools it provides. Usage skill: `idea-mcp`
  - Use the `build_project`/`get_file_problems` tools report issues
  - The `read_file` tool is better than the built-in read tool

## Project Overview
- **Multi-module Maven project**; build with JDK 17+ (client targets Java 8, server Java 11)
- **High-performance, zero-dependency** raft/kv/mq engine
- **Fiber-based concurrency** framework (coroutines)
- **Project name**: Always use "dongting" (English), never Chinese name
- **Quality Standard**: Code as a top-tier expert; prioritize performance and strive for excellence

### Module Structure
- **client**: Java 8 compatible, has module-info.java, multiple compile profiles
- **client-ex**: Development only, copied into client module during package
- **server**: Java 11+, depends on client
- **test-support**: Shared test utilities, Java 17+
- **dist**: Java 11+, packaging and scripts only
- **it-test**: Integration tests

## Architecture

### Performance
- Always prioritize performance
- Use object pools (e.g., `ByteBufferPool`)
- Minimize allocations in hot paths
- Use direct buffers for I/O
- Profile with performance constants in `PerfConsts` (the perf ops never throw exception like logger)

### Zero Dependency Principle
- Main code implements custom protobuf encoding/decoding
- Protobuf files in test directories, generated protobuf code used only in tests
- No external dependencies in production code

### ID Constraints
- Both **groupId** and **nodeId** must be positive integers (> 0). Zero and negative values are rejected.

### Fiber/Coroutines
- Each raft group runs in a fiber group (single-threaded)
- FiberFrame's doFinally method guarantees execution just like a Java finally block. Similar to Java daemon threads, when a fiber group is shut down, daemon fibers may stop at any suspension point, in which case doFinally will not execute.
- Since fiber groups in this project are dynamically created and destroyed, a fiber group shutdown does not mean the process is shutting down. Therefore, daemon fibers must not hold any resources that need to be returned or cleaned up (e.g., objects borrowed from ByteBufferPool, QueueFile counters), because cleanup cannot be guaranteed.
- Group exit waits for futures with registered callbacks (see `FiberFuture.registerCallback()` javadoc); such futures must have a completion path during shutdown, or group exit hangs.
- See com.github.dtprj.dongting.fiber package for more details

### Class Inheritance Pattern
For encapsulation purposes, some classes have a single implementation:
- `DtChannel` → `DtChannelImpl`
- `RaftGroupConfig` → `RaftGroupConfigEx`
- `RaftNode` → `RaftNodeEx`
- `RaftGroup` → `RaftGroupImpl`
- `RaftStatus` → `RaftStatusImpl`

**Rule**: When you have a parent class instance but need child class methods, cast directly to the child class—this is safe and intentional.

### ByteBuffer.array() Usage
- If the ByteBuffer is external (not allocated/wrapped within the current class), always include `arrayOffset()` in index calculations — it may have been sliced.

### Error Handling
- Use `BugLog` for unexpected errors (safer than assert): `BugLog.log(exception)`
- Events that are expected to happen must never be logged to BugLog. If a BugLog entry appears, either eliminate the bug or change it to log.error
- Search logs with `grep BugLog` to find unexpected issues
- Fiber errors: `Fiber.fatal()` for unrecoverable errors

### Testing
- Use `Tick.tick(millis)` to scale timeouts based on `-Dtick=N`
- Use `WaitUtil.waitUtil()` for polling conditions
- In unit tests, do not sleep longer than `Thread.sleep(tick(5))` unless necessary
- Do not over-test; unit tests should finish running as quickly as possible. The more tests pile up, the harder they are to maintain and the slower they run.
- After major code changes, check whether any unit tests can be updated or deleted.
- Rare timing cases do not need unit test coverage; write a temporary test during development (class name starting with TempTest) and delete it before committing.

## Coding Conventions

### Comments
- Do not add comments unless necessary; leave a single line only at key points
- Comments should only point out the key points and direction; details can be seen in the code, so there is no need to write at length
- When modifying existing comments, do not pile on more text; consider simplifying — one line is best

### Field Access Pattern
- Internal usage: Access public/package-private fields directly
- External API: Use getter/setter methods when additional logic is needed

### git
- git push is forbidden
- Do not commit without user's permission
- A git commit message is usually a single line — only a very complex commit may use a one-line summary plus 2-3 lines of detail
