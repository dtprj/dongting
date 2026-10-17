---
name: build-and-test
description: Build/test commands, and unit testing key points including BugLog handling and tick/sleep timing rules.
---

# Build and Test

## Quick Reference

### Build Commands

| Command | Purpose |
|---------|---------|
| `mvn clean package -DskipUTs` | Full build (skips unit tests) |
| `mvn clean package -DskipUTs -PminimalDist` | Minimal dist (no slf4j/logback) |
| `mvn clean package -DskipUTs -PaddJRE` | Dist with trimmed JRE (jlink) |
| `mvn clean package -DskipUTs -PminimalDist,addJRE` | Minimal dist with trimmed JRE (no slf4j/logback/java.xml) |
| `mvn clean compile test-compile` | Compile with protobuf |

### Unit Testing

| Command | Purpose |
|---------|---------|
| `mvn clean test -Dtick=3` | Run all unit tests, takes about 25 seconds to run. It is recommended to redirect to a temporary file, as the large amount of output will be truncated otherwise. |
| `mvn -pl module -am test -Dtest=ClassName -Dtick=3 -Dsurefire.failIfNoSpecifiedTests=false` | Run single test class |
| `mvn -pl module -am test -Dtest=ClassName#method -Dtick=3 -Dsurefire.failIfNoSpecifiedTests=false` | Run single test method |

**Tip**: Use `-Dtick=3` to increase test stability.

### Integration Testing

| Command | Purpose |
|---------|---------|
| `mvn verify -DskipUTs -Dtick=3` | Run all integration tests, takes about 100 seconds to run. It is recommended to redirect to a temporary file, as the large amount of output will be truncated otherwise. |
| `mvn -pl it-test -am verify -DskipUTs=true -Dit.test=ClassName -Dtick=3` | Run single integration test |

**Note**: Package must be completed before running integration tests. After modifying code, always run `mvn clean package -DskipUTs` first.

## Compilation Notes

Due to the project's complex build process (multiple compilation phases in pom.xml, protobuf compilation), Java LSP may frequently show compilation errors. Use the following approach:

1. **If IntelliJ-IDEA MCP is available** (recommended): Use it for accurate code analysis and building. See the [idea-mcp skill](../idea-mcp/SKILL.md) for usage instructions.
2. **Otherwise** (e.g., IDEA not running): Use Maven commands directly.

## Test Timing Rules

- Scale all sleeps/waits/timeouts with `Tick.tick(n)`; `Thread.sleep` no longer than `tick(5)`, unless the sleep is the behavior under test
- **Stability bar**: tests must pass almost every time with `-Dtick=1` on a modern mainstream computer, and must be guaranteed to pass with `-Dtick=3` on very slow machines
- Prefer event-driven waits (blocking read, `Future.get`, `WaitUtil` for conditions without an event source) over sleeps
- When waiting for a production timeout, shrink it under test (e.g. `maxPacketDecodeNanos = tick(10)`, with `cleanInterval`/`selectTimeout` = 1ms), but keep it several times any work that must finish first

## BugLog in Unit Tests

The project uses `BugLogExtension` to check BugLog state after each test. If BugLog was triggered, the test fails with "BugLog count should be 0".

### Key Points

1. **BugLog state is global** - One test not resetting BugLog causes ALL subsequent tests to fail
2. **Only fix the FIRST failing test** - When multiple tests fail due to BugLog, find and fix only the first one; the others will pass automatically
3. **Not all time adjustments trigger BugLog** - Only operations that cause "nanoTime go back" or other error conditions trigger BugLog

### When to Reset BugLog

Reset BugLog only in tests that intentionally trigger error conditions:
- Tests that call `TestUtil.updateTimestamp()` to simulate time passage (may cause "nanoTime go back")
- Tests that intentionally create invalid states to test error handling
- Tests that throw mock exceptions to test retry logic

### Example

```java
@Test
void testLockExpiration() {
    // ... test code ...
    TestUtil.updateTimestamp(ts, ts.nanoTime + 100_000_000L, ts.wallClockMillis + 100);
    // ... operations that cause Timestamp.refresh() or update() to detect "nanoTime go back" ...
    // The time adjustment + subsequent operations trigger BugLog
    BugLog.reset();
    // ... more test code ...
}
```

### Rules

- **DO** reset immediately after the code that triggers BugLog
- **DO** add a brief comment explaining why BugLog might be triggered
- **DO NOT** reset in `@BeforeEach`/`@AfterEach`/`@BeforeAll`/`@AfterAll` - this hides problems
- **DO NOT** reset in test base classes
- **DO NOT** wrap entire test methods in try-finally just for BugLog.reset()

## Build Requirements

- **Java version**: build with JDK 17+ (test-support); server targets 11, client targets 8
