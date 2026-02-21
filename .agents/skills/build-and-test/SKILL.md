---
name: build-and-test
description: Build and test commands for the Dongting project. Use for compiling, running unit tests, and executing integration tests. Supports full build, protobuf compilation, and various test execution patterns.
---

# Build and Test

This skill provides commands and patterns for building and testing the Dongting project.

## Quick Reference

### Build Commands

| Command | Purpose |
|---------|---------|
| `mvn clean package -DskipUTs` | Full build (skips unit tests) |
| `mvn clean compile test-compile` | Compile with protobuf |

### Unit Testing

| Command | Purpose |
|---------|---------|
| `mvn clean test -Dtick=5` | Run all unit tests |
| `mvn -pl module -am test -Dtest=ClassName -Dtick=5 -Dsurefire.failIfNoSpecifiedTests=false` | Run single test class |
| `mvn -pl module -am test -Dtest=ClassName#method -Dtick=5 -Dsurefire.failIfNoSpecifiedTests=false` | Run single test method |

**Tip**: Use `-Dtick=5` to increase test stability.

### Integration Testing

| Command | Purpose |
|---------|---------|
| `mvn verify -DskipUTs -Dtick=5` | Run all integration tests |
| `mvn -pl it-test -am verify -DskipUTs=true -Dit.test=ClassName -Dtick=5` | Run single integration test |

**Note**: Package must be completed before running integration tests. After modifying code, always run `mvn clean package -DskipUTs` first.

## Compilation Notes

Due to the project's complex build process (multiple compilation phases in pom.xml, protobuf compilation), Java LSP may frequently show compilation errors. Use the following approach:

1. **If IntelliJ-IDEA MCP is available** (recommended): Use it for accurate code analysis and building. See the [idea-mcp skill](../idea-mcp/SKILL.md) for usage instructions.
2. **Otherwise** (e.g., IDEA not running): Use Maven commands directly.

## BugLog in Unit Tests

The project uses `BugLogExtension` to check BugLog state after each test. If BugLog was triggered, the test fails with "BugLog count should be 0".

### Key Points

1. **BugLog state is global** - One test not resetting BugLog causes ALL subsequent tests to fail
2. **Only fix the FIRST failing test** - When multiple tests fail due to BugLog, find and fix only the first one; the others will pass automatically
3. **Not all time adjustments trigger BugLog** - Only operations that cause "nanoTime go back" or other error conditions trigger BugLog
4. **Reset immediately after the triggering code** - Don't wrap the entire test in try-finally; reset right after the code that triggers BugLog

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

- **Java version**: Java 11 for server, Java 8 for client module
- **Build tool**: Maven
- **Module structure**: Multi-module Maven project

See [project-structure.md](references/project-structure.md) for module details.
