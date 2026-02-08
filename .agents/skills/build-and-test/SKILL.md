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

## Build Requirements

- **Java version**: Java 11 for server, Java 8 for client module
- **Build tool**: Maven
- **Module structure**: Multi-module Maven project

See [project-structure.md](references/project-structure.md) for module details.
