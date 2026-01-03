# Development Environment Setup

<cite>
**Referenced Files in This Document**   
- [pom.xml](file://pom.xml)
- [client/pom.xml](file://client/pom.xml)
- [server/pom.xml](file://server/pom.xml)
- [client/src/main/java/module-info.java](file://client/src/main/java/module-info.java)
- [server/src/main/java/module-info.java](file://server/src/main/java/module-info.java)
- [docs/developer.md](file://docs/developer.md)
- [README.md](file://README.md)
</cite>

## Table of Contents
1. [Introduction](#introduction)
2. [Prerequisites](#prerequisites)
3. [Build Process](#build-process)
4. [IDE Configuration](#ide-configuration)
5. [Zero-Dependency Architecture and SLF4J Integration](#zero-dependency-architecture-and-slf4j-integration)
6. [Common Setup Issues](#common-setup-issues)
7. [Development Workflow Tips](#development-workflow-tips)
8. [Conclusion](#conclusion)

## Introduction
This document provides comprehensive guidance for setting up a development environment for the Dongting project, a high-performance engine integrating RAFT, configuration server, messaging queues, and low-level RPC. The setup process involves understanding the project's unique Java version requirements, Maven build configuration, module system usage, and IDE integration challenges. This guide covers all aspects from initial prerequisites to advanced development workflows, ensuring developers can efficiently work with the codebase.

**Section sources**
- [README.md](file://README.md#L4-L141)

## Prerequisites

### Java Version Requirements
The Dongting project has a sophisticated multi-version Java requirement structure:
- **dongting-client**: Requires Java 8 for runtime, but uses Java 11 features during compilation
- **dongting-server**: Requires Java 11 for both compilation and runtime
- **Full project compilation**: Requires Java 17 due to the benchmark module's requirements
- **Testing and demos**: Can run with Java 11 or higher

This multi-version approach allows the client library to maintain compatibility with Java 8 environments while leveraging newer Java features where available.

### Maven Requirements
Apache Maven 3.6.0 or higher is required for building the project. The project uses standard Maven conventions with a multi-module structure defined in the root `pom.xml` file. No special Maven plugins or configurations beyond what's included in the project are required for command-line builds.

### IDE Requirements
Either IntelliJ IDEA (2020.3 or higher) or Eclipse (2020-12 or higher) can be used for development. Both IDEs must support Java 17 for full project compilation, though developers can work with specific modules using lower Java versions.

**Section sources**
- [README.md](file://README.md#L58-L59)
- [docs/developer.md](file://docs/developer.md#L9-L10)
- [pom.xml](file://pom.xml#L127)
- [client/pom.xml](file://client/pom.xml#L52-L54)
- [server/pom.xml](file://server/pom.xml#L47-L49)
- [benchmark/pom.xml](file://benchmark/pom.xml#L51-L53)

## Build Process

### Maven Build Lifecycle
The Dongting project follows standard Maven build phases with specific configurations for its multi-module structure. The root `pom.xml` defines the overall project structure with modules including client, server, test-support, benchmark, and demos.

### Compilation
To compile the entire project:
```bash
mvn clean compile
```

The build process handles multiple compilation targets:
- Client module compiles with Java 8 compatibility for most classes
- Specific Java 11 features are compiled separately for enhanced performance
- The unsafe package uses Java 8 compilation to maintain compatibility

### Testing
Execute unit tests with:
```bash
mvn clean test -Dtick=5
```

The test configuration includes specific JVM arguments for memory management:
```xml
<argLine>@{argLine} -Xmx256M -XX:MaxDirectMemorySize=128M --add-modules ALL-MODULE-PATH</argLine>
```

### Packaging
The project produces two main JAR files:
- `dongting-client-{version}.jar`: Lightweight client library (~500KB)
- `dongting-server-{version}.jar`: Server components with fiber and RAFT implementations

Build and package with:
```bash
mvn clean package
```

### Protocol Buffer Compilation
The project uses Protocol Buffers for serialization, requiring the protobuf-maven-plugin:
```xml
<plugin>
    <groupId>org.xolstice.maven.plugins</groupId>
    <artifactId>protobuf-maven-plugin</artifactId>
    <version>0.6.1</version>
    <extensions>true</extensions>
    <executions>
        <execution>
            <goals>
                <goal>compile</goal>
                <goal>test-compile</goal>
            </goals>
        </execution>
    </executions>
    <configuration>
        <protocArtifact>com.google.protobuf:protoc:4.29.3:exe:${os.detected.classifier}</protocArtifact>
    </configuration>
</plugin>
```

**Section sources**
- [pom.xml](file://pom.xml#L114-L194)
- [client/pom.xml](file://client/pom.xml#L42-L111)
- [docs/developer.md](file://docs/developer.md#L17-L19)

## IDE Configuration

### Project Import
When importing into IntelliJ IDEA or Eclipse:
1. Open the project root directory containing the root `pom.xml`
2. Allow the IDE to detect and import all modules automatically
3. Ensure Maven import recognizes the multi-module structure

### Source Directory Configuration
After initial import, configure source directories:
1. Run `mvn clean compile test-compile` to generate Protocol Buffer sources
2. Add generated sources to the IDE's source path:
   - `target/generated-sources/protobuf/java`
   - `target/generated-test-sources/protobuf/java`

IntelliJ IDEA typically handles this automatically during Maven project synchronization.

### Module Configuration
The project uses Java modules with `module-info.java` files:
- **client module**: `dongting.client` module with exports for all public packages
- **server module**: `dongting.server` module with transitive dependency on client

Ensure your IDE properly recognizes the module-path vs. classpath distinction.

### Annotation Processing
No annotation processors are used in the project, so annotation processing can be disabled in the IDE settings to improve build performance.

### Compiler Settings
Configure the IDE to handle multiple Java versions:
- Set project SDK to Java 17 for full compilation
- Configure module-specific language levels:
  - client module: Java 8 (with Java 11 for specific compilation units)
  - server module: Java 11
  - test-support and benchmark: Java 17

**Section sources**
- [docs/developer.md](file://docs/developer.md#L15-L31)
- [client/src/main/java/module-info.java](file://client/src/main/java/module-info.java#L16-L28)
- [server/src/main/java/module-info.java](file://server/src/main/java/module-info.java#L16-L27)

## Zero-Dependency Architecture and SLF4J Integration

### Zero-Dependency Design
Dongting follows a strict zero-dependency philosophy:
- No third-party libraries are required at runtime
- The entire project compiles to two JAR files under 1MB combined
- Transitive dependencies are eliminated

This design enables easy embedding into applications without dependency conflicts.

### Optional SLF4J Integration
Logging is handled through an abstraction layer:
- If SLF4J is present in the classpath, it is used automatically
- If SLF4J is not available, the JDK logging system is used as fallback
- SLF4J dependency is marked as optional in Maven:

```xml
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-api</artifactId>
    <optional>true</optional>
</dependency>
```

To enable SLF4J logging, simply add the SLF4J API and binding (e.g., logback-classic) to your application's classpath.

### Logging Implementation
The logging abstraction is implemented in the `com.github.dtprj.dongting.log` package:
- `DtLogs`: Factory for obtaining loggers
- `DtLog`: Interface for logging operations
- `Slf4jLog` and `JdkLog`: Implementations for SLF4J and JDK logging
- `MessageFormatter`: Handles message formatting with parameter substitution

**Section sources**
- [README.md](file://README.md#L47-L57)
- [pom.xml](file://pom.xml#L102-L111)
- [client/src/main/java/com/github/dtprj/dongting/log/MessageFormatter.java](file://client/src/main/java/com/github/dtprj/dongting/log/MessageFormatter.java#L237-L386)

## Common Setup Issues

### Module-Path vs Classpath Conflicts
The most common issue arises from the project's use of Java modules alongside traditional classpath dependencies:
- **Symptom**: "module not found" or "split package" errors
- **Solution**: Ensure the IDE properly distinguishes between module-path and classpath
- Add required JVM arguments: `--add-modules ALL-MODULE-PATH`

### Record Compilation Errors
Since the project supports Java 8, it cannot use Java 14+ records. Any attempt to use records will cause compilation failures in the client module. Use traditional classes with private fields and public accessors instead.

### Test Execution Configuration
Tests require specific JVM arguments for proper execution:
- **Memory settings**: `-Xmx256M -XX:MaxDirectMemorySize=128M`
- **Module configuration**: `--add-modules ALL-MODULE-PATH`
- **Issue**: Tests failing with OutOfMemoryError
- **Solution**: Ensure test runner includes the required JVM arguments

### Protocol Buffer Generation Issues
- **Symptom**: Missing generated classes from `.proto` files
- **Solution**: Run `mvn compile` before importing into IDE or manually refresh generated sources
- Verify that `protoc` is properly resolved by the protobuf-maven-plugin

### Multi-Version Compilation Conflicts
The client module's multi-version compilation can cause IDE confusion:
- **Symptom**: IDE showing compilation errors for Java 11 code in Java 8 project
- **Solution**: Configure IDE to recognize multiple compilation units with different source levels
- In IntelliJ IDEA: Configure module settings to include Java 11 source paths

**Section sources**
- [pom.xml](file://pom.xml#L177-L179)
- [client/pom.xml](file://client/pom.xml#L56-L60)
- [docs/developer.md](file://docs/developer.md#L12-L14)

## Development Workflow Tips

### Incremental Builds
For faster development cycles:
1. Focus on specific modules rather than building the entire project
2. Use `mvn compile` instead of `mvn package` for code changes
3. For client changes: `cd client && mvn compile`
4. For server changes: `cd server && mvn compile`

### Debugging Setup
Configure your IDE debugger with:
- **Source attachment**: Ensure generated Protocol Buffer sources are available
- **Remote debugging**: Use standard Java remote debugging for server components
- **Test debugging**: Run individual test classes rather than the entire suite

### Efficient Development Practices
1. **Use the demos**: The `demos` directory contains runnable examples that can be executed directly from the IDE
2. **Leverage benchmarks**: The `benchmark` module provides performance testing capabilities
3. **Incremental testing**: Run specific test classes rather than the entire test suite
4. **Profile memory usage**: The project uses direct memory and object pooling, so monitor memory patterns

### Code Navigation
- **Client API**: Start with `com.github.dtprj.dongting.net.NioClient` and `NioServer`
- **RAFT implementation**: Explore `com.github.dtprj.dongting.raft` packages
- **Fiber framework**: Examine `com.github.dtprj.dongting.fiber` for coroutine-like concurrency
- **Logging**: Review `com.github.dtprj.dongting.log` for the logging abstraction

### Performance Considerations
- The project is optimized for high throughput and low latency
- Object pooling is used extensively (ByteBufferPool, etc.)
- Minimize object allocation in hot paths
- Use the provided performance callback interfaces for monitoring

**Section sources**
- [README.md](file://README.md#L72-L122)
- [docs/developer.md](file://docs/developer.md#L33-L36)
- [demos/pom.xml](file://demos/pom.xml#L44-L47)

## Conclusion
Setting up the Dongting development environment requires attention to its unique multi-version Java requirements and modular architecture. By following the guidelines in this document, developers can successfully configure their environment for efficient development. The key points to remember are the Java version requirements (8 for client, 11 for server, 17 for full build), proper IDE configuration for multi-version compilation, and understanding the zero-dependency architecture with optional SLF4J integration. With the environment properly configured, developers can leverage the comprehensive demos and testing infrastructure to explore and extend the Dongting framework.