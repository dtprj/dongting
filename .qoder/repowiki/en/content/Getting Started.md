# Getting Started

<cite>
**Referenced Files in This Document**   
- [README.md](file://README.md)
- [developer.md](file://docs/developer.md)
- [StandaloneDemoServer.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/standalone/StandaloneDemoServer.java)
- [DemoServer1.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/cluster/DemoServer1.java)
- [DemoServer2.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/cluster/DemoServer2.java)
- [DemoServer3.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/cluster/DemoServer3.java)
- [DemoClient.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/cluster/DemoClient.java)
- [PeriodPutClient.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/cluster/PeriodPutClient.java)
- [ChangeLeader.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/cluster/ChangeLeader.java)
- [EmbeddedDemo.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/embedded/EmbeddedDemo.java)
- [WatchDemoClient.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/watch/WatchDemoClient.java)
- [TtlDemoClient.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/ttl/TtlDemoClient.java)
- [LockDemoClient.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/lock/LockDemoClient.java)
- [DemoClientBase.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/base/DemoClientBase.java)
- [DemoKvServerBase.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/base/DemoKvServerBase.java)
</cite>

## Table of Contents
1. [Introduction](#introduction)
2. [Development Environment Setup](#development-environment-setup)
3. [Running Demo Applications](#running-demo-applications)
4. [Running a 3-Node RAFT Cluster](#running-a-3-node-raft-cluster)
5. [Leader Transfer Demonstration](#leader-transfer-demonstration)
6. [Basic Operations](#basic-operations)
7. [Troubleshooting Common Issues](#troubleshooting-common-issues)
8. [Conclusion](#conclusion)

## Introduction

The Dongting framework is a high-performance engine that integrates RAFT consensus algorithm, distributed configuration server (DtKV), low-level RPC, and planned message queue capabilities. This getting started guide provides step-by-step instructions for onboarding new users to the Dongting framework, focusing on setting up the development environment, running demo applications, and understanding basic operations.

The framework is designed with performance in mind, achieving impressive throughput metrics even on standard hardware. With zero external dependencies and minimal JAR size (under 1MB combined), Dongting can be easily embedded into applications. The client requires Java 8 while the server requires Java 11, making it accessible for a wide range of projects.

All demo applications are located in the `demos` directory and require no configuration - they can be run directly by executing their main methods, preferably within an IDE for easier debugging and observation.

**Section sources**
- [README.md](file://README.md#L4-L141)

## Development Environment Setup

To set up your development environment for working with the Dongting framework, follow these steps:

1. **Install Required Software**:
   - JDK 17 (required for compiling the entire project, particularly the benchmark module)
   - Maven 3.6+
   - An IDE such as IntelliJ IDEA or Eclipse

2. **Clone the Repository**:
   ```bash
   git clone https://github.com/dtprj/dongting.git
   cd dongting
   ```

3. **Generate Protocol Buffer Files**:
   Before importing the project into your IDE, generate the necessary source files from protocol buffer definitions:
   ```bash
   mvn clean compile test-compile
   ```

4. **IDE Configuration**:
   When importing into IntelliJ IDEA:
   - Ensure the generated source paths are added to the project sources
   - The IDE should automatically handle the `--add-exports` and `--add-reads` parameters after Maven project synchronization
   - The client module uses multi-release JAR features that support Java versions from 8 to 25

5. **Verify Setup**:
   Run unit tests to confirm your environment is properly configured:
   ```bash
   mvn clean test -Dtick=5
   ```

After completing these steps, you'll be able to build and run the project within your IDE, set breakpoints, and debug the code effectively.

**Section sources**
- [developer.md](file://docs/developer.md#L1-L45)
- [pom.xml](file://pom.xml#L1-L195)

## Running Demo Applications

The Dongting framework includes several demo applications that demonstrate different deployment configurations and use cases. These demos require no configuration and can be executed by running their main methods directly.

### Standalone Configuration

The standalone demo runs a single-node RAFT group, useful for development and testing:

1. Locate the `StandaloneDemoServer` class in `demos/src/main/java/com/github/dtprj/dongting/demos/standalone/`
2. Run the main method
3. The server will start with node ID 1, listening on ports 4001 (replication) and 5001 (service)

This configuration is ideal for understanding the basic operation of the framework without the complexity of cluster coordination.

### Cluster Configuration

The cluster demo demonstrates a 3-node RAFT cluster for high availability:

1. Run `DemoServer1`, `DemoServer2`, and `DemoServer3` in separate processes or terminals
2. Each server will start with its respective node ID (1, 2, or 3)
3. The RAFT cluster will typically be ready within one second
4. Nodes communicate via replication ports (4001-4003) and service ports (5001-5003)

### Embedded Configuration

The embedded demo runs multiple servers and a client within a single process:

1. Run the `EmbeddedDemo` class
2. This will start three RAFT servers (nodes 1, 2, and 3) and a client within the same JVM
3. The demo automatically waits for all servers to be ready before proceeding with operations

This configuration is useful for testing and development, as it eliminates network latency and simplifies debugging.

**Section sources**
- [README.md](file://README.md#L70-L94)
- [StandaloneDemoServer.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/standalone/StandaloneDemoServer.java#L1-L33)
- [DemoServer1.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/cluster/DemoServer1.java#L1-L33)
- [DemoServer2.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/cluster/DemoServer2.java#L1-L33)
- [DemoServer3.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/cluster/DemoServer3.java#L1-L33)
- [EmbeddedDemo.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/embedded/EmbeddedDemo.java#L1-L61)

## Running a 3-Node RAFT Cluster

This section provides detailed instructions for launching and interacting with a 3-node RAFT cluster.

### Starting the Cluster

1. **Launch the Server Nodes**:
   - Run `DemoServer1.main()` - starts node 1 with replication port 4001 and service port 5001
   - Run `DemoServer2.main()` - starts node 2 with replication port 4002 and service port 5002  
   - Run `DemoServer3.main()` - starts node 3 with replication port 4003 and service port 5003
   - The RAFT cluster will automatically form and elect a leader within seconds

2. **Run the Performance Demo Client**:
   - Execute `DemoClient.main()`
   - This client will send 1 million put operations followed by 1 million get operations
   - Results will be displayed showing throughput and response times
   - Expected output format:
     ```
     ----------------------------------------------
     Unbelievable! 1000000 linearizable puts finished in X ms, 1000000 linearizable lease gets finished in Y ms
     Throughput: Z puts/s, W gets/s
     Windows 11 with 12 cores
     ----------------------------------------------
     ```

### Testing Resilience with Periodic Operations

To demonstrate the cluster's resilience to node failures:

1. **Start Continuous Operations**:
   - Run `PeriodPutClient.main()`
   - This client sends a PUT request every second to a rotating set of keys
   - Each operation logs its execution time

2. **Simulate Node Failure**:
   - While `PeriodPutClient` is running, terminate one of the server processes (e.g., `DemoServer2`)
   - Observe that the client continues to operate without interruption
   - The RAFT cluster will automatically elect a new leader from the remaining nodes
   - Restart the terminated server to see it rejoin the cluster

The `PeriodPutClient` demonstrates the framework's fault tolerance - operations continue seamlessly during node failures and recoveries, maintaining linearizability.

**Section sources**
- [README.md](file://README.md#L79-L87)
- [DemoClient.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/cluster/DemoClient.java#L1-L39)
- [PeriodPutClient.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/cluster/PeriodPutClient.java#L1-L51)
- [DemoClientBase.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/base/DemoClientBase.java#L1-L86)

## Leader Transfer Demonstration

This section explains how to manually transfer leadership between nodes in a RAFT cluster.

### Executing Leader Transfer

1. **Run the ChangeLeader Demo**:
   - Execute `ChangeLeader.main()`
   - This program connects to the cluster using the replication ports (4001-4003)
   - It first identifies the current leader by calling `fetchLeader()`

2. **Understanding the Process**:
   - The program determines the current leader node ID
   - It selects a new target node (different from the current leader)
   - Calls `transferLeader()` with a 5-second timeout
   - Upon successful transfer, it prints "transfer leader success"

3. **Expected Output**:
   ```
   current leader is node X
   transfer leader success
   ```

### Observing the Effect

While the leader transfer is in progress:

1. Keep the `PeriodPutClient` running to monitor cluster availability
2. Observe that operations continue without interruption during the transfer
3. After the transfer completes, verify that the new node is now the leader
4. The client applications automatically redirect requests to the new leader

This demonstrates the non-disruptive nature of leader transfers in the Dongting framework, allowing for planned maintenance and load balancing without affecting service availability.

**Section sources**
- [README.md](file://README.md#L86-L87)
- [ChangeLeader.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/cluster/ChangeLeader.java#L1-L47)

## Basic Operations

The Dongting framework provides several distributed data structure operations through its DtKV component. This section covers the basic operations demonstrated in the various demo applications.

### Key-Value Operations

The core functionality includes standard key-value operations:

- **PUT/GET**: Store and retrieve values with linearizable consistency
- **REMOVE**: Delete keys from the store
- **Directory operations**: Create directories (`mkdir`) that can contain sub-keys

These operations are demonstrated in the cluster demos and form the foundation for more advanced features.

### Watch Operations

The watch demo shows how to monitor changes to keys:

1. Run `WatchDemoClient.main()`
2. The client sets up a listener for changes to specific keys
3. Operations trigger events with different states:
   - `STATE_VALUE_EXISTS`: Key has a value
   - `STATE_DIRECTORY_EXISTS`: Directory exists (with sub-keys)
   - `STATE_NOT_EXISTS`: Key or directory has been deleted

The watch mechanism allows applications to react to changes in the data store in real-time.

### TTL (Time-To-Live) Operations

The TTL demo demonstrates automatic key expiration:

1. Run `TtlDemoClient.main()`
2. The client sets a key with a 3-second TTL using `putTemp()`
3. Initially, the key can be retrieved
4. After 5 seconds (longer than the TTL), the key is automatically deleted
5. Subsequent GET operations return null

This feature is useful for implementing cache-like behavior or temporary locks.

### Distributed Locks

The lock demo shows both manual and automatic distributed locking:

1. Run `LockDemoClient.main()`
2. Create a distributed lock with `createLock()`
3. Acquire the lock with `tryLock(timeout, leaseTime)`
4. Release with `unlock()`

The framework also supports auto-renewing locks for leader election scenarios, where the lease is automatically extended as long as the client is alive.

**Section sources**
- [README.md](file://README.md#L112-L121)
- [WatchDemoClient.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/watch/WatchDemoClient.java#L1-L90)
- [TtlDemoClient.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/ttl/TtlDemoClient.java#L1-L58)
- [LockDemoClient.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/lock/LockDemoClient.java#L1-L50)

## Troubleshooting Common Issues

This section addresses common issues encountered when setting up and running the Dongting framework demos.

### IDE Compilation Errors

**Problem**: Importing the project into an IDE results in compilation errors despite successful command-line builds.

**Solution**:
1. Run `mvn clean compile test-compile` to generate protocol buffer sources
2. Ensure the generated source directories are added to your IDE's source paths
3. For IntelliJ IDEA, the paths are typically:
   - `client/target/generated-sources/protobuf/java`
   - `client/target/generated-test-sources/protobuf/java`
   - `server/target/generated-sources/protobuf/java`
   - `server/target/generated-test-sources/protobuf/java`

### Port Conflicts

**Problem**: "Address already in use" errors when starting demo servers.

**Solution**:
1. Check if previous demo instances are still running
2. Use `netstat -an | grep 400[1-3]` or `lsof -i :4001` to identify processes using the ports
3. Terminate conflicting processes or modify the port numbers in the demo code

### RAFT Cluster Formation Issues

**Problem**: Cluster fails to form or leader election doesn't complete.

**Solution**:
1. Verify all three `DemoServer` instances are running
2. Check that the server configurations match (particularly the member list "1,2,3")
3. Ensure network connectivity between the nodes
4. Review log output for any error messages

### Client Connection Failures

**Problem**: Clients cannot connect to the RAFT cluster.

**Solution**:
1. Verify the client is using service ports (5001-5003) not replication ports (4001-4003)
2. Check that the server cluster is fully formed and has elected a leader
3. Ensure the client's server list matches the actual server configuration

### Performance Issues

**Problem**: Lower than expected throughput.

**Solution**:
1. Ensure you're using appropriate JVM settings (ZGC garbage collector recommended)
2. Verify the `syncForce` setting in `DemoKvServerBase` - setting it to false improves performance
3. Run on hardware with SSD storage for better I/O performance
4. Use the async write mode for higher throughput (at the cost of slightly higher potential data loss risk)

**Section sources**
- [DemoKvServerBase.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/base/DemoKvServerBase.java#L1-L84)
- [README.md](file://README.md#L40-L46)

## Conclusion

The Dongting framework provides a comprehensive, high-performance solution for distributed systems requiring strong consistency. Through the various demo applications, you've learned how to:

- Set up a development environment for working with the framework
- Launch standalone, clustered, and embedded configurations
- Run a 3-node RAFT cluster and interact with it using client applications
- Demonstrate fault tolerance by simulating node failures
- Perform leader transfers without service disruption
- Utilize basic distributed data structure operations including key-value storage, watches, TTL, and distributed locks

The framework's zero-dependency design and small footprint make it easy to integrate into existing applications, while its performance characteristics enable it to handle demanding workloads. Although currently in alpha stage (v0.8.x-ALPHA), the demos provide a solid foundation for understanding the framework's capabilities and potential use cases.

For further exploration, consider examining the multi-RAFT demos for sharding scenarios, or the configuration change demos for dynamic cluster reconfiguration. The source code in the `demos` directory provides clear examples of how to use the various APIs, serving as a valuable reference for building your own applications on top of the Dongting framework.