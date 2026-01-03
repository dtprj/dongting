# Resource Allocation Guidelines

<cite>
**Referenced Files in This Document**   
- [RaftServerConfig.java](file://server/src/main/java/com/github/dtprj/dongting/raft/server/RaftServerConfig.java)
- [NioServerConfig.java](file://client/src/main/java/com/github/dtprj/dongting/net/NioServerConfig.java)
- [DefaultRaftLog.java](file://server/src/main/java/com/github/dtprj/dongting/raft/store/DefaultRaftLog.java)
- [SimpleByteBufferPool.java](file://client/src/main/java/com/github/dtprj/dongting/buf/SimpleByteBufferPool.java)
- [DefaultPoolFactory.java](file://client/src/main/java/com/github/dtprj/dongting/buf/DefaultPoolFactory.java)
- [RaftPerfCallback.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/raft/RaftPerfCallback.java)
- [RaftServer.java](file://server/src/main/java/com/github/dtprj/dongting/raft/server/RaftServer.java)
- [RaftProcessor.java](file://server/src/main/java/com/github/dtprj/dongting/raft/server/RaftProcessor.java)
</cite>

## Table of Contents
1. [Introduction](#introduction)
2. [CPU Allocation Strategies](#cpu-allocation-strategies)
3. [Memory Requirements](#memory-requirements)
4. [Disk I/O Guidelines](#disk-io-guidelines)
5. [Network Bandwidth Considerations](#network-bandwidth-considerations)
6. [Impact of Resource Constraints on Consensus](#impact-of-resource-constraints-on-consensus)
7. [Conclusion](#conclusion)

## Introduction
This document provides comprehensive resource allocation guidelines for the Dongting distributed system to ensure optimal performance and stability. The guidelines cover CPU, memory, disk I/O, and network bandwidth requirements, with specific recommendations for configuring the system to handle RAFT consensus operations efficiently. The document focuses on the blockIoThreads configuration, memory requirements for RAFT logs and snapshots, disk I/O performance characteristics, and network bandwidth considerations for inter-node communication.

**Section sources**
- [README.md](file://README.md#L1-L141)

## CPU Allocation Strategies
The Dongting system employs a sophisticated thread model to optimize CPU utilization for different types of operations. The primary configuration parameter for CPU allocation is `blockIoThreads`, which is specifically designed to handle disk I/O operations in a non-blocking manner.

The `blockIoThreads` parameter is configured in the `RaftServerConfig` class and defaults to `max(availableProcessors * 2, 4)`. This formula ensures that there are sufficient threads to handle disk I/O operations without overwhelming the system with too many threads. The rationale behind this configuration is to provide enough parallelism for disk operations while maintaining efficient CPU utilization.

For general network I/O operations, the system uses a different thread allocation strategy implemented in the `NioServerConfig` class. The number of I/O threads is calculated based on the number of available processors with the following logic:
- 1 thread for 3 or fewer processors
- 2 threads for 4-6 processors
- 3 threads for 7-12 processors
- 4 threads for 13-24 processors
- 5 threads for 25-40 processors
- 6 threads for 41-64 processors
- 7 threads for 65-100 processors
- 8 threads for more than 100 processors

This graduated approach ensures optimal thread allocation across different hardware configurations, preventing thread contention on systems with fewer cores while providing sufficient parallelism on systems with many cores.

The business threads (non-I/O threads) are configured to use `availableProcessors * 2`, with a minimum of 6 and a maximum of 16 threads. This configuration balances the need for parallel processing with the overhead of thread management.

**Section sources**
- [RaftServerConfig.java](file://server/src/main/java/com/github/dtprj/dongting/raft/server/RaftServerConfig.java#L37)
- [NioServerConfig.java](file://client/src/main/java/com/github/dtprj/dongting/net/NioServerConfig.java#L45-L63)

## Memory Requirements
The Dongting system has specific memory requirements for maintaining RAFT logs, snapshots, and in-memory data structures. Proper memory allocation is critical for system stability and performance.

### Buffer Pool Configuration
The system uses a sophisticated buffer pool mechanism implemented in the `SimpleByteBufferPool` and `DefaultPoolFactory` classes. The buffer pool is designed to minimize garbage collection overhead by reusing ByteBuffer objects. The default configuration includes multiple buffer sizes (128, 256, 512, 1024, 2048, 4096, 8192, 16384 bytes) with corresponding minimum and maximum counts for each size.

The global buffer pools are configured with the following parameters:
- Direct buffer pool: 557,056 bytes minimum, 18,874,368 bytes maximum
- Heap buffer pool: Same size configuration as direct pool

The buffer pools are cleaned periodically (every second) to release unused buffers and prevent memory leaks. This automatic cleanup mechanism helps maintain stable memory usage over time.

### RAFT Log and Snapshot Memory
The RAFT log implementation in `DefaultRaftLog` maintains in-memory data structures for tracking log entries and index files. The system uses a two-level storage approach with separate files for log data and index information. The memory footprint of these structures depends on the number of log entries and the frequency of snapshot operations.

The system also maintains a tail cache (`TailCache`) in memory to optimize access to recent log entries. This cache improves performance by reducing disk I/O for frequently accessed data.

### Heap Sizing and Garbage Collection
For optimal performance, heap sizing should consider the following factors:
1. The size of the buffer pools (approximately 19MB for each of direct and heap pools)
2. The size of RAFT logs and snapshots in memory
3. The overhead of in-memory data structures for tracking RAFT state
4. Additional memory for application-specific state machine data

Garbage collection tuning should focus on minimizing pause times, as long pauses can disrupt RAFT consensus operations. The use of buffer pools significantly reduces allocation rate and garbage collection pressure. For systems with large heaps, consider using low-pause collectors like ZGC or Shenandoah.

**Section sources**
- [SimpleByteBufferPool.java](file://client/src/main/java/com/github/dtprj/dongting/buf/SimpleByteBufferPool.java#L37-L64)
- [DefaultPoolFactory.java](file://client/src/main/java/com/github/dtprj/dongting/buf/DefaultPoolFactory.java#L37-L41)
- [DefaultRaftLog.java](file://server/src/main/java/com/github/dtprj/dongting/raft/store/DefaultRaftLog.java#L59-L62)

## Disk I/O Guidelines
Disk I/O performance is critical for the stability and performance of the RAFT consensus algorithm. The system has specific requirements for both throughput and latency.

### RAFT Log Storage Requirements
The RAFT log is the most critical storage component, as all consensus operations must be durably recorded before they can be committed. The system uses separate files for log data and index information, with configurable file sizes:

- Index files: Default 1,000 items per file
- Log files: Default 1GB size

The log files are stored in a dedicated "log" directory, while index files are stored in an "idx" directory. This separation allows for independent optimization of the two file types.

### Throughput and Latency Requirements
For optimal performance, the storage system should meet the following requirements:
- High write throughput: The system can generate significant write load during periods of high activity
- Low write latency: Critical for maintaining low RAFT commit latency
- Consistent performance: Avoiding performance spikes that could disrupt consensus

The README documentation indicates that SSD storage is recommended, with typical sequential throughput of 7GB/s and 4KB random IOPS of 1 million. Mechanical hard drives are not recommended due to their much lower performance (hundreds of IOPS, 200MB/s sequential throughput).

### Storage Configuration Options
The system supports two storage modes:
1. **Synchronous write**: The follower only responds to the leader after the `fsync` call returns, ensuring maximum durability but with higher latency
2. **Asynchronous write**: The follower responds as soon as data is written to the OS buffer, with `fsync` called immediately afterward to minimize data loss risk

The choice between these modes represents a trade-off between performance and durability. Synchronous write provides stronger durability guarantees but lower throughput, while asynchronous write offers higher performance with slightly increased risk of data loss in case of power failure.

**Section sources**
- [DefaultRaftLog.java](file://server/src/main/java/com/github/dtprj/dongting/raft/store/DefaultRaftLog.java#L61-L62)
- [README.md](file://README.md#L33-L38)

## Network Bandwidth Considerations
Network bandwidth is a critical resource for the Dongting system, particularly for inter-node communication in a RAFT cluster.

### Port Configuration
The system supports two network ports:
- **replicatePort**: Used for RAFT log replication and admin commands (internal server-to-server communication)
- **servicePort**: Used for client access (optional, can be disabled by setting to 0)

When both ports are configured on the same server, network bandwidth must be shared between client traffic and inter-node communication. This sharing can impact consensus performance if not properly provisioned.

The `RaftServer` class configures the network stack to handle both ports efficiently, with the replicate port always active and the service port optional. When both ports are used, the server listens on both ports simultaneously, allowing clients and other servers to connect through their respective ports.

### Bandwidth Requirements
The network bandwidth requirements depend on several factors:
1. **Cluster size**: Larger clusters require more inter-node communication
2. **Write load**: Higher write throughput generates more RAFT log replication traffic
3. **Client traffic**: The volume of client requests affects service port bandwidth
4. **Network topology**: The physical network infrastructure can impact effective bandwidth

For optimal performance, ensure that the network infrastructure can handle the combined load of:
- RAFT heartbeat messages (every 2 seconds by default)
- RAFT log replication traffic
- Client request and response traffic
- Administrative commands

### Performance Monitoring
The system includes performance metrics for monitoring network operations, including:
- `raft_replicate_rpc_time`: Time spent on replication RPC calls
- `raft_replicate_rpc_items`: Number of items in replication RPC calls
- `raft_replicate_rpc_bytes`: Bytes transferred in replication RPC calls

These metrics can be used to identify network bottlenecks and optimize bandwidth allocation.

**Section sources**
- [RaftServerConfig.java](file://server/src/main/java/com/github/dtprj/dongting/raft/server/RaftServerConfig.java#L23-L26)
- [RaftServer.java](file://server/src/main/java/com/github/dtprj/dongting/raft/server/RaftServer.java#L148-L153)
- [RaftProcessor.java](file://server/src/main/java/com/github/dtprj/dongting/raft/server/RaftProcessor.java#L56-L65)
- [RaftPerfCallback.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/raft/RaftPerfCallback.java#L47-L49)

## Impact of Resource Constraints on Consensus
Resource constraints can significantly impact the stability and performance of the RAFT consensus algorithm.

### CPU Constraints
Insufficient CPU resources can lead to:
- Increased RAFT leader election timeouts
- Delayed processing of RAFT messages
- Reduced throughput for log replication
- Potential leader instability

The system's thread model is designed to mitigate these issues by separating I/O and business operations, but extreme CPU pressure can still disrupt consensus operations.

### Memory Constraints
Memory pressure can affect consensus in several ways:
- Buffer pool exhaustion leading to increased allocation and garbage collection
- Inability to cache recent log entries, increasing disk I/O
- Out-of-memory errors that can crash the server

The buffer pool cleanup mechanism helps mitigate memory pressure, but sustained high memory usage can still impact stability.

### Disk I/O Constraints
Disk performance is perhaps the most critical factor for RAFT consensus stability:
- High disk latency directly increases RAFT commit latency
- Low disk throughput can create bottlenecks during periods of high write load
- Disk contention between log and index files can degrade performance

The system's performance benchmarks show that SSD storage can achieve over 900,000 TPS with asynchronous writes, while mechanical drives would likely struggle to reach even 10,000 TPS.

### Network Constraints
Network limitations can disrupt consensus in several ways:
- Packet loss or high latency can trigger unnecessary leader elections
- Insufficient bandwidth can delay log replication, affecting commit latency
- Port contention between client and replication traffic can create bottlenecks

When servicePort and replicatePort are configured on the same server, network bandwidth must be carefully managed to ensure that client traffic does not interfere with critical consensus operations.

**Section sources**
- [README.md](file://README.md#L42-L45)
- [devlogs/2023_07_20_现代硬件下的IO程序开发.txt](file://devlogs/2023_07_20_现代硬件下的IO程序开发.txt#L1-L12)

## Conclusion
Proper resource allocation is essential for the optimal performance and stability of the Dongting distributed system. The guidelines provided in this document cover the key aspects of CPU, memory, disk I/O, and network bandwidth allocation.

For CPU allocation, the system's default configurations for blockIoThreads and other thread pools are generally appropriate for most use cases. However, in environments with extreme workloads, these values may need adjustment based on actual performance monitoring.

Memory allocation should account for the buffer pools, RAFT logs, snapshots, and application-specific data. The buffer pool mechanism significantly reduces garbage collection pressure, but sufficient heap space must still be provisioned.

Disk I/O performance is critical for RAFT consensus stability. SSD storage is strongly recommended, and the choice between synchronous and asynchronous write modes should be based on the specific durability and performance requirements of the application.

Network bandwidth must be sufficient to handle both client traffic and inter-node communication, especially when servicePort and replicatePort are configured on the same server. Monitoring the provided performance metrics can help identify and resolve network bottlenecks.

By following these guidelines and monitoring system performance, operators can ensure that the Dongting system operates at peak efficiency and maintains stable consensus operations under various workload conditions.