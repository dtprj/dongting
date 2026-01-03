# RAFT Consensus Implementation

<cite>
**Referenced Files in This Document**   
- [RaftServer.java](file://server/src/main/java/com/github/dtprj/dongting/raft/server/RaftServer.java)
- [RaftGroup.java](file://server/src/main/java/com/github/dtprj/dongting/raft/server/RaftGroup.java)
- [RaftGroupImpl.java](file://server/src/main/java/com/github/dtprj/dongting/raft/impl/RaftGroupImpl.java)
- [AdminRaftClient.java](file://server/src/main/java/com/github/dtprj/dongting/raft/admin/AdminRaftClient.java)
- [AdminConfigChangeProcessor.java](file://server/src/main/java/com/github/dtprj/dongting/raft/rpc/AdminConfigChangeProcessor.java)
- [VoteManager.java](file://server/src/main/java/com/github/dtprj/dongting/raft/impl/VoteManager.java)
- [ReplicateManager.java](file://server/src/main/java/com/github/dtprj/dongting/raft/impl/ReplicateManager.java)
- [ApplyManager.java](file://server/src/main/java/com/github/dtprj/dongting/raft/impl/ApplyManager.java)
- [CommitManager.java](file://server/src/main/java/com/github/dtprj/dongting/raft/impl/CommitManager.java)
- [RaftLog.java](file://server/src/main/java/com/github/dtprj/dongting/raft/store/RaftLog.java)
- [RaftServerConfig.java](file://server/src/main/java/com/github/dtprj/dongting/raft/server/RaftServerConfig.java)
- [RaftGroupConfig.java](file://server/src/main/java/com/github/dtprj/dongting/raft/server/RaftGroupConfig.java)
- [MemberManager.java](file://server/src/main/java/com/github/dtprj/dongting/raft/impl/MemberManager.java)
- [StateMachine.java](file://server/src/main/java/com/github/dtprj/dongting/raft/sm/StateMachine.java)
- [StatusManager.java](file://server/src/main/java/com/github/dtprj/dongting/raft/store/StatusManager.java)
</cite>

## Table of Contents
1. [Introduction](#introduction)
2. [Project Structure](#project-structure)
3. [Core Components](#core-components)
4. [Architecture Overview](#architecture-overview)
5. [Detailed Component Analysis](#detailed-component-analysis)
6. [Dependency Analysis](#dependency-analysis)
7. [Performance Considerations](#performance-considerations)
8. [Troubleshooting Guide](#troubleshooting-guide)
9. [Conclusion](#conclusion)

## Introduction
The Dongting RAFT consensus implementation provides a robust distributed consensus algorithm with support for multiple RAFT groups within a single process. This document details the core RAFT algorithm implementation including leader election, log replication, and safety mechanisms. It covers dynamic membership changes using joint consensus, configuration management, group lifecycle operations, leadership transfer, and the relationship between RAFT log, state machine (DtKV), and snapshotting.

## Project Structure
The Dongting project is organized into several key modules:
- **benchmark**: Performance testing components
- **client**: Client-side implementations including buffer management, codecs, and network utilities
- **demos**: Example applications demonstrating various use cases
- **server**: Core RAFT consensus implementation and state machine
- **it-test**: Integration tests
- **report**: Reporting utilities
- **test-support**: Testing utilities and helpers

The RAFT consensus implementation is primarily located in the server module under `com.github.dtprj.dongting.raft`, with key components organized into subpackages for server, implementation, RPC, store, and state machine functionality.

## Core Components
The core components of the Dongting RAFT implementation include the RaftServer, RaftGroup, and associated managers for vote, replication, application, and commit operations. The system supports multiple RAFT groups within a single process, with each group maintaining its own state and participating in consensus independently.

**Section sources**
- [RaftServer.java](file://server/src/main/java/com/github/dtprj/dongting/raft/server/RaftServer.java#L1-L718)
- [RaftGroup.java](file://server/src/main/java/com/github/dtprj/dongting/raft/server/RaftGroup.java#L1-L106)

## Architecture Overview

```mermaid
graph TD
subgraph "RAFT Server"
RaftServer[RaftServer]
NodeManager[NodeManager]
GroupComponents[GroupComponents]
end
subgraph "RAFT Group"
RaftGroupImpl[RaftGroupImpl]
VoteManager[VoteManager]
ReplicateManager[ReplicateManager]
ApplyManager[ApplyManager]
CommitManager[CommitManager]
MemberManager[MemberManager]
end
subgraph "Storage & State"
RaftLog[RaftLog]
StatusManager[StatusManager]
StateMachine[StateMachine]
SnapshotManager[SnapshotManager]
end
RaftServer --> RaftGroupImpl
RaftGroupImpl --> VoteManager
RaftGroupImpl --> ReplicateManager
RaftGroupImpl --> ApplyManager
RaftGroupImpl --> CommitManager
RaftGroupImpl --> MemberManager
RaftGroupImpl --> RaftLog
RaftGroupImpl --> StatusManager
RaftGroupImpl --> StateMachine
RaftGroupImpl --> SnapshotManager
RaftServer --> NodeManager
```

**Diagram sources**
- [RaftServer.java](file://server/src/main/java/com/github/dtprj/dongting/raft/server/RaftServer.java#L1-L718)
- [RaftGroupImpl.java](file://server/src/main/java/com/github/dtprj/dongting/raft/impl/RaftGroupImpl.java#L1-L220)

## Detailed Component Analysis

### RAFT Algorithm Implementation

#### Leader Election
The leader election process in Dongting's RAFT implementation follows the standard RAFT algorithm with pre-vote and vote phases. The VoteManager class handles the election process, with nodes transitioning through follower, candidate, and leader roles.

```mermaid
sequenceDiagram
participant Follower
participant Candidate
participant Leader
Follower->>Follower : Monitor election timeout
alt Election Timeout
Follower->>Candidate : Start pre-vote
Candidate->>AllNodes : RequestPreVote(term, lastLogIndex, lastLogTerm)
AllNodes-->>Candidate : VoteGranted
Candidate->>Candidate : Check quorum
Candidate->>Candidate : Start vote
Candidate->>AllNodes : RequestVote(term, lastLogIndex, lastLogTerm)
AllNodes-->>Candidate : VoteGranted
Candidate->>Leader : Achieve quorum, become leader
Leader->>AllNodes : AppendEntries(heartbeat)
end
```

**Diagram sources**
- [VoteManager.java](file://server/src/main/java/com/github/dtprj/dongting/raft/impl/VoteManager.java#L1-L492)

#### Log Replication
Log replication is managed by the ReplicateManager, which handles both normal log replication and snapshot installation. The leader maintains replicate fibers for each follower, sending AppendEntries requests to replicate log entries.

```mermaid
sequenceDiagram
participant Leader
participant Follower
participant RaftLog
Leader->>Leader : Receive client request
Leader->>RaftLog : Append to local log
Leader->>Follower : AppendEntries(prevLogIndex, prevLogTerm, entries)
Follower->>Follower : Check log consistency
alt Log matches
Follower->>RaftLog : Append entries
Follower-->>Leader : Success=true
Leader->>Leader : Update nextIndex and matchIndex
else Log doesn't match
Follower-->>Leader : Success=false, suggestTerm, suggestIndex
Leader->>Leader : Find matching position
Leader->>Follower : InstallSnapshot or retry AppendEntries
end
```

**Diagram sources**
- [ReplicateManager.java](file://server/src/main/java/com/github/dtprj/dongting/raft/impl/ReplicateManager.java#L1-L792)

#### Safety Mechanisms
The implementation includes several safety mechanisms to ensure consistency and prevent split-brain scenarios. The Raft algorithm's safety properties are maintained through term-based leadership, log matching, and leader completeness.

```mermaid
flowchart TD
A[Start] --> B{Is Leader?}
B --> |Yes| C[Check Lease Validity]
B --> |No| D[Reject Request]
C --> E{Lease Expired?}
E --> |Yes| F[Step Down, Become Follower]
E --> |No| G[Process Request]
G --> H[Replicate to Majority]
H --> I{Replication Successful?}
I --> |Yes| J[Commit Entry]
I --> |No| K[Retry or Step Down]
J --> L[Apply to State Machine]
L --> M[Respond to Client]
```

**Diagram sources**
- [RaftGroupImpl.java](file://server/src/main/java/com/github/dtprj/dongting/raft/impl/RaftGroupImpl.java#L1-L220)

### Multi-RAFT Group System
Dongting supports multiple RAFT groups within a single process, allowing for resource efficiency and simplified deployment. The RaftServer manages multiple RaftGroup instances, each operating independently.

```mermaid
classDiagram
class RaftServer {
+ConcurrentHashMap<Integer, RaftGroupImpl> raftGroups
+NioServer nioServer
+NioClient nioClient
+NodeManager nodeManager
+createRaftGroup()
+addNode()
+removeNode()
+addGroup()
+removeGroup()
}
class RaftGroupImpl {
+GroupComponents groupComponents
+FiberGroup fiberGroup
+int groupId
+RaftStatusImpl raftStatus
+StateMachine stateMachine
}
RaftServer --> RaftGroupImpl : "contains"
```

**Diagram sources**
- [RaftServer.java](file://server/src/main/java/com/github/dtprj/dongting/raft/server/RaftServer.java#L1-L718)

### Dynamic Membership Changes
Dynamic membership changes are implemented using joint consensus, allowing for safe configuration changes without downtime. The MemberManager handles prepare, commit, and abort operations for configuration changes.

```mermaid
sequenceDiagram
participant AdminClient
participant Leader
participant Follower
AdminClient->>Leader : PrepareChange(oldConfig, newConfig)
Leader->>Leader : Append PrepareConfigChange entry
Leader->>Follower : Replicate entry
Follower-->>Leader : Acknowledge
Leader->>Leader : Commit entry
Leader->>Leader : Enter joint consensus phase
Leader->>All : Heartbeat with new configuration
AdminClient->>Leader : CommitChange(prepareIndex)
Leader->>Leader : Append CommitConfigChange entry
Leader->>Follower : Replicate entry
Follower-->>Leader : Acknowledge
Leader->>Leader : Commit entry, complete configuration change
```

**Diagram sources**
- [MemberManager.java](file://server/src/main/java/com/github/dtprj/dongting/raft/impl/MemberManager.java#L1-L800)
- [AdminConfigChangeProcessor.java](file://server/src/main/java/com/github/dtprj/dongting/raft/rpc/AdminConfigChangeProcessor.java#L1-L104)

### Configuration Management
Configuration management is handled through the RaftServerConfig and RaftGroupConfig classes, which define server-wide and group-specific parameters respectively.

```mermaid
classDiagram
class RaftServerConfig {
+String servers
+int replicatePort
+int servicePort
+int nodeId
+long electTimeout
+long rpcTimeout
+long connectTimeout
+long heartbeatInterval
+long pingInterval
+boolean checkSelf
+int blockIoThreads
}
class RaftGroupConfig {
+int groupId
+String nodeIdOfMembers
+String nodeIdOfObservers
+String dataDir
+String statusFile
+int[] ioRetryInterval
+boolean syncForce
+int raftPingCheck
+boolean disableConfigChange
+int maxReplicateItems
+long maxReplicateBytes
+int singleReplicateLimit
+int maxPendingTasks
+long maxPendingTaskBytes
+int maxCacheTasks
+long maxCacheTaskBytes
+int idxCacheSize
+int idxFlushThreshold
+boolean ioCallbackUseGroupExecutor
+PerfCallback perfCallback
+int snapshotConcurrency
+int diskSnapshotConcurrency
+int diskSnapshotBufferSize
+int replicateSnapshotConcurrency
+int replicateSnapshotBufferSize
+int saveSnapshotSeconds
+int maxKeepSnapshots
+boolean saveSnapshotWhenClose
+int autoDeleteLogDelaySeconds
+boolean deleteLogsAfterTakeSnapshot
}
```

**Diagram sources**
- [RaftServerConfig.java](file://server/src/main/java/com/github/dtprj/dongting/raft/server/RaftServerConfig.java#L1-L40)
- [RaftGroupConfig.java](file://server/src/main/java/com/github/dtprj/dongting/raft/server/RaftGroupConfig.java#L1-L76)

### Group Lifecycle Operations
The system provides comprehensive APIs for managing RAFT group lifecycle, including adding, removing, and updating groups.

```mermaid
flowchart TD
A[Start] --> B{Operation}
B --> C[Add Group]
B --> D[Remove Group]
B --> E[Update Group]
C --> F[Validate Configuration]
F --> G[Create RaftGroupImpl]
G --> H[Initialize Components]
H --> I[Start Fibers]
I --> J[Complete]
D --> K[Validate Group Exists]
K --> L[Stop Group]
L --> M[Clean Up Resources]
M --> N[Remove from Server]
N --> J
E --> O[Validate Change]
O --> P[Prepare Configuration Change]
P --> Q[Commit Configuration Change]
Q --> J
```

**Section sources**
- [RaftServer.java](file://server/src/main/java/com/github/dtprj/dongting/raft/server/RaftServer.java#L572-L666)

### Leadership Transfer
Leadership transfer is supported through the transferLeadership API, allowing for graceful handover of leadership to another node.

```mermaid
sequenceDiagram
participant Client
participant OldLeader
participant NewLeader
participant Follower
Client->>OldLeader : TransferLeadership(newLeaderId)
OldLeader->>OldLeader : Validate request
OldLeader->>NewLeader : TransferLeaderRequest
NewLeader->>NewLeader : Validate eligibility
NewLeader-->>OldLeader : Acknowledge
OldLeader->>All : Append TransferLeader entry
OldLeader->>NewLeader : Stop sending heartbeats
NewLeader->>All : Start sending heartbeats
NewLeader->>Client : Leadership transferred
```

**Section sources**
- [RaftGroupImpl.java](file://server/src/main/java/com/github/dtprj/dongting/raft/impl/RaftGroupImpl.java#L164-L169)

### Admin Interfaces
Administration of RAFT clusters is facilitated through the AdminRaftClient and associated processors, providing a comprehensive API for cluster management.

```mermaid
classDiagram
class AdminRaftClient {
+queryRaftServerStatus()
+transferLeader()
+prepareChange()
+commitChange()
+abortChange()
+serverAddGroup()
+serverRemoveGroup()
+serverAddNode()
+serverRemoveNode()
+serverListNodes()
+serverListGroups()
}
class AdminConfigChangeProcessor {
+doProcess()
+getGroupId()
+createDecoderCallback()
}
class AdminGroupAndNodeProcessor {
+doProcess()
}
AdminRaftClient --> AdminConfigChangeProcessor : "communicates with"
AdminRaftClient --> AdminGroupAndNodeProcessor : "communicates with"
```

**Diagram sources**
- [AdminRaftClient.java](file://server/src/main/java/com/github/dtprj/dongting/raft/admin/AdminRaftClient.java#L1-L235)
- [AdminConfigChangeProcessor.java](file://server/src/main/java/com/github/dtprj/dongting/raft/rpc/AdminConfigChangeProcessor.java#L1-L104)

### RAFT Log and State Machine
The relationship between the RAFT log, state machine (DtKV), and snapshotting is fundamental to the system's operation. The ApplyManager coordinates log application to the state machine.

```mermaid
sequenceDiagram
participant RaftLog
participant ApplyManager
participant StateMachine
participant SnapshotManager
RaftLog->>ApplyManager : New committed entries
ApplyManager->>ApplyManager : Load entries from log
ApplyManager->>StateMachine : exec(index, input)
StateMachine->>StateMachine : Apply operation
StateMachine-->>ApplyManager : Result
ApplyManager->>ApplyManager : Update lastApplied
ApplyManager->>SnapshotManager : Check if snapshot needed
alt Snapshot threshold reached
SnapshotManager->>StateMachine : takeSnapshot()
StateMachine->>SnapshotManager : Return snapshot
SnapshotManager->>RaftLog : Mark logs for deletion
end
```

**Diagram sources**
- [ApplyManager.java](file://server/src/main/java/com/github/dtprj/dongting/raft/impl/ApplyManager.java#L1-L548)
- [StateMachine.java](file://server/src/main/java/com/github/dtprj/dongting/raft/sm/StateMachine.java#L1-L50)

## Dependency Analysis

```mermaid
graph TD
RaftServer --> RaftGroupImpl
RaftServer --> NodeManager
RaftGroupImpl --> VoteManager
RaftGroupImpl --> ReplicateManager
RaftGroupImpl --> ApplyManager
RaftGroupImpl --> CommitManager
RaftGroupImpl --> MemberManager
RaftGroupImpl --> RaftLog
RaftGroupImpl --> StatusManager
RaftGroupImpl --> StateMachine
RaftGroupImpl --> SnapshotManager
ApplyManager --> StateMachine
ApplyManager --> RaftLog
ApplyManager --> StatusManager
CommitManager --> ApplyManager
ReplicateManager --> RaftLog
MemberManager --> NodeManager
VoteManager --> StatusManager
StatusManager --> RaftLog
```

**Diagram sources**
- [RaftServer.java](file://server/src/main/java/com/github/dtprj/dongting/raft/server/RaftServer.java#L1-L718)
- [RaftGroupImpl.java](file://server/src/main/java/com/github/dtprj/dongting/raft/impl/RaftGroupImpl.java#L1-L220)

## Performance Considerations
The Dongting RAFT implementation includes several performance optimizations:
- Fiber-based concurrency model for efficient resource utilization
- Batched log replication to reduce network overhead
- Configurable replication limits (maxReplicateItems, maxReplicateBytes)
- Asynchronous persistence with configurable sync behavior (syncForce)
- Efficient snapshotting with configurable concurrency and buffer sizes
- Tail caching for frequently accessed log entries
- Flow control to prevent overwhelming followers

The system also provides performance monitoring through the PerfCallback interface, allowing for tracking of key metrics such as replication RPC times and state machine execution times.

## Troubleshooting Guide
Common issues and their solutions:

1. **Leader Election Failures**
   - Check network connectivity between nodes
   - Verify election timeout settings are appropriate for network conditions
   - Ensure clock synchronization across nodes

2. **Log Replication Issues**
   - Check RPC timeout settings
   - Verify network bandwidth is sufficient for replication traffic
   - Monitor disk I/O performance on followers

3. **Split-Brain Prevention**
   - Ensure odd-numbered clusters for quorum
   - Configure appropriate election timeouts
   - Use stable network infrastructure

4. **Log Compaction Problems**
   - Verify snapshot creation is working correctly
   - Check disk space for snapshot storage
   - Monitor snapshot creation frequency

5. **Network Partition Handling**
   - Implement proper timeout configurations
   - Use health checks to detect partitioned nodes
   - Plan for quorum loss scenarios

**Section sources**
- [RaftServer.java](file://server/src/main/java/com/github/dtprj/dongting/raft/server/RaftServer.java#L1-L718)
- [VoteManager.java](file://server/src/main/java/com/github/dtprj/dongting/raft/impl/VoteManager.java#L1-L492)

## Conclusion
The Dongting RAFT consensus implementation provides a comprehensive, high-performance solution for distributed consensus with support for multiple RAFT groups within a single process. The system implements the core RAFT algorithm with leader election, log replication, and safety mechanisms, while extending it with features like dynamic membership changes using joint consensus, comprehensive administration interfaces, and efficient state machine integration. The architecture is designed for performance and reliability, with careful attention to failure recovery, monitoring, and operational considerations.