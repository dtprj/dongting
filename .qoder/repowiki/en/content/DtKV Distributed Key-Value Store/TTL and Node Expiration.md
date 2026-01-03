# TTL and Node Expiration

<cite>
**Referenced Files in This Document**   
- [TtlManager.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/TtlManager.java)
- [KvClient.java](file://client/src/main/java/com/github/dtprj/dongting/dtkv/KvClient.java)
- [KvImpl.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/KvImpl.java)
- [KvSnapshot.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/KvSnapshot.java)
- [TtlDemoClient.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/ttl/TtlDemoClient.java)
- [TtlDemoServer.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/ttl/TtlDemoServer.java)
- [TtlManagerTest.java](file://server/src/test/java/com/github/dtprj/dongting/dtkv/server/TtlManagerTest.java)
</cite>

## Table of Contents
1. [Introduction](#introduction)
2. [TTL Management Architecture](#ttl-management-architecture)
3. [Background Scanning and Cleanup Process](#background-scanning-and-cleanup-process)
4. [Client API for TTL Operations](#client-api-for-ttl-operations)
5. [Persistence and Snapshot Recovery](#persistence-and-snapshot-recovery)
6. [TTL Demos and Usage Examples](#ttl-demos-and-usage-examples)
7. [Expiration Timing and Watch Notifications](#expiration-timing-and-watch-notifications)
8. [Integration with Distributed Locking and Service Discovery](#integration-with-distributed-locking-and-service-discovery)
9. [Conclusion](#conclusion)

## Introduction

The DtKV TTL (Time-To-Live) management system provides automatic expiration of temporary nodes, enabling use cases such as session management, cache invalidation, and ephemeral service registration. This document details the implementation of TTL functionality in DtKV, covering the background scanning mechanism, client API, persistence model, and integration patterns. The system ensures that temporary nodes are reliably expired according to their configured TTL values while maintaining consistency across the distributed system.

**Section sources**
- [TtlManager.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/TtlManager.java#L1-L242)
- [KvClient.java](file://client/src/main/java/com/github/dtprj/dongting/dtkv/KvClient.java#L1-L771)

## TTL Management Architecture

The TTL management system in DtKV is centered around the `TtlManager` class, which coordinates the expiration of temporary nodes. The architecture consists of several key components that work together to provide reliable TTL functionality:

```mermaid
classDiagram
class TtlManager {
+Timestamp ts
+Consumer<TtlInfo> expireCallback
+TreeSet<TtlInfo> ttlQueue
+TreeSet<TtlInfo> pendingQueue
+TtlTask task
+role : RaftRole
+defaultDelayNanos : long
+retryDelayNanos : long
+initTtl(raftIndex, key, node, ctx)
+updateTtl(raftIndex, key, newNode, ctx)
+remove(node)
+roleChange(newRole)
}
class TtlTask {
+execute() : long
+shouldPause() : boolean
+shouldStop() : boolean
+defaultDelayNanos() : long
}
class TtlInfo {
+key : ByteArray
+raftIndex : long
+owner : UUID
+leaderTtlStartMillis : long
+ttlMillis : long
+expireNanos : long
+ttlInfoIndex : int
+expireFailed : boolean
+lastFailNanos : long
+compareTo(o) : int
}
class KvImpl {
+ttlManager : TtlManager
+initTtl(index, key, data, ttlMillis, operator)
+updateTtl(index, key, ttlMillis)
+expire(index, key, expectRaftIndex)
}
TtlManager --> TtlTask : "contains"
TtlManager --> TtlInfo : "manages"
TtlManager --> KvImpl : "callback"
KvImpl --> TtlManager : "delegates"
```

**Diagram sources**
- [TtlManager.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/TtlManager.java#L33-L242)
- [KvImpl.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/KvImpl.java#L100-L200)

The `TtlManager` maintains two priority queues: `ttlQueue` for nodes ready to be checked for expiration, and `pendingQueue` for nodes whose expiration processing has failed and needs to be retried. The `TtlTask` executes periodically to process these queues, while `TtlInfo` objects contain all the metadata needed to manage a node's TTL.

**Section sources**
- [TtlManager.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/TtlManager.java#L33-L242)

## Background Scanning and Cleanup Process

The TTL cleanup process is implemented as a background task that runs on the leader node of each Raft group. The `TtlTask` executes periodically, processing nodes in order of their expiration time:

```mermaid
flowchart TD
Start([Start TtlTask Execution]) --> CheckPending["Check pendingQueue for retry candidates"]
CheckPending --> ProcessPending["Process failed expirations (MAX_RETRY_BATCH=10)"]
ProcessPending --> CheckTtlQueue["Check ttlQueue for expired nodes"]
CheckTtlQueue --> ProcessExpired["Process expired nodes (MAX_EXPIRE_BATCH=50)"]
ProcessExpired --> CalculateDelay["Calculate next execution delay"]
CalculateDelay --> ReturnDelay["Return delay time"]
subgraph "Process Pending Queue"
ProcessPending --> HasPending{"pendingQueue empty?"}
HasPending --> |No| IteratePending["Iterate pendingQueue"]
IteratePending --> ShouldRetry{"expireFailed and retryDelay elapsed?"}
ShouldRetry --> |Yes| RetryLimit{"Count < MAX_RETRY_BATCH?"}
RetryLimit --> |Yes| MoveToTtl["Move to ttlQueue"]
MoveToTtl --> ContinueIterate["Continue iteration"]
ContinueIterate --> IteratePending
RetryLimit --> |No| Yield["Set yield=true"]
Yield --> BreakPending["Break loop"]
ShouldRetry --> |No| BreakPending
end
subgraph "Process TTL Queue"
ProcessExpired --> HasTtl{"ttlQueue empty?"}
HasTtl --> |No| IterateTtl["Iterate ttlQueue"]
IterateTtl --> CountLimit{"Count < MAX_EXPIRE_BATCH?"}
CountLimit --> |No| SetYield["Set yield=true"]
SetYield --> BreakTtl["Break loop"]
CountLimit --> |Yes| CheckExpired{"expireNanos <= current time?"}
CheckExpired --> |No| CalculateNext["Return time until next expiration"]
CheckExpired --> |Yes| RemoveFromTtl["Remove from ttlQueue"]
RemoveFromTtl --> AddToPending["Add to pendingQueue"]
AddToPending --> ExecuteCallback["Execute expireCallback"]
ExecuteCallback --> Success{"Callback succeeded?"}
Success --> |Yes| ContinueTtl["Continue iteration"]
Success --> |No| MarkFailed["Mark expireFailed=true"]
MarkFailed --> ContinueTtl
ContinueTtl --> IterateTtl
end
CalculateDelay --> ShouldYield{"yield=true?"}
ShouldYield --> |Yes| ReturnZero["Return 0 (execute immediately)"]
ShouldYield --> |No| ReturnDefault["Return defaultDelayNanos (1 second)"]
```

**Diagram sources**
- [TtlManager.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/TtlManager.java#L59-L109)

The cleanup process follows these key principles:
- **Batch Processing**: The system processes up to 50 expired nodes and 10 retry candidates per execution cycle to prevent long pauses
- **Leader-Only Execution**: The `shouldPause()` method ensures cleanup only runs on the leader node
- **Retry Mechanism**: Failed expiration attempts are retried after a configurable delay (default 1 second)
- **Ordered Processing**: Nodes are processed in order of expiration time using a `TreeSet` with `TtlInfo.compareTo()`

The `TtlInfo.compareTo()` method first compares expiration times, then uses a sequence index to ensure consistent ordering for nodes with identical expiration times:

```mermaid
classDiagram
class TtlInfo {
+compareTo(o) : int
}
TtlInfo --> "Compare by" ExpirationTime : "expireNanos - o.expireNanos"
ExpirationTime --> "If equal, compare by" SequenceIndex : "ttlInfoIndex - o.ttlInfoIndex"
```

**Diagram sources**
- [TtlManager.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/TtlManager.java#L228-L240)

**Section sources**
- [TtlManager.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/TtlManager.java#L59-L109)

## Client API for TTL Operations

DtKV provides a comprehensive client API for managing temporary nodes with TTL. The `KvClient` class exposes both synchronous and asynchronous methods for creating, updating, and removing temporary nodes.

```mermaid
classDiagram
class KvClient {
+putTemp(groupId, key, value, ttlMillis)
+putTemp(groupId, key, value, ttlMillis, callback)
+makeTempDir(groupId, key, ttlMillis)
+makeTempDir(groupId, key, ttlMillis, callback)
+updateTtl(groupId, key, ttlMillis)
+updateTtl(groupId, key, ttlMillis, callback)
+get(groupId, key)
+get(groupId, key, callback)
}
class KvReq {
+groupId : int
+key : ByteArray
+value : byte[]
+ttlMillis : long
+expectValue : byte[]
}
class KvResp {
+results : List<KvResult>
+bizCode : int
}
class KvNode {
+data : byte[]
+createIndex : long
+createTime : long
+updateIndex : long
+updateTime : long
+flag : int
+ttlMillis : long
+owner : UUID
}
KvClient --> KvReq : "creates"
KvClient --> KvResp : "receives"
KvClient --> KvNode : "returns"
```

**Diagram sources**
- [KvClient.java](file://client/src/main/java/com/github/dtprj/dongting/dtkv/KvClient.java#L272-L686)

The key TTL operations available to clients are:

### Creating Temporary Nodes
Clients can create temporary nodes using the `putTemp` method, which requires a positive TTL value in milliseconds:

```java
// Synchronous creation
kvClient.putTemp(GROUP_ID, "tempKey1".getBytes(), "tempValue1".getBytes(), 3000);

// Asynchronous creation
kvClient.putTemp(GROUP_ID, "tempKey2".getBytes(), "tempValue2".getBytes(), 5000, callback);
```

### Creating Temporary Directories
Temporary directories can be created using `makeTempDir`, which also accepts a TTL value:

```java
kvClient.makeTempDir(GROUP_ID, "tempDir".getBytes(), 10000);
```

### Updating TTL
The TTL of existing temporary nodes can be updated using the `updateTtl` method:

```java
// Extend TTL to 10 seconds
kvClient.updateTtl(GROUP_ID, "tempKey1".getBytes(), 10000);
```

### Retrieving Node Information
The `get` method returns a `KvNode` object that includes TTL information:

```java
KvNode node = kvClient.get(GROUP_ID, "tempKey1".getBytes());
if (node != null) {
    System.out.println("TTL: " + node.ttlMillis + "ms");
    System.out.println("Owner: " + node.owner);
}
```

The server validates TTL values to ensure they are within acceptable bounds (positive and not exceeding 100 years):

```mermaid
flowchart TD
ValidateTtl["checkTtl(ttl, data, lock)"] --> CheckMax{"ttl > MAX_TTL_MILLIS?"}
CheckMax --> |Yes| ReturnError1["Return 'ttl too large'"]
CheckMax --> |No| CheckLock{"lock operation?"}
CheckLock --> |Yes| CheckNegative{"ttl < 0?"}
CheckNegative --> |Yes| ReturnError2["Return 'ttl must be non-negative'"]
CheckNegative --> |No| CheckData{"data null or < 8 bytes?"}
CheckData --> |Yes| ReturnError3["Return 'no hold ttl'"]
CheckData --> |No| ReadHoldTtl["Read hold TTL from data"]
ReadHoldTtl --> CheckHoldPositive{"holdTtl <= 0?"}
CheckHoldPositive --> |Yes| ReturnError4["Return 'hold ttl must be positive'"]
CheckHoldPositive --> |No| CheckHoldMax{"holdTtl > MAX_TTL_MILLIS?"}
CheckHoldMax --> |Yes| ReturnError5["Return 'hold ttl too large'"]
CheckHoldMax --> |No| CheckHoldVsTtl{"holdTtl < ttl?"}
CheckHoldVsTtl --> |Yes| ReturnError6["Return 'hold ttl less than ttl'"]
CheckHoldVsTtl --> |No| ReturnNull["Return null (valid)"]
CheckLock --> |No| CheckPositive{"ttl <= 0?"}
CheckPositive --> |Yes| ReturnError7["Return 'ttl must be positive'"]
CheckPositive --> |No| ReturnNull
```

**Diagram sources**
- [KvImpl.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/KvImpl.java#L939-L965)

**Section sources**
- [KvClient.java](file://client/src/main/java/com/github/dtprj/dongting/dtkv/KvClient.java#L272-L686)
- [KvImpl.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/KvImpl.java#L939-L965)

## Persistence and Snapshot Recovery

TTL information is persisted in the RAFT log and recovered during snapshot restoration to ensure durability and consistency across restarts and leader changes.

### RAFT Log Persistence
When a temporary node is created or updated, the TTL information is recorded in the RAFT log as part of the operation:

```mermaid
sequenceDiagram
participant Client
participant KvClient
participant RaftClient
participant RaftLeader
participant RaftLog
Client->>KvClient : putTemp(key, value, ttlMillis)
KvClient->>RaftClient : sendRequest(DTKV_PUT_TEMP_NODE)
RaftClient->>RaftLeader : Forward request
RaftLeader->>RaftLog : Append entry with TTL info
RaftLog-->>RaftLeader : Entry committed
RaftLeader-->>RaftClient : Response
RaftClient-->>KvClient : Response
KvClient-->>Client : Success
```

**Diagram sources**
- [KvClient.java](file://client/src/main/java/com/github/dtprj/dongting/dtkv/KvClient.java#L272-L301)
- [KvImpl.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/KvImpl.java#L100-L200)

### Snapshot Serialization
During snapshot creation, TTL information is serialized along with the node data in the `KvSnapshot` class:

```mermaid
classDiagram
class KvSnapshot {
+readNext(buffer)
+loadNextNode()
+encodeStatus : EncodeStatus
+currentKvNode : KvNode
}
class EncodeStatus {
+keyBytes : byte[]
+valueBytes : byte[]
+createIndex : long
+createTime : long
+updateIndex : long
+updateTime : long
+flag : int
+uuid1 : long
+uuid2 : long
+ttlRaftIndex : long
+leaderTtlStartTime : long
+ttlMillis : long
}
class KvNodeEx {
+ttlInfo : TtlInfo
+data : byte[]
+flag : int
}
KvSnapshot --> EncodeStatus : "contains"
KvSnapshot --> KvNodeEx : "processes"
EncodeStatus --> KvNodeEx : "extracts TTL info"
```

**Diagram sources**
- [KvSnapshot.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/KvSnapshot.java#L1-L201)

The `loadNextNode` method in `KvSnapshot` extracts TTL information from `KvNodeEx` objects and populates the `EncodeStatus` fields for serialization:

```java
if (n.ttlInfo != null) {
    encodeStatus.uuid1 = n.ttlInfo.owner.getMostSignificantBits();
    encodeStatus.uuid2 = n.ttlInfo.owner.getLeastSignificantBits();
    encodeStatus.ttlRaftIndex = n.ttlInfo.raftIndex;
    encodeStatus.leaderTtlStartTime = n.ttlInfo.leaderTtlStartMillis;
    encodeStatus.ttlMillis = n.ttlInfo.ttlMillis;
}
```

### Snapshot Recovery
When a node recovers from a snapshot, the TTL information is restored and reinserted into the `TtlManager`:

```mermaid
sequenceDiagram
participant Follower
participant RaftLeader
participant KvSnapshot
participant TtlManager
RaftLeader->>Follower : InstallSnapshot
Follower->>KvSnapshot : installSnapshot()
loop Read snapshot data
KvSnapshot->>KvSnapshot : readNext(buffer)
KvSnapshot->>TtlManager : initTtl(raftIndex, key, node, ctx)
end
TtlManager->>TtlManager : Add to ttlQueue
KvSnapshot-->>Follower : Snapshot installed
Follower-->>RaftLeader : Acknowledgment
```

The `initTtl` method in `TtlManager` creates a new `TtlInfo` object and adds it to the expiration queue:

```java
TtlInfo ttlInfo = new TtlInfo(key, raftIndex, ctx.operator, ctx.leaderCreateTimeMillis, 
        ctx.ttlMillis, ctx.localCreateNanos + ctx.ttlMillis * 1_000_000, ttlInfoIndex++);
n.ttlInfo = ttlInfo;
ttlQueue.add(ttlInfo);
```

**Section sources**
- [KvSnapshot.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/KvSnapshot.java#L1-L201)
- [TtlManager.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/TtlManager.java#L137-L144)

## TTL Demos and Usage Examples

The DtKV repository includes demonstration code that illustrates the usage of TTL functionality. The `TtlDemoClient` and `TtlDemoServer` classes provide a complete example of creating and verifying temporary nodes.

```mermaid
sequenceDiagram
participant Client
participant Server
participant TtlManager
Client->>Server : putTemp("tempKey1", "tempValue1", 3000ms)
Server->>TtlManager : initTtl(raftIndex, key, node, ctx)
TtlManager->>TtlManager : Add to ttlQueue
Server-->>Client : Success
Client->>Server : get("tempKey1")
Server-->>Client : KvNode with value
Client->>Client : Sleep 5000ms
Client->>Server : get("tempKey1")
Server-->>Client : null (node expired)
```

**Diagram sources**
- [TtlDemoClient.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/ttl/TtlDemoClient.java#L1-L58)
- [TtlDemoServer.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/ttl/TtlDemoServer.java#L1-L33)

The demo code demonstrates the following workflow:
1. Create a `KvClient` and connect to the DtKV cluster
2. Put a temporary key-value pair with a 3-second TTL
3. Verify the node exists by retrieving it
4. Wait 5 seconds (longer than the TTL)
5. Verify the node has been automatically expired

```java
// Create temporary node with 3-second TTL
kvClient.putTemp(GROUP_ID, "tempKey1".getBytes(), "tempValue1".getBytes(), 3000);

// Verify node exists
KvNode node = kvClient.get(GROUP_ID, "tempKey1".getBytes());
System.out.println("get tempKey1, value=" + new String(node.data));

// Wait longer than TTL
Thread.sleep(5000);

// Verify node has expired
node = kvClient.get(GROUP_ID, "tempKey1".getBytes());
System.out.println("get tempKey1, value=" + (node == null ? "null" : new String(node.data)));
```

The demo output shows:
```
put tempKey1 with value tempValue1
get tempKey1, value=tempValue1
sleep 5000 millis
get tempKey1, value=null
```

This confirms that the TTL mechanism correctly expired the node after approximately 3 seconds.

**Section sources**
- [TtlDemoClient.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/ttl/TtlDemoClient.java#L1-L58)
- [TtlDemoServer.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/ttl/TtlDemoServer.java#L1-L33)

## Expiration Timing and Watch Notifications

The TTL system provides predictable expiration timing while integrating with DtKV's watch notification mechanism to inform clients of node removals.

### Expiration Timing Accuracy
The expiration timing is designed to be accurate within the constraints of distributed systems:

- **Minimum Delay**: The `defaultDelayNanos` is set to 1 second, meaning expired nodes will be processed within 1 second of their expiration time
- **Batch Processing**: Up to 50 nodes are processed per cycle, which may introduce slight delays when many nodes expire simultaneously
- **Retry Mechanism**: Failed expiration attempts are retried after `retryDelayNanos` (1 second), ensuring eventual consistency

The expiration time is calculated using nanosecond precision:

```java
TtlInfo ttlInfo = new TtlInfo(key, raftIndex, ctx.operator, ctx.leaderCreateTimeMillis, 
        ctx.ttlMillis, ctx.localCreateNanos + ctx.ttlMillis * 1_000_000, ttlInfoIndex++);
```

This ensures that expiration timing is based on the node's creation time with millisecond precision for the TTL value.

### Watch Notifications
When a temporary node expires, the system generates watch notifications to inform registered clients:

```mermaid
sequenceDiagram
participant Client
participant WatchManager
participant TtlManager
participant KvImpl
TtlManager->>KvImpl : expireCallback(ttlInfo)
KvImpl->>KvImpl : expireInLock(index, h)
KvImpl->>KvImpl : doRemoveInLock(index, h)
KvImpl->>WatchManager : notifyWatch(key, REMOVED)
WatchManager->>Client : Push notification
Client->>Client : Handle node removal
```

**Diagram sources**
- [TtlManager.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/TtlManager.java#L98-L99)
- [KvImpl.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/KvImpl.java#L967-L1003)

The `expireCallback` in `TtlManager` triggers the expiration process in `KvImpl`, which ultimately calls the watch notification system to inform clients that the node has been removed. This ensures that clients can react promptly to the expiration of temporary nodes.

**Section sources**
- [TtlManager.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/TtlManager.java#L98-L99)
- [KvImpl.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/KvImpl.java#L967-L1003)

## Integration with Distributed Locking and Service Discovery

The TTL system complements DtKV's distributed locking and service discovery patterns by providing mechanisms for automatic cleanup and lease management.

### Distributed Locking
Temporary nodes are used to implement distributed locks, where the TTL serves as the lock lease time:

```mermaid
classDiagram
class DistributedLock {
+acquire()
+release()
+isHeldByCurrentThread()
}
class AutoRenewalLock {
+acquire()
+release()
+renew()
}
class KvClient {
+createLock(groupId, key)
+createAutoRenewalLock(groupId, key, leaseMillis, listener)
}
KvClient --> DistributedLock : "creates"
KvClient --> AutoRenewalLock : "creates"
AutoRenewalLock --> DistributedLock : "extends"
```

**Diagram sources**
- [KvClient.java](file://client/src/main/java/com/github/dtprj/dongting/dtkv/KvClient.java#L697-L740)

The `AutoRenewalLock` automatically renews the lock's TTL before it expires, preventing accidental release due to network delays or processing pauses. When a lock holder fails, the lock automatically expires after the TTL period, allowing other nodes to acquire it.

### Service Discovery
Temporary nodes are ideal for service discovery, where services register themselves with a TTL that must be periodically renewed:

```mermaid
sequenceDiagram
participant ServiceA
participant ServiceB
participant DtKV
participant Client
ServiceA->>DtKV : putTemp("/services/A", info, 30000ms)
ServiceB->>DtKV : putTemp("/services/B", info, 30000ms)
loop Every 15 seconds
ServiceA->>DtKV : updateTtl("/services/A", 30000ms)
ServiceB->>DtKV : updateTtl("/services/B", 30000ms)
end
Client->>DtKV : list("/services")
DtKV-->>Client : [A, B]
Note over ServiceB : Service B crashes
DtKV->>DtKV : Expire "/services/B" after 30s
Client->>DtKV : list("/services")
DtKV-->>Client : [A] only
```

This pattern ensures that only active services appear in the discovery results, as failed services are automatically removed when their TTL expires.

**Section sources**
- [KvClient.java](file://client/src/main/java/com/github/dtprj/dongting/dtkv/KvClient.java#L697-L740)

## Conclusion

The DtKV TTL management system provides a robust mechanism for automatic expiration of temporary nodes through a combination of background scanning, RAFT log persistence, and snapshot recovery. The `TtlManager` implements an efficient cleanup process that runs on the leader node, processing expired nodes in batches while ensuring reliability through retry mechanisms. The client API exposes intuitive methods for creating and managing temporary nodes with configurable TTL values. TTL information is persisted in the RAFT log and snapshots, ensuring durability across restarts and leader changes. The system integrates seamlessly with watch notifications to inform clients of node expirations and complements distributed locking and service discovery patterns by providing automatic cleanup of stale entries. This comprehensive TTL implementation enables reliable, distributed applications that require time-sensitive data management.

**Section sources**
- [TtlManager.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/TtlManager.java#L1-L242)
- [KvClient.java](file://client/src/main/java/com/github/dtprj/dongting/dtkv/KvClient.java#L1-L771)
- [KvImpl.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/KvImpl.java#L939-L965)
- [KvSnapshot.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/KvSnapshot.java#L1-L201)