# MPSC Queue Implementation

<cite>
**Referenced Files in This Document**   
- [MpscLinkedQueue.java](file://client/src/main/java/com/github/dtprj/dongting/queue/MpscLinkedQueue.java)
- [Java8MpscLinkedQueue.java](file://client/src/main/java/com/github/dtprj/dongting/java8/Java8MpscLinkedQueue.java)
- [Java11MpscLinkedQueue.java](file://client/src/main/java/com/github/dtprj/dongting/java11/Java11MpscLinkedQueue.java)
- [VersionFactory.java](file://client/src/main/java/com/github/dtprj/dongting/common/VersionFactory.java)
- [Java8Factory.java](file://client/src/main/java/com/github/dtprj/dongting/java8/Java8Factory.java)
- [Java11Factory.java](file://client/src/main/java/com/github/dtprj/dongting/java11/Java11Factory.java)
- [LinkedNode.java](file://client/src/main/java/com/github/dtprj/dongting/queue/LinkedNode.java)
- [LinkedQueuePadding1.java](file://client/src/main/java/com/github/dtprj/dongting/queue/LinkedQueuePadding1.java)
- [LinkedQueueProducerRef.java](file://client/src/main/java/com/github/dtprj/dongting/queue/LinkedQueueProducerRef.java)
- [Padding0.java](file://client/src/main/java/com/github/dtprj/dongting/common/Padding0.java)
- [MpscLinkedQueueTest.java](file://client/src/test/java/com/github/dtprj/dongting/java11/MpscLinkedQueueTest.java)
- [IoWorkerQueue.java](file://client/src/main/java/com/github/dtprj/dongting/net/IoWorkerQueue.java)
- [DtUnsafe.java](file://client/src/main/java/com/github/dtprj/dongting/unsafe/DtUnsafe.java)
</cite>

## Table of Contents
1. [Introduction](#introduction)
2. [Architecture Overview](#architecture-overview)
3. [Core Components](#core-components)
4. [Detailed Component Analysis](#detailed-component-analysis)
5. [Memory Layout and False Sharing Prevention](#memory-layout-and-false-sharing-prevention)
6. [Producer-Consumer Coordination](#producer-consumer-coordination)
7. [Shutdown Propagation Mechanism](#shutdown-propagation-mechanism)
8. [Java Version-Specific Optimizations](#java-version-specific-optimizations)
9. [Integration and Usage Patterns](#integration-and-usage-patterns)
10. [Performance Considerations](#performance-considerations)
11. [Common Issues and Mitigation](#common-issues-and-mitigation)

## Introduction
The MPSC (Multi-Producer Single-Consumer) queue implementation in the Dongting project provides a high-performance, lock-free data structure optimized for scenarios where multiple threads produce data and a single thread consumes it. This document details the design, implementation, and usage of the MpscLinkedQueue and its Java version-specific variants, focusing on the lock-free design using atomic operations, memory layout optimizations, and integration patterns.

**Section sources**
- [MpscLinkedQueue.java](file://client/src/main/java/com/github/dtprj/dongting/queue/MpscLinkedQueue.java#L1-L155)

## Architecture Overview
The MPSC queue architecture is built around a node-based linked structure with specialized implementations for different Java versions. The core design leverages atomic operations and relaxed memory semantics to achieve high throughput while maintaining thread safety. The architecture includes version-specific implementations that utilize the most efficient atomic primitives available for each Java version.

```mermaid
graph TD
subgraph "Core Components"
MpscLinkedQueue["MpscLinkedQueue<br/>Abstract Base Class"]
Java8Impl["Java8MpscLinkedQueue<br/>AtomicReferenceFieldUpdater"]
Java11Impl["Java11MpscLinkedQueue<br/>VarHandle"]
LinkedNode["LinkedNode<br/>Value & Next Pointer"]
end
subgraph "Factory System"
VersionFactory["VersionFactory<br/>Abstract Factory"]
Java8Factory["Java8Factory"]
Java11Factory["Java11Factory"]
end
subgraph "Memory Layout"
Padding0["Padding0<br/>ProducerRef Padding"]
LinkedQueueProducerRef["LinkedQueueProducerRef<br/>Tail & Shutdown"]
LinkedQueuePadding1["LinkedQueuePadding1<br/>Head Padding"]
end
MpscLinkedQueue --> Java8Impl
MpscLinkedQueue --> Java11Impl
MpscLinkedQueue --> LinkedNode
VersionFactory --> Java8Factory
VersionFactory --> Java11Factory
Java8Factory --> Java8Impl
Java11Factory --> Java11Impl
LinkedQueueProducerRef --> Padding0
LinkedQueuePadding1 --> LinkedQueueProducerRef
```

**Diagram sources**
- [MpscLinkedQueue.java](file://client/src/main/java/com/github/dtprj/dongting/queue/MpscLinkedQueue.java#L27-L155)
- [Java8MpscLinkedQueue.java](file://client/src/main/java/com/github/dtprj/dongting/java8/Java8MpscLinkedQueue.java#L27-L57)
- [Java11MpscLinkedQueue.java](file://client/src/main/java/com/github/dtprj/dongting/java11/Java11MpscLinkedQueue.java#L28-L61)
- [VersionFactory.java](file://client/src/main/java/com/github/dtprj/dongting/common/VersionFactory.java#L28-L76)
- [Java8Factory.java](file://client/src/main/java/com/github/dtprj/dongting/java8/Java8Factory.java#L34-L110)
- [Java11Factory.java](file://client/src/main/java/com/github/dtprj/dongting/java11/Java11Factory.java#L33-L79)
- [LinkedNode.java](file://client/src/main/java/com/github/dtprj/dongting/queue/LinkedNode.java#L21-L42)
- [LinkedQueueProducerRef.java](file://client/src/main/java/com/github/dtprj/dongting/queue/LinkedQueueProducerRef.java#L23-L27)
- [LinkedQueuePadding1.java](file://client/src/main/java/com/github/dtprj/dongting/queue/LinkedQueuePadding1.java#L22-L39)
- [Padding0.java](file://client/src/main/java/com/github/dtprj/dongting/common/Padding0.java#L22-L40)

## Core Components
The MPSC queue implementation consists of several core components that work together to provide a high-performance, lock-free queue. The base class MpscLinkedQueue defines the abstract interface and common functionality, while concrete implementations for Java 8 and Java 11 provide optimized atomic operations. The LinkedNode class represents individual queue elements, and the factory system selects the appropriate implementation based on the Java version.

**Section sources**
- [MpscLinkedQueue.java](file://client/src/main/java/com/github/dtprj/dongting/queue/MpscLinkedQueue.java#L27-L155)
- [LinkedNode.java](file://client/src/main/java/com/github/dtprj/dongting/queue/LinkedNode.java#L21-L42)
- [VersionFactory.java](file://client/src/main/java/com/github/dtprj/dongting/common/VersionFactory.java#L28-L76)

## Detailed Component Analysis

### MpscLinkedQueue Analysis
The MpscLinkedQueue class serves as the abstract base for the MPSC queue implementation. It provides the core queue operations including offer, relaxedPoll, and shutdown, while delegating atomic operations to concrete implementations. The queue uses a linked node structure with separate head and tail pointers to enable lock-free operation.

```mermaid
classDiagram
class MpscLinkedQueue {
+static final VersionFactory VERSION_FACTORY
+static final LinkedNode SHUTDOWN_NODE
+LinkedNode<E> head
+MpscLinkedQueue()
+static <E> MpscLinkedQueue<E> newInstance()
+E relaxedPoll()
+boolean offer(E value)
+void shutdown()
+boolean isConsumeFinished()
+abstract LinkedNode<E> getAndSetTailRelease(LinkedNode<E> nextNode)
+abstract LinkedNode<E> getNextAcquire(LinkedNode<E> node)
+abstract void setNextRelease(LinkedNode<E> node, LinkedNode<E> nextNode)
}
MpscLinkedQueue <|-- Java8MpscLinkedQueue
MpscLinkedQueue <|-- Java11MpscLinkedQueue
```

**Diagram sources**
- [MpscLinkedQueue.java](file://client/src/main/java/com/github/dtprj/dongting/queue/MpscLinkedQueue.java#L27-L155)

### Java8MpscLinkedQueue Analysis
The Java8MpscLinkedQueue implementation uses AtomicReferenceFieldUpdater to provide atomic operations on the tail pointer. This approach is compatible with Java 8's memory model and provides good performance through the use of lazySet for the next pointer update.

```mermaid
classDiagram
class Java8MpscLinkedQueue {
+static final AtomicReferenceFieldUpdater<LinkedQueueProducerRef, LinkedNode> PRODUCER_NODE
+static final AtomicReferenceFieldUpdater<LinkedNode, LinkedNode> NEXT
+getAndSetTailRelease(LinkedNode<E> nextNode) LinkedNode<E>
+getNextAcquire(LinkedNode<E> node) LinkedNode<E>
+setNextRelease(LinkedNode<E> node, LinkedNode<E> nextNode) void
}
Java8MpscLinkedQueue --|> MpscLinkedQueue
```

**Diagram sources**
- [Java8MpscLinkedQueue.java](file://client/src/main/java/com/github/dtprj/dongting/java8/Java8MpscLinkedQueue.java#L27-L57)

### Java11MpscLinkedQueue Analysis
The Java11MpscLinkedQueue implementation leverages VarHandle for atomic operations, which provides more efficient memory ordering controls compared to AtomicReferenceFieldUpdater. The implementation uses getAndSetRelease for the tail update and getAcquire/setRelease for the next pointer operations.

```mermaid
classDiagram
class Java11MpscLinkedQueue {
+static final VarHandle TAIL
+static final VarHandle NEXT
+getAndSetTailRelease(LinkedNode<E> nextNode) LinkedNode<E>
+getNextAcquire(LinkedNode<E> node) LinkedNode<E>
+setNextRelease(LinkedNode<E> node, LinkedNode<E> nextNode) void
}
Java11MpscLinkedQueue --|> MpscLinkedQueue
```

**Diagram sources**
- [Java11MpscLinkedQueue.java](file://client/src/main/java/com/github/dtprj/dongting/java11/Java11MpscLinkedQueue.java#L28-L61)

### LinkedNode Analysis
The LinkedNode class represents individual elements in the queue, containing the value and a reference to the next node. It also includes a shutdown status field used for graceful shutdown propagation.

```mermaid
classDiagram
class LinkedNode {
+E value
+volatile LinkedNode<E> next
+volatile int shutdownStatus
+static final int SHUTDOWN_STATUS_BEFORE
+static final int SHUTDOWN_STATUS_AFTER
+LinkedNode(E value)
+E getValue()
+void setValue(E value)
}
```

**Diagram sources**
- [LinkedNode.java](file://client/src/main/java/com/github/dtprj/dongting/queue/LinkedNode.java#L21-L42)

## Memory Layout and False Sharing Prevention
The MPSC queue implementation employs memory padding to prevent false sharing between the producer and consumer sides of the queue. The padding is implemented through a hierarchy of classes that add byte fields to ensure that critical fields are on separate cache lines.

```mermaid
classDiagram
class Padding0 {
+byte b000-b167
}
class LinkedQueueProducerRef {
+volatile boolean shutdown
+volatile LinkedNode<E> tail
}
class LinkedQueuePadding1 {
+byte b000-b177
}
Padding0 <|-- LinkedQueueProducerRef
LinkedQueueProducerRef <|-- LinkedQueuePadding1
LinkedQueuePadding1 <|-- MpscLinkedQueue
```

**Diagram sources**
- [Padding0.java](file://client/src/main/java/com/github/dtprj/dongting/common/Padding0.java#L22-L40)
- [LinkedQueueProducerRef.java](file://client/src/main/java/com/github/dtprj/dongting/queue/LinkedQueueProducerRef.java#L23-L27)
- [LinkedQueuePadding1.java](file://client/src/main/java/com/github/dtprj/dongting/queue/LinkedQueuePadding1.java#L22-L39)

## Producer-Consumer Coordination
The MPSC queue uses a lock-free algorithm for coordination between multiple producers and a single consumer. Producers use atomic operations to update the tail pointer, while the consumer advances the head pointer after processing elements.

```mermaid
sequenceDiagram
participant Producer1 as "Producer Thread 1"
participant Producer2 as "Producer Thread 2"
participant Consumer as "Consumer Thread"
participant Queue as "MpscLinkedQueue"
Producer1->>Queue : offer(value)
activate Queue
Queue->>Queue : new LinkedNode(value)
Queue->>Queue : getAndSetTailRelease(newNode)
Queue->>Queue : setNextRelease(oldTail, newNode)
deactivate Queue
Producer2->>Queue : offer(value)
activate Queue
Queue->>Queue : new LinkedNode(value)
Queue->>Queue : getAndSetTailRelease(newNode)
Queue->>Queue : setNextRelease(oldTail, newNode)
deactivate Queue
Consumer->>Queue : relaxedPoll()
activate Queue
Queue->>Queue : getNextAcquire(head)
alt next != null
Queue->>Queue : value = next.getValue()
Queue->>Queue : next.setValue(null)
Queue->>Queue : head = next
Queue-->>Consumer : value
else next == null
Queue-->>Consumer : null
end
deactivate Queue
```

**Diagram sources**
- [MpscLinkedQueue.java](file://client/src/main/java/com/github/dtprj/dongting/queue/MpscLinkedQueue.java#L43-L108)
- [Java8MpscLinkedQueue.java](file://client/src/main/java/com/github/dtprj/dongting/java8/Java8MpscLinkedQueue.java#L42-L55)
- [Java11MpscLinkedQueue.java](file://client/src/main/java/com/github/dtprj/dongting/java11/Java11MpscLinkedQueue.java#L45-L58)

## Shutdown Propagation Mechanism
The MPSC queue implements a sophisticated shutdown mechanism that ensures all elements offered before shutdown are processed, while preventing new elements from being added after shutdown. The mechanism uses a special SHUTDOWN_NODE and status propagation to coordinate between producers and consumers.

```mermaid
flowchart TD
Start([Offer During Shutdown]) --> CheckShutdown["Check shutdown flag"]
CheckShutdown --> |true| CheckNode["Check if node is SHUTDOWN_NODE"]
CheckNode --> |SHUTDOWN_NODE| ForwardPropagation["Set oldTail.status = BEFORE"]
CheckNode --> |Not SHUTDOWN_NODE| CheckOldTail["Check if oldTail is SHUTDOWN_NODE"]
CheckOldTail --> |Yes| BackwardPropagation["Set newTail.status = AFTER"]
CheckOldTail --> |No| SpinLoop["Spin with status checks"]
SpinLoop --> |oldTail.status=AFTER| SetAfter["Set newTail.status = AFTER"]
SpinLoop --> |newTail.status=BEFORE| SetBefore["Set oldTail.status = BEFORE"]
SpinLoop --> |No match| ContinueSpin["Continue spinning"]
SetAfter --> CheckStatus["Check newTail.status"]
SetBefore --> CheckStatus
ForwardPropagation --> CheckStatus
BackwardPropagation --> CheckStatus
CheckStatus --> |AFTER| ReturnFalse["Return false"]
CheckStatus --> |BEFORE| LinkNode["Link node in queue"]
LinkNode --> ReturnTrue["Return true"]
ReturnFalse --> End([End])
ReturnTrue --> End
```

**Diagram sources**
- [MpscLinkedQueue.java](file://client/src/main/java/com/github/dtprj/dongting/queue/MpscLinkedQueue.java#L62-L108)
- [LinkedNode.java](file://client/src/main/java/com/github/dtprj/dongting/queue/LinkedNode.java#L26-L28)

## Java Version-Specific Optimizations
The MPSC queue implementation uses the VERSION_FACTORY to select the optimal implementation based on the Java version. This allows the queue to leverage the most efficient atomic operations available for each Java version.

```mermaid
graph TD
Start([VersionFactory.getInstance()]) --> CheckJavaVersion["Check DtUtil.JAVA_VER > 8"]
CheckJavaVersion --> |true| UseJava11["Use Java11Factory"]
CheckJavaVersion --> |false| UseJava8["Use Java8Factory"]
UseJava11 --> Java11Impl["Java11MpscLinkedQueue with VarHandle"]
UseJava8 --> Java8Impl["Java8MpscLinkedQueue with AtomicReferenceFieldUpdater"]
Java11Impl --> |getAndSetRelease| VarHandle["VarHandle TAIL"]
Java11Impl --> |getAcquire/setRelease| VarHandle["VarHandle NEXT"]
Java8Impl --> |getAndSet| AtomicReferenceFieldUpdater["PRODUCER_NODE"]
Java8Impl --> |get/lazySet| AtomicReferenceFieldUpdater["NEXT"]
```

**Diagram sources**
- [VersionFactory.java](file://client/src/main/java/com/github/dtprj/dongting/common/VersionFactory.java#L58-L62)
- [Java8Factory.java](file://client/src/main/java/com/github/dtprj/dongting/java8/Java8Factory.java#L67-L69)
- [Java11Factory.java](file://client/src/main/java/com/github/dtprj/dongting/java11/Java11Factory.java#L41-L43)
- [Java8MpscLinkedQueue.java](file://client/src/main/java/com/github/dtprj/dongting/java8/Java8MpscLinkedQueue.java#L30-L38)
- [Java11MpscLinkedQueue.java](file://client/src/main/java/com/github/dtprj/dongting/java11/Java11MpscLinkedQueue.java#L34-L41)

## Integration and Usage Patterns
The MPSC queue is integrated into various components of the Dongting system, particularly in the network I/O subsystem where it coordinates between business threads and I/O worker threads.

```mermaid
sequenceDiagram
participant BizThread as "Business Thread"
participant IoWorkerQueue as "IoWorkerQueue"
participant IoWorker as "IoWorker"
participant Consumer as "Consumer Thread"
BizThread->>IoWorkerQueue : writeFromBizThread(data)
activate IoWorkerQueue
IoWorkerQueue->>IoWorkerQueue : perfCallback.takeTime()
IoWorkerQueue->>IoWorkerQueue : queue.offer(data)
deactivate IoWorkerQueue
BizThread->>IoWorkerQueue : scheduleFromBizThread(runnable)
activate IoWorkerQueue
IoWorkerQueue->>IoWorkerQueue : queue.offer(runnable)
deactivate IoWorkerQueue
IoWorker->>IoWorkerQueue : dispatchActions()
activate IoWorkerQueue
loop While queue not empty
IoWorkerQueue->>IoWorkerQueue : queue.relaxedPoll()
alt data is PacketInfo
IoWorkerQueue->>IoWorkerQueue : processWriteData(data)
else data is Runnable
IoWorkerQueue->>IoWorkerQueue : runnable.run()
end
end
deactivate IoWorkerQueue
IoWorker->>IoWorkerQueue : close()
activate IoWorkerQueue
IoWorkerQueue->>IoWorkerQueue : queue.shutdown()
deactivate IoWorkerQueue
```

**Diagram sources**
- [IoWorkerQueue.java](file://client/src/main/java/com/github/dtprj/dongting/net/IoWorkerQueue.java#L34-L67)
- [IoWorkerQueue.java](file://client/src/main/java/com/github/dtprj/dongting/net/IoWorkerQueue.java#L44-L52)
- [IoWorkerQueue.java](file://client/src/main/java/com/github/dtprj/dongting/net/IoWorkerQueue.java#L58-L67)
- [IoWorkerQueue.java](file://client/src/main/java/com/github/dtprj/dongting/net/IoWorkerQueue.java#L143-L145)

## Performance Considerations
The MPSC queue is designed for high-throughput scenarios with multiple producers. The lock-free design minimizes contention, and the use of relaxed memory operations reduces overhead. The implementation includes optimizations for different Java versions and prevents false sharing through memory padding.

**Section sources**
- [MpscLinkedQueue.java](file://client/src/main/java/com/github/dtprj/dongting/queue/MpscLinkedQueue.java#L133-L143)
- [Java8MpscLinkedQueue.java](file://client/src/main/java/com/github/dtprj/dongting/java8/Java8MpscLinkedQueue.java#L54-L55)
- [Java11MpscLinkedQueue.java](file://client/src/main/java/com/github/dtprj/dongting/java11/Java11MpscLinkedQueue.java#L57-L58)

## Common Issues and Mitigation
The MPSC queue implementation addresses several common issues in concurrent programming, including thread starvation, backpressure, and performance degradation under high contention. The spin-wait strategy in the shutdown mechanism helps prevent excessive CPU usage, and the graceful shutdown ensures all pending elements are processed.

**Section sources**
- [MpscLinkedQueue.java](file://client/src/main/java/com/github/dtprj/dongting/queue/MpscLinkedQueue.java#L133-L143)
- [MpscLinkedQueue.java](file://client/src/main/java/com/github/dtprj/dongting/queue/MpscLinkedQueue.java#L65-L99)
- [MpscLinkedQueueTest.java](file://client/src/test/java/com/github/dtprj/dongting/java11/MpscLinkedQueueTest.java#L54-L101)