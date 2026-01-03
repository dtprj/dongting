# Performance Benchmarking

<cite>
**Referenced Files in This Document**   
- [BenchBase.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/BenchBase.java)
- [SimplePerfCallback.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/SimplePerfCallback.java)
- [PrometheusPerfCallback.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/PrometheusPerfCallback.java)
- [TestProps.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/TestProps.java)
- [FiberTest.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/fiber/FiberTest.java)
- [IoTest.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/io/IoTest.java)
- [MpscQueueTest.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/queue/MpscQueueTest.java)
- [RpcBenchmark.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/rpc/RpcBenchmark.java)
- [RaftPerfCallback.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/raft/RaftPerfCallback.java)
- [RpcPerfCallback.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/rpc/RpcPerfCallback.java)
- [PerfCallback.java](file://client/src/main/java/com/github/dtprj/dongting/common/PerfCallback.java)
- [SyncTest.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/io/SyncTest.java)
- [CreateFiberTest.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/fiber/CreateFiberTest.java)
- [OrderMapTest.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/map/OrderMapTest.java)
- [pom.xml](file://benchmark/pom.xml)
</cite>

## Table of Contents
1. [Introduction](#introduction)
2. [Benchmark Architecture](#benchmark-architecture)
3. [Core Components](#core-components)
4. [Performance Callback Mechanisms](#performance-callback-mechanisms)
5. [Benchmark Design and Execution](#benchmark-design-and-execution)
6. [Core Component Benchmarks](#core-component-benchmarks)
7. [Result Interpretation](#result-interpretation)
8. [Prometheus Integration](#prometheus-integration)
9. [Creating New Benchmarks](#creating-new-benchmarks)
10. [Best Practices](#best-practices)
11. [Conclusion](#conclusion)

## Introduction
The Dongting performance benchmarking framework provides a comprehensive suite for measuring and analyzing the performance of core components in the Dongting system. This documentation details the architecture, usage patterns, and best practices for conducting performance benchmarks on critical system components such as fiber scheduling, IO operations, and queue performance. The framework is designed to provide accurate, reproducible performance measurements with detailed metrics on throughput, latency, and resource utilization.

**Section sources**
- [BenchBase.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/BenchBase.java#L1-L153)

## Benchmark Architecture

The Dongting benchmark suite follows a modular architecture built around a base class hierarchy that provides common benchmarking functionality. The framework is organized into specialized benchmark categories, each targeting specific system components.

```mermaid
graph TD
BenchBase["BenchBase\n- Thread management\n- Warmup/test phases\n- Metrics collection"]
FiberBench["Fiber Benchmarks\n- Fiber scheduling\n- Channel operations\n- Fiber creation"]
IOBench["IO Benchmarks\n- Async/Sync IO\n- MMAP operations\n- File system sync"]
QueueBench["Queue Benchmarks\n- MPSC queues\n- Concurrent queues\n- Blocking queues"]
RpcBench["RPC Benchmarks\n- Network throughput\n- Latency measurement\n- Connection handling"]
RaftBench["Raft Benchmarks\n- Consensus algorithm\n- Log replication\n- State machine"]
BenchBase --> FiberBench
BenchBase --> IOBench
BenchBase --> QueueBench
BenchBase --> RpcBench
BenchBase --> RaftBench
PerfCallback["PerfCallback\n- Performance event interface"]
SimplePerfCallback["SimplePerfCallback\n- Basic metrics collection"]
PrometheusPerfCallback["PrometheusPerfCallback\n- Prometheus integration"]
PerfCallback --> SimplePerfCallback
PerfCallback --> PrometheusPerfCallback
FiberBench --> PerfCallback
IOBench --> PerfCallback
QueueBench --> PerfCallback
RpcBench --> PerfCallback
RaftBench --> PerfCallback
```

**Diagram sources**
- [BenchBase.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/BenchBase.java#L30-L152)
- [PerfCallback.java](file://client/src/main/java/com/github/dtprj/dongting/common/PerfCallback.java#L21-L109)

## Core Components

The benchmark framework is built around several core components that provide the foundation for performance testing. The `BenchBase` class serves as the foundation for all benchmarks, providing thread management, warmup periods, and test execution phases. It handles the lifecycle of benchmark execution, including initialization, warmup, testing, and shutdown phases.

```mermaid
classDiagram
class BenchBase {
+int threadCount
+long testTime
+long warmupTime
+AtomicInteger state
+LongAdder successCount
+LongAdder failCount
+boolean logRt
+LongAdder totalNanos
+AtomicLong maxNanos
+BenchBase(int, long, long)
+void init()
+void afterWarmup()
+void shutdown()
+void start()
+void run(int)
+abstract void test(int, long, int)
+void logRt(long, int)
+void success(int)
+void fail(int)
+void setLogRt(boolean)
}
class FiberTest {
+Dispatcher dispatcher
+FiberGroup group
+FiberChannel channel
+FiberTest(int, long, long)
+void init()
+void shutdown()
+void test(int, long, int)
}
class MpscQueueTest {
+Object data
+MpscLinkedQueue queue
+MpscQueueTest(int, long, long)
+void init()
+void consumerRun()
+void test(int, long, int)
}
class IoTest {
+int FILE_SIZE
+int BUFFER_SIZE
+int MAX_PENDING
+int COUNT
+byte[] DATA
+File createFile(String)
+void start()
+void test1()
+void test2()
}
BenchBase <|-- FiberTest
BenchBase <|-- MpscQueueTest
BenchBase <|-- IoTest
```

**Diagram sources**
- [BenchBase.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/BenchBase.java#L30-L152)
- [FiberTest.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/fiber/FiberTest.java#L32-L88)
- [MpscQueueTest.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/queue/MpscQueueTest.java#L14-L52)
- [IoTest.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/io/IoTest.java#L34-L195)

**Section sources**
- [BenchBase.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/BenchBase.java#L30-L152)
- [FiberTest.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/fiber/FiberTest.java#L32-L88)
- [MpscQueueTest.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/queue/MpscQueueTest.java#L14-L52)
- [IoTest.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/io/IoTest.java#L34-L195)

## Performance Callback Mechanisms

The performance callback system in Dongting provides a flexible mechanism for collecting detailed performance metrics during benchmark execution. The framework supports multiple callback implementations, from simple console output to integration with monitoring systems like Prometheus.

```mermaid
classDiagram
class PerfCallback {
+boolean useNanos
+PerfCallback(boolean)
+final void refresh(Timestamp)
+long takeTime(int)
+long takeTime(int, Timestamp)
+void fireTime(int, long, int, long, Timestamp)
+void fireTime(int, long)
+void fireTime(int, long, int, long)
+void fire(int, int, long)
+void fire(int)
+abstract boolean accept(int)
+abstract void onEvent(int, long, int, long)
}
class SimplePerfCallback {
+ConcurrentHashMap map
+String prefix
+volatile boolean started
+SimplePerfCallback(String)
+boolean accept(int)
+void onEvent(int, long, int, long)
+String getName(int)
+void start()
+void printStats()
}
class PrometheusPerfCallback {
+static DtLog log
+volatile boolean started
+PrometheusPerfCallback(boolean)
+Summary createSummary(String)
+void start()
+void printTime(Summary)
+void printValue(Summary)
+void printCount(String, LongAdder)
+String getName(SimpleCollector)
}
class RpcPerfCallback {
+Summary rpcAcquire
+Summary rpcWorkerQueue
+Summary rpcChannelQueue
+Summary rpcWorkerSel
+Summary rpcWorkerWork
+LongAdder rpcMarkRead
+LongAdder rpcMarkWrite
+Summary rpcReadTime
+Summary rpcReadBytes
+Summary rpcWriteTime
+Summary rpcWriteBytes
+RpcPerfCallback(boolean, String)
+boolean accept(int)
+void onEvent(int, long, int, long)
+void printStats()
}
class RaftPerfCallback {
+Summary fiberPoll
+Summary fiberWork
+Summary raftLeaderRunnerFiberLatency
+Summary raftLogEncodeAndWrite
+Summary raftLogWriteTime
+Summary raftLogWriteFinishTime
+Summary raftLogWriteItems
+Summary raftLogWriteBytes
+Summary raftLogSyncTime
+Summary raftLogSyncItems
+Summary raftLogSyncBytes
+Summary raftIdxPosNotReady
+Summary raftLogPosNotReady
+Summary raftIdxFileAlloc
+Summary raftLogFileAlloc
+Summary raftIdxBlock
+Summary raftIdxWriteTime
+Summary raftIdxWriteBytes
+Summary raftIdxForceTime
+Summary raftIdxForceBytes
+Summary raftReplicateRpcTime
+Summary raftReplicateRpcItems
+Summary raftReplicateRpcBytes
+Summary raftStateMachineExec
+RaftPerfCallback(boolean, String)
+boolean accept(int)
+void onEvent(int, long, int, long)
+void printStats()
}
PerfCallback <|-- SimplePerfCallback
PerfCallback <|-- PrometheusPerfCallback
PrometheusPerfCallback <|-- RpcPerfCallback
PrometheusPerfCallback <|-- RaftPerfCallback
```

**Diagram sources**
- [PerfCallback.java](file://client/src/main/java/com/github/dtprj/dongting/common/PerfCallback.java#L21-L109)
- [SimplePerfCallback.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/SimplePerfCallback.java#L29-L151)
- [PrometheusPerfCallback.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/PrometheusPerfCallback.java#L31-L102)
- [RpcPerfCallback.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/rpc/RpcPerfCallback.java#L26-L117)
- [RaftPerfCallback.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/raft/RaftPerfCallback.java#L26-L183)

**Section sources**
- [PerfCallback.java](file://client/src/main/java/com/github/dtprj/dongting/common/PerfCallback.java#L21-L109)
- [SimplePerfCallback.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/SimplePerfCallback.java#L29-L151)
- [PrometheusPerfCallback.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/PrometheusPerfCallback.java#L31-L102)
- [RpcPerfCallback.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/rpc/RpcPerfCallback.java#L26-L117)
- [RaftPerfCallback.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/raft/RaftPerfCallback.java#L26-L183)

## Benchmark Design and Execution

The benchmark execution workflow follows a standardized pattern across all benchmark types. Each benchmark extends the `BenchBase` class and implements the required methods to define the specific test behavior. The execution process includes warmup, testing, and shutdown phases to ensure accurate performance measurement.

```mermaid
sequenceDiagram
participant User as "User"
participant BenchBase as "BenchBase"
participant Benchmark as "Specific Benchmark"
participant PerfCallback as "Performance Callback"
User->>BenchBase : start()
BenchBase->>Benchmark : init()
BenchBase->>BenchBase : Create threads
loop For each thread
BenchBase->>Benchmark : run(threadIndex)
end
alt Warmup period > 0
BenchBase->>BenchBase : Sleep(warmupTime)
BenchBase->>Benchmark : afterWarmup()
end
BenchBase->>BenchBase : Set state to STATE_TEST
BenchBase->>BenchBase : Sleep(testTime)
BenchBase->>BenchBase : Set state to STATE_BEFORE_SHUTDOWN
loop For each thread
BenchBase->>Benchmark : Wait for thread completion
end
BenchBase->>Benchmark : shutdown()
BenchBase->>BenchBase : Set state to STATE_AFTER_SHUTDOWN
BenchBase->>BenchBase : Calculate and output results
alt Performance monitoring enabled
PerfCallback->>PerfCallback : Print statistics
end
```

**Diagram sources**
- [BenchBase.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/BenchBase.java#L65-L107)
- [RpcBenchmark.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/rpc/RpcBenchmark.java#L63-L161)

**Section sources**
- [BenchBase.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/BenchBase.java#L65-L107)
- [RpcBenchmark.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/rpc/RpcBenchmark.java#L63-L161)

## Core Component Benchmarks

The Dongting benchmark suite includes specialized benchmarks for various core components. Each benchmark category targets specific aspects of system performance, providing detailed insights into different subsystems.

### Fiber Scheduling Benchmarks
The fiber scheduling benchmarks measure the performance of Dongting's fiber-based concurrency model. These benchmarks evaluate fiber creation, scheduling, and communication through channels.

```mermaid
flowchart TD
Start["FiberTest Execution"] --> Init["Initialize Dispatcher and FiberGroup"]
Init --> CreateProducer["Create Producer Fiber"]
CreateProducer --> StartProducer["Start Producer Fiber"]
StartProducer --> ProducerLoop["Producer Loop"]
ProducerLoop --> CreateConsumer["Create Consumer Fiber"]
CreateConsumer --> StartConsumer["Start Consumer Fiber"]
StartConsumer --> ConsumerWait["Consumer waits on channel"]
ConsumerWait --> DataTransfer["Data transferred via channel"]
DataTransfer --> Success["Mark success"]
Success --> Continue["Continue loop"]
Continue --> ProducerLoop
ProducerLoop --> End["End of test"]
```

**Diagram sources**
- [FiberTest.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/fiber/FiberTest.java#L46-L73)
- [CreateFiberTest.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/fiber/CreateFiberTest.java#L50-L67)

### IO Operation Benchmarks
The IO benchmarks evaluate different IO strategies, including asynchronous IO, synchronous IO, and memory-mapped files. These benchmarks help identify the most efficient IO approach for different workloads.

```mermaid
flowchart TD
Start["SyncTest Execution"] --> Setup["Setup test files"]
Setup --> Warmup["Warmup phase"]
Warmup --> TestBIO["Test BIO with sync"]
TestBIO --> TestSyncIO["Test Sync IO with force"]
TestSyncIO --> TestAsyncIO["Test Async IO with force"]
TestAsyncIO --> TestMmap["Test MMAP with force"]
TestMmap --> Output["Output results"]
Output --> End["End of test"]
```

**Diagram sources**
- [IoTest.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/io/IoTest.java#L67-L187)
- [SyncTest.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/io/SyncTest.java#L56-L162)

### Queue Performance Benchmarks
The queue benchmarks measure the performance of different queue implementations, focusing on multi-producer single-consumer (MPSC) patterns which are common in high-performance systems.

```mermaid
flowchart TD
Start["MpscQueueTest Execution"] --> Init["Initialize MpscLinkedQueue"]
Init --> StartConsumer["Start consumer thread"]
StartConsumer --> ConsumerLoop["Consumer loop"]
ConsumerLoop --> Poll["Poll queue"]
Poll --> Success["Mark success if data received"]
Success --> Continue["Continue loop"]
Continue --> ConsumerLoop
ConsumerLoop --> EndConsumer["End consumer"]
Init --> TestLoop["Test loop (producer)"]
TestLoop --> Offer["Offer data to queue"]
Offer --> ContinueTest["Continue loop"]
ContinueTest --> TestLoop
TestLoop --> EndTest["End test"]
```

**Diagram sources**
- [MpscQueueTest.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/queue/MpscQueueTest.java#L27-L51)
- [ConcurrentLinkedQueueTest.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/queue/ConcurrentLinkedQueueTest.java#L1-L30)

**Section sources**
- [FiberTest.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/fiber/FiberTest.java#L32-L88)
- [CreateFiberTest.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/fiber/CreateFiberTest.java#L30-L111)
- [IoTest.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/io/IoTest.java#L34-L195)
- [SyncTest.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/io/SyncTest.java#L32-L166)
- [MpscQueueTest.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/queue/MpscQueueTest.java#L14-L52)
- [ConcurrentLinkedQueueTest.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/queue/ConcurrentLinkedQueueTest.java#L1-L30)

## Result Interpretation

The benchmark framework provides comprehensive metrics for interpreting performance results. Key metrics include throughput, latency, and resource utilization, which are essential for understanding system performance characteristics.

### Throughput Metrics
Throughput is measured in operations per second (ops) and represents the system's capacity to process requests. The framework calculates throughput based on the number of successful operations during the test period.

```mermaid
flowchart TD
Start["Calculate Throughput"] --> CountSuccess["Count successful operations"]
CountSuccess --> CalculateOps["Calculate ops = successCount / testTime * 1000"]
CalculateOps --> FormatOutput["Format output with comma separator"]
FormatOutput --> Output["Output: success sc:{count}, ops:{formatted}"]
Output --> End["End calculation"]
```

**Diagram sources**
- [BenchBase.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/BenchBase.java#L93-L97)

### Latency Metrics
Latency metrics measure the time taken for individual operations, providing insights into response times and system responsiveness. The framework can track maximum and average latency when enabled.

```mermaid
flowchart TD
Start["Latency Measurement"] --> CheckLogRt["Check if logRt is enabled"]
CheckLogRt --> |Yes| RecordStart["Record start time"]
RecordStart --> ExecuteTest["Execute test operation"]
ExecuteTest --> CalculateTime["Calculate time = end - start"]
CalculateTime --> UpdateMax["Update maxNanos if current > max"]
UpdateMax --> AddToTotal["Add to totalNanos"]
AddToTotal --> End["End measurement"]
CheckLogRt --> |No| SkipMeasurement["Skip latency measurement"]
SkipMeasurement --> End
```

**Diagram sources**
- [BenchBase.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/BenchBase.java#L114-L134)
- [BenchBase.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/BenchBase.java#L100-L102)

### Resource Utilization Metrics
Resource utilization metrics, particularly thread utilization, help identify bottlenecks and inefficiencies in system resource usage. The framework calculates thread utilization rates based on time spent in different processing phases.

```mermaid
flowchart TD
Start["Calculate Thread Utilization"] --> GetTimes["Get poll and work times"]
GetTimes --> CalculateTotal["Calculate total = poll + work"]
CalculateTotal --> CalculateWork["Calculate work rate = work / total"]
CalculateWork --> FormatRate["Format rate as percentage"]
FormatRate --> Output["Output: thread utilization rate: {rate}%"]
Output --> End["End calculation"]
```

**Diagram sources**
- [RpcPerfCallback.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/rpc/RpcPerfCallback.java#L110-L113)
- [RaftPerfCallback.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/raft/RaftPerfCallback.java#L176-L179)

**Section sources**
- [BenchBase.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/BenchBase.java#L93-L102)
- [RpcPerfCallback.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/rpc/RpcPerfCallback.java#L110-L113)
- [RaftPerfCallback.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/raft/RaftPerfCallback.java#L176-L179)

## Prometheus Integration

The benchmark framework provides seamless integration with Prometheus for monitoring and visualization of performance metrics. The `PrometheusPerfCallback` class and its derivatives enable detailed metric collection that can be exposed to Prometheus.

```mermaid
sequenceDiagram
participant Benchmark as "Benchmark"
participant PerfCallback as "PrometheusPerfCallback"
participant Prometheus as "Prometheus Server"
Benchmark->>PerfCallback : onEvent(perfType, costTime, count, sum)
PerfCallback->>PerfCallback : Check if started
PerfCallback->>PerfCallback : Map perfType to Summary
PerfCallback->>PerfCallback : Observe costTime in Summary
PerfCallback->>PerfCallback : Update counters for count and sum
loop Periodically
Prometheus->>PerfCallback : HTTP GET /metrics
PerfCallback->>Prometheus : Return metrics in Prometheus format
Prometheus->>Prometheus : Parse and store metrics
end
Benchmark->>PerfCallback : printStats()
PerfCallback->>PerfCallback : Calculate quantiles and averages
PerfCallback->>PerfCallback : Log formatted statistics
```

**Diagram sources**
- [PrometheusPerfCallback.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/PrometheusPerfCallback.java#L50-L72)
- [RpcPerfCallback.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/rpc/RpcPerfCallback.java#L58-L93)
- [RaftPerfCallback.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/raft/RaftPerfCallback.java#L86-L147)

**Section sources**
- [PrometheusPerfCallback.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/PrometheusPerfCallback.java#L31-L102)
- [RpcPerfCallback.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/rpc/RpcPerfCallback.java#L26-L117)
- [RaftPerfCallback.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/raft/RaftPerfCallback.java#L26-L183)

## Creating New Benchmarks

Creating new benchmarks in the Dongting framework follows a consistent pattern that ensures compatibility with the existing infrastructure. New benchmarks should extend `BenchBase` and implement the required methods to define the specific test behavior.

```mermaid
flowchart TD
Start["Create New Benchmark"] --> ExtendBenchBase["Extend BenchBase class"]
ExtendBenchBase --> ImplementMethods["Implement required methods"]
ImplementMethods --> Init["init(): Setup test environment"]
ImplementMethods --> Test["test(): Core test logic"]
ImplementMethods --> Shutdown["shutdown(): Cleanup resources"]
ImplementMethods --> Optional["Optional: afterWarmup()"]
Optional --> AddConfig["Add configuration parameters"]
AddConfig --> ImplementMain["Implement main() method"]
ImplementMain --> CreateInstance["Create benchmark instance"]
CreateInstance --> SetParameters["Set threadCount, testTime, warmupTime"]
SetParameters --> CallStart["Call start() method"]
CallStart --> RunTest["Run test"]
RunTest --> AnalyzeResults["Analyze results"]
AnalyzeResults --> End["End benchmark creation"]
```

**Diagram sources**
- [BenchBase.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/BenchBase.java#L30-L152)
- [FiberTest.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/fiber/FiberTest.java#L32-L88)
- [MpscQueueTest.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/queue/MpscQueueTest.java#L14-L52)

**Section sources**
- [BenchBase.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/BenchBase.java#L30-L152)
- [FiberTest.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/fiber/FiberTest.java#L32-L88)
- [MpscQueueTest.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/queue/MpscQueueTest.java#L14-L52)

## Best Practices

To ensure accurate and meaningful performance measurements, several best practices should be followed when conducting benchmarks with the Dongting framework.

### JVM Warmup
Proper JVM warmup is critical for obtaining accurate performance results. The framework includes a dedicated warmup phase to allow the JIT compiler to optimize code paths before measurements begin.

```mermaid
flowchart TD
Start["Benchmark Execution"] --> WarmupPhase["Warmup Phase"]
WarmupPhase --> ExecuteOperations["Execute operations without measurement"]
ExecuteOperations --> JITOptimization["Allow JIT to optimize code paths"]
JITOptimization --> CodeStabilization["Allow code to stabilize"]
CodeStabilization --> TestPhase["Test Phase"]
TestPhase --> MeasurePerformance["Measure performance with stabilized code"]
MeasurePerformance --> End["Accurate results"]
```

**Diagram sources**
- [BenchBase.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/BenchBase.java#L75-L77)
- [BenchBase.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/BenchBase.java#L79-L80)

### Garbage Collection Considerations
Garbage collection can significantly impact performance measurements. Benchmarks should be designed to minimize GC interference and results should be interpreted with GC behavior in mind.

```mermaid
flowchart TD
Start["Minimize GC Impact"] --> ObjectReuse["Reuse objects when possible"]
ObjectReuse --> Pooling["Use object pooling"]
Pooling --> AvoidAllocation["Avoid unnecessary allocations"]
AvoidAllocation --> MonitorGC["Monitor GC activity"]
MonitorGC --> AnalyzeGCLogs["Analyze GC logs for pauses"]
AnalyzeGCLogs --> AdjustHeap["Adjust heap size if necessary"]
AdjustHeap --> RunMultiple["Run multiple iterations"]
RunMultiple --> CheckConsistency["Check result consistency"]
CheckConsistency --> End["Reliable measurements"]
```

**Diagram sources**
- [BenchBase.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/BenchBase.java#L41-L42)
- [BenchBase.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/BenchBase.java#L90-L97)

### Hardware Consistency
Consistent hardware conditions are essential for reproducible benchmark results. Tests should be conducted on dedicated hardware with minimal external interference.

```mermaid
flowchart TD
Start["Ensure Hardware Consistency"] --> DedicatedMachine["Use dedicated machine"]
DedicatedMachine --> DisableTurbo["Disable CPU turbo boost"]
DisableTurbo --> PinProcesses["Pin processes to specific cores"]
PinProcesses --> DisablePowerSave["Disable power saving modes"]
DisablePowerSave --> MinimizeIO["Minimize disk and network I/O"]
MinimizeIO --> ControlTemperature["Control ambient temperature"]
ControlTemperature --> RunMultiple["Run multiple iterations"]
RunMultiple --> CheckVariance["Check result variance"]
CheckVariance --> End["Consistent results"]
```

**Section sources**
- [BenchBase.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/BenchBase.java#L75-L80)
- [TestProps.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/TestProps.java#L27-L36)

## Conclusion
The Dongting performance benchmarking framework provides a comprehensive and flexible system for measuring the performance of core components. By following the patterns and best practices outlined in this documentation, developers can create accurate, reproducible benchmarks that provide valuable insights into system performance. The framework's modular architecture, combined with detailed performance monitoring and Prometheus integration, enables thorough analysis of throughput, latency, and resource utilization across various subsystems including fiber scheduling, IO operations, and queue performance.

**Section sources**
- [BenchBase.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/BenchBase.java#L1-L153)
- [SimplePerfCallback.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/SimplePerfCallback.java#L1-L152)
- [PrometheusPerfCallback.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/PrometheusPerfCallback.java#L1-L103)
- [TestProps.java](file://benchmark/src/main/java/com/github/dtprj/dongting/bench/common/TestProps.java#L1-L47)