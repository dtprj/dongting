# Watch机制详细文档

<cite>
**本文档中引用的文件**
- [ServerWatchManager.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/ServerWatchManager.java)
- [WatchManager.java](file://client/src/main/java/com/github/dtprj/dongting/dtkv/WatchManager.java)
- [WatchProcessor.java](file://client/src/main/java/com/github/dtprj/dongting/dtkv/WatchProcessor.java)
- [dt_kv.proto](file://server/src/test/proto/dt_kv.proto)
- [ServerWatchManagerTest.java](file://server/src/test/java/com/github/dtprj/dongting/dtkv/server/ServerWatchManagerTest.java)
- [WatchManagerTest.java](file://server/src/test/java/com/github/dtprj/dongting/dtkv/server/WatchManagerTest.java)
- [KvClient.java](file://client/src/main/java/com/github/dtprj/dongting/dtkv/KvClient.java)
</cite>

## 目录
1. [简介](#简介)
2. [项目结构](#项目结构)
3. [核心组件](#核心组件)
4. [架构概览](#架构概览)
5. [详细组件分析](#详细组件分析)
6. [依赖关系分析](#依赖关系分析)
7. [性能考虑](#性能考虑)
8. [故障排除指南](#故障排除指南)
9. [结论](#结论)

## 简介

Dongting的Watch机制是一个高效的数据变更监听系统，用于实时跟踪键值存储中的数据变化。该机制通过ServerWatchManager和WatchManager两个核心组件协同工作，实现了客户端与服务器之间的事件通知和状态同步。

Watch机制的主要特点包括：
- 基于树形结构的WatchHolder管理
- 智能的事件分发和批量处理
- 自动重试机制和连接故障恢复
- 支持大批量通知和流量控制
- 高效的内存管理和资源回收

## 项目结构

Watch机制涉及以下关键文件：

```mermaid
graph TB
subgraph "客户端模块"
A[KvClient.java]
B[WatchManager.java]
C[WatchProcessor.java]
D[WatchReq.java]
E[WatchNotifyReq.java]
end
subgraph "服务端模块"
F[ServerWatchManager.java]
G[WatchProcessor.java]
H[WatchReqCallback.java]
end
subgraph "协议定义"
I[dt_kv.proto]
end
A --> B
A --> C
B --> D
C --> E
F --> G
G --> H
D --> I
E --> I
```

**图表来源**
- [KvClient.java](file://client/src/main/java/com/github/dtprj/dongting/dtkv/KvClient.java#L1-L50)
- [ServerWatchManager.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/ServerWatchManager.java#L1-L50)

## 核心组件

### ServerWatchManager（服务器端Watch管理器）

ServerWatchManager是Watch机制的核心组件，负责管理客户端连接、事件分发和重试机制。

主要职责：
- 管理客户端通道信息
- 维护WatchHolder树形结构
- 处理事件通知和批量推送
- 实现自动重试和故障恢复
- 控制通知频率和流量限制

### WatchManager（客户端Watch管理器）

WatchManager运行在客户端，负责与服务器建立连接并维护Watch状态。

主要职责：
- 管理本地Watch状态
- 发送同步请求到服务器
- 处理服务器响应和错误
- 实现心跳检测和服务器发现
- 协调事件回调处理

**章节来源**
- [ServerWatchManager.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/ServerWatchManager.java#L46-L117)
- [WatchManager.java](file://client/src/main/java/com/github/dtprj/dongting/dtkv/WatchManager.java#L45-L100)

## 架构概览

Watch机制采用分层架构设计，分为客户端和服务端两部分：

```mermaid
sequenceDiagram
participant Client as 客户端
participant WatchMgr as WatchManager
participant Server as 服务器
participant ServerWatchMgr as ServerWatchManager
participant KVStore as 键值存储
Client->>WatchMgr : 添加Watch监听
WatchMgr->>Server : 同步请求(Sync Request)
Server->>ServerWatchMgr : 处理同步请求
ServerWatchMgr->>KVStore : 查找节点状态
ServerWatchMgr-->>Server : 返回节点信息
Server-->>WatchMgr : 同步响应
WatchMgr-->>Client : 注册成功
KVStore->>ServerWatchMgr : 数据变更事件
ServerWatchMgr->>ServerWatchMgr : 创建通知消息
ServerWatchMgr->>Server : 推送通知(Push Notify)
Server-->>WatchMgr : 通知响应
WatchMgr-->>Client : 触发事件回调
```

**图表来源**
- [ServerWatchManager.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/ServerWatchManager.java#L285-L321)
- [WatchManager.java](file://client/src/main/java/com/github/dtprj/dongting/dtkv/WatchManager.java#L200-L250)

## 详细组件分析

### ServerWatchManager详细分析

#### 核心数据结构

ServerWatchManager使用多种数据结构来管理Watch状态：

```mermaid
classDiagram
class ServerWatchManager {
-IdentityHashMap~DtChannel,ChannelInfo~ channelInfoMap
-LinkedHashSet~ChannelInfo~ needNotifyChannels
-PriorityQueue~ChannelInfo~ retryQueue
-LinkedHashSet~WatchHolder~ needDispatch
-Timestamp ts
-KvConfig config
+addOrUpdateActiveQueue(ChannelInfo)
+removeByChannel(DtChannel)
+dispatch() boolean
+sync(KvImpl, DtChannel, boolean, ByteArray[], long[])
}
class ChannelInfo {
+DtChannel channel
+HashMap~ByteArray,ChannelWatch~ watches
+long lastNotifyNanos
+long lastActiveNanos
+LinkedHashSet~ChannelWatch~ needNotify
+long retryNanos
+int failCount
+boolean remove
+addNeedNotify(ChannelWatch)
}
class WatchHolder {
+HashSet~ChannelWatch~ watches
+ByteArray key
+KvNodeHolder nodeHolder
+WatchHolder parentWatchHolder
+long lastRemoveIndex
+boolean waitingDispatch
+getChild(ByteArray) WatchHolder
+addChild(ByteArray, WatchHolder)
+removeChild(ByteArray)
}
class ChannelWatch {
+WatchHolder watchHolder
+ChannelInfo channelInfo
+long notifiedIndex
+long notifiedIndexPending
+boolean pending
+boolean removed
}
ServerWatchManager --> ChannelInfo : "管理"
ServerWatchManager --> WatchHolder : "维护"
ChannelInfo --> ChannelWatch : "包含"
WatchHolder --> ChannelWatch : "关联"
```

**图表来源**
- [ServerWatchManager.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/ServerWatchManager.java#L57-L117)
- [ServerWatchManager.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/ServerWatchManager.java#L570-L629)

#### sync方法工作原理

sync方法负责处理客户端的同步请求，建立或更新Watch关系：

```mermaid
flowchart TD
Start([接收同步请求]) --> CheckParams{"检查参数"}
CheckParams --> |syncAll=true且无keys| RemoveChannel["移除通道信息"]
CheckParams --> |正常参数| GetChannelInfo["获取或创建ChannelInfo"]
GetChannelInfo --> AddToActive["添加到活跃队列"]
AddToActive --> CheckSyncAll{"是否全量同步?"}
CheckSyncAll --> |是| MarkAllForRemoval["标记所有Watch为待删除"]
CheckSyncAll --> |否| ProcessKeys["处理指定键"]
MarkAllForRemoval --> ProcessKeys
ProcessKeys --> CheckKnownIndex{"已知Raft索引有效?"}
CheckKnownIndex --> |有效| CheckExisting{"检查现有Watch"}
CheckKnownIndex --> |无效| RemoveWatch["移除Watch"]
CheckExisting --> |存在| UpdateIndex["更新通知索引"]
CheckExisting --> |不存在| CreateWatch["创建新Watch"]
UpdateIndex --> AddToNotify["添加到通知队列"]
CreateWatch --> AddToNotify
RemoveWatch --> CheckSyncAll
AddToNotify --> CheckMoreKeys{"还有更多键?"}
CheckMoreKeys --> |是| ProcessKeys
CheckMoreKeys --> |否| CheckFullSync{"全量同步完成?"}
CheckFullSync --> |是| CleanupWatches["清理待删除的Watch"]
CheckFullSync --> |否| CheckEmpty["检查是否为空"]
CleanupWatches --> CheckEmpty
CheckEmpty --> |空| RemoveChannel
CheckEmpty --> |非空| End([结束])
RemoveChannel --> End
```

**图表来源**
- [ServerWatchManager.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/ServerWatchManager.java#L631-L730)

#### dispatch方法工作原理

dispatch方法负责事件分发和通知推送：

```mermaid
flowchart TD
Start([开始分发]) --> CheckNeedDispatch{"有需要分发的Watch?"}
CheckNeedDispatch --> |否| CheckNeedNotify{"有需要通知的通道?"}
CheckNeedDispatch --> |是| ProcessNeedDispatch["处理需要分发的Watch"]
ProcessNeedDispatch --> IterateWatchHolders["遍历WatchHolder"]
IterateWatchHolders --> CheckBatchSize{"达到批次大小?"}
CheckBatchSize --> |是| SetResultFalse["设置返回结果为false"]
CheckBatchSize --> |否| ProcessWatch["处理单个Watch"]
ProcessWatch --> CheckRemoved{"Watch已移除或正在处理?"}
CheckRemoved --> |是| NextWatch["下一个Watch"]
CheckRemoved --> |否| AddToNotify["添加到通知队列"]
AddToNotify --> CheckFailCount{"失败计数为0且未挂起?"}
CheckFailCount --> |是| AddToNeedNotify["添加到需要通知队列"]
CheckFailCount --> |否| NextWatch
AddToNeedNotify --> NextWatch
NextWatch --> MoreWatches{"还有更多Watch?"}
MoreWatches --> |是| ProcessWatch
MoreWatches --> |否| ClearNeedDispatch["清空需要分发队列"]
ClearNeedDispatch --> CheckBatchSizeReached{"批次大小已满?"}
CheckBatchSizeReached --> |是| SetResultFalse
CheckBatchSizeReached --> |否| CheckNeedNotify
SetResultFalse --> CheckNeedNotify
CheckNeedNotify --> |否| ProcessRetryQueue["处理重试队列"]
CheckNeedNotify --> |是| ProcessNeedNotify["处理需要通知的通道"]
ProcessNeedNotify --> CopyToTempList["复制到临时列表"]
CopyToTempList --> IterateNeedNotify["遍历需要通知的通道"]
IterateNeedNotify --> CheckFailCountZero{"失败计数为0?"}
CheckFailCountZero --> |否| NextChannel["下一个通道"]
CheckFailCountZero --> |是| CheckBatchSizeNotify{"达到批次大小?"}
CheckBatchSizeNotify --> |是| SetResultFalse2["设置返回结果为false"]
CheckBatchSizeNotify --> |否| AddToList["添加到处理列表"]
AddToList --> NextChannel
NextChannel --> MoreChannels{"还有更多通道?"}
MoreChannels --> |是| CheckFailCountZero
MoreChannels --> |否| ProcessPushNotify["处理推送通知"]
ProcessPushNotify --> ClearTempList["清空临时列表"]
ClearTempList --> ProcessRetryQueue
ProcessRetryQueue --> PeekRetryQueue["查看重试队列头部"]
PeekRetryQueue --> CheckRetryTime{"重试时间已到?"}
CheckRetryTime --> |否| End([结束])
CheckRetryTime --> |是| CheckRetryBatch{"达到重试批次大小?"}
CheckRetryBatch --> |是| SetResultFalse3["设置返回结果为false"]
CheckRetryBatch --> |否| PollQueue["从队列中取出"]
PollQueue --> PushNotifyRetry["推送重试通知"]
PushNotifyRetry --> PeekRetryQueue
SetResultFalse --> End
SetResultFalse2 --> End
SetResultFalse3 --> End
```

**图表来源**
- [ServerWatchManager.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/ServerWatchManager.java#L180-L280)

#### pushNotify方法工作原理

pushNotify方法负责向客户端推送通知消息：

```mermaid
flowchart TD
Start([开始推送通知]) --> CheckRemoved{"通道已标记移除?"}
CheckRemoved --> |是| End([结束])
CheckRemoved --> |否| CheckChannelOpen{"通道是否打开?"}
CheckChannelOpen --> |否| RemoveByChannel["移除通道"]
CheckChannelOpen --> |是| CheckNeedNotify{"有需要通知的Watch?"}
RemoveByChannel --> End
CheckNeedNotify --> |否| SetPendingFalse["设置pending=false"]
CheckNeedNotify --> |是| ProcessNeedNotify["处理需要通知的Watch"]
ProcessNeedNotify --> IterateNeedNotify["遍历需要通知的Watch"]
IterateNeedNotify --> CheckRemoved2{"Watch已移除或正在处理?"}
CheckRemoved2 --> |是| RemoveFromIterator["从迭代器移除"]
CheckRemoved2 --> |否| CreateNotify["创建通知对象"]
CreateNotify --> CheckNotifyNotNull{"通知不为空?"}
CheckNotifyNotNull --> |否| RemoveFromIterator
CheckNotifyNotNull --> |是| AddToList["添加到临时列表"]
AddToList --> UpdateBytes["更新字节计数"]
UpdateBytes --> CheckByteLimit{"超过字节限制?"}
CheckByteLimit --> |是| BreakLoop["跳出循环"]
CheckByteLimit --> |否| RemoveFromIterator
RemoveFromIterator --> MoreWatches{"还有更多Watch?"}
MoreWatches --> |是| IterateNeedNotify
MoreWatches --> |否| CheckListEmpty{"临时列表为空?"}
BreakLoop --> CheckListEmpty
CheckListEmpty --> |是| SetPendingFalse
CheckListEmpty --> |否| SetPendingTrue["设置pending=true"]
SetPendingTrue --> UpdateLastNotify["更新最后通知时间"]
UpdateLastNotify --> PrepareLists["准备Watch列表和通知列表"]
PrepareLists --> CreateRequest["创建WatchNotifyReq"]
CreateRequest --> SendRequest["发送请求"]
SendRequest --> End
SetPendingFalse --> End
```

**图表来源**
- [ServerWatchManager.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/ServerWatchManager.java#L285-L321)

### WatchManager详细分析

#### 客户端状态管理

WatchManager使用GroupWatches类来管理每个Raft组的Watch状态：

```mermaid
classDiagram
class WatchManager {
-RaftClient raftClient
-HashMap~Integer,GroupWatches~ watches
-ReentrantLock lock
-KeyWatch notifyQueueHead
-KeyWatch notifyQueueTail
+addWatch(int, byte[][])
+removeWatch(int, byte[][])
+syncGroupInLock(GroupWatches)
}
class GroupWatches {
+int groupId
+HashMap~ByteArray,KeyWatch~ watches
+RaftNode server
+long serversEpoch
+boolean busy
+boolean needSync
+boolean fullSync
+ScheduledFuture~?~ scheduledFuture
+boolean removed
}
class KeyWatch {
+ByteArray key
+GroupWatches gw
+boolean needRegister
+boolean needRemove
+long raftIndex
+WatchEvent event
+KeyWatch next
}
WatchManager --> GroupWatches : "管理多个"
GroupWatches --> KeyWatch : "包含多个"
```

**图表来源**
- [WatchManager.java](file://client/src/main/java/com/github/dtprj/dongting/dtkv/WatchManager.java#L60-L100)
- [WatchManager.java](file://client/src/main/java/com/github/dtprj/dongting/dtkv/WatchManager.java#L100-L150)

#### 心跳检测和服务器发现

WatchManager实现了心跳检测机制来确保服务器连接的可靠性：

```mermaid
sequenceDiagram
participant Timer as 定时器
participant WatchMgr as WatchManager
participant RaftClient as Raft客户端
participant Server as 服务器
loop 每隔心跳间隔
Timer->>WatchMgr : 触发心跳检查
WatchMgr->>WatchMgr : 检查服务器状态
alt 服务器未连接
WatchMgr->>RaftClient : 查找可用服务器
RaftClient->>Server : 尝试连接
Server-->>RaftClient : 连接结果
RaftClient-->>WatchMgr : 更新服务器状态
else 服务器已连接
WatchMgr->>Server : 查询状态
Server-->>WatchMgr : 状态响应
alt 状态正常
WatchMgr->>WatchMgr : 清除重试标志
WatchMgr->>WatchMgr : 执行同步操作
else 状态异常
WatchMgr->>RaftClient : 尝试下一个服务器
end
end
end
```

**图表来源**
- [WatchManager.java](file://client/src/main/java/com/github/dtprj/dongting/dtkv/WatchManager.java#L200-L250)

**章节来源**
- [ServerWatchManager.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/ServerWatchManager.java#L46-L730)
- [WatchManager.java](file://client/src/main/java/com/github/dtprj/dongting/dtkv/WatchManager.java#L45-L400)

## 依赖关系分析

Watch机制的依赖关系如下图所示：

```mermaid
graph TB
subgraph "外部依赖"
A[RaftClient]
B[NioClient]
C[Timestamp]
D[KvConfig]
end
subgraph "核心组件"
E[ServerWatchManager]
F[WatchManager]
G[WatchProcessor]
end
subgraph "数据结构"
H[ChannelInfo]
I[WatchHolder]
J[ChannelWatch]
K[KeyWatch]
end
subgraph "协议定义"
L[WatchReq]
M[WatchNotifyReq]
N[WatchNotify]
end
A --> F
B --> F
C --> E
D --> E
D --> F
E --> H
E --> I
E --> J
F --> K
G --> L
G --> M
L --> N
M --> N
```

**图表来源**
- [ServerWatchManager.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/ServerWatchManager.java#L20-L40)
- [WatchManager.java](file://client/src/main/java/com/github/dtprj/dongting/dtkv/WatchManager.java#L20-L40)

**章节来源**
- [ServerWatchManager.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/ServerWatchManager.java#L1-L50)
- [WatchManager.java](file://client/src/main/java/com/github/dtprj/dongting/dtkv/WatchManager.java#L1-L50)

## 性能考虑

### 批量处理优化

Watch机制实现了多项性能优化措施：

1. **批量通知处理**：每次推送最多处理`watchMaxBatchSize`个通知
2. **字节限制控制**：单次推送不超过`watchMaxReqBytes`字节
3. **智能重试策略**：使用指数退避算法进行重试
4. **内存池化**：复用临时对象减少GC压力

### 内存管理

- 使用`ArrayList`和`HashMap`的预分配容量
- 及时清理不再使用的Watch对象
- 通过`WeakReference`避免内存泄漏

### 并发控制

- 使用`ReentrantLock`保护共享状态
- 无锁队列用于事件分发
- 原子操作保证状态一致性

## 故障排除指南

### 常见问题及解决方案

#### 连接超时问题

**症状**：Watch请求频繁超时
**原因**：网络延迟或服务器负载过高
**解决方案**：
1. 调整`heartbeatIntervalMillis`参数
2. 增加`watchMaxBatchSize`以减少请求频率
3. 检查服务器资源使用情况

#### 重试次数过多

**症状**：Watch通知持续失败
**原因**：网络不稳定或服务器配置问题
**解决方案**：
1. 检查网络连通性
2. 调整重试间隔数组
3. 验证服务器配置

#### 内存泄漏

**症状**：长时间运行后内存占用持续增长
**原因**：Watch对象未正确清理
**解决方案**：
1. 确保调用`removeWatch`方法
2. 检查异常处理逻辑
3. 监控`channelInfoMap`大小

**章节来源**
- [ServerWatchManager.java](file://server/src/main/java/com/github/dtprj/dongting/dtkv/server/ServerWatchManager.java#L570-L629)
- [WatchManager.java](file://client/src/main/java/com/github/dtprj/dongting/dtkv/WatchManager.java#L200-L250)

## 结论

Dongting的Watch机制是一个设计精良的事件通知系统，具有以下优势：

1. **高可靠性**：完善的重试机制和故障恢复策略
2. **高性能**：批量处理和智能调度算法
3. **可扩展性**：支持大规模并发连接
4. **易维护**：清晰的代码结构和完善的测试覆盖

该机制为分布式键值存储提供了强大的数据变更监听能力，是实现低延迟事件驱动应用的重要基础设施。通过合理配置参数和监控系统状态，可以确保Watch机制在生产环境中的稳定运行。