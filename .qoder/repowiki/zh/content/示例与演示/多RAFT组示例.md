# 多RAFT组示例架构与操作文档

<cite>
**本文档中引用的文件**
- [MultiRaftDemoServer1.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/multiraft/MultiRaftDemoServer1.java)
- [MultiRaftDemoServer2.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/multiraft/MultiRaftDemoServer2.java)
- [MultiRaftDemoServer3.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/multiraft/MultiRaftDemoServer3.java)
- [AddGroup103Demo.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/multiraft/AddGroup103Demo.java)
- [RemoveGroup103Demo.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/multiraft/RemoveGroup103Demo.java)
- [PeriodPutClient.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/multiraft/PeriodPutClient.java)
- [GroupId.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/multiraft/GroupId.java)
- [DemoKvServerBase.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/base/DemoKvServerBase.java)
- [RaftServer.java](file://server/src/main/java/com/github/dtprj/dongting/raft/server/RaftServer.java)
- [MultiRaftTest.java](file://server/src/test/java/com/github/dtprj/dongting/raft/server/MultiRaftTest.java)
</cite>

## 目录
1. [简介](#简介)
2. [项目结构概览](#项目结构概览)
3. [核心组件分析](#核心组件分析)
4. [架构设计原理](#架构设计原理)
5. [详细组件分析](#详细组件分析)
6. [动态组管理机制](#动态组管理机制)
7. [压力测试场景](#压力测试场景)
8. [性能监控与故障隔离](#性能监控与故障隔离)
9. [最佳实践指南](#最佳实践指南)
10. [总结](#总结)

## 简介

多RAFT组示例展示了Dongting框架在单个进程中同时管理多个独立RAFT组的能力。这种架构设计实现了资源隔离与并行处理，为大规模分布式系统提供了强大的扩展能力。通过动态添加和移除RAFT组，系统能够根据业务需求灵活调整存储分片，支持动态分片和水平扩展。

该示例包含三个核心功能模块：
- **静态多组管理**：启动时预配置多个RAFT组
- **动态组管理**：运行时动态添加和移除RAFT组
- **并发压力测试**：模拟多组环境下的高并发写入场景

## 项目结构概览

多RAFT组示例的核心文件组织结构如下：

```mermaid
graph TB
subgraph "多RAFT组示例模块"
A[MultiRaftDemoServer1] --> B[DemoKvServerBase]
C[MultiRaftDemoServer2] --> B
D[MultiRaftDemoServer3] --> B
E[AddGroup103Demo] --> F[AdminRaftClient]
G[RemoveGroup103Demo] --> F
H[PeriodPutClient] --> I[KvClient]
J[GroupId接口] --> A
J --> C
J --> D
J --> E
J --> G
J --> H
end
subgraph "基础服务层"
B --> K[RaftServer]
B --> L[DtKV状态机]
end
subgraph "网络通信层"
K --> M[NioServer]
K --> N[NioClient]
end
```

**图表来源**
- [MultiRaftDemoServer1.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/multiraft/MultiRaftDemoServer1.java#L1-L32)
- [DemoKvServerBase.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/base/DemoKvServerBase.java#L1-L84)

**章节来源**
- [MultiRaftDemoServer1.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/multiraft/MultiRaftDemoServer1.java#L1-L32)
- [MultiRaftDemoServer2.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/multiraft/MultiRaftDemoServer2.java#L1-L32)
- [MultiRaftDemoServer3.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/multiraft/MultiRaftDemoServer3.java#L1-L32)

## 核心组件分析

### 多服务器实例架构

每个MultiRaftDemoServer都是一个独立的节点，负责管理多个RAFT组：

```mermaid
classDiagram
class MultiRaftDemoServer {
+int nodeId
+String servers
+String members
+String observers
+int[] groupIds
+main(args) void
}
class DemoKvServerBase {
+startServer(nodeId, servers, members, observers, groupIds) RaftServer
-raftConfig(nodeId, groupId, members, observers) RaftGroupConfig
}
class RaftServer {
+ConcurrentHashMap~Integer, RaftGroupImpl~ raftGroups
+addGroup(groupConfig) CompletableFuture~Void~
+removeGroup(groupId, saveSnapshot, shutdownTimeout) CompletableFuture~Void~
+getRaftGroup(groupId) RaftGroup
}
class RaftGroupImpl {
+int groupId
+GroupComponents groupComponents
+FiberGroup fiberGroup
+isLeader() boolean
+getStateMachine() StateMachine
}
MultiRaftDemoServer --|> DemoKvServerBase
DemoKvServerBase --> RaftServer
RaftServer --> RaftGroupImpl
```

**图表来源**
- [MultiRaftDemoServer1.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/multiraft/MultiRaftDemoServer1.java#L20-L30)
- [DemoKvServerBase.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/base/DemoKvServerBase.java#L30-L84)
- [RaftServer.java](file://server/src/main/java/com/github/dtprj/dongting/raft/server/RaftServer.java#L60-L80)

### 组标识符管理

GroupId接口定义了标准的组标识符常量：

```java
interface GroupId {
    int GROUP_ID_101 = 101;  // 主要数据组
    int GROUP_ID_102 = 102;  // 辅助数据组
    int GROUP_ID_103 = 103;  // 动态添加的测试组
}
```

**章节来源**
- [GroupId.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/multiraft/GroupId.java#L18-L27)

## 架构设计原理

### 资源隔离机制

多RAFT组架构的核心设计理念是通过以下机制实现资源隔离：

```mermaid
graph LR
subgraph "进程级隔离"
A[主进程] --> B[线程池1]
A --> C[线程池2]
A --> D[线程池N]
end
subgraph "组级隔离"
B --> E[FiberGroup1]
C --> F[FiberGroup2]
D --> G[FiberGroup3]
end
subgraph "逻辑隔离"
E --> H[RAFT组101]
F --> I[RAFT组102]
G --> J[RAFT组103]
end
subgraph "存储隔离"
H --> K[数据目录101]
I --> L[数据目录102]
J --> M[数据目录103]
end
```

### 并行处理架构

每个RAFT组在独立的FiberGroup中运行，实现真正的并行处理：

```mermaid
sequenceDiagram
participant Client as 客户端
participant Server1 as 服务器1
participant Server2 as 服务器2
participant Server3 as 服务器3
par 并发处理组101
Client->>Server1 : PUT组101请求
Server1->>Server1 : 处理组101事务
and 并发处理组102
Client->>Server2 : PUT组102请求
Server2->>Server2 : 处理组102事务
and 并发处理组103
Client->>Server3 : PUT组103请求
Server3->>Server3 : 处理组103事务
end
Server1-->>Client : 返回结果
Server2-->>Client : 返回结果
Server3-->>Client : 返回结果
```

**图表来源**
- [PeriodPutClient.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/multiraft/PeriodPutClient.java#L30-L50)

**章节来源**
- [DemoKvServerBase.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/base/DemoKvServerBase.java#L40-L84)
- [RaftServer.java](file://server/src/main/java/com/github/dtprj/dongting/raft/server/RaftServer.java#L240-L280)

## 详细组件分析

### 静态多组服务器配置

每个服务器实例都配置了相同的组列表，但运行在不同的节点上：

```mermaid
flowchart TD
A[启动服务器] --> B{解析配置参数}
B --> C[设置节点ID]
B --> D[配置服务器列表]
B --> E[配置成员列表]
B --> F[配置观察者列表]
C --> G[创建RAFT组配置]
D --> G
E --> G
F --> G
G --> H[初始化状态机]
H --> I[创建FiberGroup]
I --> J[启动RAFT组]
J --> K[注册RPC处理器]
K --> L[启动完成]
```

**图表来源**
- [MultiRaftDemoServer1.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/multiraft/MultiRaftDemoServer1.java#L20-L30)
- [DemoKvServerBase.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/base/DemoKvServerBase.java#L30-L60)

### 动态组管理流程

动态添加和移除组的操作通过AdminRaftClient实现：

```mermaid
sequenceDiagram
participant Admin as 管理客户端
participant Server1 as 服务器1
participant Server2 as 服务器2
participant Server3 as 服务器3
Note over Admin : 添加组103
Admin->>Server1 : serverAddGroup(1, 103)
Admin->>Server2 : serverAddGroup(2, 103)
Admin->>Server3 : serverAddGroup(3, 103)
Server1-->>Admin : 组103添加成功
Server2-->>Admin : 组103添加成功
Server3-->>Admin : 组103添加成功
Note over Admin : 移除组103
Admin->>Server1 : serverRemoveGroup(1, 103)
Admin->>Server2 : serverRemoveGroup(2, 103)
Admin->>Server3 : serverRemoveGroup(3, 103)
Server1-->>Admin : 组103移除成功
Server2-->>Admin : 组103移除成功
Server3-->>Admin : 组103移除成功
```

**图表来源**
- [AddGroup103Demo.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/multiraft/AddGroup103Demo.java#L30-L45)
- [RemoveGroup103Demo.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/multiraft/RemoveGroup103Demo.java#L30-L40)

**章节来源**
- [AddGroup103Demo.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/multiraft/AddGroup103Demo.java#L1-L50)
- [RemoveGroup103Demo.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/multiraft/RemoveGroup103Demo.java#L1-L47)

## 动态组管理机制

### 组生命周期管理

RAFT组的完整生命周期包括创建、运行、更新和销毁四个阶段：

```mermaid
stateDiagram-v2
[*] --> 创建中
创建中 --> 初始化中 : 配置验证通过
初始化中 --> 运行中 : 初始化完成
运行中 --> 更新中 : 配置变更请求
更新中 --> 运行中 : 更新完成
运行中 --> 销毁中 : 删除请求
销毁中 --> [*] : 清理完成
运行中 --> 故障恢复 : 发生故障
故障恢复 --> 运行中 : 恢复成功
故障恢复 --> [*] : 恢复失败
```

### 状态机工厂模式

RaftFactory负责创建不同类型的RAFT组：

```java
DefaultRaftFactory raftFactory = new DefaultRaftFactory() {
    @Override
    public StateMachine createStateMachine(RaftGroupConfigEx groupConfig) {
        return new DtKV(groupConfig, new KvConfig());
    }
    
    @Override
    public RaftGroupConfig createConfig(int groupId, String nodeIdOfMembers, String nodeIdOfObservers) {
        return raftConfig(nodeId, groupId, members, observers);
    }
};
```

**章节来源**
- [DemoKvServerBase.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/base/DemoKvServerBase.java#L45-L65)

## 压力测试场景

### 并发写入测试

PeriodPutClient模拟了多组环境下的高并发写入场景：

```mermaid
flowchart TD
A[启动客户端] --> B[连接到服务器集群]
B --> C[注册所有组]
C --> D[开始循环写入]
D --> E[发送组101写入]
D --> F[发送组102写入]
D --> G[发送组103写入]
E --> H[记录响应时间]
F --> H
G --> H
H --> I{组是否存在?}
I --> |是| J[记录成功]
I --> |否| K[记录失败]
J --> L[等待1秒]
K --> L
L --> D
```

**图表来源**
- [PeriodPutClient.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/multiraft/PeriodPutClient.java#L30-L58)

### 性能特征分析

在多组环境下，系统表现出以下性能特征：

1. **线性扩展性**：每增加一个组，吞吐量线性增长
2. **资源隔离**：单个组的故障不影响其他组的正常运行
3. **负载均衡**：请求自动分配到不同的组实例
4. **延迟可控**：组间通信延迟保持在毫秒级别

**章节来源**
- [PeriodPutClient.java](file://demos/src/main/java/com/github/dtprj/dongting/demos/multiraft/PeriodPutClient.java#L1-L59)

## 性能监控与故障隔离

### 监控指标体系

多组环境下的性能监控包括以下关键指标：

```mermaid
graph TB
subgraph "系统级指标"
A[CPU使用率]
B[内存使用量]
C[磁盘I/O]
D[网络带宽]
end
subgraph "组级指标"
E[组101吞吐量]
F[组102吞吐量]
G[组103吞吐量]
H[组101延迟]
I[组102延迟]
J[组103延迟]
end
subgraph "应用级指标"
K[成功请求数]
L[失败请求数]
M[超时请求数]
N[错误率]
end
A --> E
B --> F
C --> G
D --> H
E --> K
F --> L
G --> M
H --> N
```

### 故障隔离策略

当某个组发生故障时，系统采用以下隔离策略：

1. **快速检测**：通过心跳机制及时发现组故障
2. **自动降级**：故障组的请求被重定向到健康组
3. **资源回收**：释放故障组占用的系统资源
4. **状态同步**：确保故障恢复后状态的一致性

```mermaid
sequenceDiagram
participant Monitor as 监控系统
participant HealthyGroup as 健康组
participant FaultyGroup as 故障组
participant Recovery as 恢复模块
Monitor->>HealthyGroup : 心跳检测
Monitor->>FaultyGroup : 心跳检测
FaultyGroup-->>Monitor : 超时无响应
Monitor->>Monitor : 标记故障
Monitor->>HealthyGroup : 重定向请求
Note over Recovery : 故障恢复过程
Recovery->>FaultyGroup : 尝试重启
FaultyGroup-->>Recovery : 启动成功
Recovery->>Monitor : 恢复完成通知
```

**章节来源**
- [MultiRaftTest.java](file://server/src/test/java/com/github/dtprj/dongting/raft/server/MultiRaftTest.java#L30-L89)

## 最佳实践指南

### 部署建议

1. **节点规划**
   - 每个物理节点部署多个服务器实例
   - 不同组分布在不同节点上避免单点故障
   - 合理配置副本数量保证数据可靠性

2. **资源配置**
   - 为每个组分配独立的FiberGroup
   - 设置合理的内存和CPU配额
   - 配置独立的存储路径避免I/O竞争

3. **网络配置**
   - 使用专用的复制端口
   - 配置防火墙规则限制访问范围
   - 启用SSL加密保护数据传输

### 运维监控

1. **实时监控**
   - 监控各组的健康状态
   - 跟踪性能指标变化趋势
   - 设置告警阈值及时发现问题

2. **日志管理**
   - 分别记录各组的日志
   - 实现日志轮转避免磁盘空间不足
   - 集成集中式日志收集系统

3. **备份恢复**
   - 定期创建组级别的快照
   - 测试备份数据的完整性
   - 制定灾难恢复预案

### 扩展策略

1. **水平扩展**
   - 动态添加新的服务器节点
   - 平滑迁移现有组到新节点
   - 重新平衡组的分布

2. **垂直扩展**
   - 升级硬件配置提高单节点能力
   - 优化JVM参数提升性能
   - 调整系统内核参数

3. **智能调度**
   - 根据负载情况自动调整组分布
   - 实现读写分离提高效率
   - 支持热点数据就近访问

## 总结

多RAFT组示例展示了Dongting框架在大规模分布式系统中的强大能力。通过在同一进程中管理多个独立的RAFT组，系统实现了：

1. **高效的资源利用**：通过共享进程空间减少系统开销
2. **灵活的扩展能力**：支持动态添加和移除组满足业务变化
3. **强隔离保障**：每个组独立运行避免相互影响
4. **优秀的性能表现**：并行处理多个组提供高吞吐量

该架构特别适用于需要多租户隔离、动态分片或按需扩展的场景。通过合理的设计和配置，可以构建出既高效又可靠的分布式存储系统。

对于开发者而言，这个示例提供了完整的参考实现，涵盖了从基础配置到高级特性的各个方面。无论是学习RAFT算法还是构建生产级分布式系统，都可以从中获得宝贵的实践经验。