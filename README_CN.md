[![Java CI with Maven](https://github.com/dtprj/dongting/actions/workflows/maven.yml/badge.svg)](https://github.com/dtprj/dongting/actions/workflows/maven.yml)
[![codecov](https://codecov.io/github/dtprj/dongting/branch/master/graph/badge.svg)](https://app.codecov.io/github/dtprj/dongting)

[English](README.md)

## 目录

- [项目介绍](#项目介绍)
- [10倍吞吐](#10倍吞吐)
- [零依赖](#零依赖)
- [TRY IT](#try-it)
  - [构建项目](#构建项目)
  - [启动服务器](#启动服务器)
  - [运行基准测试](#运行基准测试)
  - [客户端使用](#客户端使用)
  - [server配置](#server配置)
- [集群运维](#集群运维)
  - [配置一个多节点集群](#配置一个多节点集群)
  - [运行管理工具](#运行管理工具)
- [进阶](#进阶)
  - [将项目导入IDE](#将项目导入ide)
  - [通过代码构建 raft 服务器](#通过代码构建-raft-服务器)
- [关于我](#关于我)

# 项目介绍

Dongting是一个高性能的分布式引擎，集成了RAFT共识算法、配置服务器和消息队列功能。

主要特性如下：

* **Multi RAFT 组支持**：在同一进程内运行多个RAFT组，支持动态添加、删除和更新RAFT group（分片），实现集群的动态扩展。状态机可以在raft框架中自定义。
* **分布式配置服务器 DtKV**：树形结构，支持线性一致性的通用K/V操作、监听(watch)、TTL过期和分布式锁，类似于etcd
  * `DtKV`是内存型数据库，因此数据总量不能太大，但它使用raft log作为redo日志，定期做快照，掉电也不会丢失一条数据。
  * 原生支持树状目录，很多操作的复杂度是O(1)
  * 支持临时目录，在TTL过期后自动删除整个目录，该删除操作为原子操作
  * 不支持事务，但是提供了CAS和非常易用的分布式锁
* **（计划）MQ**：使用RAFT日志作为消息队列日志。

# 10倍吞吐

Dongting使用面向性能编程（POP）的方式开发。

在简单的同机测试中，现代高性能PC使用服务器默认配置即可轻松通过基准测试程序实现超过100万TPS，且响应时间(RT)也不会太大。

# 零依赖

你还在为一传十、十传百的依赖问题烦恼吗？你曾经因为1GB的镜像大小而被嘲笑吗？现在你有救了！

Dongting没有依赖。它不依赖任何第三方库，不需要第三方jar文件。
Dongting核心只有两个JAR包，客户端和服务器端加起来不到1MB。
它也没有传递性依赖，因此你可以轻松将其嵌入到你的应用程序中。

SLF4J是可选的，如果类路径中不存在，项目将使用JDK logging。

Dongting对JDK的要求也不高：客户端仅需Java 8，服务器端仅需Java 11。

Dongting不需要使用高性能硬件，如RDMA或Optane什么的，它也不依赖Amazon或其他云服务提供商提供的任何第三方服务，如存储服务。
它甚至可以在 HDD 硬盘和树莓派上良好运行。

Dongting不需要您调整Linux内核参数以达到最佳性能（生成环境你甚至可能没有权限这样做）。

# TRY IT

## 构建项目

首先构建项目，构建产物位于target/dongting-dist目录：
```sh
mvn clean package -DskipUTs
```

构建后的目录结构如下：
```
dongting-dist/
├── bin/                     # 脚本目录
│   ├── benchmark.sh         # 基准测试脚本
│   ├── benchmark.bat        # Windows基准测试脚本
│   ├── start-dongting.sh    # 启动服务器脚本（Linux/Mac）
│   ├── start-dongting.bat   # 启动服务器脚本
│   ├── stop-dongting.sh     # 停止服务器脚本
│   ├── stop-dongting.bat    # 停止服务器脚本
│   ├── dongting-admin.sh    # 管理工具脚本（Linux/Mac）
│   └── dongting-admin.bat   # 管理工具脚本
├── lib/                     # JAR包目录
│   ├── dongting-client-x.y.z-SNAPSHOT.jar
│   ├── dongting-server-x.y.z-SNAPSHOT.jar
│   ├── dongting-dist-x.y.z-SNAPSHOT.jar # 引导jar，如果使用代码构建raft server，它不是必须的
│   ├── slf4j-api-x.y.z.jar
│   ├── logback-x.y.z.jar
│   └── logback-x.y.z.jar
├── conf/                     # 配置文件目录
│   ├── config.properties
│   ├── servers.properties
│   ├── client.properties
│   ├── logback-server.xml
│   ├── logback-admin.xml
│   └── logback-benchmark.xml
├── docs/                     # 文档目录
├── data/                     # 数据目录（运行时生成）
└── logs/                     # 日志目录
    ├── dongting-server.log   # 服务器日志（运行时生成）
    └── dongting-stats.log    # 统计日志（运行时生成）
```

## 启动服务器

在bin目录下，运行以下命令启动服务器：
```sh
./start-dongting.sh
```
服务器将启动并监听 9331 端口（用于server内部通信，如raft复制）和 9332 端口（服务端口）。
默认会启动一个`DtKV`实例，groupId为0。

运行下面的命令可以（根据data目录下的pid文件找到进程）停止服务器：
```sh
./stop-dongting.sh
```

## 运行基准测试

运行以下命令启动基准测试客户端：
```sh
./benchmark.sh -g 0
```

你可能需要调整参数以获得最大吞吐量，例如：
```sh
./benchmark.sh -g 0 --max-pending 10000 --client-count 2
```

尝试使用Java 21虚拟线程进行多线程压测（需要 Java 21）：
```sh
./benchmark.sh -g 0 --sync --thread-count 4000
```

看看我的测试结果吧（AMD 9700X 6C12T，致态 TiPro 9000 运行在 PCI-E 4.0 模式）：
```powershell
PS D:\dongting-dist\bin> .\benchmark.bat -g 0
Configuration:
  Config file: D:\dongting-dist\conf\client.properties
  Servers: 1,127.0.0.1:9332
  Group ID: 0

Benchmark config:
  Java 21, async put, 10000 keys, 256 bytes value, 2000 total maxPending
  1 clients, one thread per client

Warming up for 3 seconds...

[Warmup] TPS: 1,133,471, Success: 1,133,471, Fail: 0, Avg: 1,754 us, Max: 30,000 us
[Warmup] TPS: 1,355,100, Success: 1,355,100, Fail: 0, Avg: 1,483 us, Max: 13,000 us
[Warmup] TPS: 1,349,302, Success: 1,349,302, Fail: 0, Avg: 1,456 us, Max: 14,000 us
Warmup complete, starting benchmark...

[Now] TPS: 1,389,229, Success: 1,389,229, Fail: 0, Avg: 1,440 us, Max: 13,000 us
[Now] TPS: 1,331,667, Success: 1,331,667, Fail: 0, Avg: 1,494 us, Max: 13,000 us
[Now] TPS: 1,324,262, Success: 1,324,262, Fail: 0, Avg: 1,504 us, Max: 12,000 us
[Now] TPS: 1,369,651, Success: 1,369,651, Fail: 0, Avg: 1,453 us, Max: 13,000 us
[Now] TPS: 1,380,549, Success: 1,380,549, Fail: 0, Avg: 1,461 us, Max: 13,000 us
[Now] TPS: 1,344,967, Success: 1,344,967, Fail: 0, Avg: 1,479 us, Max: 19,000 us
[Now] TPS: 1,327,548, Success: 1,327,548, Fail: 0, Avg: 1,499 us, Max: 24,000 us
[Now] TPS: 1,363,815, Success: 1,363,815, Fail: 0, Avg: 1,458 us, Max: 12,000 us
[Now] TPS: 1,359,154, Success: 1,359,154, Fail: 0, Avg: 1,472 us, Max: 13,000 us
[Now] TPS: 1,345,889, Success: 1,345,889, Fail: 0, Avg: 1,476 us, Max: 13,000 us

Benchmark config:
  Java 21, async put, 10000 keys, 256 bytes value, 2000 total maxPending
  1 clients, one thread per client

[Final] TPS: 1,353,905, Success: 13,539,049, Fail: 0, Avg: 1,473 us, Max: 24,000 us

PS D:\dongting-dist\bin>
```

## 客户端使用

Dongting内置的DtKV支持以下功能：

* get/batchGet
* put/batchPut
* remove/batchRemove
* list
* mkdir
* compareAndSet
* putTemp/makeTempDir（带 TTL 的节点）
* createLock/createAutoRenewalLock

`DtKV` 使用 `.` 作为 key 的分隔符。例如，您可以使用 `"dir1.key1"` 访问 `dir1` 目录下的 `key1`，value不能为空值或空串。

要使用`DtKV`客户端，需要引入 `dongting-client.jar`（300+KB），无其它依赖。

以下是初始化`DtKV`客户端的简单示例：
```java
// dongting 支持multi-raft，因此需要指定group id，默认group id是0
int groupId = 0;
KvClient kvClient = new KvClient();
kvClient.start();
// 在运行时添加node定义，每个node有一个唯一的正整数id 和一个 host:servicePort 地址
kvClient.getRaftClient().clientAddNode("1,127.0.0.1:9332");
// kvClient.getRaftClient().clientAddNode("1,192.168.0.1:9332;2,192.168.0.2:9332;3,192.168.0.3:9332");
// 在运行时添加group定义，这里添加一个 groupId 为 0 的组，包含 id 为 1、2、3 的 3 个节点
kvClient.getRaftClient().clientAddOrUpdateGroup(groupId, new int[]{1,2,3});
```

请确保指定正确的端口。每个raft node开放两个端口：
一个是**复制端口**，默认9331，用于 raft 节点之间的内部通信，如复制。`AdminRaftClient` 也连接到此端口。
另一个是**服务端口**，默认9332，用于 `KvClient` 等客户端的连接。

`KvClient` 提供同步和异步接口。对于单个 `KvClient`，异步操作可以获得最大吞吐量，而同步操作需要多个线程（或虚拟线程）才能达到较高吞吐量。
需要注意的是，异步操作的回调可能在 raft 线程或 IO 线程上执行。
因此，你不应在这些回调中执行任何阻塞或 CPU 密集型操作。 如果你不确定或缺乏高级技能，强烈建议使用同步接口。

以下是使用 `KvClient` 的简单示例：
```java
// 同步put
kvClient.put(groupId, "key1".getBytes(), "value1".getBytes());
kvClient.put(groupId, "key1".getBytes(), "value1".getBytes(), (raftIndex, ex) -> {
    // 执行一些既不阻塞也不消耗 CPU 的操作
});
```

有关 `KvClient` 类的详细用法，请参阅 Javadocs。

## server配置

服务器配置主要包含两个配置文件：`config.properties` 和 `servers.properties`。

### config.properties

此文件配置 Raft 服务器的基本参数：

- **nodeId**：每个服务器必须有一个唯一的正整数 node ID，注意是从1开始，如果只有一个node通常就设置为1
- **replicatePort**：用于服务器内部通信的端口，例如 Raft 复制（默认值：9331）
- **servicePort**：用于客户端-服务器通信的端口（默认值：9332）
- **electTimeout**：Raft 选举超时时间，单位毫秒（默认值：15000）
- **heartbeatInterval**：Raft 心跳间隔时间，单位毫秒（默认值：2000）
- **blockIoThreads**：处理阻塞 IO 的线程数（如果没有设置，默认值运行时根据CPU核心数计算生成）

### Raft group公共配置（可选）

以下配置在 `config.properties` 中，影响 Raft group 的行为：

- **dataDir**：数据目录（默认值指向dongting-dist目录下的data目录）
- **syncForce**：如果为 true，任何操作都会在响应 leader 和计算法定人数之前持久化（fsync）到磁盘。此选项对性能有显著影响（默认值：true）
- **saveSnapshotSeconds**：保存快照的间隔秒数（默认值：3600）
- **maxKeepSnapshots**：最多保留的快照数量（默认值：2）
- **saveSnapshotWhenClose**：关闭时（通常是服务器关闭时）是否保存快照（默认值：true）
- **deleteLogsAfterTakeSnapshot**：创建快照后是否删除不再需要的raft日志文件（默认值：true）

### servers.properties

此文件配置集群拓扑和 Raft group：

- **servers**：node 列表，格式为 `nodeId,ip:replicatePort`。多个 node 用分号分隔。示例：
  ```properties
  # 单节点示例
  servers = 1,127.0.0.1:9331

  # 多节点示例
  servers = 1,192.168.0.1:9331;2,192.168.0.2:9331;3,192.168.0.3:9331

  # 本地多节点示例（使用不同端口隔离）
  servers = 1,127.0.0.1:4001;2,127.0.0.1:4002;3,127.0.0.1:4003
  ```

- **Raft group member 配置**：格式为 `group.<groupId>.nodeIdOfMembers = nodeId1,nodeId2,...`。node ID 必须在 `servers` 属性中定义。
  ```properties
  group.0.nodeIdOfMembers = 1,2,3
  ```

- **Raft group observer 配置**：格式为 `group.<groupId>.nodeIdOfObservers = nodeId1,nodeId2,...`。observer 不会参与 leader 选举。
  ```properties
  group.0.nodeIdOfObservers = 4
  ```

observer会接收leader的数据复制，但是不会参与raft投票。

### client.properties

client.properties文件被benchmark使用，注意里面配置是的servers参数连接**服务端口**。

# 集群运维

## 配置一个多节点集群

默认情况下，直接运行start-dongting脚本会启动一个单节点集群监听本机9331端口和9332端口。
如果需要启动多节点集群，以3节点为例，需要做以下工作：

1. 准备3份dongting-dist目录，如果是自己做测试，在同一个机器上也可以，但要修改端口。
2. 修改每个config.properties文件中的nodeId。nodeId从1开始，不同node必须不一样。**注意服务器一旦启动，nodeId就不能再修改了（除非清空data目录系的数据）**。
3. （可选）同一个机器上做测试的话，要修改每个config.properties文件中的端口。
4. 修改servers.properties文件中的servers参数，3节点的话可能是`1,192.168.0.1:9331;2,192.168.0.2:9331;3,192.168.0.3:9331`或者`1,127.0.0.1:4001;2,127.0.0.1:4002;3,127.0.0.1:4003`，根据你的ip和端口配置而定
5. 修改servers.properties文件中的`group.0.nodeIdOfMembers`参数，3节点的话可能是`1,2,3`

以上2、3每个node配置不一样，而4、5每个节点配置是一样的。

如果要做benchmark，还要修改client.properties文件中的servers参数，连接**服务端口**（默认9332）。

配置好以后就可以分别启动了，可以随意杀掉一个进程，看看集群的表现。

## 运行管理工具

bin目录下的 dongting-admin 脚本是一个服务器管理工具，可用于：

* 更改raft group member/observer
* 转移leader
* 添加/删除组（multi-raft）
* 添加/删除集群节点
* 查询服务器状态

运行时不带参数可查看使用方法。

可以通过`AdminRaftClient`类执行所有的管理功能，记得连接到复制端口。

# 进阶

## 将项目导入IDE

将项目导入后，要设置 IDE，可以参考 [开发指南](docs/developer_CN.md)。

## 通过代码构建 raft 服务器

所有示例都在 `demos` 目录中。它们不需要配置，直接执行 `main` 方法即可运行。
建议在 IDE 中运行，以便更方便地设置断点和观察。

[cluster](demos/src/main/java/com/github/dtprj/dongting/demos/advanced/cluster) 目录包含一个运行 3 节点 raft 集群的示例。
分别运行 `DemoServer1`、`DemoServer2` 和 `DemoServer3`，raft 集群通常会在一秒内就绪。
运行 `DemoClient`，它将发送 100 万次 put 和 get 请求，同时记录完成时间。
运行 `PeriodPutClient`，它每秒持续发送一个 put 请求，不会停止。
您可以随时关闭一个服务器，`PeriodPutClient` 将不受影响。
此外，执行 `ChangeLeader` 将 Raft leader 切换到指定节点。
正在运行的 `PeriodPutClient` 将完全不受影响。

[standalone](demos/src/main/java/com/github/dtprj/dongting/demos/advanced/standalone) 目录包含一个运行单节点 raft 组的示例。

[embedded](demos/src/main/java/com/github/dtprj/dongting/demos/advanced/embedded) 目录包含一个将 3 个服务器和 1 个客户端嵌入到单个进程中的示例。

[configchange](demos/src/main/java/com/github/dtprj/dongting/demos/advanced/configchange) 目录包含在运行时动态更改 Raft 成员的示例。
首先，运行 `ConfigChangeDemoServer1`、`ConfigChangeDemoServer2`、`ConfigChangeDemoServer3` 和 `ConfigChangeDemoServer4`。
默认情况下，将启动包含节点 1、2、3 的 Raft 组。
执行 `ChangeTo234Client` 将 Raft 成员更改为节点 2、3、4。
执行 `ChangeTo123Client` 将 Raft 成员恢复为节点 1、2、3。

[multiraft](demos/src/main/java/com/github/dtprj/dongting/demos/advanced/multiraft) 目录包含运行多 raft 的示例，通常用于分片或动态分片。
运行 `MultiRaftDemoServer1`、`MultiRaftDemoServer2` 和 `MultiRaftDemoServer3` 默认启动两个（静态）raft 组，ID 为 101 和 102。
执行 `PeriodPutClient` 将每秒向 raft 组 101、102 和 103 发送一个 put 请求。
由于组 103 不存在，每秒将有两个成功的 put 操作和一个失败的操作。
运行 `AddGroup103Demo` 在运行时添加 raft 组 103，之后 `PeriodPutClient` 将每秒输出三个成功的操作。
执行 `RemoveGroup103Demo` 将删除 raft 组 103。

[watch](demos/src/main/java/com/github/dtprj/dongting/demos/advanced/watch) 目录包含使用客户端监控指定 key 变化的示例，同时演示如何监控目录，客户端将收到其子节点任何变化的通知。

[ttl](demos/src/main/java/com/github/dtprj/dongting/demos/advanced/ttl) 目录包含使用客户端设置带 TTL 的 key 的示例，TTL 过期后，key 将被自动删除。

[lock](demos/src/main/java/com/github/dtprj/dongting/demos/advanced/lock) 目录包含使用分布式锁的示例。分布式锁可以手动使用 tryLock/unlock 操作，也可以使用 tryLock/updateLease 完全自动化（可用于业务代码中的 leader 选举）。

# 关于我

https://weibo.com/dtprj

微信公众号：

![公众号](docs/imgs/qrcode_wechat.jpg)
