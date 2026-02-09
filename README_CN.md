[![Java CI with Maven](https://github.com/dtprj/dongting/actions/workflows/maven.yml/badge.svg)](https://github.com/dtprj/dongting/actions/workflows/maven.yml)
[![codecov](https://codecov.io/github/dtprj/dongting/branch/master/graph/badge.svg)](https://app.codecov.io/github/dtprj/dongting)

[English](README.md)

# 项目介绍

Dongting是一个高性能的分布式引擎，集成了RAFT共识算法、配置服务器和消息队列功能。

主要特性如下：

* **Multi RAFT 组支持**：在同一进程内运行多个RAFT组，支持动态添加、删除和更新 RAFT 组，实现集群的动态扩展。状态机可以在raft框架中自定义。
* **分布式配置服务器 DtKV**：树形结构，支持线性一致性的通用K/V操作、监听(watch)、TTL过期和分布式锁。
* **（计划）线性一致性消息队列 MQ**：使用RAFT日志作为消息队列日志。

# 10倍吞吐

Dongting使用面向性能编程（POP）的方式开发。

在简单的同机测试中，现代高性能PC使用服务器默认配置即可轻松通过基准测试程序实现超过100万TPS，且响应时间(RT)也不会太大。

# 零依赖，体积仅为 1%

你还在为一传十、十传百的依赖问题烦恼吗？你曾经因为1GB的镜像大小而被嘲笑吗？

Dongting没有依赖。它不依赖任何第三方库，不需要第三方jar文件。SLF4J是可选的，如果类路径中不存在，项目将使用JDK logging。

Dongting核心只有两个JAR包，客户端和服务器端加起来不到1MB。 它也没有传递性依赖，因此你可以轻松将其嵌入到你的应用程序中。

Dongting对JDK的要求也不高：客户端仅需Java 8，服务器端仅需Java 11。

Dongting不需要使用高性能硬件，如RDMA或Optane什么的，它也不依赖Amazon或其他云服务提供商提供的任何第三方服务，如存储服务。
它甚至可以在 HDD 硬盘和树莓派上良好运行。

Dongting不需要您调整Linux内核参数以达到最佳性能（生成环境你甚至可能没有权限这样做）。

# TRY IT

## 启动服务器

首先构建项目，构建产物位于target/dongting-dist目录：
```sh
mvn clean package -DskipUTs
```

在bin目录下，运行以下命令启动服务器：
```sh
./start-dongting.sh
```
服务器将启动并监听 9331 端口（用于server内部通信，如raft复制）和 9332 端口（服务端口）。

## 运行基准测试

运行以下命令启动基准测试客户端：
```sh
./start-benchmark.sh -g 0
```

你可能需要调整参数以获得最大吞吐量，例如：
```sh
./start-benchmark.sh -g 0 --max-pending 10000 --client-count 2
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

## 运行管理工具

bin目录下的 dongting-admin 脚本是一个服务器管理工具，可用于：

* 更改raft组成员
* 转移leader
* 添加/删除组（multi-raft）
* 添加/删除集群节点
* 查询服务器状态

运行时不带参数可查看使用方法。

## 高级用法（通过代码构建 raft 服务器）

所有示例都在 `demos` 目录中。它们不需要配置，直接执行 `main` 方法即可运行。
建议在 IDE 中运行，以便更方便地设置断点和观察。
所有演示都使用 DtKV 作为 Raft 状态机，它是一个内存中的 KV 数据库。

要设置 IDE，可以参考 [开发指南](docs/developer_CN.md)。

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
