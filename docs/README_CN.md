在 0.8.x 版本中，Dongting 提供了一个通用的 Raft 框架和一个名为 `DtKV` 的内置状态机。`DtKV` 支持树形结构的 K-V 配置，适合作为分布式配置中心。当前的 `DtKV` 支持以下命令：

* get/batchGet
* put/batchPut
* remove/batchRemove
* list
* mkdir
* compareAndSet
* putTemp/makeTempDir（带 TTL 的节点）
* createLock/createAutoRenewalLock

DtKV 使用 `.` 作为 key 的分隔符。例如，您可以使用 `"dir1.key1"` 访问 `dir1` 目录下的 `key1`。要访问根目录，可以使用 null 或空字符串。但是，null 和空字符串不支持作为值。

# 客户端使用

要使用 `DtKV` 客户端，需要引入 `dongting-client.jar`（254 KB）依赖。`dongting-client.jar` 只有一个可选的 SLF4J 依赖。如果在运行时找不到 SLF4J，日志将输出到 JDK 日志。

以下是初始化 `DtKV` 客户端的简单示例：
```java
// dongting 支持多 raft，因此需要指定组 id
int groupId = 0;
KvClient kvClient = new KvClient();
kvClient.start();
// 在运行时添加节点定义，每个节点有一个唯一的正整数 id 和一个 host:servicePort 地址
kvClient.getRaftClient().clientAddNode("1,192.168.0.1:5000;2,192.168.0.2:5000;3,192.168.0.3:5000");
// 在运行时添加组定义，这里添加一个 id 为 0 的组，包含 id 为 1、2、3 的 3 个节点
kvClient.getRaftClient().clientAddOrUpdateGroup(groupId, new int[]{1,2,3});
```

请确保指定正确的端口。每个 raft 节点通常暴露两个端口：
一个是**复制端口**，用于 raft 节点之间的内部通信，如复制。`AdminRaftClient` 也连接到此端口。
另一个是**服务端口**，用于 `KvClient` 等客户端的连接。

`KvClient` 提供同步和异步接口。对于单个 `KvClient`，异步操作可以获得最大吞吐量，而同步操作需要多个线程才能达到较高吞吐量。
需要注意的是，异步操作的回调可能在 raft 线程或 IO 线程上执行。
因此，您不应在这些回调中执行任何阻塞或 CPU 密集型操作。
如果您不确定或缺乏高级技能，建议使用同步接口。

以下是使用 `KvClient` 的简单示例：
```java
// 同步 put，返回 put 操作的 raft 日志索引，在大多数情况下无用
kvClient.put(groupId, "key1".getBytes(), "value1".getBytes());
// 异步 put
kvClient.put(groupId, "key1".getBytes(), "value1".getBytes(), (raftIndex, ex) -> {
    // 执行一些既不阻塞也不消耗 CPU 的操作
});
```

有关 `KvClient` 类的详细用法，请参阅 Javadocs。

# 服务器使用

要设置 `DtKV` 服务器，需要引入 `dongting-server.jar`（500 KB）依赖。`dongting-server.jar` 只依赖 `dongting-client.jar`。

以下是初始化 `DtKV` 服务器节点 1 的简单示例：

```java
import java.util.Collections;

int groupId = 0;
RaftServerConfig serverConfig = new RaftServerConfig();
serverConfig.servers = "1,192.168.0.1:4000;2,192.168.0.2:4000;3,192.168.0.3:4000"; // 使用复制端口
serverConfig.nodeId = 1; // 节点 2、3 应将此行改为 2、3
serverConfig.replicatePort = 4000;
serverConfig.servicePort = 5000;

RaftGroupConfig groupConfig = RaftGroupConfig.newInstance(groupId, "1,2,3", "");

DefaultRaftFactory raftFactory = new DefaultRaftFactory() {
    @Override
    public StateMachine createStateMachine(RaftGroupConfigEx groupConfig) {
        return new DtKV(groupConfig, new KvConfig());
    }
};

RaftServer raftServer = new RaftServer(serverConfig, Collections.singleton(groupConfig), raftFactory);
// 注册 DtKV rpc 处理器
KvServerUtil.initKvServer(raftServer);

raftServer.start();
```

# 更多示例

更多示例请参见 `demos` 模块。
