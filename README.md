[![Java CI with Maven](https://github.com/dtprj/dongting/actions/workflows/maven.yml/badge.svg)](https://github.com/dtprj/dongting/actions/workflows/maven.yml)
[![codecov](https://codecov.io/github/dtprj/dongting/branch/master/graph/badge.svg)](https://app.codecov.io/github/dtprj/dongting)

[中文](README_CN.md)

## Table of Contents

- [Introduce](#introduce)
- [10X Throughput](#10x-throughput)
- [Zero Dependencies](#zero-dependencies)
- [Try it](#try-it)
  - [build](#build)
  - [run server](#run-server)
  - [run benchmark](#run-benchmark)
  - [client usage](#client-usage)
  - [server configuration](#server-configuration)
- [cluster management](#cluster-management)
  - [Configure a multi-node cluster](#configure-a-multi-node-cluster)
  - [run admin tools](#run-admin-tools)
- [Advanced](#advanced)
  - [Import project to IDE](#import-project-to-ide)
  - [Build raft server through code](#build-raft-server-through-code)
- [About me](#about-me)

# Introduce
The Dongting project is a high-performance engine that integrates RAFT, configuration server, messaging queues.
Features are as follows:

* **Multi RAFT group support**: Running multiple RAFT groups within a same process. Dynamic addition,
  removal, and updating of RAFT groups (sharding), allowing your cluster to scale dynamically.
  The state machine runs in the raft framework can be customized.
* **Distributed configuration server DtKV**: Tree-based structure, supports linear consistency general
  K/V operations, watch, TTL expiration, and distributed lock, similar to etcd.
  * `DtKV` is an in-memory database, so the total data size cannot be too large, but it uses raft log as redo log, creates snapshots periodically, and will not lose a single record even in power failure.
  * Natively supports tree-like directories, the complexity of many operations is O(1).
  * Supports temporary directories, which will be automatically deleted as a whole after TTL expires. The deletion operation is atomic.
  * Does not support transactions, but provides CAS and very easy-to-use distributed locks.
* **(Planned) MQ (message queues)**: Use RAFT log as message queue log.

# 10X Throughput
Dongting is developed using performance-oriented programming.

In simple tests where both server and client run on the same machine, modern high-performance PCs using server default 
settings can easily achieve over 1 million TPS with the benchmark program, and RT won't be too large either.

# Zero Dependencies

Are you still troubled by dependency management issues that spread like wildfire? Have you ever been ridiculed for a 1GB image size? You have been saved now!

Dongting has no dependencies. It does not rely on any third-party libraries, no third-party jar files are needed.
Dongting core has only two JAR packages, client and server together are less than 1MB.
It also has no transitive dependencies, so you can easily embed it into your application.

SLF4J is optional. If it is not in the classpath, the project will use JDK logging.
Dongting does not place high demands on your JDK: client only requires Java 8, server only requires Java 11.

Dongting does not require using high-performance hardware, such as RDMA or Optane, and it does not rely on any third-party services such as storage services provided by Amazon or other cloud service providers.
It can even run well on HDD disks and Raspberry Pi.

Dongting does not require you to adjust Linux kernel parameters to achieve optimal performance (in production environments you may not even have permission to do so).

# Try it

## build

First build, the artifacts are under target/dongting-dist:
```sh
mvn clean package -DskipUTs
```

The directory structure after build is as follows:
```
dongting-dist/
├── bin/                      # Scripts directory
│   ├── benchmark.sh          # Benchmark script
│   ├── benchmark.bat         # Benchmark script (Windows)
│   ├── start-dongting.sh     # Start server script (Linux/Mac)
│   ├── start-dongting.bat    # Start server script (Windows)
│   ├── stop-dongting.sh      # Stop server script (Linux/Mac)
│   ├── stop-dongting.bat     # Stop server script (Windows)
│   ├── dongting-admin.sh     # Admin tool script (Linux/Mac)
│   └── dongting-admin.bat    # Admin tool script (Windows)
├── lib/                      # JAR packages directory
│   ├── dongting-client-x.y.z-SNAPSHOT.jar
│   ├── dongting-server-x.y.z-SNAPSHOT.jar
│   ├── dongting-dist-x.y.z-SNAPSHOT.jar # Bootstrap jar, not required if building raft server through code
│   ├── slf4j-api-x.y.z.jar
│   ├── logback-x.y.z.jar
│   └── logback-x.y.z.jar
├── conf/                     # Configuration files directory
│   ├── config.properties
│   ├── servers.properties
│   ├── client.properties
│   ├── logback-server.xml
│   ├── logback-admin.xml
│   └── logback-benchmark.xml
├── docs/                     # Documentation directory
└── logs/                     # Logs directory
    ├── dongting-server.log   # Server log (generated at runtime)
    └── dongting-stats.log    # Stats log (generated at runtime)
```

## run server

In the bin directory, run the following command to start the server:
```sh
./start-dongting.sh
```
The server will start and listen on port 9331(servers internal communication, e.g., raft replication)
and 9332(service port).
By default, a DtKV instance with groupId 0 will be started.

Run the following command to stop the server:
```sh
./stop-dongting.sh
```

## run benchmark

Run the following command to start the benchmark client:
```sh
./benchmark.sh -g 0
```

You may need to adjust parameters to achieve maximum throughput, for example:
```sh
./benchmark.sh -g 0 --max-pending 10000 --client-count 2
```

Try Java 21 virtual threads (need Java 21)
```sh
./benchmark.sh -g 0 --sync --thread-count 4000
```

See my results (AMD 9700X 6C12T, ZhiTai TiPro 9000 running in PCI-E 4.0 mode):
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

## client usage

The built-in DtKV in Dongting supports the following features:

* get/batchGet
* put/batchPut
* remove/batchRemove
* list
* mkdir
* compareAndSet
* putTemp/makeTempDir (node with ttl)
* createLock/createAutoRenewalLock

`DtKV` uses `.` as a separator for keys. For example, you can access `key1` under the `dir1` directory
using `"dir1.key1"`. The value cannot be null or empty string.

To use the `DtKV` client, you need to include the `dongting-client.jar` (300+KB), with no other dependencies.

The following is a simple example of initializing the `DtKV` client:
```java
// dongting supports multi-raft, so we need to specify the group id, default group id is 0
int groupId = 0;
KvClient kvClient = new KvClient();
kvClient.start();
// add node definition at runtime, each node has a unique positive integer id and a host:servicePort address
kvClient.getRaftClient().clientAddNode("1,127.0.0.1:9332");
// kvClient.getRaftClient().clientAddNode("1,192.168.0.1:9332;2,192.168.0.2:9332;3,192.168.0.3:9332");
// add group definition at runtime, here we add a group with groupId 0 and 3 nodes with ids 1, 2, and 3
kvClient.getRaftClient().clientAddOrUpdateGroup(groupId, new int[]{1,2,3});
```

Make sure to specify the correct port. Each raft node exposes two ports:
One is the **replicate port**, default 9331, which is used for internal communication such as replication between raft nodes.
The `AdminRaftClient` also connects to this port.
The other is the **service port**, default 9332, which is used for connections from clients like `KvClient`.

`KvClient` provides synchronous and asynchronous interfaces. For a single `KvClient`, asynchronous operations
achieve maximum throughput, while synchronous operations require many threads (or virtual threads) to reach higher throughput.
It is important to note that callbacks for asynchronous operations may be executed on raft thread or IO threads.
Therefore, you should never perform any blocking or CPU-intensive operations within these callbacks.
If you are unsure or lack advanced skills, it is strongly recommended to use the synchronous interface.

Here is a simple example of using the `KvClient`:
```java
// sync put
kvClient.put(groupId, "key1".getBytes(), "value1".getBytes());
kvClient.put(groupId, "key1".getBytes(), "value1".getBytes(), (raftIndex, ex) -> {
    // do something that neither blocks nor consumes CPU.
});
```

For detailed usage of the `KvClient` class, please refer to the Javadocs.

## server configuration

Server configuration mainly consists of two configuration files: `config.properties` and `servers.properties`.

### config.properties

This file configures basic parameters of the Raft server:

- **nodeId**: Each server must have a unique positive integer node ID, starting from 1. If there is only one node, it is usually set to 1.
- **replicatePort**: Port used for server internal communication, e.g., Raft replication (default value: 9331)
- **servicePort**: Port used for client-server communication (default value: 9332)
- **electTimeout**: Raft election timeout in milliseconds (default value: 15000)
- **heartbeatInterval**: Raft heartbeat interval in milliseconds (default value: 2000)
- **blockIoThreads**: Number of threads for handling blocking IO. If not set, the default value is calculated at runtime based on CPU cores.

### Raft group common configuration (optional)

The following configurations in `config.properties` affect the behavior of Raft group:

- **dataDir**: Data directory (default value points to the data directory under dongting-dist)
- **syncForce**: If true, any operation will be persisted (fsync) to disk before responding to leader and computing quorum. This option has a significant impact on performance (default value: true)
- **saveSnapshotSeconds**: Interval in seconds to save snapshots (default value: 3600)
- **maxKeepSnapshots**: Maximum number of snapshots to keep (default value: 2)
- **saveSnapshotWhenClose**: Whether to save snapshot when closing (usually when the server is shutting down) (default value: true)
- **deleteLogsAfterTakeSnapshot**: Whether to delete unnecessary raft log files after creating a snapshot (default value: true)

### servers.properties

This file configures cluster topology and Raft group:

- **servers**: List of nodes, format is `nodeId,ip:replicatePort`. Multiple nodes are separated by semicolons. Examples:
  ```properties
  # Single node example
  servers = 1,127.0.0.1:9331

  # Multi-node example
  servers = 1,192.168.0.1:9331;2,192.168.0.2:9331;3,192.168.0.3:9331

  # Local multi-node example (using different ports for isolation)
  servers = 1,127.0.0.1:4001;2,127.0.0.1:4002;3,127.0.0.1:4003
  ```

- **Raft group member configuration**: Format is `group.<groupId>.nodeIdOfMembers = nodeId1,nodeId2,...`. Node IDs must be defined in the `servers` property.
  ```properties
  group.0.nodeIdOfMembers = 1,2,3
  ```

- **Raft group observer configuration**: Format is `group.<groupId>.nodeIdOfObservers = nodeId1,nodeId2,...`. Observers will not participate in leader election.
  ```properties
  group.0.nodeIdOfObservers = 4
  ```

Observers will receive data replication from the leader but will not participate in raft voting.

### client.properties

The client.properties file is used by benchmark. Note that the configured servers parameter connects to the **service port**.

# cluster management

## Configure a multi-node cluster

By default, running the start-dongting script directly will start a single-node cluster listening on local ports 9331 and 9332.
If you need to start a multi-node cluster, taking a 3-node cluster as an example, you need to do the following work:

1. Prepare 3 copies of the dongting-dist directory. For your own testing, you can also use the same machine, but you need to modify the ports.
2. Modify the nodeId in each config.properties file. Node ID starts from 1, and different nodes must be different. **Note that once the server is started, the nodeId cannot be modified (unless you clear the data in the data directory)**.
3. (Optional) If testing on the same machine, modify the ports in each config.properties file.
4. Modify the servers parameter in the servers.properties file. For 3 nodes, it might be `1,192.168.0.1:9331;2,192.168.0.2:9331;3,192.168.0.3:9331` or `1,127.0.0.1:4001;2,127.0.0.1:4002;3,127.0.0.1:4003`, depending on your IP and port configuration.
5. Modify the `group.0.nodeIdOfMembers` parameter in the servers.properties file. For 3 nodes, it might be `1,2,3`.

The above 2 and 3 are different for each node, while 4 and 5 are the same for all nodes.

If you want to run a benchmark, you also need to modify the servers parameter in the client.properties file, connecting to the **service port** (default 9332).

After configuration, you can start them separately. You can kill a process at will to see the cluster's performance.


## run admin tools

The dongting-admin script in bin directory is a tool for managing servers such as:

* change raft group member/observer
* transfer leader
* add/remove group (multi raft)
* add/remove nodes to the cluster
* query server status

Run it without parameters to see the usage.

You can use the `AdminRaftClient` class to perform all management functions. Remember to connect to the replicate port.


# Advanced

## Import project to IDE

To set up the IDE you can follow the [develop guide](docs/developer.md).

## Build raft server through code

All the examples are in the `demos` directory.
They require no configuration and can be run directly by executing the `main` method.
It is recommended to run them in an IDE for easier breakpoint setting and observation.


The [cluster](demos/src/main/java/com/github/dtprj/dongting/demos/advanced/cluster) directory contains an example of
running a 3-node raft cluster.
Run ```DemoServer1```, ```DemoServer2```, and ```DemoServer3``` separately, the raft cluster will typically 
be ready within one second.
Run ```DemoClient```, which will send 1 million put and get requests while recording the completion time.
Run ```PeriodPutClient```, which continuously sends a put request every second without stopping. 
You can shut down one server at any time, and ```PeriodPutClient``` will remain unaffected.
Additionally, execute ```ChangeLeader``` to switch the Raft leader to a specified node.
The running ```PeriodPutClient``` will not be affected in any way.

The [standalone](demos/src/main/java/com/github/dtprj/dongting/demos/advanced/standalone) directory contains an example
of running a single-node raft group.

The [embedded](demos/src/main/java/com/github/dtprj/dongting/demos/advanced/embedded) directory contains an example of 
embedding 3 servers and 1 client into single process.

The [configchange](demos/src/main/java/com/github/dtprj/dongting/demos/advanced/configchange) directory contains examples 
of dynamically changing Raft members at runtime.
First, run ```ConfigChangeDemoServer1```, ```ConfigChangeDemoServer2```, ```ConfigChangeDemoServer3```, 
and ```ConfigChangeDemoServer4```. By default, a Raft group with node 1, 2, and 3 will be started.
Executing ```ChangeTo234Client``` will change the Raft members to node 2, 3, and 4.
Executing ```ChangeTo123Client``` will revert the Raft members back to node 1, 2, and 3.

The [multiraft](demos/src/main/java/com/github/dtprj/dongting/demos/advanced/multiraft) directory contains examples of
running multi-raft, which is typically used for sharding or dynamic sharding.
Run ```MultiRaftDemoServer1```, ```MultiRaftDemoServer2```, and ```MultiRaftDemoServer3``` to start two (static) 
raft groups by default, with IDs 101 and 102.
Executing ```PeriodPutClient``` will send a put request every second to raft groups 101, 102, and 103.
Since group 103 does not exist, there will be two successful put operations and one failed operation per second.
Run ```AddGroup103Demo``` to add raft group 103 at runtime, after which ```PeriodPutClient``` will output 
three successful operations per second.
Executing ```RemoveGroup103Demo``` will remove raft group 103.

The [watch](demos/src/main/java/com/github/dtprj/dongting/demos/advanced/watch) directory an example of
using a client to monitor changes to a specified key, while also demonstrating how to monitor 
a directory, where the client receives notifications for any changes to its child nodes.

The [ttl](demos/src/main/java/com/github/dtprj/dongting/demos/advanced/ttl) directory contains an example of using a client to 
set a key with a TTL, after the TTL expires, the key will be deleted automatically.

The [lock](demos/src/main/java/com/github/dtprj/dongting/demos/advanced/lock) directory contains examples of using distributed 
locks. Distributed locks can be manually operated with tryLock/unlock, or can be fully automated with 
tryLock/updateLease (which can be used for leader election in business code).

# About me

https://weibo.com/dtprj

The WeChat Official Account:

![公众号](docs/imgs/qrcode_wechat.jpg)
