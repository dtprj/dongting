[![Java CI with Maven](https://github.com/dtprj/dongting/actions/workflows/maven.yml/badge.svg)](https://github.com/dtprj/dongting/actions/workflows/maven.yml)
[![codecov](https://codecov.io/github/dtprj/dongting/branch/master/graph/badge.svg)](https://app.codecov.io/github/dtprj/dongting)

[中文](README_CN.md)

# Introduce
The Dongting project is a high-performance engine that integrates RAFT, configuration server, messaging queues.
Features are as follows:

* Multi RAFT group support. Running multiple RAFT groups within a same process. Dynamic addition, 
  removal, and updating of RAFT Groups, allowing your cluster to scale dynamically. 
  The state machine runs in the raft framework can be customized.
* Tree-based distribute configuration server with linearizability named DtKV. Supports general
  K/V operations, watch, ttl, and distributed lock.
* (Planned) MQ (message queues) with linearizability. Use RAFT log as message queue log.

# 10X Throughput
Dongting is developed using performance-oriented programming.

In simple tests where both server and client run on the same machine, modern high-performance PCs using server default 
settings can easily achieve over 1 million TPS with the benchmark program, and RT won't be too large either.

# Zero Dependencies and Only 1% of the Size
The Dongting project is zero-dependency.

Dongting does not rely on any third-party libraries. No third-party jar files are needed.
Slf4j is optional, if it is not in the classpath, the project will use the jdk logger.

Are you still troubled by the dependency management issues that spread like wildfire?
Are you still being ridiculed for having a 1GB image size?
Dongting has only two JAR packages, the client and the server, which together are less than 1MB.
It does not have transitive dependencies either. Therefore, you can easily embed it into your application.

Dongting does not place excessive demands on your JDK; it only requires Java 8 for the client and Java 11 for
the server, that’s all.

Dongting does not require the use of high-performance hardware, such as RDMA or Optane.
It can even run well on HDD Disks and Raspberry Pis.

Dongting does not rely on any third-party services such as storage services provided by Amazon or
any other cloud service providers.

Dongting does not require you to adjust Linux kernel parameters to achieve optimal performance
(you might not even have the permission to do so).

# Try it

## run server

First build, the artifacts are under target/dongting-dist:
```sh
mvn clean package -DskipUTs
```

In the bin directory, run the following command to start the server:
```sh
./start-dongting.sh
```
The server will start and listen on port 9331(servers internal communication, e.g., raft replication)
and 9332(service port).

## run benchmark

Run the following command to start the benchmark client:
```sh
./start-benchmark.sh -g 0
```

You may need to adjust parameters to achieve maximum throughput, for example:
```sh
./start-benchmark.sh -g 0 --max-pending 10000 --client-count 2
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

## run admin tools

The dongting-admin script in bin directory is a tool for managing servers such as:

* change raft group members
* transfer leader
* add/remove group (multi raft)
* add/remove nodes to the cluster
* query server status

Run it without parameters to see the usage.

## Advanced usage (build raft server through code)

All the examples are in the `demos` directory. 
They require no configuration and can be run directly by executing the `main` method.
It is recommended to run them in an IDE for easier breakpoint setting and observation.
All demos use DtKV as the Raft state machine, which is an in-memory KV database.

To set up the IDE you can follow the [develop guide](docs/developer.md).

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
