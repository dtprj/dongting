[![Java CI with Maven](https://github.com/dtprj/dongting/actions/workflows/maven.yml/badge.svg)](https://github.com/dtprj/dongting/actions/workflows/maven.yml)
[![codecov](https://codecov.io/github/dtprj/dongting/branch/master/graph/badge.svg)](https://app.codecov.io/github/dtprj/dongting)

# Introduce
The Dongting project is a high-performance engine that integrates RAFT, configuration server, messaging queues,
and low-level RPC. Features are as follows:

* (Under testing) Multi RAFT group support. Running multiple RAFT Groups within a same process. Dynamic addition, 
  removal, and updating of RAFT Groups, allowing your cluster to scale dynamically. 
  The state machine runs in the raft framework can be customized.
* (Under testing) Tree-based distribute configuration server with linearizability named DtKV. Supports general
  K/V operations, watch, ttl, and distributed lock.
* (Under testing) Low-level RPC. Used by Donging itself.
* (Planned) MQ (message queues) with linearizability. Use RAFT log as message queue log.

## 10X Throughput
Dongting is developed using performance-oriented programming.

I conducted a simple performance test on a PC with the following specifications:

* AMD 5600X 6-core CPU, 12 threads
* 32GB DDR4 3600MHz RAM
* 1TB PCI-E 3.0 SSD, which may be the performance bottleneck in my tests.
* Windows 11
* JDK 17 with -XX:+UseZGC -Xmx4G -Xms4G

The test results are as follows:
* 1 client, 1 server or 3 servers raft group run in same machine, same process, communicate using TCP 127.0.0.1
* a simple K/V implementation using Dongting raft framework
* client send requests to server asynchronously, 2000 max pending requests
* value is 128 bytes

*sync write to storage*, means that only after the ```fsync``` call (FileChannel.force()) returns will the follower 
respond to the leader, and only then will the leader proceed with the RAFT commit.

*async write to storage* means that as soon as the data is written to the OS, the follower respond to the leader 
and leader proceed with the RAFT commit. Nevertheless, when the data write is completed, ```fsync``` will be called 
immediately without any delay, ensuring that data loss is minimized in the event of a power failure.

The test results are as follows:

|                        | 1 server                     | 3 servers in single RAFT group |
|------------------------|------------------------------|--------------------------------|
| sync write to storage  | 704,900 TPS, AVG RT 2.8 ms   | 272,540 TPS, AVG RT 7.3 ms     |
| async write to storage | 1,777,224 TPS, AVG RT 1.1 ms | 903,760 TPS, AVG RT 2.2 ms     |

## Zero Dependencies and Only 1% of the Size
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

## Try it

All the examples are in the `demos` directory. 
They require no configuration and can be run directly by executing the `main` method.
It is recommended to run them in an IDE for easier breakpoint setting and observation.
All demos use DtKV as the Raft state machine, which is an in-memory KV database.

To set up the IDE you can follow the [develop guide](docs/developer.md).

The [cluster](demos/src/main/java/com/github/dtprj/dongting/demos/cluster) directory contains an example of
running a 3-node raft cluster.
Run ```DemoServer1```, ```DemoServer2```, and ```DemoServer3``` separately, the raft cluster will typically 
be ready within one second.
Run ```DemoClient```, which will send 1 million put and get requests while recording the completion time.
Run ```PeriodPutClient```, which continuously sends a put request every second without stopping. 
You can shut down one server at any time, and ```PeriodPutClient``` will remain unaffected.
Additionally, execute ```ChangeLeader``` to switch the Raft leader to a specified node.
The running ```PeriodPutClient``` will not be affected in any way.

The [standalone](demos/src/main/java/com/github/dtprj/dongting/demos/standalone) directory contains an example
of running a single-node raft group.

The [embedded](demos/src/main/java/com/github/dtprj/dongting/demos/embedded) directory contains an example of 
embedding 3 servers and 1 client into single process.

The [configchange](demos/src/main/java/com/github/dtprj/dongting/demos/configchange) directory contains examples 
of dynamically changing Raft members at runtime.
First, run ```ConfigChangeDemoServer1```, ```ConfigChangeDemoServer2```, ```ConfigChangeDemoServer3```, 
and ```ConfigChangeDemoServer4```. By default, a Raft group with node 1, 2, and 3 will be started.
Executing ```ChangeTo234Client``` will change the Raft members to node 2, 3, and 4.
Executing ```ChangeTo123Client``` will revert the Raft members back to node 1, 2, and 3.

The [multiraft](demos/src/main/java/com/github/dtprj/dongting/demos/multiraft) directory contains examples of
running multi-raft, which is typically used for sharding or dynamic sharding.
Run ```MultiRaftDemoServer1```, ```MultiRaftDemoServer2```, and ```MultiRaftDemoServer3``` to start two (static) 
raft groups by default, with IDs 101 and 102.
Executing ```PeriodPutClient``` will send a put request every second to raft groups 101, 102, and 103.
Since group 103 does not exist, there will be two successful put operations and one failed operation per second.
Run ```AddGroup103Demo``` to add raft group 103 at runtime, after which ```PeriodPutClient``` will output 
three successful operations per second.
Executing ```RemoveGroup103Demo``` will remove raft group 103.

The [watch](demos/src/main/java/com/github/dtprj/dongting/demos/watch) directory an example of
using a client to monitor changes to a specified key, while also demonstrating how to monitor 
a directory, where the client receives notifications for any changes to its child nodes.

The [ttl](demos/src/main/java/com/github/dtprj/dongting/demos/ttl) directory contains an example of using a client to 
set a key with a TTL, after the TTL expires, the key will be deleted automatically.

The [lock](demos/src/main/java/com/github/dtprj/dongting/demos/lock) directory contains examples of using distributed 
locks. Distributed locks can be manually operated with tryLock/unlock, or can be fully automated with 
tryLock/updateLease (which can be used for leader election in business code).


## Under development

Unfortunately, the project is still under development. All current demos can run, 
but the entire project requires further internal testing and is not production-ready.
The latest version is v0.8.x-ALPHA, you can check out it by git tag.

Additionally, the following features have not yet been implemented:

* (MQ) message queues.

## About me
if you can read Chinese, you can visit my project Weibo:

https://weibo.com/dtprj

and WeChat Public Account:

![公众号](devlogs/imgs/qrcode_wechat.jpg)
