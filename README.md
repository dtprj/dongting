[![Java CI with Maven](https://github.com/dtprj/dongting/actions/workflows/maven.yml/badge.svg)](https://github.com/dtprj/dongting/actions/workflows/maven.yml)
[![codecov](https://codecov.io/github/dtprj/dongting/branch/master/graph/badge.svg)](https://app.codecov.io/github/dtprj/dongting)

# Introduce
The Dongting project is a high-performance engine that integrates RAFT, configuration server, messaging queues,
and low-level RPC. Features are as follows:

* (Under testing) Multi RAFT group support. Running multiple RAFT Groups within a same process. Dynamic addition, 
  removal, and updating of RAFT Groups, allowing your cluster to scale dynamically. 
  The state machine runs in the raft framework can be customized.
* (Under testing) Low-level RPC. Used by Donging itself.
* (Planned) Distribute configuration server with linearizability.
* (Planned) MQ (message queues) with linearizability. Use RAFT log as message queue log.

## Zero Dependencies and Only 1% of the Size
The Dongting project is zero-dependency.

Dongting does not rely on any third-party libraries. No third-party jar files are needed. 
Slf4j is optional, if it is not in the classpath, the project will use the jdk logger.

Are you still troubled by the dependency management issues that spread like wildfire? 
Are you still being ridiculed for having a 1GB image size?
Dongting has only two JAR packages, the client and the server, which together are less than 1MB. 
It does not have transitive dependencies either.

Dongting does not place excessive demands on your JDK; it only requires Java 8 for the client and Java 11 for 
the server, that’s all.

Dongting does not require the use of high-performance hardware, such as RDMA or Optane.
It can even run well on HDD Disks and Raspberry Pis.

Dongting does not rely on any third-party services such as storage services provided by Amazon or 
any other cloud service providers.

Dongting does not require you to adjust Linux kernel parameters to achieve optimal performance 
(you might not even have the permission to do so).

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

*async write to storage* means that as soon as the data is written to the OS, all the above actions are performed.

The test results are as follows:

|                        | 1 server                     | 3 servers in single RAFT group |
|------------------------|------------------------------|--------------------------------|
| sync write to storage  | 704,900 TPS, AVG RT 2.8 ms   | 272,540 TPS, AVG RT 7.3 ms     |
| async write to storage | 1,777,224 TPS, AVG RT 1.1 ms | 903,760 TPS, AVG RT 2.2 ms     |

## Under development

Unfortunately, the Dongting project is still under development (even though I have been developing this project 
full-time for almost two years). Currently, all the features of the RAFT framework have been developed, 
but they still need to be tested. Unit tests are not completed yet, but simple demos and performance tests are available.

## About me
if you can read Chinese, you can visit my project Weibo:

https://weibo.com/dtprj

and WeChat Public Account:

![公众号](devlogs/imgs/qrcode_wechat.jpg)
