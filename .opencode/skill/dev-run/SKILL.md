---
name: dev-run
description: 用于在开发中以脚本方式启动或停止本项目的server，或运行DtAdmin进行测试集群进行运维操作，或运行benchmark。
---

# 构建和基本说明
执行`mvn clean package -DskipUTs -DskipITs`后，在项目根目录的target下会包含dongting-dist目录，其下包含：

- bin：各种启动、停止脚本，每个脚本提供bat/ps1/sh三种格式，下面仅以sh格式为例
- lib：依赖jar包目录
- conf：配置文件目录
- logs：日志文件目录
- data：数据目录，运行时生成

# bin

- start-dongting.sh，启动dongting（DtKV） server，运行完毕后java进程在后台运行。入口为Bootstrap类。
- stop-dongting.sh，停止dongting（DtKV） server，执行完毕后进程退出
- benchmark.sh，运行benchmark，执行完毕后进程退出，不带任何参数运行会打印usage。入口为DtBenchmark类。
- dongting-admin.sh，运行运维工具，执行完毕后进程退出，不带任何参数运行会打印usage。入口为DtAdmin类。

build完成以后，如果不做任何修改直接运行start-dongting.sh，默认会启动一个单节点集群，监听9331（replicatePort）
和9332（servicePort）端口，nodeId为1，里面包含一个groupId为0的DtKV server。

以上脚本除了stop-dongting，都是要启动java进程的，可以通过设置JAVA_HOME来指定jdk。

# conf

- config.properties：dongting（DtKV） server的配置文件，指定nodeId、replicatePort、servicePort、dataDir等
- client.properties：benchmark用的配置文件，指定servers列表，benchmark需要连接servicePort
- servers.properties：dongting（DtKV） server的配置文件,dongting-admin也读它，指定servers列表（replicatePort），同时还定义了group及其配置
- logback-server.xml：dongting（DtKV） server的日志配置
- logback-admin.xml：dongting-admin的日志配置
- logback-benchmark.xml：benchmark的日志配置

# log

- dongting-server.log：dongting-server.sh启动的Java进程的logback日志
- start.log：start-dongting.sh启动的Java进程控制台日志
- dongting-admin.log：dongting-admin.sh启动的Java进程的logback日志
- dongting-benchmark.log：benchmark.sh启动的Java进程的logback日志

