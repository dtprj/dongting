[![Java CI with Maven](https://github.com/dtprj/dongting/actions/workflows/maven.yml/badge.svg)](https://github.com/dtprj/dongting/actions/workflows/maven.yml)
[![codecov](https://codecov.io/github/dtprj/dongting/branch/master/graph/badge.svg)](https://app.codecov.io/github/dtprj/dongting)

# dongting
a raft/mq/config/rpc engine in single jar with excellent performance

This project is under development.

dongting项目是一个高性能raft/消息队列/配置/分布式rpc四合一引擎，项目开发中，目前还不可用。项目目标如下：

## 10倍性能
相比传统项目，目标是要提供2~10倍的tps。

## 1/100的安装包大小
你还在为一传十、十传百的依赖治理烦恼吗？你还因为1GB的镜像大小受人耻笑吗？
dongting项目是0依赖，会用最高标准从新打造每一个轮子，目标是在1MB的级别实现所有核心功能，jar包大小只有传统项目的1/10至1/100。

## 100倍的启动速度
因为没有依赖，所以可以用GraalVM轻松编译为本地可执行文件，实现0.1秒启动，启动耗时低至传统项目的1/10至1/100。

## 广泛的应用
* 配置管理、消息队列、raft服务、服务发现、分布式rpc（low level）开箱可用。
* 因为只有一个精简内核，所以也可以在上面二次定制出更加复杂的平台。
* 高性能、安装包小、启动快、内存占用低，助力开发短周期运行的函数，以及在容器环境中实现快速扩缩容，也适合嵌入在各种系统和软件中。
* 提供适合其它语言调用的API，比如用C++调用Java非常少见，但由于dongting的项目特性，用C++直接使用dongting的API，可以在没有原生C++实现的情况下，提供一个堪用的实现。

## 联系我
微博：
https://weibo.com/dtprj

微信公众号：

![公众号](devlogs/imgs/qrcode_wechat.jpg)
