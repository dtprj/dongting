---
trigger: always_on
alwaysApply: true
---

项目注意事项说明：

1. 本项目的英文名称是dongting，中文名称应该是洞庭湖的“洞庭”，但是在文档中，应该全部使用英文名称。

2. 本项目的目标是建立一个高性能、零依赖的raft/kv/mq引擎。你应该表现出顶级专家的水平，代码应该以性能为优先考虑事项，要尽可能完美。

3. java源代码和其中的注释都使用英文，文档可以用中文。

4. 对于字段及其getter/setter，如果这个字段的getter/setter预期以后不会有额外行为，那么对外API可以生成getter/setter方法，
   内部使用的可以直接访问public或package private字段。

5. 本项目中，除去测试代码，DtChannel类只有一个子类DtChannelImpl，RaftGroupConfig只有一个子类RaftGroupConfigEx，
   RaftNode只有一个子类RaftNodeEx，RaftGroup只有一个子类RaftGroupImpl，RaftStatus只有一个子类RaftStatusImpl（等等，可能还有类似的例子）。
   这样做的目的是为了实现封装，将一些内部字段、方法隐藏起来不暴露给外部，所以，如果遇到父类的实例，但是要访问子类的方法时，
   可以直接类型转换为子类，不会出错。

6. 单元测试很多地方都依赖时间的流逝来进行测试，比如通过sleep，或者设置一个很短的超时时间然后等待请求超时等，这样的测试结果最真实。
   然而如果sleep或者超时时间设置太长，整个项目的单测运行时间就会变得很长，影响开发效率。所以在单测中很多地方往往设置的很短，但是这样在
   性能较低的电脑上（或者因为单元测试时负载较高）又容易导致测试不通过，因此com.github.dtprj.dongting.test.Tick这个类被引入进来，
   很多单测计算等待和超时时间的时候调用tick方法，将等待时间乘以一个倍数（默认是1），在单测运行时可以通过-Dtick=N来设置倍数。

7. 每个raft group运行在一个fiber group中，默认是单线程模型。DtKV可以通过useSeparateExecutor让DtKV的操作运行在另一个单独的线程中。

8. BugLog这个类的作用类似于java assert关键字，但assert会抛出Error，然后整个进程有很大的可能就完蛋了，
   而BugLog可以在不造成灾难的情况下记录更多信息。通过在日志中grep BugLog就能查找预期外的错误。

9. test目录下的dt_packet.proto/dt_kv.proto/dt_raft_server.proto文件描述了dongting的protobuf对象，它们生成的java文件只在测试中用
   （可以通过mvn clean compile test-compile来生成），main中的代码对于protobuf是自己编码和解码的（因为项目是零依赖）。 
   尽管如此，test目录中的protobuf文件描述的message和main代码中的定义是一致的。

10. client模块依赖java8，但有部分java11代码会在java11运行环境下激活，也有module-info，mvn构建时client模块会按不同的配置多次编译，
   详见client/pom.xmlclient-ex模块只在开发是存在（也是因为build相关的原因），package的时候client-ex模块会复制到client模块的包中
  （mvn需要指定-Pmerge参数）。server模块需要java11以上。client.jar和server.jar就包含完整的功能。dist模块（java11+）只是包装了启动
   脚本和配置文件。