# 运行单元测试

```
mvn clean test -Dtick=5
```

# 构建和 IDE 设置

Dongting 项目为用户输出两个 jar 包：dongting-client.jar 需要 Java 8，dongting-server.jar 需要 Java 11。
但是，编译整个项目需要 Java 17，因为基准测试模块需要 Java 17。

虽然 mvn 命令行构建可以成功，但将整个项目导入 IDE 可能会导致编译错误。

首先，测试代码依赖于 protobuf 编译器生成的一些文件。
运行以下命令生成源文件：
```
mvn clean compile test-compile
```

然后将插件生成的源文件路径添加到 IDE 的 *sources* 中（IntelliJ IDEA 可能会自动设置）。如下图所示：
![IDEA 配置](imgs/IDEA.png)

其次，client 模块在 pom.xml 中定义了多个编译配置（支持从 Java 8 到 Java 25），但 IDE 可能无法很好地处理。在 IntelliJ IDEA 中按如下方式设置（--add-exports/--add-reads 应在 maven 同步项目后自动添加）：

![IDEA 配置](imgs/IDEA2.webp)

现在您可以在 IDE 中构建和运行项目了。

# 在 IDE 中运行演示

demos 目录包含一些使用 `dongting` 的示例，它们不需要配置，可以直接通过 `main` 方法运行。

# 运行简单基准测试

运行 main 方法 RaftBenchmark 即可。

源代码中有一些可以设置的参数，请参见注释。
