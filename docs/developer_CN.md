# JDK要求

Dongting 项目为用户输出两个 jar 包：dongting-client.jar 需要 Java 8，dongting-server.jar 需要 Java 11。
但是，编译整个项目需要 Java 17，因为基准测试模块需要 Java 17。

部署模块dongting-dist要求Java11。

# 构建和测试

单元测试
```
mvn clean test -Dtick=5
```

构建打包，输出在根目录的target下
```
mvn clean package -DskipUTs
```

注意本项目要跳过单元测试是-DskipUTs，而不是-DskipTests。这是因为有的时候我想仅运行某个集成测试，
-DskipTests 会跳过所有测试包括集成测试，我没有办法搞定，就把它改了。

如果你想要安装maven artifact：
```sh
mvn clean install -Dmaven.test.skip=true
```

# IDE 设置

虽然 mvn 命令行构建可以成功，但将整个项目导入 IDE 可能会导致编译错误。

首先，测试代码依赖于 protobuf 编译器生成的一些文件。
运行以下命令生成源文件（你用上面的命令运行过测试或者打包也行）：
```
mvn clean compile test-compile
```

然后将插件生成的源文件路径添加到 IDE 的 *sources* 中（IntelliJ IDEA 可能会自动设置）。如下图所示：
![IDEA 配置](imgs/IDEA.png)

其次，client 模块在 pom.xml 中定义了多个编译配置（支持从 Java 8 到 Java 25），但 IDE 可能无法很好地处理。在 IntelliJ IDEA 中按如下方式设置（--add-exports/--add-reads 应在 maven 同步项目后自动添加）：

![IDEA 配置](imgs/IDEA2.webp)

现在您可以在 IDE 中构建和运行项目了。

# 在 IDE 中运行演示

demos 模块包含一些使用 `dongting` 的示例，它们不需要配置，可以直接通过 `main` 方法运行。
