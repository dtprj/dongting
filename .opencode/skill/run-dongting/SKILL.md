---
name: run
description: Used to start or stop the project's server in development mode via scripts, or run DtAdmin for test cluster O&M operations, or run benchmarks.
---

# Build and Basic Instructions
After executing `mvn clean package -DskipUTs`, the target directory under the project root will contain the dongting-dist directory, which includes:

- bin: Various startup and shutdown scripts, each providing bat/ps1/sh formats; examples below use sh format
- lib: Dependency JAR directory
- conf: Configuration file directory
- logs: Log file directory
- data: Data directory, generated at runtime

# bin

- start-dongting.sh: Starts the dongting (DtKV) server; after execution, the Java process runs in the background. Entry point is the Bootstrap class.
- stop-dongting.sh: Stops the dongting (DtKV) server; after execution, the process exits
- benchmark.sh: Runs the benchmark; after execution, the process exits. Running without parameters prints usage. Entry point is the DtBenchmark class.
- dongting-admin.sh: Runs the O&M tool; after execution, the process exits. Running without parameters prints usage. Entry point is the DtAdmin class.

After build completes, if start-dongting.sh is run without any modifications, it will start a single-node cluster by default, listening on port 9331 (replicatePort) and 9332 (servicePort), with nodeId 1, containing a DtKV server with groupId 0.

All scripts except stop-dongting start a Java process. The JDK can be specified by setting JAVA_HOME environment variable.

# conf

- config.properties: Configuration file for dongting (DtKV) server, specifying nodeId, replicatePort, servicePort, dataDir, etc.
- client.properties: Configuration file for benchmark, specifying servers list; benchmark needs to connect to servicePort
- servers.properties: Configuration file for dongting (DtKV) server, also read by dongting-admin, specifying servers list (replicatePort), and defining groups and their configurations
- logback-server.xml: Log configuration for dongting (DtKV) server
- logback-admin.xml: Log configuration for dongting-admin
- logback-benchmark.xml: Log configuration for benchmark

# log

- dongting-server.log: Logback log for Java process started by dongting-server.sh
- start.log: Console log for Java process started by start-dongting.sh
- dongting-admin.log: Logback log for Java process started by dongting-admin.sh
- dongting-benchmark.log: Logback log for Java process started by benchmark.sh

