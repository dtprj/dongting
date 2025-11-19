In the current version (0.8.x), Dongting provides a general Raft framework and a built-in state machine 
called `DtKV`. `DtKV` supports a tree-structured K-V configuration, making it suitable as a distributed 
configuration center. The current `DtKV` supports the following commands:

* get/batchGet
* put/batchPut
* remove/batchRemove
* list
* mkdir
* compareAndSet
* putTemp/makeTempDir (node with ttl)
* createLock/createAutoRenewalLock

DtKV uses `.` as a separator for keys. For example, you can access `key1` under the `dir1` directory
using `"dir1.key1"`. To access the root directory, you can use null or an empty string.
However, null and empty strings are not supported as values.

# client usage

To use the `DtKV` client, you need to include the `dongting-client.jar` (254 KB) dependency. The 
`dongting-client.jar` only has an optional dependency on SLF4J. If SLF4J is not found at runtime, 
logs will be output to the JDK logger.

The following is a simple example of initializing the `DtKV` client:
```java
// dongting supports multi-raft, so we need to specify the group id
int groupId = 0;
KvClient kvClient = new KvClient();
kvClient.start();
// add node definition at runtime, each node has a unique positive integer id and a host:servicePort address
kvClient.getRaftClient().clientAddNode("1,192.168.0.1:5000;2,192.168.0.2:5000;3,192.168.0.3:5000");
// add group definition at runtime, here we add a group with id 0 and 3 nodes with ids 1, 2, and 3
kvClient.getRaftClient().clientAddOrUpdateGroup(groupId, new int[]{1,2,3});
```

Make sure to specify the correct port. Each raft node typically exposes two ports:
one is called the **replicate port**, which is used for internal communication such as replication between raft nodes.
The `AdminRaftClient` also connects to this port.
The other port is called the **service port**, which is used for connections from clients like `KvClient`.

`KvClient` provides synchronous and asynchronous interfaces. For a single `KvClient`, asynchronous operations 
achieve maximum throughput, while synchronous operations require many threads to reach higher throughput.
It is important to note that callbacks for asynchronous operations may be executed on raft thread or IO threads.
Therefore, you should never perform any blocking or CPU-intensive operations within these callbacks.
If you are unsure or lack advanced skills, it is recommended to use the synchronous interface instead.

Here is a simple example of using the `KvClient`:
```java
// sync put, return raft log index of the put operation, it is useless in most cases.
kvClient.put(groupId, "key1".getBytes(), "value1".getBytes());
// async put
kvClient.put(groupId, "key1".getBytes(), "value1".getBytes(), (raftIndex, ex) -> {
    // do something that neither blocks nor consumes CPU.
});
```

For detailed usage of the `KvClient` class, please refer to the Javadocs.

# server usage

To set up a `DtKV` server, you need to include the `dongting-server.jar` (500 KB) dependency. The `dongting-server.jar`
only depends on `dongting-client.jar`.

The following is a simple example of initializing the `DtKV` server node 1:

```java
import java.util.Collections;

int groupId = 0;
RaftServerConfig serverConfig = new RaftServerConfig();
serverConfig.servers = "1,192.168.0.1:4000;2,192.168.0.2:4000;3,192.168.0.3:4000"; // use replicate port
serverConfig.nodeId = 1; // node 2, 3 should change this line to 2, 3
serverConfig.replicatePort = 4000;
serverConfig.servicePort = 5000;

RaftGroupConfig groupConfig = RaftGroupConfig.newInstance(groupId, "1,2,3", "");

DefaultRaftFactory raftFactory = new DefaultRaftFactory() {
    @Override
    public StateMachine createStateMachine(RaftGroupConfigEx groupConfig) {
        return new DtKV(groupConfig, new KvConfig());
    }
};

RaftServer raftServer = new RaftServer(serverConfig, Collections.singleton(groupConfig), raftFactory);
// register DtKV rpc processor
KvServerUtil.initKvServer(raftServer);

raftServer.start();
```

# more demos

See `demos` module for more demos.













