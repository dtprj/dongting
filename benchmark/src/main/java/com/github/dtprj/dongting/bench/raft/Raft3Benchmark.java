/*
 * Copyright The Dongting Project
 *
 * The Dongting Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.github.dtprj.dongting.bench.raft;

import com.github.dtprj.dongting.bench.BenchBase;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.dtkv.KvClient;
import com.github.dtprj.dongting.dtkv.server.DtKV;
import com.github.dtprj.dongting.dtkv.server.KvServerUtil;
import com.github.dtprj.dongting.fiber.Dispatcher;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.raft.RaftNode;
import com.github.dtprj.dongting.raft.server.DefaultRaftFactory;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftServer;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.sm.StateMachine;

import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class Raft3Benchmark extends BenchBase {
    private static final DtLog log = DtLogs.getLogger(Raft3Benchmark.class);
    private static final String DATA_DIR = "target/raftlog";
    private static final int GROUP_ID = 0;

    private static final boolean SYNC = false;
    private static final int DATA_LEN = 128;
    private static final byte[] DATA = new byte[DATA_LEN];

    private final RaftServer[] raftServers = new RaftServer[3];
    private final DefaultRaftFactory[] raftFactories = new DefaultRaftFactory[3];
    private KvClient client;

    public static void main(String[] args) throws Exception {
        Raft3Benchmark benchmark = new Raft3Benchmark(1, 1000, 200);
        benchmark.start();
    }

    public Raft3Benchmark(int threadCount, long testTime, long warmupTime) {
        super(threadCount, testTime, warmupTime);
    }

    private Pair<RaftServer, DefaultRaftFactory> createServer(int nodeId, int replicatePort, int servicePort,
                                                              String servers, String nodeIdOfMembers) {
        RaftServerConfig serverConfig = new RaftServerConfig();
        serverConfig.setServers(servers);
        serverConfig.setNodeId(nodeId);
        serverConfig.setReplicatePort(replicatePort);
        serverConfig.setServicePort(servicePort);

        RaftGroupConfig groupConfig = new RaftGroupConfig(GROUP_ID, nodeIdOfMembers, "");
        groupConfig.setDataDir(DATA_DIR + "-" + nodeId);
        groupConfig.setSyncForce(true);

        DefaultRaftFactory raftFactory = new DefaultRaftFactory(serverConfig) {
            @Override
            public StateMachine createStateMachine(RaftGroupConfigEx groupConfig) {
                return new DtKV(groupConfig);
            }

            @Override
            public FiberGroup createFiberGroup(RaftGroupConfig groupConfig) {
                // we start 3 node in same jvm, so use node id as part of dispatcher name
                Dispatcher dispatcher = new Dispatcher("node-" + nodeId);
                dispatcher.start();
                return new FiberGroup("group-" + GROUP_ID + "-node-" + nodeId, dispatcher);
            }
        };
        raftFactory.start();

        RaftServer raftServer = new RaftServer(serverConfig, Collections.singletonList(groupConfig), raftFactory);
        KvServerUtil.initKvServer(raftServer);
        raftServer.start();
        return new Pair<>(raftServer, raftFactory);
    }

    @Override
    public void init() throws Exception {
        new Random().nextBytes(DATA);
        String serversStr = "1,127.0.0.1:4001;2,127.0.0.1:4002;3,127.0.0.1:4003";
        String memberIds = "1,2,3";
        Pair<RaftServer, DefaultRaftFactory> p1 = createServer(1, 4001, 5001,
                serversStr, memberIds);
        Pair<RaftServer, DefaultRaftFactory> p2 = createServer(2, 4002, 5002,
                serversStr, memberIds);
        Pair<RaftServer, DefaultRaftFactory> p3 = createServer(3, 4003, 5003,
                serversStr, memberIds);
        raftServers[0] = p1.getLeft();
        raftServers[1] = p2.getLeft();
        raftServers[2] = p3.getLeft();
        raftFactories[0] = p1.getRight();
        raftFactories[1] = p2.getRight();
        raftFactories[2] = p3.getRight();

        for (RaftServer s : raftServers) {
            s.getReadyFuture().get();
        }

        log.info("raft servers started");

        // wait election
        LOOP: while (true) {
            for (RaftServer s : raftServers) {
                if (s.getRaftGroup(GROUP_ID).isLeader()) {
                    break LOOP;
                }
            }
            //noinspection BusyWait
            Thread.sleep(10);
        }

        log.info("begin init raft client");

        client = new KvClient(new NioClientConfig());
        client.start();
        RaftNode n1 = new RaftNode(1, new HostPort("127.0.0.1", 5001));
        RaftNode n2 = new RaftNode(2, new HostPort("127.0.0.1", 5002));
        RaftNode n3 = new RaftNode(3, new HostPort("127.0.0.1", 5003));
        client.getRaftClient().addOrUpdateGroup(GROUP_ID, Arrays.asList(n1, n2, n3));

        // make client find the leader
        client.get(GROUP_ID, "kkk", new DtTime(5, TimeUnit.SECONDS)).get();
    }

    @Override
    public void shutdown() {
        DtTime timeout = new DtTime(3, TimeUnit.SECONDS);
        DtUtil.stop(timeout, client);
        DtUtil.stop(timeout, raftServers);
        DtUtil.stop(timeout, raftFactories);
    }

    @Override
    public void test(int threadIndex, long startTime, int state) {
        try {
            final DtTime timeout = new DtTime(800, TimeUnit.MILLISECONDS);
            CompletableFuture<Void> f = client.put(GROUP_ID, "key1", DATA, timeout);

            if (SYNC) {
                f.get();
                success(state);
            } else {
                f.whenComplete((result, ex) -> {
                    logRt(startTime, state);
                    if (ex != null) {
                        fail(state);
                    } else {
                        success(state);
                    }
                });
            }
        } catch (Exception e) {
            fail(state);
        } finally {
            if (SYNC) {
                logRt(startTime, state);
            }
        }
    }


}
