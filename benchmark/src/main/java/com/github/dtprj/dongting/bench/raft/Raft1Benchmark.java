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

import com.github.dtprj.dongting.bench.common.BenchBase;
import com.github.dtprj.dongting.bench.common.SimplePerfCallback;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.PerfCallback;
import com.github.dtprj.dongting.dtkv.KvClient;
import com.github.dtprj.dongting.dtkv.server.DtKV;
import com.github.dtprj.dongting.dtkv.server.KvServerUtil;
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

import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class Raft1Benchmark extends BenchBase {
    private static final DtLog log = DtLogs.getLogger(Raft1Benchmark.class);
    private static final String DATA_DIR = "target/raftlog";
    private static final int REPLICATE_PORT = 4000;
    private static final int SERVICE_PORT = 4001;
    private static final int NODE_ID = 1;
    private static final int GROUP_ID = 0;

    private static final boolean SYNC = false;
    private static final int DATA_LEN = 256;
    private static final byte[] DATA = new byte[DATA_LEN];
    private static final int CLIENT_MAX_OUT_REQUESTS = 2000;
    private static final boolean PERF = false;
    private static final boolean SYNC_FORCE = true;
    private static final int KEYS = 100_000;

    private RaftServer raftServer;
    private RaftGroupConfig groupConfig;
    private DefaultRaftFactory raftFactory;
    private KvClient[] client;

    public static void main(String[] args) throws Exception {
        Raft1Benchmark benchmark = new Raft1Benchmark(1, 10000, 200);
        benchmark.setLogRt(true);
        benchmark.start();
    }

    public Raft1Benchmark(int threadCount, long testTime, long warmupTime) {
        super(threadCount, testTime, warmupTime);
    }

    @Override
    public void init() throws Exception {
        new Random().nextBytes(DATA);
        RaftServerConfig serverConfig = new RaftServerConfig();
        serverConfig.setServers(NODE_ID + ",127.0.0.1:" + REPLICATE_PORT);
        serverConfig.setNodeId(NODE_ID);
        serverConfig.setReplicatePort(REPLICATE_PORT);
        serverConfig.setServicePort(SERVICE_PORT);

        groupConfig = new RaftGroupConfig(GROUP_ID, String.valueOf(NODE_ID), "");
        groupConfig.setDataDir(DATA_DIR);
        groupConfig.setSyncForce(SYNC_FORCE);
        groupConfig.setSaveSnapshotMillis(1000);

        if (PERF) {
            groupConfig.setPerfCallback(new RaftPerfCallback(true, ""));
        }

        raftFactory = new DefaultRaftFactory(serverConfig) {
            @Override
            public StateMachine createStateMachine(RaftGroupConfigEx groupConfig) {
                return new DtKV();
            }
        };
        raftFactory.start();

        raftServer = new RaftServer(serverConfig, Collections.singletonList(groupConfig), raftFactory);
        KvServerUtil.initKvServer(raftServer);
        raftServer.start();

        raftServer.getAllGroupReadyFuture().get(60, TimeUnit.SECONDS);
        log.info("raft servers started");

        client = new KvClient[threadCount];
        for (int i = 0; i < threadCount; i++) {
            NioClientConfig nioClientConfig = new NioClientConfig();
            nioClientConfig.setMaxOutRequests(CLIENT_MAX_OUT_REQUESTS / threadCount);
            KvClient c = new KvClient(nioClientConfig);
            c.start();
            RaftNode node = new RaftNode(NODE_ID, new HostPort("127.0.0.1", SERVICE_PORT));
            c.getRaftClient().addOrUpdateGroup(GROUP_ID, Collections.singletonList(node));
            client[i] = c;
        }

        //noinspection rawtypes
        CompletableFuture[] futures = new CompletableFuture[threadCount];
        for (int i = 0; i < threadCount; i++) {
            // make c find the leader
            futures[i] = client[i].get(GROUP_ID, "kkk", new DtTime(5, TimeUnit.SECONDS));
        }
        CompletableFuture.allOf(futures).get();
    }

    @Override
    protected void afterWarmup() {
        PerfCallback c = groupConfig.getPerfCallback();
        if (c instanceof SimplePerfCallback) {
            ((SimplePerfCallback) c).start();
        }
    }

    @Override
    public void shutdown() {
        DtUtil.stop(new DtTime(3, TimeUnit.SECONDS), client);
        DtUtil.stop(new DtTime(3, TimeUnit.SECONDS), raftServer, raftFactory);
        if (groupConfig.getPerfCallback() instanceof RaftPerfCallback) {
            System.out.println("----------------------- raft perf stats----------------------");
            ((RaftPerfCallback) groupConfig.getPerfCallback()).printStats();
            System.out.println("-------------------------------------------------------------");
        }
    }

    @Override
    public void test(int threadIndex, long startTime, int state) {
        try {
            int k = Integer.reverse((int) startTime);
            k = k % KEYS;
            final DtTime timeout = new DtTime(800, TimeUnit.MILLISECONDS);
            CompletableFuture<Void> f = client[threadIndex].put(GROUP_ID, String.valueOf(k), DATA, timeout);

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
