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
import com.github.dtprj.dongting.dtkv.server.DtKV;
import com.github.dtprj.dongting.raft.server.DefaultRaftFactory;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftServer;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.sm.StateMachine;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class Raft1Benchmark extends BenchBase {
    private RaftServer raftServer;
    private DefaultRaftFactory raftFactory;
    private static final String DATA_DIR = "target/raftlog";
    private static final int REPLICATE_PORT = 4000;
    private static final int SERVICE_PORT = 4001;
    private static final int NODE_ID = 1;

    public static void main(String[] args) throws Exception {
        Raft1Benchmark benchmark = new Raft1Benchmark(1, 1000, 200);
        benchmark.start();
    }

    public Raft1Benchmark(int threadCount, long testTime, long warmupTime) {
        super(threadCount, testTime, warmupTime);
    }

    @Override
    public void init() throws Exception {
        RaftServerConfig serverConfig = new RaftServerConfig();
        serverConfig.setServers(NODE_ID + ",127.0.0.1:" + REPLICATE_PORT);
        serverConfig.setNodeId(NODE_ID);
        serverConfig.setReplicatePort(REPLICATE_PORT);
        serverConfig.setServicePort(SERVICE_PORT);

        RaftGroupConfig groupConfig = new RaftGroupConfig(0, String.valueOf(NODE_ID), "");
        groupConfig.setDataDir(DATA_DIR);

        raftFactory = new DefaultRaftFactory(serverConfig) {
            @Override
            public StateMachine createStateMachine(RaftGroupConfigEx groupConfig) {
                return new DtKV(groupConfig);
            }
        };
        raftFactory.start();

        raftServer = new RaftServer(serverConfig, Collections.singletonList(groupConfig), raftFactory);
        raftServer.start();
        raftServer.getReadyFuture().get();
    }

    @Override
    public void shutdown() {
        raftServer.stop(new DtTime(3, TimeUnit.SECONDS));
        raftFactory.stop(new DtTime(3, TimeUnit.SECONDS));
    }

    @Override
    public void test(int threadIndex, long startTime, int state) {
        success(state);
    }


}
