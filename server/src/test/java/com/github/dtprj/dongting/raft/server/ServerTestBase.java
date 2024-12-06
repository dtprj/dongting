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
package com.github.dtprj.dongting.raft.server;

import com.github.dtprj.dongting.buf.DefaultPoolFactory;
import com.github.dtprj.dongting.dtkv.server.DtKV;
import com.github.dtprj.dongting.dtkv.server.KvConfig;
import com.github.dtprj.dongting.dtkv.server.KvServerUtil;
import com.github.dtprj.dongting.fiber.Dispatcher;
import com.github.dtprj.dongting.raft.impl.GroupComponents;
import com.github.dtprj.dongting.raft.impl.ImplAccessor;
import com.github.dtprj.dongting.raft.impl.RaftGroupImpl;
import com.github.dtprj.dongting.raft.sm.StateMachine;
import com.github.dtprj.dongting.raft.store.TestDir;
import com.github.dtprj.dongting.raft.test.MockExecutors;

import java.util.Collections;
import java.util.concurrent.ExecutorService;

import static com.github.dtprj.dongting.util.Tick.tick;

/**
 * @author huangli
 */
public class ServerTestBase {

    protected static final String DATA_DIR = TestDir.testDir("raftlog");

    protected static class ServerInfo {
        public RaftServer raftServer;
        public int nodeId;
        public RaftGroupImpl group;
        public GroupComponents gc;
    }

    protected ServerInfo createServer(int nodeId, String servers, String nodeIdOfMembers, String nodeIdOfObservers) {
        int replicatePort = 4000 + nodeId;
        int servicePort = 5000 + nodeId;
        int groupId = 1;
        RaftServerConfig serverConfig = new RaftServerConfig();
        serverConfig.setServers(servers);
        serverConfig.setNodeId(nodeId);
        serverConfig.setReplicatePort(replicatePort);
        serverConfig.setServicePort(servicePort);
        serverConfig.setElectTimeout(tick(10));
        serverConfig.setHeartbeatInterval(tick(4));

        RaftGroupConfig groupConfig = RaftGroupConfig.newInstance(groupId, nodeIdOfMembers, nodeIdOfObservers);
        groupConfig.setDataDir(DATA_DIR + "-" + nodeId);

        DefaultRaftFactory raftFactory = createRaftFactory(nodeId);

        RaftServer raftServer = new RaftServer(serverConfig, Collections.singletonList(groupConfig), raftFactory);
        KvServerUtil.initKvServer(raftServer);

        RaftGroupImpl g = (RaftGroupImpl) raftServer.getRaftGroup(groupId);
        GroupComponents gc = g.getGroupComponents();
        ImplAccessor.updateNodeManager(gc.getNodeManager());
        ImplAccessor.updateMemberManager(gc.getMemberManager());
        ImplAccessor.updateVoteManager(gc.getVoteManager());

        raftServer.start();

        ServerInfo serverInfo = new ServerInfo();
        serverInfo.raftServer = raftServer;
        serverInfo.nodeId = nodeId;
        serverInfo.group = g;
        serverInfo.gc = gc;

        return serverInfo;
    }

    private DefaultRaftFactory createRaftFactory(int nodeId) {
        return new DefaultRaftFactory() {
            @Override
            public StateMachine createStateMachine(RaftGroupConfigEx groupConfig) {
                return new DtKV(groupConfig, new KvConfig());
            }

            @Override
            public Dispatcher createDispatcher(RaftGroupConfig groupConfig) {
                // we start multi nodes in same jvm, so use node id as part of dispatcher name
                return new Dispatcher("node-" + nodeId + "-dispatcher", new DefaultPoolFactory(),
                        groupConfig.getPerfCallback());
            }

            @Override
            public ExecutorService createBlockIoExecutor(RaftServerConfig serverConfig) {
                return MockExecutors.ioExecutor();
            }
        };
    }
}
