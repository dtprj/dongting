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
package com.github.dtprj.dongting.demos.base;

import com.github.dtprj.dongting.dtkv.server.DtKV;
import com.github.dtprj.dongting.dtkv.server.KvConfig;
import com.github.dtprj.dongting.dtkv.server.KvServerUtil;
import com.github.dtprj.dongting.raft.server.DefaultRaftFactory;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftServer;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.sm.StateMachine;

import java.util.ArrayList;
import java.util.List;

/**
 * @author huangli
 */
public abstract class DemoKvServer {

    public static RaftServer startServer(int nodeId, String servers, String members,
                                            String observers, int[] groupIds) {
        RaftServerConfig serverConfig = new RaftServerConfig();
        serverConfig.setServers(servers);
        serverConfig.setNodeId(nodeId);
        // internal use for raft log replication (server to server), and admin commands
        serverConfig.setReplicatePort(4000 + nodeId);
        serverConfig.setServicePort(5000 + nodeId); // use for client access
        // since it is demo, use little timeout values to make election faster
        serverConfig.setElectTimeout(3000);
        serverConfig.setHeartbeatInterval(1000);

        // multi raft group support
        List<RaftGroupConfig> groupConfigs = new ArrayList<>();
        for (int groupId : groupIds) {
            groupConfigs.add(raftConfig(nodeId, groupId, members, observers));
        }

        DefaultRaftFactory raftFactory = new DefaultRaftFactory() {
            @Override
            public StateMachine createStateMachine(RaftGroupConfigEx groupConfig) {
                // the state machine can be customized, here use DtKV, a simple key-value store
                return new DtKV(groupConfig, new KvConfig());
            }

            // called when add group at runtime
            @Override
            public RaftGroupConfig createConfig(int groupId, String nodeIdOfMembers, String nodeIdOfObservers) {
                return raftConfig(nodeId, groupId, members, observers);
            }
        };

        RaftServer raftServer = new RaftServer(serverConfig, groupConfigs, raftFactory);
        // register DtKV rpc processor
        KvServerUtil.initKvServer(raftServer);

        raftServer.start();
        return raftServer;
    }

    private static RaftGroupConfig raftConfig(int nodeId, int groupId, String members, String observers) {
        RaftGroupConfig groupConfig = RaftGroupConfig.newInstance(groupId, members, observers);
        groupConfig.setDataDir("target/raft_data_group" + groupId + "_node" + nodeId);
        return groupConfig;
    }
}
