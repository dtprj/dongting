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
package com.github.dtprj.dongting.raft.impl;

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.net.NioServer;
import com.github.dtprj.dongting.net.NioServerConfig;
import com.github.dtprj.dongting.raft.RaftNode;
import com.github.dtprj.dongting.raft.rpc.NodePingProcessor;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class NodeManagerTest {

    static class NodeInfo {
        NodeManager nodeManager;
        NioServer server;
        NioClient client;
        RaftStatusImpl raftStatus;
    }

    private NodeInfo createManager(int nodeId, String members, String observers) {
        NodeInfo nodeInfo = new NodeInfo();
        List<RaftNode> nodes = new ArrayList<>();
        for (Integer id : RaftUtil.strToIdSet(members)) {
            nodes.add(new RaftNode(id, new HostPort("127.0.0.1", 15200 + id)));
        }
        for (Integer id : RaftUtil.strToIdSet(observers)) {
            nodes.add(new RaftNode(id, new HostPort("127.0.0.1", 15200 + id)));
        }
        nodeInfo.client = new NioClient(new NioClientConfig());
        RaftServerConfig raftServerConfig = new RaftServerConfig();
        raftServerConfig.setNodeId(nodeId);
        NioServerConfig nioServerConfig = new NioServerConfig();
        nioServerConfig.setPort(15200 + nodeId);
        nodeInfo.server = new NioServer(nioServerConfig);
        nodeInfo.nodeManager = new NodeManager(raftServerConfig, nodes, nodeInfo.client, nodes.size());
        nodeInfo.server.register(Commands.NODE_PING, new NodePingProcessor(nodeId, nodeInfo.nodeManager.getUuid()));
        nodeInfo.server.start();
        nodeInfo.client.start();
        nodeInfo.client.waitStart();

        nodeInfo.raftStatus = new RaftStatusImpl(new Timestamp());
        nodeInfo.raftStatus.setNodeIdOfMembers(RaftUtil.strToIdSet(members));
        nodeInfo.raftStatus.setNodeIdOfObservers(RaftUtil.strToIdSet(observers));

        GroupComponents gc = new GroupComponents();
        gc.setRaftStatus(nodeInfo.raftStatus);
        gc.setGroupConfig(new RaftGroupConfigEx(1, members, observers));
        ConcurrentHashMap<Integer, RaftGroupImpl> raftGroups = new ConcurrentHashMap<>();
        raftGroups.put(1, new RaftGroupImpl(gc));

        nodeInfo.nodeManager.initNodes(raftGroups);
        return nodeInfo;
    }

    private void closeManager(NodeInfo... nodeInfos) {
        for (NodeInfo nodeInfo : nodeInfos) {
            DtTime t = new DtTime(1, TimeUnit.SECONDS);
            nodeInfo.server.stop(t);
            nodeInfo.client.stop(t);
            nodeInfo.nodeManager.stop(t);
        }
    }

    @Test
    void testPing() throws Exception {
        NodeInfo n1 = createManager(1, "1,2,3", "4");
        NodeInfo n2 = createManager(2, "1,2,3", "4");
        NodeInfo n3 = createManager(3, "1,2,3", "4");
        NodeInfo n4 = createManager(4, "1,2,3", "4");
        n1.nodeManager.start();
        n2.nodeManager.start();
        n3.nodeManager.start();
        n4.nodeManager.start();
        CompletableFuture.allOf(n1.nodeManager.getNodePingReadyFuture(), n2.nodeManager.getNodePingReadyFuture(),
                        n3.nodeManager.getNodePingReadyFuture(), n4.nodeManager.getNodePingReadyFuture())
                .get(5, TimeUnit.SECONDS);
        closeManager(n1, n2, n3, n4);
    }
}
