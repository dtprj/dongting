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
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.net.NioServer;
import com.github.dtprj.dongting.net.NioServerConfig;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.RaftNode;
import com.github.dtprj.dongting.raft.rpc.NodePingProcessor;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.github.dtprj.dongting.raft.test.TestUtil.waitUtil;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

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
        List<RaftNode> nodes = new ArrayList<>();
        for (Integer id : RaftUtil.strToIdSet(members)) {
            nodes.add(new RaftNode(id, new HostPort("127.0.0.1", 15200 + id)));
        }
        for (Integer id : RaftUtil.strToIdSet(observers)) {
            nodes.add(new RaftNode(id, new HostPort("127.0.0.1", 15200 + id)));
        }
        NioClient client = new NioClient(new NioClientConfig());
        RaftServerConfig raftServerConfig = new RaftServerConfig();
        raftServerConfig.setNodeId(nodeId);
        NioServerConfig nioServerConfig = new NioServerConfig();
        nioServerConfig.setPort(15200 + nodeId);
        NioServer server = new NioServer(nioServerConfig);
        NodeManager nodeManager = new NodeManager(raftServerConfig, nodes, client, nodes.size());
        nodeManager.pingIntervalMillis = 1;
        server.register(Commands.NODE_PING, new NodePingProcessor(nodeId, nodeManager));
        server.start();
        client.start();
        client.waitStart();

        RaftStatusImpl raftStatus = new RaftStatusImpl(new Timestamp());
        raftStatus.nodeIdOfMembers = RaftUtil.strToIdSet(members);
        raftStatus.nodeIdOfObservers = RaftUtil.strToIdSet(observers);

        GroupComponents gc = new GroupComponents();
        gc.setRaftStatus(raftStatus);
        gc.setGroupConfig(new RaftGroupConfigEx(1, members, observers));
        ConcurrentHashMap<Integer, RaftGroupImpl> raftGroups = new ConcurrentHashMap<>();
        raftGroups.put(1, new RaftGroupImpl(gc));

        nodeManager.initNodes(raftGroups);

        NodeInfo nodeInfo = new NodeInfo();
        nodeInfo.nodeManager = nodeManager;
        nodeInfo.server = server;
        nodeInfo.client = client;
        nodeInfo.raftStatus = raftStatus;
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
        // init and test ping
        NodeInfo n1 = createManager(1, "1,2", "3");
        NodeInfo n2 = createManager(2, "1,2", "3");
        n1.nodeManager.start();
        n2.nodeManager.start();
        NodeInfo n3 = createManager(3, "1,2", "3");
        n3.nodeManager.start();
        CompletableFuture.allOf(n1.nodeManager.getNodePingReadyFuture(),
                        n2.nodeManager.getNodePingReadyFuture(),
                        n3.nodeManager.getNodePingReadyFuture())
                .get(5, TimeUnit.SECONDS);
        closeManager(n1);
        waitUtil(() -> n2.nodeManager.currentReadyNodes == 2, DtUtil.SCHEDULED_SERVICE);
        n1 = createManager(1, "1,2", "3");
        n1.nodeManager.start();
        n1.nodeManager.getNodePingReadyFuture().get(2, TimeUnit.SECONDS);
        waitUtil(() -> n2.nodeManager.currentReadyNodes == 3, DtUtil.SCHEDULED_SERVICE);

        // test add and remove node
        CompletableFuture<Void> f = n1.nodeManager.removeNode(1);
        try {
            f.get();
        } catch (ExecutionException e) {
            assertInstanceOf(RaftException.class, e.getCause());
        }
        n1.nodeManager.addNode(new RaftNode(9, new HostPort("127.0.0.1", 15209))).get();
        n1.nodeManager.removeNode(9).get();
        n1.nodeManager.removeNode(10000).get();

        NodeInfo n4 = createManager(4, "1,2,3", "4");
        n4.nodeManager.start();

        n2.nodeManager.addNode(new RaftNode(4, new HostPort("127.0.0.1", 15204))).get();
        n3.nodeManager.addNode(new RaftNode(4, new HostPort("127.0.0.1", 15204))).get();
        waitUtil(() -> n2.nodeManager.currentReadyNodes == 4, DtUtil.SCHEDULED_SERVICE);
        waitUtil(() -> n3.nodeManager.currentReadyNodes == 4, DtUtil.SCHEDULED_SERVICE);

        NodeInfo finalN1 = n1;
        waitUtil(() -> finalN1.nodeManager.currentReadyNodes == 3, DtUtil.SCHEDULED_SERVICE);
        waitUtil(() -> n2.nodeManager.currentReadyNodes == 4, DtUtil.SCHEDULED_SERVICE);
        waitUtil(() -> n3.nodeManager.currentReadyNodes == 4, DtUtil.SCHEDULED_SERVICE);
        waitUtil(() -> n4.nodeManager.currentReadyNodes == 3, DtUtil.SCHEDULED_SERVICE);

        closeManager(n1, n2, n3, n4);
    }
}
