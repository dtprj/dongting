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

import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.IntObjMap;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.raft.server.RaftNode;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.function.BiConsumer;

/**
 * @author huangli
 */
public class NodeManager extends AbstractLifeCircle implements BiConsumer<EventType, Object> {
    private static final DtLog log = DtLogs.getLogger(NodeManager.class);
    private final UUID uuid = UUID.randomUUID();
    private final int selfNodeId;
    private final NioClient client;
    private final RaftServerConfig config;
    private final RaftGroups raftGroups;

    private List<RaftNode> allRaftNodesOnlyForInit;
    private IntObjMap<RaftNodeEx> allNodesEx;

    private ScheduledFuture<?> scheduledFuture;

    private int currentReadyNodes;

    public NodeManager(RaftServerConfig config, List<RaftNode> allRaftNodes, NioClient client,
                       RaftGroups raftGroups) {
        this.selfNodeId = config.getNodeId();
        this.allRaftNodesOnlyForInit = allRaftNodes;
        this.client = client;
        this.config = config;
        this.raftGroups = raftGroups;

        raftGroups.forEach((groupId, g) -> {
            RaftStatusImpl raftStatus = g.getGroupComponents().getRaftStatus();
            for (int nodeId : raftStatus.getNodeIdOfMembers()) {
                RaftNodeEx nodeEx = allNodesEx.get(nodeId);
                nodeEx.setUseCount(nodeEx.getUseCount() + 1);
            }
            for (int nodeId : raftStatus.getNodeIdOfObservers()) {
                RaftNodeEx nodeEx = allNodesEx.get(nodeId);
                nodeEx.setUseCount(nodeEx.getUseCount() + 1);
            }
        });
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop(DtTime timeout, boolean force) {

    }

    @Override
    public void accept(EventType eventType, Object o) {
    }
}
