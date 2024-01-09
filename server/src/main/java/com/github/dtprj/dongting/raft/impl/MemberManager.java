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

import com.github.dtprj.dongting.common.IntObjMap;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

/**
 * @author huangli
 */
public class MemberManager {
    private static final DtLog log = DtLogs.getLogger(MemberManager.class);
    private final RaftServerConfig serverConfig;
    private final RaftStatusImpl raftStatus;
    private final int groupId;
    private final NioClient client;

    private final EventBus eventBus;

    public MemberManager(RaftServerConfig serverConfig, NioClient client,
                         RaftStatusImpl raftStatus, EventBus eventBus) {
        this.serverConfig = serverConfig;
        this.client = client;
        this.raftStatus = raftStatus;
        this.groupId = raftStatus.getGroupId();

        this.eventBus = eventBus;
    }

    public void init(IntObjMap<RaftNodeEx> allNodes) {
        for (int nodeId : raftStatus.getNodeIdOfMembers()) {
            RaftNodeEx node = allNodes.get(nodeId);
            RaftMember m = new RaftMember(node);
            if (node.isSelf()) {
                initSelf(node, m, RaftRole.follower);
            }
            raftStatus.getMembers().add(m);
        }
        if (raftStatus.getNodeIdOfObservers().size() > 0) {
            List<RaftMember> observers = new ArrayList<>();
            for (int nodeId : raftStatus.getNodeIdOfObservers()) {
                RaftNodeEx node = allNodes.get(nodeId);
                RaftMember m = new RaftMember(node);
                if (node.isSelf()) {
                    initSelf(node, m, RaftRole.observer);
                }
                observers.add(m);
            }
            raftStatus.setObservers(observers);
        } else {
            raftStatus.setObservers(emptyList());
        }
        raftStatus.setPreparedMembers(emptyList());
        computeDuplicatedData(raftStatus);
        if (!raftStatus.getNodeIdOfMembers().contains(serverConfig.getNodeId())
                && !raftStatus.getNodeIdOfObservers().contains(serverConfig.getNodeId())) {
            raftStatus.setSelf(null);
            raftStatus.setRole(RaftRole.observer);
        }
    }

    private void initSelf(RaftNodeEx node, RaftMember m, RaftRole role) {
        m.setReady(true);
        m.setNodeEpoch(node.getStatus().getEpoch());
        raftStatus.setSelf(m);
        raftStatus.setRole(role);
    }

    static void computeDuplicatedData(RaftStatusImpl raftStatus) {
        ArrayList<RaftMember> replicateList = new ArrayList<>();
        Set<Integer> memberIds = new HashSet<>();
        Set<Integer> observerIds = new HashSet<>();
        Set<Integer> jointMemberIds = new HashSet<>();
        Set<Integer> jointObserverIds = new HashSet<>();
        for (RaftMember m : raftStatus.getMembers()) {
            replicateList.add(m);
            memberIds.add(m.getNode().getNodeId());
        }
        for (RaftMember m : raftStatus.getObservers()) {
            replicateList.add(m);
            observerIds.add(m.getNode().getNodeId());
        }
        for (RaftMember m : raftStatus.getPreparedMembers()) {
            replicateList.add(m);
            jointMemberIds.add(m.getNode().getNodeId());
        }
        for (RaftMember m : raftStatus.getPreparedObservers()) {
            jointObserverIds.add(m.getNode().getNodeId());
        }
        raftStatus.setReplicateList(replicateList.size() == 0 ? emptyList() : replicateList);
        raftStatus.setNodeIdOfMembers(memberIds.size() == 0 ? emptySet() : memberIds);
        raftStatus.setNodeIdOfObservers(observerIds.size() == 0 ? emptySet() : observerIds);
        raftStatus.setNodeIdOfPreparedMembers(jointMemberIds.size() == 0 ? emptySet() : jointMemberIds);
        raftStatus.setNodeIdOfPreparedObservers(jointObserverIds.size() == 0 ? emptySet() : jointObserverIds);

        raftStatus.setElectQuorum(RaftUtil.getElectQuorum(raftStatus.getMembers().size()));
        raftStatus.setRwQuorum(RaftUtil.getRwQuorum(raftStatus.getMembers().size()));
    }

}
