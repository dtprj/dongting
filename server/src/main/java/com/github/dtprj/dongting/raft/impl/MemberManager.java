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
import com.github.dtprj.dongting.common.IntObjMap;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.PeerStatus;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.raft.rpc.RaftPingFrameCallback;
import com.github.dtprj.dongting.raft.rpc.RaftPingProcessor;
import com.github.dtprj.dongting.raft.rpc.RaftPingWriteFrame;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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
    private final RaftGroupConfigEx groupConfig;
    private final NioClient client;

    private final EventBus eventBus;
    private final ReplicateManager replicateManager;

    private final CompletableFuture<Void> startReadyFuture;
    private final int startReadyQuorum;

    public MemberManager(RaftServerConfig serverConfig, RaftGroupConfigEx groupConfig, NioClient client,
                         RaftStatusImpl raftStatus, EventBus eventBus, ReplicateManager replicateManager) {
        this.serverConfig = serverConfig;
        this.groupConfig = groupConfig;
        this.client = client;
        this.raftStatus = raftStatus;
        this.groupId = raftStatus.getGroupId();
        this.eventBus = eventBus;
        this.replicateManager = replicateManager;
        if (raftStatus.getMembers().size() == 0) {
            this.startReadyFuture = CompletableFuture.completedFuture(null);
            this.startReadyQuorum = 0;
        } else {
            this.startReadyQuorum = RaftUtil.getElectQuorum(raftStatus.getMembers().size());
            this.startReadyFuture = new CompletableFuture<>();
        }
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

    public Fiber createRaftPingFiber() {
        FiberFrame<Void> fiberFrame = new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                ensureRaftMemberStatus();
                replicateManager.tryStartReplicateFibers();
                return Fiber.sleep(1000, this);
            }
        };
        // daemon fiber
        return new Fiber("raftPing", groupConfig.getFiberGroup(), fiberFrame, true);
    }

    public void ensureRaftMemberStatus() {
        List<RaftMember> replicateList = raftStatus.getReplicateList();
        for (RaftMember member : replicateList) {
            check(member);
        }
    }

    private void check(RaftMember member) {
        RaftNodeEx node = member.getNode();
        NodeStatus nodeStatus = node.getStatus();
        if (!nodeStatus.isReady()) {
            setReady(member, false);
        } else if (nodeStatus.getEpoch() != member.getNodeEpoch()) {
            setReady(member, false);
            if (!member.isPinging()) {
                raftPing(node, member, nodeStatus.getEpoch());
            }
        }
    }

    private void raftPing(RaftNodeEx raftNodeEx, RaftMember member, int nodeEpochWhenStartPing) {
        if (raftNodeEx.getPeer().getStatus() != PeerStatus.connected) {
            setReady(member, false);
            return;
        }

        member.setPinging(true);
        DtTime timeout = new DtTime(serverConfig.getRpcTimeout(), TimeUnit.MILLISECONDS);
        RaftPingWriteFrame f = new RaftPingWriteFrame(groupId, serverConfig.getNodeId(),
                raftStatus.getNodeIdOfMembers(), raftStatus.getNodeIdOfObservers());
        client.sendRequest(raftNodeEx.getPeer(), f, RaftPingProcessor.DECODER, timeout)
                .whenCompleteAsync((rf, ex) -> processPingResult(raftNodeEx, member, rf, ex, nodeEpochWhenStartPing),
                        groupConfig.getFiberGroup().getDispatcher().getExecutor());
    }

    private void processPingResult(RaftNodeEx raftNodeEx, RaftMember member,
                                   ReadFrame<RaftPingFrameCallback> rf, Throwable ex, int nodeEpochWhenStartPing) {
        member.setPinging(false);
        RaftPingFrameCallback callback = rf.getBody();
        if (ex != null) {
            log.warn("raft ping fail, remote={}", raftNodeEx.getHostPort(), ex);
            setReady(member, false);
        } else {
            if (callback.nodeId != raftNodeEx.getNodeId() || callback.groupId != groupId) {
                log.error("raft ping error, groupId or nodeId not found, groupId={}, remote={}",
                        groupId, raftNodeEx.getHostPort());
                setReady(member, false);
            } else if (checkRemoteConfig(callback)) {
                NodeStatus currentNodeStatus = member.getNode().getStatus();
                if (currentNodeStatus.isReady() && nodeEpochWhenStartPing == currentNodeStatus.getEpoch()) {
                    log.info("raft ping success, id={}, remote={}", callback.nodeId, raftNodeEx.getHostPort());
                    setReady(member, true);
                    member.setNodeEpoch(nodeEpochWhenStartPing);
                } else {
                    log.warn("raft ping success but current node status not match. "
                                    + "id={}, remoteHost={}, nodeReady={}, nodeEpoch={}, pingEpoch={}",
                            callback.nodeId, raftNodeEx.getHostPort(), currentNodeStatus.isReady(),
                            currentNodeStatus.getEpoch(), nodeEpochWhenStartPing);
                    setReady(member, false);
                }
            } else {
                log.error("raft ping error, group ids not match: remote={}, localIds={}, remoteIds={}, localObservers={}, remoteObservers={}",
                        raftNodeEx, raftStatus.getNodeIdOfMembers(), callback.nodeIdOfMembers, raftStatus.getNodeIdOfObservers(), callback.nodeIdOfObservers);
                setReady(member, false);
            }
        }
    }

    private boolean checkRemoteConfig(RaftPingFrameCallback callback) {
        if (serverConfig.isStaticConfig()) {
            return raftStatus.getNodeIdOfMembers().equals(callback.nodeIdOfMembers)
                    && raftStatus.getNodeIdOfObservers().equals(callback.nodeIdOfObservers);
        }
        return true;
    }

    public void setReady(RaftMember member, boolean ready) {
        if (ready && !startReadyFuture.isDone()) {
            int readyCount = getReadyCount(raftStatus.getMembers());
            if (readyCount >= startReadyQuorum) {
                log.info("member manager is ready: groupId={}", groupId);
                startReadyFuture.complete(null);
            }
        }
        member.setReady(ready);
    }

    private int getReadyCount(List<RaftMember> list) {
        int count = 0;
        for (RaftMember m : list) {
            if (m.isReady()) {
                count++;
            }
        }
        return count;
    }

    public CompletableFuture<Void> getStartReadyFuture() {
        return startReadyFuture;
    }
}
