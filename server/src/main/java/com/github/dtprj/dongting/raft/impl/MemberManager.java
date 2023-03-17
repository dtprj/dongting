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
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.PeerStatus;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.raft.rpc.RaftPingFrameCallback;
import com.github.dtprj.dongting.raft.rpc.RaftPingProcessor;
import com.github.dtprj.dongting.raft.rpc.RaftPingWriteFrame;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class MemberManager {
    private static final DtLog log = DtLogs.getLogger(MemberManager.class);
    private final RaftServerConfig serverConfig;
    private final RaftStatus raftStatus;
    private final int groupId;
    private final NioClient client;
    private final RaftExecutor executor;

    private Set<Integer> nodeIdOfMembers;
    private Set<Integer> nodeIdOfObservers;

    private final List<RaftMember> members;
    private List<RaftMember> observers;
    private final List<RaftMember> replicateList;

    private Set<Integer> nodeIdOfJointConsensusMembers = Collections.emptySet();
    private final List<RaftMember> jointConsensusMembers;
    private List<RaftMember> jointConsensusObservers = Collections.emptyList();

    private final EventSource eventSource;

    public MemberManager(RaftServerConfig serverConfig, NioClient client, RaftExecutor executor,
                         RaftStatus raftStatus, int groupId, Set<Integer> nodeIdOfMembers,
                         Set<Integer> nodeIdOfObservers) {
        this.serverConfig = serverConfig;
        this.client = client;
        this.executor = executor;
        this.raftStatus = raftStatus;
        this.groupId = groupId;
        this.nodeIdOfMembers = nodeIdOfMembers;
        this.nodeIdOfObservers = Objects.requireNonNullElse(nodeIdOfObservers, Collections.emptySet());

        this.members = raftStatus.getMembers();
        this.jointConsensusMembers = raftStatus.getJointConsensusMembers();
        this.replicateList = raftStatus.getReplicateList();

        this.eventSource = new EventSource(executor);
    }

    public void init(IntObjMap<RaftNodeEx> allNodes) {
        for (int nodeId : nodeIdOfMembers) {
            RaftMember m = new RaftMember(allNodes.get(nodeId));
            members.add(m);
        }
        if (nodeIdOfObservers.size() > 0) {
            observers = new ArrayList<>();
            for (int nodeId : nodeIdOfObservers) {
                RaftMember m = new RaftMember(allNodes.get(nodeId));
                observers.add(m);
            }
        }
        computeDuplicatedData();
    }

    private void computeDuplicatedData(){
        replicateList.clear();
        Set<Integer> memberIds = new HashSet<>();
        Set<Integer> observerIds = new HashSet<>();
        Set<Integer> jointMemberIds = new HashSet<>();
        for (RaftMember m : members) {
            replicateList.add(m);
            memberIds.add(m.getNode().getNodeId());
        }
        for (RaftMember m : observers) {
            replicateList.add(m);
            observerIds.add(m.getNode().getNodeId());
        }
        for (RaftMember m : jointConsensusMembers) {
            replicateList.add(m);
            jointMemberIds.add(m.getNode().getNodeId());
        }
        this.nodeIdOfMembers = memberIds;
        this.nodeIdOfObservers = observerIds.size() == 0 ? Collections.emptySet() : observerIds;
        this.nodeIdOfJointConsensusMembers = jointMemberIds.size() == 0 ? Collections.emptySet() : jointMemberIds;
    }

    @SuppressWarnings("ForLoopReplaceableByForEach")
    public void ensureRaftMemberStatus() {
        List<RaftMember> replicateList = this.replicateList;
        int len = replicateList.size();
        for (int i = 0; i < len; i++) {
            RaftMember member = replicateList.get(i);
            check(member);
        }
    }

    private void check(RaftMember member) {
        RaftNodeEx node = member.getNode();
        NodeStatus nodeStatus = node.getStatus();
        if (!nodeStatus.isReady()) {
            setReady(member, false);
        } else if (nodeStatus.getEpoch() != member.getEpoch()) {
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
        RaftPingWriteFrame f = new RaftPingWriteFrame(groupId, serverConfig.getNodeId(), nodeIdOfMembers, nodeIdOfObservers);
        client.sendRequest(raftNodeEx.getPeer(), f, RaftPingProcessor.DECODER, timeout)
                .whenCompleteAsync((rf, ex) -> processPingResult(raftNodeEx, member, rf, ex, nodeEpochWhenStartPing), executor);
    }

    private void processPingResult(RaftNodeEx raftNodeEx, RaftMember member,
                                   ReadFrame rf, Throwable ex, int nodeEpochWhenStartPing) {
        RaftPingFrameCallback callback = (RaftPingFrameCallback) rf.getBody();
        executor.schedule(() -> member.setPinging(false), 1000);
        if (ex != null) {
            log.warn("raft ping fail, remote={}", raftNodeEx.getHostPort(), ex);
            setReady(member, false);
        } else {
            if (callback.nodeId == 0 && callback.groupId == 0) {
                log.error("raft ping error, group not found, groupId={}, remote={}",
                        groupId, raftNodeEx.getHostPort());
                setReady(member, false);
            } else if (checkMembers(callback)) {
                NodeStatus currentNodeStatus = member.getNode().getStatus();
                if (currentNodeStatus.isReady() && nodeEpochWhenStartPing == currentNodeStatus.getEpoch()) {
                    log.info("raft ping success, id={}, remote={}", callback.nodeId, raftNodeEx.getHostPort());
                    setReady(member, true);
                    member.setEpoch(nodeEpochWhenStartPing);
                } else {
                    log.warn("raft ping success but current node status not match. "
                                    + "id={}, remoteHost={}, nodeReady={}, nodeEpoch={}, pingEpoch={}",
                            callback.nodeId, raftNodeEx.getHostPort(), currentNodeStatus.isReady(),
                            currentNodeStatus.getEpoch(), nodeEpochWhenStartPing);
                    setReady(member, false);
                }
            } else {
                log.error("raft ping error, group ids not match: remote={}, localIds={}, remoteIds={}, localObservers={}, remoteObservers={}",
                        raftNodeEx, nodeIdOfMembers, callback.nodeIdOfMembers, nodeIdOfObservers, callback.nodeIdOfObservers);
                setReady(member, false);
            }
        }
    }

    private boolean checkMembers(RaftPingFrameCallback callback) {
        return eq(nodeIdOfMembers, callback.nodeIdOfMembers) && eq(nodeIdOfObservers, callback.nodeIdOfObservers);
    }

    private boolean eq(Collection<?> c1, Collection<?> c2) {
        if (c1 == null) {
            return c2 == null;
        }
        return c1.equals(c2);
    }

    public void setReady(RaftMember member, boolean ready) {
        if (ready == member.isReady()) {
            return;
        }
        member.setReady(ready);
        eventSource.fireInExecutorThread();
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

    public Set<Integer> getNodeIdOfMembers() {
        return nodeIdOfMembers;
    }

    public Set<Integer> getNodeIdOfObservers() {
        return nodeIdOfObservers;
    }

    public CompletableFuture<Void> createReadyFuture(int targetReadyCount) {
        return eventSource.registerInOtherThreads(() -> getReadyCount(members) >= targetReadyCount);
    }

    public boolean checkLeader(int nodeId) {
        RaftMember leader = raftStatus.getCurrentLeader();
        if (leader != null && leader.getNode().getNodeId() == nodeId) {
            return true;
        }
        return checkMember(nodeId);
    }

    public boolean checkMember(int nodeId) {
        return nodeIdOfMembers.contains(nodeId) || nodeIdOfJointConsensusMembers != null && nodeIdOfJointConsensusMembers.contains(nodeId);
    }

    public CompletableFuture<Void> prepareJointConsensus(List<RaftNodeEx> newMemberNodes, List<RaftNodeEx> newObserverNodes) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        executor.execute(() -> {
            if (nodeIdOfJointConsensusMembers.size() != 0 || jointConsensusObservers.size() != 0 || jointConsensusMembers.size() != 0) {
                f.completeExceptionally(new IllegalStateException("joint consensus is prepared"));
                return;
            }

            IntObjMap<RaftMember> currentNodes = new IntObjMap<>();
            for (RaftMember m : members) {
                currentNodes.put(m.getNode().getNodeId(), m);
            }
            for (RaftMember m : observers) {
                currentNodes.put(m.getNode().getNodeId(), m);
            }

            List<RaftMember> newMembers = new ArrayList<>();
            List<RaftMember> newObservers = new ArrayList<>();
            for (RaftNodeEx node : newMemberNodes) {
                RaftMember m = currentNodes.get(node.getNodeId());
                newMembers.add(Objects.requireNonNullElseGet(m, () -> new RaftMember(node)));
            }
            for (RaftNodeEx node : newObserverNodes) {
                RaftMember m = currentNodes.get(node.getNodeId());
                newObservers.add(Objects.requireNonNullElseGet(m, () -> new RaftMember(node)));
            }
            this.jointConsensusMembers.addAll(newMembers);
            this.jointConsensusObservers = newObservers;

            computeDuplicatedData();

            f.complete(null);
        });
        return f;
    }

    public CompletableFuture<Set<Integer>> dropJointConsensus() {
        CompletableFuture<Set<Integer>> f = new CompletableFuture<>();
        executor.execute(() -> {
            if (nodeIdOfJointConsensusMembers.size() == 0 || jointConsensusObservers.size() == 0 || jointConsensusMembers.size() == 0) {
                f.completeExceptionally(new IllegalStateException("joint consensus not prepared"));
                return;
            }
            HashSet<Integer> ids = new HashSet<>(nodeIdOfJointConsensusMembers);
            for (RaftMember m : jointConsensusObservers) {
                ids.add(m.getNode().getNodeId());
            }

            this.jointConsensusMembers.clear();
            this.jointConsensusObservers = Collections.emptyList();
            computeDuplicatedData();
            f.complete(ids);
        });
        return f;
    }

    public CompletableFuture<Set<Integer>> commitJointConsensus() {
        CompletableFuture<Set<Integer>> f = new CompletableFuture<>();
        executor.execute(() -> {
            if (nodeIdOfJointConsensusMembers.size() == 0 || jointConsensusObservers.size() == 0 || jointConsensusMembers.size() == 0) {
                f.completeExceptionally(new IllegalStateException("joint consensus not prepared"));
                return;
            }

            HashSet<Integer> ids = new HashSet<>(nodeIdOfMembers);
            ids.addAll(nodeIdOfObservers);

            this.members.clear();
            this.members.addAll(jointConsensusMembers);

            this.observers.clear();
            this.observers.addAll(jointConsensusObservers);

            this.jointConsensusMembers.clear();
            this.jointConsensusObservers = Collections.emptyList();
            computeDuplicatedData();
            f.complete(ids);
        });
        return f;
    }
}
