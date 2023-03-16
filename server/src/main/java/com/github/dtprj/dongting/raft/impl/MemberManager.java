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

    private Set<Integer> jointConsensusMembers;
    private Set<Integer> jointConsensusObservers;

    private final List<RaftMember> allMembers;
    private final List<RaftMember> observers;

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

        this.allMembers = raftStatus.getMembers();
        this.observers = raftStatus.getObservers();

        this.eventSource = new EventSource(executor);
    }


    public void init(IntObjMap<RaftNodeEx> allNodes) {
        for (int nodeId : nodeIdOfMembers) {
            RaftNodeEx node = allNodes.get(nodeId);
            allMembers.add(new RaftMember(node));
        }
        for (int nodeId : nodeIdOfObservers) {
            RaftNodeEx node = allNodes.get(nodeId);
            observers.add(new RaftMember(node));
        }
    }

    public void ensureRaftMemberStatus() {
        ensureRaftMemberStatus(allMembers);
        ensureRaftMemberStatus(observers);
    }

    @SuppressWarnings("ForLoopReplaceableByForEach")
    private void ensureRaftMemberStatus(List<RaftMember> list) {
        int len = list.size();
        for (int i = 0; i < len; i++) {
            RaftMember member = list.get(i);
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
        return eventSource.registerInOtherThreads(() -> getReadyCount(allMembers) >= targetReadyCount);
    }

    public boolean checkLeader(int nodeId) {
        RaftMember leader = raftStatus.getCurrentLeader();
        if (leader != null && leader.getNode().getNodeId() == nodeId) {
            return true;
        }
        return checkMember(nodeId);
    }

    public boolean checkMember(int nodeId) {
        return nodeIdOfMembers.contains(nodeId) || jointConsensusMembers != null && jointConsensusMembers.contains(nodeId);
    }

    private CompletableFuture<Void> run(Runnable runnable) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        executor.execute(() -> {
            runnable.run();
            f.complete(null);
        });
        return f;
    }

    public CompletableFuture<Void> prepareJointConsensus(Set<Integer> members, Set<Integer> observers) {
        return run(() -> {
            this.jointConsensusMembers = new HashSet<>(members);
            if (observers != null) {
                this.jointConsensusObservers = new HashSet<>(observers);
            } else {
                this.jointConsensusObservers = Collections.emptySet();
            }
        });
    }

    public CompletableFuture<Set<Integer>> dropJointConsensus() {
        CompletableFuture<Set<Integer>> f = new CompletableFuture<>();
        executor.execute(() -> {
            if (jointConsensusMembers == null || jointConsensusObservers == null) {
                f.completeExceptionally(new IllegalStateException("joint consensus not prepared"));
                return;
            }
            HashSet<Integer> ids = new HashSet<>(jointConsensusMembers);
            ids.addAll(jointConsensusObservers);

            this.jointConsensusMembers = null;
            this.jointConsensusObservers = null;
            f.complete(ids);
        });
        return f;
    }

    public CompletableFuture<Set<Integer>> commitJointConsensus() {
        CompletableFuture<Set<Integer>> f = new CompletableFuture<>();
        executor.execute(() -> {
            if (jointConsensusMembers == null || jointConsensusObservers == null) {
                f.completeExceptionally(new IllegalStateException("joint consensus not prepared"));
                return;
            }

            HashSet<Integer> ids = new HashSet<>(nodeIdOfMembers);
            ids.addAll(nodeIdOfObservers);

            this.nodeIdOfMembers = jointConsensusMembers;
            this.nodeIdOfObservers = jointConsensusObservers;
            this.jointConsensusMembers = null;
            this.jointConsensusObservers = null;
            f.complete(ids);
        });
        return f;
    }
}
