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
import com.github.dtprj.dongting.raft.rpc.TransferLeaderReq;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.NotLeaderException;
import com.github.dtprj.dongting.raft.server.RaftException;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftOutput;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
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
    private final NioClient client;
    private final RaftExecutor executor;

    private final FutureEventSource futureEventSource;
    private final EventBus eventBus;

    public MemberManager(RaftServerConfig serverConfig, NioClient client, RaftExecutor executor,
                         RaftStatusImpl raftStatus, EventBus eventBus) {
        this.serverConfig = serverConfig;
        this.client = client;
        this.executor = executor;
        this.raftStatus = raftStatus;
        this.groupId = raftStatus.getGroupId();

        this.futureEventSource = new FutureEventSource(executor);
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

    @SuppressWarnings("ForLoopReplaceableByForEach")
    public void ensureRaftMemberStatus() {
        List<RaftMember> replicateList = raftStatus.getReplicateList();
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
                .whenCompleteAsync((rf, ex) -> processPingResult(raftNodeEx, member, rf, ex, nodeEpochWhenStartPing), executor);
    }

    private void processPingResult(RaftNodeEx raftNodeEx, RaftMember member,
                                   ReadFrame<RaftPingFrameCallback> rf, Throwable ex, int nodeEpochWhenStartPing) {
        RaftPingFrameCallback callback = rf.getBody();
        executor.schedule(() -> member.setPinging(false), 1000);
        if (ex != null) {
            log.warn("raft ping fail, remote={}", raftNodeEx.getHostPort(), ex);
            setReady(member, false);
        } else {
            if (callback.nodeId == 0 && callback.groupId == 0) {
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
        return false;
    }

    public void setReady(RaftMember member, boolean ready) {
        if (ready == member.isReady()) {
            return;
        }
        member.setReady(ready);
        futureEventSource.fireInExecutorThread();
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

    public CompletableFuture<Void> createReadyFuture(int targetReadyCount) {
        return futureEventSource.registerInOtherThreads(() -> {
            if (raftStatus.getMembers().size() == 0) {
                return true;
            }
            return getReadyCount(raftStatus.getMembers()) >= targetReadyCount;
        });
    }

    public boolean checkLeader(int nodeId) {
        RaftMember leader = raftStatus.getCurrentLeader();
        if (leader != null && leader.getNode().getNodeId() == nodeId) {
            return true;
        }
        return validCandidate(raftStatus, nodeId);
    }

    public static boolean validCandidate(RaftStatusImpl raftStatus, int nodeId) {
        return raftStatus.getNodeIdOfMembers().contains(nodeId)
                || raftStatus.getNodeIdOfPreparedMembers().contains(nodeId);
    }

    public void transferLeadership(int nodeId, CompletableFuture<Void> f, DtTime deadline) {
        Runnable r = () -> {
            if (raftStatus.getRole() != RaftRole.leader) {
                raftStatus.setHoldRequest(false);
                f.completeExceptionally(new NotLeaderException(raftStatus.getCurrentLeaderNode()));
                return;
            }
            RaftMember newLeader = null;
            for (RaftMember m : raftStatus.getMembers()) {
                if (m.getNode().getNodeId() == nodeId) {
                    newLeader = m;
                    break;
                }
            }
            if (newLeader == null) {
                for (RaftMember m : raftStatus.getPreparedMembers()) {
                    if (m.getNode().getNodeId() == nodeId) {
                        newLeader = m;
                        break;
                    }
                }
            }
            if (newLeader == null) {
                raftStatus.setHoldRequest(false);
                f.completeExceptionally(new RaftException("nodeId not found: " + nodeId));
                return;
            }
            if (deadline.isTimeout()) {
                raftStatus.setHoldRequest(false);
                f.completeExceptionally(new RaftException("transfer leader timeout"));
                return;
            }
            if (f.isCancelled()) {
                raftStatus.setHoldRequest(false);
                return;
            }

            boolean lastLogCommit = raftStatus.getCommitIndex() == raftStatus.getLastLogIndex();
            boolean newLeaderHasLastLog = newLeader.getMatchIndex() == raftStatus.getLastLogIndex();

            if (newLeader.isReady() && lastLogCommit && newLeaderHasLastLog) {
                raftStatus.setHoldRequest(false);
                RaftUtil.changeToFollower(raftStatus, newLeader.getNode().getNodeId());
                TransferLeaderReq req = new TransferLeaderReq();
                req.term = raftStatus.getCurrentTerm();
                req.logIndex = raftStatus.getLastLogIndex();
                req.oldLeaderId = serverConfig.getNodeId();
                req.groupId = groupId;
                TransferLeaderReq.TransferLeaderReqWriteFrame frame = new TransferLeaderReq.TransferLeaderReqWriteFrame(req);
                client.sendRequest(newLeader.getNode().getPeer(), frame,
                        null, new DtTime(5, TimeUnit.SECONDS))
                        .whenComplete((rf, ex) -> {
                            if (ex != null) {
                                log.error("transfer leader failed, groupId={}", groupId, ex);
                                f.completeExceptionally(ex);
                            } else {
                                log.info("transfer leader success, groupId={}", groupId);
                                f.complete(null);
                            }
                        });
            } else {
                // retry
                transferLeadership(nodeId, f, deadline);
            }
        };
        executor.schedule(r, 3);
    }

    private void leaderConfigChange(int type, ByteBuffer data, CompletableFuture<Void> f) {
        if (raftStatus.getRole() != RaftRole.leader) {
            String stageStr;
            switch (type) {
                case LogItem.TYPE_PREPARE_CONFIG_CHANGE:
                    stageStr = "prepare";
                    break;
                case LogItem.TYPE_COMMIT_CONFIG_CHANGE:
                    stageStr = "commit";
                    break;
                case LogItem.TYPE_DROP_CONFIG_CHANGE:
                    stageStr = "abort";
                    break;
                default:
                    throw new IllegalArgumentException(String.valueOf(type));
            }
            log.error("leader config change {}, not leader, role={}, groupId={}",
                    stageStr, raftStatus.getRole(), groupId);
            f.completeExceptionally(new NotLeaderException(raftStatus.getCurrentLeader().getNode()));
        }
        CompletableFuture<RaftOutput> outputFuture = new CompletableFuture<>();
        RaftInput input = new RaftInput(0, null, data, null, 0);
        RaftTask rt = new RaftTask(raftStatus.getTs(), type, input, outputFuture);
        eventBus.fire(EventType.raftExec, Collections.singletonList(rt));

        outputFuture.whenComplete((v, ex) -> {
            if (ex != null) {
                f.completeExceptionally(ex);
            } else {
                f.complete(null);
            }
        });
    }

    public void leaderPrepareJointConsensus(Set<Integer> newMemberNodes, Set<Integer> newObserverNodes,
                                            CompletableFuture<Void> f) {
        leaderConfigChange(LogItem.TYPE_PREPARE_CONFIG_CHANGE, getInputData(newMemberNodes, newObserverNodes), f);
    }

    private ByteBuffer getInputData(Set<Integer> newMemberNodes, Set<Integer> newObserverNodes) {
        StringBuilder sb = new StringBuilder(64);
        appendSet(sb, raftStatus.getNodeIdOfMembers());
        appendSet(sb, raftStatus.getNodeIdOfObservers());
        appendSet(sb, newMemberNodes);
        appendSet(sb, newObserverNodes);
        sb.deleteCharAt(sb.length() - 1);
        return ByteBuffer.wrap(sb.toString().getBytes());
    }

    private void appendSet(StringBuilder sb, Set<Integer> set) {
        if (set.size() > 0) {
            for (int nodeId : set) {
                sb.append(nodeId).append(',');
            }
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append(';');
    }


    public void doPrepare(List<RaftNodeEx> newMemberNodes, List<RaftNodeEx> newObserverNodes,
                          Throwable ex, Runnable callback) {
        if (ex != null) {
            raftStatus.setError(true);
            return;
        }

        IntObjMap<RaftMember> currentNodes = new IntObjMap<>();
        for (RaftMember m : raftStatus.getMembers()) {
            currentNodes.put(m.getNode().getNodeId(), m);
        }
        for (RaftMember m : raftStatus.getObservers()) {
            currentNodes.put(m.getNode().getNodeId(), m);
        }

        List<RaftMember> newMembers = new ArrayList<>();
        List<RaftMember> newObservers = new ArrayList<>();
        for (RaftNodeEx node : newMemberNodes) {
            RaftMember m = currentNodes.get(node.getNodeId());
            if (m == null) {
                m = new RaftMember(node);
                if (node.getNodeId() == serverConfig.getNodeId()) {
                    initSelf(node, m, RaftRole.follower);
                }
            }
            newMembers.add(m);
        }
        for (RaftNodeEx node : newObserverNodes) {
            RaftMember m = currentNodes.get(node.getNodeId());
            if (m == null) {
                m = new RaftMember(node);
                if (node.getNodeId() == serverConfig.getNodeId()) {
                    initSelf(node, m, RaftRole.observer);
                }
            }
            newObservers.add(m);
        }
        raftStatus.setPreparedMembers(newMembers);
        raftStatus.setPreparedObservers(newObservers);

        computeDuplicatedData(raftStatus);

        eventBus.fire(EventType.cancelVote, null);
        callback.run();

    }

    public void leaderAbortJointConsensus(CompletableFuture<Void> f) {
        leaderConfigChange(LogItem.TYPE_DROP_CONFIG_CHANGE, null, f);
    }

    public void leaderCommitJointConsensus(CompletableFuture<Void> f) {
        leaderConfigChange(LogItem.TYPE_COMMIT_CONFIG_CHANGE, null, f);
    }

}
