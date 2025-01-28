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

import com.github.dtprj.dongting.codec.DecoderCallbackCreator;
import com.github.dtprj.dongting.common.ByteArray;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.PbIntWritePacket;
import com.github.dtprj.dongting.net.PeerStatus;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.RpcCallback;
import com.github.dtprj.dongting.net.SimpleWritePacket;
import com.github.dtprj.dongting.raft.QueryStatusResp;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.RaftNode;
import com.github.dtprj.dongting.raft.rpc.RaftPing;
import com.github.dtprj.dongting.raft.rpc.TransferLeaderReq;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.NotLeaderException;
import com.github.dtprj.dongting.raft.server.RaftCallback;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;

import java.util.ArrayList;
import java.util.HashMap;
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
    private final GroupComponents gc;
    private final NioClient client;

    private final RaftServerConfig serverConfig;
    private final RaftStatusImpl raftStatus;
    private final int groupId;
    private final RaftGroupConfigEx groupConfig;

    private ReplicateManager replicateManager;
    private NodeManager nodeManager;

    private final CompletableFuture<Void> pingReadyFuture;
    private final int startReadyQuorum;

    int daemonSleepInterval = 1000;

    public MemberManager(NioClient client, GroupComponents gc) {
        this.client = client;
        this.gc = gc;
        this.serverConfig = gc.getServerConfig();
        this.groupConfig = gc.getGroupConfig();
        this.raftStatus = gc.getRaftStatus();
        this.groupId = raftStatus.getGroupId();

        if (raftStatus.getNodeIdOfMembers().isEmpty()) {
            this.pingReadyFuture = CompletableFuture.completedFuture(null);
            this.startReadyQuorum = 0;
        } else {
            this.startReadyQuorum = RaftUtil.getElectQuorum(raftStatus.getNodeIdOfMembers().size());
            this.pingReadyFuture = new CompletableFuture<>();
        }
    }

    public void postInit() {
        this.replicateManager = gc.getReplicateManager();
        this.nodeManager = gc.getNodeManager();
    }

    /**
     * invoke by RaftServer init thread or schedule thread
     */
    public void init() {
        raftStatus.setMembers(new ArrayList<>());
        for (int nodeId : raftStatus.getNodeIdOfMembers()) {
            RaftNodeEx node = nodeManager.allNodesEx.get(nodeId);
            RaftMember m = createMember(node, RaftRole.follower);
            raftStatus.getMembers().add(m);
        }
        if (!raftStatus.getNodeIdOfObservers().isEmpty()) {
            List<RaftMember> observers = new ArrayList<>();
            for (int nodeId : raftStatus.getNodeIdOfObservers()) {
                RaftNodeEx node = nodeManager.allNodesEx.get(nodeId);
                RaftMember m = createMember(node, RaftRole.observer);
                observers.add(m);
            }
            raftStatus.setObservers(observers);
        } else {
            raftStatus.setObservers(emptyList());
        }
        raftStatus.setPreparedMembers(emptyList());
        raftStatus.setPreparedObservers(emptyList());
        computeDuplicatedData(raftStatus);
        if (!raftStatus.getNodeIdOfMembers().contains(serverConfig.getNodeId())
                && !raftStatus.getNodeIdOfObservers().contains(serverConfig.getNodeId())) {
            raftStatus.setSelf(null);
            raftStatus.setRole(RaftRole.observer);
        } else {
            // to update startReadyFuture
            setReady(raftStatus.getSelf(), true);
        }
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
        raftStatus.setReplicateList(replicateList.isEmpty() ? emptyList() : replicateList);
        raftStatus.setNodeIdOfMembers(memberIds.isEmpty() ? emptySet() : memberIds);
        raftStatus.setNodeIdOfObservers(observerIds.isEmpty() ? emptySet() : observerIds);
        raftStatus.setNodeIdOfPreparedMembers(jointMemberIds.isEmpty() ? emptySet() : jointMemberIds);
        raftStatus.setNodeIdOfPreparedObservers(jointObserverIds.isEmpty() ? emptySet() : jointObserverIds);

        raftStatus.setElectQuorum(RaftUtil.getElectQuorum(raftStatus.getMembers().size()));
        raftStatus.setRwQuorum(RaftUtil.getRwQuorum(raftStatus.getMembers().size()));
    }

    public Fiber createRaftPingFiber() {
        FiberFrame<Void> fiberFrame = new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                try {
                    ensureRaftMemberStatus();
                    replicateManager.tryStartReplicateFibers();
                    return Fiber.sleep(daemonSleepInterval, this);
                } catch (Throwable e) {
                    throw Fiber.fatal(e);
                }
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
        if (node.isSelf()) {
            return;
        }
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
        try {
            DtTime timeout = new DtTime(serverConfig.getRpcTimeout(), TimeUnit.MILLISECONDS);

            SimpleWritePacket f = RaftUtil.buildRaftPingPacket(serverConfig.getNodeId(), raftStatus);
            f.setCommand(Commands.RAFT_PING);

            RpcCallback<RaftPing> callback = RpcCallback.fromHandlerAsync(groupConfig.getFiberGroup().getExecutor(),
                    (result, ex) -> processPingResult(raftNodeEx, member, result, ex, nodeEpochWhenStartPing));
            client.sendRequest(raftNodeEx.getPeer(), f, ctx -> ctx.toDecoderCallback(new RaftPing()),
                    timeout, callback);
        } catch (Exception e) {
            log.error("raft ping error, remote={}", raftNodeEx.getHostPort(), e);
            member.setPinging(false);
        }
    }

    private void processPingResult(RaftNodeEx raftNodeEx, RaftMember member,
                                   ReadPacket<RaftPing> rf, Throwable ex, int nodeEpochWhenStartPing) {
        member.setPinging(false);
        if (ex != null) {
            log.warn("raft ping fail, remote={}", raftNodeEx.getHostPort(), ex);
            setReady(member, false);
        } else {
            RaftPing ping = rf.getBody();
            String s = null;
            if (groupConfig.isStaticConfig()) {
                s = checkRemoteConfig(ping);
            }
            if (s != null) {
                log.error("raft ping static check fail: {}", s);
                setReady(member, false);
            } else {
                NodeStatus currentNodeStatus = member.getNode().getStatus();
                if (currentNodeStatus.isReady() && nodeEpochWhenStartPing == currentNodeStatus.getEpoch()) {
                    log.info("raft ping success, id={}, remote={}", ping.nodeId, raftNodeEx.getHostPort());
                    setReady(member, true);
                    member.setNodeEpoch(nodeEpochWhenStartPing);
                    replicateManager.tryStartReplicateFibers();
                } else {
                    log.warn("raft ping success but current node status not match. "
                                    + "id={}, remoteHost={}, nodeReady={}, nodeEpoch={}, pingEpoch={}",
                            ping.nodeId, raftNodeEx.getHostPort(), currentNodeStatus.isReady(),
                            currentNodeStatus.getEpoch(), nodeEpochWhenStartPing);
                    setReady(member, false);
                }
            }
        }
    }

    private String checkRemoteConfig(RaftPing ping) {
        String s = checkRemoteConfig("members", raftStatus.getMembers(), ping.members);
        if (s != null) {
            return s;
        }
        s = checkRemoteConfig("observers", raftStatus.getObservers(), ping.observers);
        if (s != null) {
            return s;
        }
        s = checkRemoteConfig("preparedMembers", raftStatus.getPreparedMembers(), ping.preparedMembers);
        if (s != null) {
            return s;
        }
        return checkRemoteConfig("preparedObservers", raftStatus.getPreparedObservers(), ping.preparedObservers);
    }

    private String checkRemoteConfig(String s, List<RaftMember> localServers, String remoteServers) {
        List<RaftNode> remotes = RaftNode.parseServers(remoteServers);
        if (localServers.size() != remotes.size()) {
            return s + " size not match, local=" + RaftNode.formatServers(localServers, RaftMember::getNode)
                    + ", remote=" + remoteServers;
        }
        for (RaftNode rn : remotes) {
            RaftMember localMember = null;
            for (RaftMember m : localServers) {
                if (m.getNode().getNodeId() == rn.getNodeId()) {
                    localMember = m;
                    break;
                }
            }
            if (localMember == null || !localMember.getNode().getHostPort().equals(rn.getHostPort())) {
                return s + " not match, local=" + RaftNode.formatServers(localServers, RaftMember::getNode)
                        + ", remote=" + remoteServers;
            }
        }
        return null;
    }

    public void setReady(RaftMember member, boolean ready) {
        member.setReady(ready);
        if (ready && !pingReadyFuture.isDone()) {
            int readyCount = getReadyCount(raftStatus.getMembers());
            if (readyCount >= startReadyQuorum) {
                log.info("member manager is ready: groupId={}", groupId);
                pingReadyFuture.complete(null);
            }
        }
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

    public FiberFrame<Void> leaderPrepareJointConsensus(Set<Integer> members, Set<Integer> observers,
                                                        Set<Integer> prepareMembers, Set<Integer> prepareObservers,
                                                        Set<Integer> newMemberNodes, Set<Integer> newObserverNodes,
                                                        CompletableFuture<Long> f) {
        return new FiberFrame<>() {
            @Override
            protected FrameCallResult handle(Throwable ex) {
                log.error("leader prepare joint consensus error", ex);
                f.completeExceptionally(ex);
                return Fiber.frameReturn();
            }

            @Override
            public FrameCallResult execute(Void input) {
                if (!raftStatus.getNodeIdOfMembers().equals(members)
                        || !raftStatus.getNodeIdOfObservers().equals(observers)
                        || !raftStatus.getNodeIdOfPreparedMembers().equals(prepareMembers)
                        || !raftStatus.getNodeIdOfPreparedObservers().equals(prepareObservers)) {
                    log.error("old members or observers not match, groupId={}", groupId);
                    f.completeExceptionally(new RaftException("old members or observers not match"));
                    return Fiber.frameReturn();
                }
                FiberFuture<Void> f = nodeManager.checkLeaderPrepare(newMemberNodes, newObserverNodes);
                return f.await(this::afterCheck);
            }

            private FrameCallResult afterCheck(Void unused) {
                leaderConfigChange(LogItem.TYPE_PREPARE_CONFIG_CHANGE,
                        getInputData(newMemberNodes, newObserverNodes), f);
                return Fiber.frameReturn();
            }
        };
    }

    public FiberFrame<Void> leaderAbortJointConsensus(CompletableFuture<Long> f) {
        return new FiberFrame<>() {
            @Override
            protected FrameCallResult handle(Throwable ex) {
                log.error("leader abort joint consensus error", ex);
                f.completeExceptionally(ex);
                return Fiber.frameReturn();
            }

            @Override
            public FrameCallResult execute(Void input) {
                leaderConfigChange(LogItem.TYPE_DROP_CONFIG_CHANGE, null, f);
                return Fiber.frameReturn();
            }
        };
    }

    public FiberFrame<Void> leaderCommitJointConsensus(CompletableFuture<Long> finalFuture, long prepareIndex) {
        return new LeaderCommitFrame(finalFuture, prepareIndex);
    }

    private class LeaderCommitFrame extends FiberFrame<Void> {
        private final CompletableFuture<Long> finalFuture;
        private final long prepareIndex;
        private final HashMap<Integer, CompletableFuture<Boolean>> resultMap = new HashMap<>();

        private LeaderCommitFrame(CompletableFuture<Long> finalFuture, long prepareIndex) {
            this.finalFuture = finalFuture;
            this.prepareIndex = prepareIndex;
        }

        @Override
        protected FrameCallResult handle(Throwable ex) {
            log.error("leader commit joint consensus error", ex);
            finalFuture.completeExceptionally(ex);
            return Fiber.frameReturn();
        }

        @Override
        public FrameCallResult execute(Void input) {
            if (lastConfigIndexNotMatch()) {
                return Fiber.frameReturn();
            }

            for (RaftMember m : raftStatus.getMembers()) {
                queryPrepareStatus(m);
            }
            for (RaftMember m : raftStatus.getPreparedMembers()) {
                if (resultMap.containsKey(m.getNode().getNodeId())) {
                    continue;
                }
                queryPrepareStatus(m);
            }

            List<CompletableFuture<Boolean>> list = new ArrayList<>(resultMap.values());
            for (CompletableFuture<Boolean> resultFuture : list) {
                resultFuture.thenRunAsync(this::checkPrepareStatus, groupConfig.getFiberGroup().getExecutor());
            }
            return Fiber.frameReturn();
        }

        private void queryPrepareStatus(RaftMember m) {
            RaftNodeEx n = m.getNode();
            if (n.isSelf()) {
                log.info("self prepare status, groupId={}, lastApplied={}, prepareIndex={}",
                        groupId, raftStatus.getLastApplied(), prepareIndex);
                boolean result = raftStatus.getLastApplied() >= prepareIndex;
                resultMap.put(n.getNodeId(), CompletableFuture.completedFuture(result));
            } else {
                try {
                    resultMap.put(n.getNodeId(), sendQuery(n));
                } catch (Exception e) {
                    log.error("query prepare status failed", e);
                    resultMap.put(n.getNodeId(), CompletableFuture.completedFuture(false));
                }
            }
        }

        private CompletableFuture<Boolean> sendQuery(RaftNodeEx n) {
            CompletableFuture<ReadPacket<QueryStatusResp>> f = new CompletableFuture<>();
            client.sendRequest(n.getPeer(), new PbIntWritePacket(Commands.RAFT_QUERY_STATUS, groupId),
                    QueryStatusResp.DECODER, new DtTime(3, TimeUnit.SECONDS), RpcCallback.fromFuture(f));
            return f.handle((resp, ex) -> {
                if (ex != null) {
                    log.warn("query prepare status failed, groupId={}, remoteId={}", n.getNodeId(), groupId, ex);
                    return Boolean.FALSE;
                } else {
                    QueryStatusResp body = resp.getBody();
                    log.info("query prepare status success, groupId={}, remoteId={}, lastApplied={}, prepareIndex={}",
                            groupId, n.getNodeId(), body.getLastApplied(), prepareIndex);
                    return body.getLastApplied() >= prepareIndex;
                }
            });
        }

        private boolean lastConfigIndexNotMatch() {
            if (prepareIndex != raftStatus.getLastConfigChangeIndex()) {
                log.error("prepareIndex not match. prepareIndex={}, lastConfigChangeIndex={}",
                        prepareIndex, raftStatus.getLastConfigChangeIndex());
                finalFuture.completeExceptionally(new RaftException("prepareIndex not match. prepareIndex="
                        + prepareIndex + ", lastConfigChangeIndex=" + raftStatus.getLastConfigChangeIndex()));
                return true;
            }
            return false;
        }

        private void checkPrepareStatus() {
            try {
                if (finalFuture.isDone()) {
                    // prevent duplicate change
                    return;
                }
                if (lastConfigIndexNotMatch()) {
                    return;
                }
                int memberReadyCount = 0;
                int memberNotReadyCount = 0;
                for (RaftMember m : raftStatus.getMembers()) {
                    CompletableFuture<Boolean> queryResult = resultMap.get(m.getNode().getNodeId());
                    if (queryResult == null) {
                        BugLog.getLog().error("queryResult is null");
                        finalFuture.completeExceptionally(new RaftException("queryResult is null"));
                        return;
                    }
                    if (queryResult.isCompletedExceptionally()) {
                        memberNotReadyCount++;
                    } else if (queryResult.isDone()) {
                        if (queryResult.get()) {
                            memberReadyCount++;
                        } else {
                            memberNotReadyCount++;
                        }
                    }
                }
                int preparedMemberReadyCount = 0;
                int preparedMemberNotReadyCount = 0;
                for (RaftMember m : raftStatus.getPreparedMembers()) {
                    CompletableFuture<Boolean> queryResult = resultMap.get(m.getNode().getNodeId());
                    if (queryResult == null) {
                        BugLog.getLog().error("queryResult is null");
                        finalFuture.completeExceptionally(new RaftException("queryResult is null"));
                        return;
                    }
                    if (queryResult.isCompletedExceptionally()) {
                        preparedMemberNotReadyCount++;
                    } else if (queryResult.isDone()) {
                        if (queryResult.get()) {
                            preparedMemberReadyCount++;
                        } else {
                            preparedMemberNotReadyCount++;
                        }
                    }
                }
                int prepareMemberCount = raftStatus.getPreparedMembers().size();
                int prepareQuorum = prepareMemberCount == 0 ? 0 : RaftUtil.getElectQuorum(prepareMemberCount);
                if (memberReadyCount >= raftStatus.getElectQuorum() && preparedMemberReadyCount >= prepareQuorum) {
                    log.info("members prepare status check success, groupId={}, memberReadyCount={}, preparedMemberReadyCount={}",
                            groupId, memberReadyCount, preparedMemberReadyCount);

                    leaderConfigChange(LogItem.TYPE_COMMIT_CONFIG_CHANGE, null, finalFuture);
                } else if (memberNotReadyCount >= raftStatus.getElectQuorum() ||
                        (prepareQuorum > 0 && preparedMemberNotReadyCount >= prepareQuorum)) {
                    log.error("members prepare status check failed, groupId={}, memberNotReadyCount={}, preparedMemberNotReadyCount={}",
                            groupId, memberNotReadyCount, preparedMemberNotReadyCount);
                    finalFuture.completeExceptionally(new RaftException("members prepare status check failed:"
                            + memberNotReadyCount + "," + preparedMemberNotReadyCount));
                }
            } catch (Throwable e) {
                log.error("check prepare status error", e);
                finalFuture.completeExceptionally(e);
            }
        }
    }

    private byte[] getInputData(Set<Integer> newMemberNodes, Set<Integer> newObserverNodes) {
        StringBuilder sb = new StringBuilder(64);
        appendSet(sb, raftStatus.getNodeIdOfMembers());
        appendSet(sb, raftStatus.getNodeIdOfObservers());
        appendSet(sb, newMemberNodes);
        appendSet(sb, newObserverNodes);
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString().getBytes();
    }

    private void appendSet(StringBuilder sb, Set<Integer> set) {
        if (!set.isEmpty()) {
            for (int nodeId : set) {
                sb.append(nodeId).append(',');
            }
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append(';');
    }

    private void leaderConfigChange(int type, byte[] data, CompletableFuture<Long> f) {
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
            f.completeExceptionally(new NotLeaderException(raftStatus.getCurrentLeaderNode()));
        }
        RaftInput input = new RaftInput(0, null, data == null ? null : new ByteArray(data),
                null, false);
        // use runner fiber to execute to avoid race condition
        gc.getLinearTaskRunner().submitRaftTaskInBizThread(type, input, new RaftCallback() {
            @Override
            public void success(long raftIndex, Object nullResult) {
                f.complete(raftIndex);
            }

            @Override
            public void fail(Throwable ex) {
                f.completeExceptionally(ex);
            }
        });
    }

    private RaftMember findExistMember(int nodeId) {
        for (RaftMember m : raftStatus.getMembers()) {
            if (m.getNode().getNodeId() == nodeId) {
                return m;
            }
        }
        for (RaftMember m : raftStatus.getObservers()) {
            if (m.getNode().getNodeId() == nodeId) {
                return m;
            }
        }
        for (RaftMember m : raftStatus.getPreparedMembers()) {
            if (m.getNode().getNodeId() == nodeId) {
                return m;
            }
        }
        for (RaftMember m : raftStatus.getPreparedObservers()) {
            if (m.getNode().getNodeId() == nodeId) {
                return m;
            }
        }
        return null;
    }

    private RaftMember createMember(RaftNodeEx node, RaftRole role) {
        RaftMember m = new RaftMember(node, groupConfig.getFiberGroup());
        if (node.isSelf()) {
            m.setReady(true);
            raftStatus.setSelf(m);
            raftStatus.setRole(role);
            raftStatus.copyShareStatus();
        }
        return m;
    }

    public FrameCallResult doPrepare(Set<Integer> newMemberIds, Set<Integer> newObserverIds) {
        ApplyConfigFrame f = new ApplyConfigFrame("prepare config change", raftStatus.getNodeIdOfMembers(),
                raftStatus.getNodeIdOfObservers(), newMemberIds, newObserverIds);
        return Fiber.call(f, v -> Fiber.frameReturn());
    }

    public FrameCallResult doAbort() {
        HashSet<Integer> preparedMemberIds = new HashSet<>(raftStatus.getNodeIdOfPreparedMembers());
        if (preparedMemberIds.isEmpty()) {
            log.info("no pending config change, ignore abort, groupId={}", raftStatus.getGroupId());
            return Fiber.frameReturn();
        }
        ApplyConfigFrame f = new ApplyConfigFrame("abort config change", raftStatus.getNodeIdOfMembers(),
                raftStatus.getNodeIdOfObservers(), emptySet(), emptySet());
        return Fiber.call(f, v -> Fiber.frameReturn());
    }

    public FrameCallResult doCommit() {
        if (raftStatus.getPreparedMembers().isEmpty()) {
            log.warn("no prepared config change, ignore commit, groupId={}", raftStatus.getGroupId());
            return Fiber.frameReturn();
        }
        ApplyConfigFrame f = new ApplyConfigFrame("commit config change", raftStatus.getNodeIdOfPreparedMembers(),
                raftStatus.getNodeIdOfPreparedObservers(), emptySet(), emptySet());
        return Fiber.call(f, v -> Fiber.frameReturn());
    }

    public FiberFrame<Void> applyConfigFrame(String msg, Set<Integer> newMembers, Set<Integer> observerIds,
                                             Set<Integer> preparedMemberIds, Set<Integer> preparedObserverIds) {
        return new ApplyConfigFrame(msg, newMembers, observerIds, preparedMemberIds, preparedObserverIds);
    }

    private class ApplyConfigFrame extends FiberFrame<Void> {
        private final String msg;
        private final Set<Integer> newMembers;
        private final Set<Integer> observerIds;
        private final Set<Integer> preparedMemberIds;
        private final Set<Integer> preparedObserverIds;

        ApplyConfigFrame(String msg, Set<Integer> newMembers, Set<Integer> observerIds,
                         Set<Integer> preparedMemberIds, Set<Integer> preparedObserverIds) {
            this.msg = msg;
            this.newMembers = newMembers;
            this.observerIds = observerIds;
            this.preparedMemberIds = preparedMemberIds;
            this.preparedObserverIds = preparedObserverIds;
        }

        @Override
        public FrameCallResult execute(Void v) {
            if (raftStatus.getNodeIdOfMembers().equals(newMembers)
                    && raftStatus.getNodeIdOfObservers().equals(observerIds)
                    && raftStatus.getNodeIdOfPreparedMembers().equals(preparedMemberIds)
                    && raftStatus.getNodeIdOfPreparedObservers().equals(preparedObserverIds)) {
                return Fiber.frameReturn();
            }
            log.info("{} , groupId={}, oldMember={}, oldObserver={}, oldPreparedMember={}, oldPreparedObserver={}, newMember={}, newObserver={}, newPreparedMember={}, newPreparedObserver={}",
                    msg, groupId, raftStatus.getNodeIdOfMembers(), raftStatus.getNodeIdOfObservers(),
                    raftStatus.getNodeIdOfPreparedMembers(), raftStatus.getNodeIdOfPreparedObservers(),
                    newMembers, observerIds, preparedMemberIds, preparedObserverIds);
            return nodeManager.doApplyConfig(
                            raftStatus.getNodeIdOfMembers(), raftStatus.getNodeIdOfObservers(),
                            raftStatus.getNodeIdOfPreparedMembers(), raftStatus.getNodeIdOfPreparedObservers(),
                            newMembers, observerIds, preparedMemberIds, preparedObserverIds)
                    .await(result -> postConfigChange(msg, result));
        }

        private FrameCallResult postConfigChange(String msg, List<List<RaftNodeEx>> result) {
            List<RaftNodeEx> newMemberNodes = result.get(0);
            List<RaftNodeEx> newObserverNodes = result.get(1);
            List<RaftNodeEx> preparedMemberNodes = result.get(2);
            List<RaftNodeEx> preparedObserverNodes = result.get(3);

            List<RaftMember> newMembers = createMembers(newMemberNodes);
            List<RaftMember> newObservers = createMembers(newObserverNodes);
            List<RaftMember> newPreparedMembers = createMembers(preparedMemberNodes);
            List<RaftMember> newPreparedObservers = createMembers(preparedObserverNodes);

            raftStatus.setMembers(newMembers);
            raftStatus.setObservers(newObservers);
            raftStatus.setPreparedMembers(newPreparedMembers);
            raftStatus.setPreparedObservers(newPreparedObservers);
            computeDuplicatedData(raftStatus);

            int selfNodeId = serverConfig.getNodeId();
            boolean selfIsMember = raftStatus.getNodeIdOfMembers().contains(selfNodeId)
                    || raftStatus.getNodeIdOfPreparedMembers().contains(selfNodeId);
            int currentLeaderId = raftStatus.getCurrentLeader() == null ? -1 :
                    raftStatus.getCurrentLeader().getNode().getNodeId();
            if (raftStatus.getRole() == RaftRole.observer && selfIsMember) {
                RaftUtil.changeToFollower(raftStatus, currentLeaderId, "apply config change");
            } else if (raftStatus.getRole() != RaftRole.observer && !selfIsMember) {
                RaftUtil.changeToObserver(raftStatus, currentLeaderId);
            }
            gc.getVoteManager().cancelVote("config change");
            log.info("{} success, groupId={}", msg, groupId);
            return Fiber.frameReturn();
        }
    }

    private List<RaftMember> createMembers(List<RaftNodeEx> nodes) {
        List<RaftMember> newMembers = new ArrayList<>();
        for (RaftNodeEx node : nodes) {
            RaftMember m = findExistMember(node.getNodeId());
            if (m == null) {
                m = createMember(node, RaftRole.observer);
            }
            newMembers.add(m);
        }
        return newMembers;
    }


    public boolean isValidCandidate(int nodeId) {
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
        if (!groupConfig.getFiberGroup().fireFiber("transfer-leader",
                new TranferLeaderFiberFrame(nodeId, f, deadline))) {
            f.completeExceptionally(new RaftException("fire transfer leader fiber failed"));
        }
    }

    private class TranferLeaderFiberFrame extends FiberFrame<Void> {

        private final int nodeId;
        private final CompletableFuture<Void> f;
        private final DtTime deadline;

        TranferLeaderFiberFrame(int nodeId, CompletableFuture<Void> f, DtTime deadline) {
            this.nodeId = nodeId;
            this.f = f;
            this.deadline = deadline;
        }

        @Override
        protected FrameCallResult handle(Throwable ex) {
            RaftUtil.clearTransferLeaderCondition(raftStatus);
            f.completeExceptionally(ex);
            return Fiber.frameReturn();
        }

        @Override
        public FrameCallResult execute(Void input) {
            if (raftStatus.getTransferLeaderCondition() != null) {
                f.completeExceptionally(new RaftException("transfer leader in progress"));
                return Fiber.frameReturn();
            }
            raftStatus.setTransferLeaderCondition(groupConfig.getFiberGroup().newCondition("transferLeader"));
            return checkBeforeTransferLeader(null);
        }

        private FrameCallResult checkBeforeTransferLeader(Void v) {
            if (raftStatus.getRole() != RaftRole.leader) {
                f.completeExceptionally(new NotLeaderException(raftStatus.getCurrentLeaderNode()));
                RaftUtil.clearTransferLeaderCondition(raftStatus);
                return Fiber.frameReturn();
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
                f.completeExceptionally(new RaftException("nodeId not found: " + nodeId));
                RaftUtil.clearTransferLeaderCondition(raftStatus);
                return Fiber.frameReturn();
            }

            if (deadline.isTimeout()) {
                f.completeExceptionally(new RaftException("transfer leader timeout"));
                RaftUtil.clearTransferLeaderCondition(raftStatus);
                return Fiber.frameReturn();
            }
            if (f.isCancelled()) {
                RaftUtil.clearTransferLeaderCondition(raftStatus);
                return Fiber.frameReturn();
            }

            boolean lastLogCommit = raftStatus.getCommitIndex() == raftStatus.getLastLogIndex();
            boolean newLeaderHasLastLog = newLeader.getMatchIndex() == raftStatus.getLastLogIndex();

            if (newLeader.isReady() && lastLogCommit && newLeaderHasLastLog) {
                PbIntWritePacket req = new PbIntWritePacket(Commands.RAFT_QUERY_STATUS, groupId);
                CompletableFuture<ReadPacket<QueryStatusResp>> queryFuture = new CompletableFuture<>();
                client.sendRequest(newLeader.getNode().getPeer(), req, QueryStatusResp.DECODER,
                        new DtTime(3, TimeUnit.SECONDS), RpcCallback.fromFuture(queryFuture));
                RaftNodeEx newLeaderNode = newLeader.getNode();
                queryFuture.whenCompleteAsync((resp, ex) -> {
                    if (ex != null) {
                        f.completeExceptionally(ex);
                    } else {
                        execTransferLeader(newLeaderNode, resp, f);
                    }
                }, groupConfig.getFiberGroup().getExecutor());
                return Fiber.frameReturn();
            } else {
                return Fiber.sleep(1, this::checkBeforeTransferLeader);
            }
        }
    }

    private void execTransferLeader(RaftNodeEx newLeader, ReadPacket<QueryStatusResp> resp,
                                    CompletableFuture<Void> finalFuture) {
        try {
            QueryStatusResp s = resp.getBody();
            if (!s.members.equals(raftStatus.getNodeIdOfMembers())
                    || !s.observers.equals(raftStatus.getNodeIdOfObservers())
                    || !s.preparedMembers.equals(raftStatus.getNodeIdOfPreparedMembers())
                    || !s.preparedObservers.equals(raftStatus.getNodeIdOfPreparedObservers())) {
                log.error("config not match, groupId={}", groupId);
                finalFuture.completeExceptionally(new RaftException("config not match"));
                return;
            }
            RaftUtil.clearTransferLeaderCondition(raftStatus);
            RaftUtil.changeToFollower(raftStatus, newLeader.getNodeId(), "transfer leader");
            TransferLeaderReq req = new TransferLeaderReq();
            req.term = raftStatus.getCurrentTerm();
            req.logIndex = raftStatus.getLastLogIndex();
            req.oldLeaderId = serverConfig.getNodeId();
            req.newLeaderId = newLeader.getNodeId();
            req.groupId = groupId;
            SimpleWritePacket frame = new SimpleWritePacket(req);
            frame.setCommand(Commands.RAFT_TRANSFER_LEADER);
            DecoderCallbackCreator<Void> dc = DecoderCallbackCreator.VOID_DECODE_CALLBACK_CREATOR;
            client.sendRequest(newLeader.getPeer(), frame, dc, new DtTime(5, TimeUnit.SECONDS),
                    new RpcCallback<>() {
                        @Override
                        public void success(ReadPacket<Void> result) {
                            log.info("transfer leader success, groupId={}", groupId);
                            finalFuture.complete(null);
                        }

                        @Override
                        public void fail(Throwable ex) {
                            log.error("transfer leader failed, groupId={}", groupId, ex);
                            finalFuture.completeExceptionally(ex);
                        }
                    });
        } catch (Exception e) {
            log.error("", e);
            finalFuture.completeExceptionally(e);
        }
    }


    public CompletableFuture<Void> getPingReadyFuture() {
        return pingReadyFuture;
    }
}
