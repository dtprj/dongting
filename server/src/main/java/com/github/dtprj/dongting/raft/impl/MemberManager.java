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
import com.github.dtprj.dongting.common.VersionFactory;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
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
        this.serverConfig = gc.serverConfig;
        this.groupConfig = gc.groupConfig;
        this.raftStatus = gc.raftStatus;
        this.groupId = raftStatus.groupId;

        if (raftStatus.nodeIdOfMembers.isEmpty()) {
            this.pingReadyFuture = CompletableFuture.completedFuture(null);
            this.startReadyQuorum = 0;
        } else {
            this.startReadyQuorum = RaftUtil.getElectQuorum(raftStatus.nodeIdOfMembers.size());
            this.pingReadyFuture = new CompletableFuture<>();
        }
    }

    public void postInit() {
        this.replicateManager = gc.replicateManager;
        this.nodeManager = gc.nodeManager;
    }

    /**
     * invoke by RaftServer init thread or schedule thread
     */
    public void init() {
        raftStatus.members = new ArrayList<>();
        for (int nodeId : raftStatus.nodeIdOfMembers) {
            RaftNodeEx node = nodeManager.allNodesEx.get(nodeId);
            RaftMember m = createMember(node, RaftRole.follower);
            raftStatus.members.add(m);
        }
        if (!raftStatus.nodeIdOfObservers.isEmpty()) {
            List<RaftMember> observers = new ArrayList<>();
            for (int nodeId : raftStatus.nodeIdOfObservers) {
                RaftNodeEx node = nodeManager.allNodesEx.get(nodeId);
                RaftMember m = createMember(node, RaftRole.observer);
                observers.add(m);
            }
            raftStatus.observers = observers;
        } else {
            raftStatus.observers = emptyList();
        }
        raftStatus.preparedMembers = emptyList();
        raftStatus.preparedObservers = emptyList();
        if (raftStatus.self == null) {
            // the current node is not in members and observers
            RaftNodeEx node = nodeManager.allNodesEx.get(serverConfig.nodeId);
            createMember(node, RaftRole.none);
            // for RaftRole.none, don't ping member when init
            pingReadyFuture.complete(null);
        }
        computeDuplicatedData(raftStatus);

        // to update startReadyFuture
        setReady(raftStatus.self, true);
    }

    static void computeDuplicatedData(RaftStatusImpl raftStatus) {
        ArrayList<RaftMember> replicateList = new ArrayList<>();
        Set<Integer> memberIds = new HashSet<>();
        Set<Integer> observerIds = new HashSet<>();
        Set<Integer> jointMemberIds = new HashSet<>();
        Set<Integer> jointObserverIds = new HashSet<>();
        for (RaftMember m : raftStatus.members) {
            replicateList.add(m);
            memberIds.add(m.node.nodeId);
        }
        for (RaftMember m : raftStatus.observers) {
            replicateList.add(m);
            observerIds.add(m.node.nodeId);
        }
        for (RaftMember m : raftStatus.preparedMembers) {
            replicateList.add(m);
            jointMemberIds.add(m.node.nodeId);
        }
        for (RaftMember m : raftStatus.preparedObservers) {
            jointObserverIds.add(m.node.nodeId);
        }
        raftStatus.replicateList = replicateList.isEmpty() ? emptyList() : replicateList;
        raftStatus.nodeIdOfMembers = memberIds.isEmpty() ? emptySet() : Collections.unmodifiableSet(memberIds);
        raftStatus.nodeIdOfObservers = observerIds.isEmpty() ? emptySet() : Collections.unmodifiableSet(observerIds);
        raftStatus.nodeIdOfPreparedMembers = jointMemberIds.isEmpty() ? emptySet() : Collections.unmodifiableSet(jointMemberIds);
        raftStatus.nodeIdOfPreparedObservers = jointObserverIds.isEmpty() ? emptySet() : Collections.unmodifiableSet(jointObserverIds);

        raftStatus.electQuorum = RaftUtil.getElectQuorum(raftStatus.members.size());
        raftStatus.rwQuorum = RaftUtil.getRwQuorum(raftStatus.members.size());

        raftStatus.membersInfo = new MembersInfo(raftStatus.nodeIdOfMembers, raftStatus.nodeIdOfObservers,
                raftStatus.nodeIdOfPreparedMembers, raftStatus.nodeIdOfPreparedObservers);
    }

    public Fiber createRaftPingFiber() {
        FiberFrame<Void> fiberFrame = new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                try {
                    if (isGroupShouldStopPlain()) {
                        return Fiber.frameReturn();
                    }
                    ensureRaftMemberStatus();
                    replicateManager.tryStartReplicateFibers();
                    return Fiber.sleep(daemonSleepInterval, this);
                } catch (Throwable e) {
                    throw Fiber.fatal(e);
                }
            }
        };
        // daemon fiber
        return new Fiber("raftPing", groupConfig.fiberGroup, fiberFrame, true);
    }

    public void ensureRaftMemberStatus() {
        List<RaftMember> replicateList = raftStatus.replicateList;
        for (RaftMember member : replicateList) {
            check(member);
        }
    }

    private void check(RaftMember member) {
        RaftNodeEx node = member.node;
        NodeStatus nodeStatus = node.status;
        if (node.self) {
            return;
        }
        if (!nodeStatus.isReady()) {
            setReady(member, false);
        } else if (nodeStatus.getEpoch() != member.nodeEpoch) {
            setReady(member, false);
            if (!member.pinging) {
                raftPing(node, member, nodeStatus.getEpoch());
            }
        }
    }

    private void raftPing(RaftNodeEx raftNodeEx, RaftMember member, int nodeEpochWhenStartPing) {
        if (raftNodeEx.peer.status != PeerStatus.connected) {
            setReady(member, false);
            return;
        }

        member.pinging = true;
        try {
            DtTime timeout = new DtTime(serverConfig.rpcTimeout, TimeUnit.MILLISECONDS);

            SimpleWritePacket f = RaftUtil.buildRaftPingPacket(serverConfig.nodeId, raftStatus);
            f.command = Commands.RAFT_PING;

            Executor executor = groupConfig.fiberGroup.getExecutor();
            RpcCallback<RaftPing> callback = (result, ex) -> executor.execute(
                    () -> processPingResult(raftNodeEx, member, result, ex, nodeEpochWhenStartPing));
            client.sendRequest(raftNodeEx.peer, f, ctx -> ctx.toDecoderCallback(new RaftPing()),
                    timeout, callback);
        } catch (Exception e) {
            log.error("raft ping error, remote={}", raftNodeEx.hostPort, e);
            member.pinging = false;
        }
    }

    private void processPingResult(RaftNodeEx raftNodeEx, RaftMember member,
                                   ReadPacket<RaftPing> rf, Throwable ex, int nodeEpochWhenStartPing) {
        member.pinging = false;
        try {
            if (ex != null) {
                log.warn("raft ping fail, remote={}", raftNodeEx.hostPort, ex);
                setReady(member, false);
            } else {
                RaftPing ping = rf.getBody();
                String s = checkRemoteConfig(ping);
                if (s != null) {
                    log.error("raft ping static check fail: {}", s);
                    setReady(member, false);
                } else {
                    NodeStatus currentNodeStatus = member.node.status;
                    if (currentNodeStatus.isReady() && nodeEpochWhenStartPing == currentNodeStatus.getEpoch()) {
                        log.info("raft ping success, id={}, remote={}", ping.nodeId, raftNodeEx.hostPort);
                        setReady(member, true);
                        member.nodeEpoch = nodeEpochWhenStartPing;
                        replicateManager.tryStartReplicateFibers();
                    } else {
                        log.warn("raft ping success but current node status not match. "
                                        + "id={}, remoteHost={}, nodeReady={}, nodeEpoch={}, pingEpoch={}",
                                ping.nodeId, raftNodeEx.hostPort, currentNodeStatus.isReady(),
                                currentNodeStatus.getEpoch(), nodeEpochWhenStartPing);
                        setReady(member, false);
                    }
                }
            }
        } catch (Exception e) {
            log.error("process ping result error", e);
            setReady(member, false);
        }
    }

    private String checkRemoteConfig(RaftPing ping) {
        String s = checkRemoteConfig("members", raftStatus.members, ping.members);
        if (s != null) {
            return s;
        }
        s = checkRemoteConfig("observers", raftStatus.observers, ping.observers);
        if (s != null) {
            return s;
        }
        s = checkRemoteConfig("preparedMembers", raftStatus.preparedMembers, ping.preparedMembers);
        if (s != null) {
            return s;
        }
        return checkRemoteConfig("preparedObservers", raftStatus.preparedObservers, ping.preparedObservers);
    }

    private String checkRemoteConfig(String s, List<RaftMember> localServers, String remoteServers) {
        List<RaftNode> remotes = RaftNode.parseServers(remoteServers);
        int check = groupConfig.raftPingCheck;
        if (check > 0) {
            if (localServers.size() != remotes.size()) {
                return s + " size not match, local=" + RaftNode.formatServers(localServers, m -> m.node)
                        + ", remote=" + remoteServers;
            }
            for (RaftNode rn : remotes) {
                RaftMember localMember = null;
                for (RaftMember m : localServers) {
                    if (m.node.nodeId == rn.nodeId) {
                        localMember = m;
                        break;
                    }
                }
                boolean fail = localMember == null ||
                        (check > 1 && !localMember.node.hostPort.equals(rn.hostPort));
                if (fail) {
                    return s + " not match, local=" + RaftNode.formatServers(localServers, m -> m.node)
                            + ", remote=" + remoteServers;
                }
            }
        }
        return null;
    }

    public void setReady(RaftMember member, boolean ready) {
        member.ready = ready;
        if (ready && !pingReadyFuture.isDone()) {
            int readyCount = getReadyCount(raftStatus.members);
            if (readyCount >= startReadyQuorum) {
                log.info("member manager is ready: groupId={}", groupId);
                pingReadyFuture.complete(null);
            }
        }
    }

    private int getReadyCount(List<RaftMember> list) {
        int count = 0;
        for (RaftMember m : list) {
            if (m.ready) {
                count++;
            }
        }
        return count;
    }

    public FiberFrame<Void> leaderPrepareJointConsensus(Set<Integer> members, Set<Integer> observers,
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
                if (!raftStatus.nodeIdOfMembers.equals(members)
                        || !raftStatus.nodeIdOfObservers.equals(observers)) {
                    log.error("old members or observers not match, groupId={}", groupId);
                    f.completeExceptionally(new RaftException("old members or observers not match"));
                    return Fiber.frameReturn();
                }
                nodeManager.checkLeaderPrepare(newMemberNodes, newObserverNodes);
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
        private boolean fireCommit;

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

            for (RaftMember m : raftStatus.members) {
                queryPrepareStatus(m);
            }
            for (RaftMember m : raftStatus.preparedMembers) {
                if (resultMap.containsKey(m.node.nodeId)) {
                    continue;
                }
                queryPrepareStatus(m);
            }

            List<CompletableFuture<Boolean>> list = new ArrayList<>(resultMap.values());
            for (CompletableFuture<Boolean> resultFuture : list) {
                resultFuture.thenRunAsync(this::checkPrepareStatus, groupConfig.fiberGroup.getExecutor());
            }
            return Fiber.frameReturn();
        }

        private void queryPrepareStatus(RaftMember m) {
            RaftNodeEx n = m.node;
            if (n.self) {
                log.info("self prepare status, groupId={}, lastApplied={}, prepareIndex={}",
                        groupId, raftStatus.getLastApplied(), prepareIndex);
                boolean result = raftStatus.getLastApplied() >= prepareIndex;
                resultMap.put(n.nodeId, CompletableFuture.completedFuture(result));
            } else {
                try {
                    resultMap.put(n.nodeId, sendQuery(n));
                } catch (Exception e) {
                    log.error("query prepare status failed", e);
                    resultMap.put(n.nodeId, CompletableFuture.completedFuture(false));
                }
            }
        }

        private CompletableFuture<Boolean> sendQuery(RaftNodeEx n) {
            CompletableFuture<ReadPacket<QueryStatusResp>> f = new CompletableFuture<>();
            client.sendRequest(n.peer, new PbIntWritePacket(Commands.RAFT_QUERY_STATUS, groupId),
                    QueryStatusResp.DECODER, new DtTime(3, TimeUnit.SECONDS), RpcCallback.fromFuture(f));
            return f.handle((resp, ex) -> {
                if (ex != null) {
                    log.warn("query prepare status failed, groupId={}, remoteId={}", groupId, n.nodeId, ex);
                    return Boolean.FALSE;
                } else {
                    QueryStatusResp body = resp.getBody();
                    boolean result = body.lastApplied >= prepareIndex;
                    log.info("query prepare status success, result={}, groupId={}, remoteId={}, lastApplied={}, prepareIndex={}",
                            result, groupId, n.nodeId, body.lastApplied, prepareIndex);
                    return result;
                }
            });
        }

        private boolean lastConfigIndexNotMatch() {
            if (prepareIndex != raftStatus.lastConfigChangeIndex) {
                log.error("prepareIndex not match. prepareIndex={}, lastConfigChangeIndex={}",
                        prepareIndex, raftStatus.lastConfigChangeIndex);
                finalFuture.completeExceptionally(new RaftException("prepareIndex not match. prepareIndex="
                        + prepareIndex + ", lastConfigChangeIndex=" + raftStatus.lastConfigChangeIndex));
                return true;
            }
            return false;
        }

        private void checkPrepareStatus() {
            try {
                if (fireCommit) {
                    // prevent duplicate change
                    return;
                }
                if (lastConfigIndexNotMatch()) {
                    return;
                }
                int memberReadyCount = 0;
                int memberNotReadyCount = 0;
                for (RaftMember m : raftStatus.members) {
                    CompletableFuture<Boolean> queryResult = resultMap.get(m.node.nodeId);
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
                for (RaftMember m : raftStatus.preparedMembers) {
                    CompletableFuture<Boolean> queryResult = resultMap.get(m.node.nodeId);
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
                int prepareMemberCount = raftStatus.preparedMembers.size();
                int prepareQuorum = prepareMemberCount == 0 ? 0 : RaftUtil.getElectQuorum(prepareMemberCount);
                if (memberReadyCount >= raftStatus.electQuorum && preparedMemberReadyCount >= prepareQuorum) {
                    log.info("members prepare status check success, groupId={}, memberReadyCount={}, preparedMemberReadyCount={}",
                            groupId, memberReadyCount, preparedMemberReadyCount);
                    fireCommit = true;
                    leaderConfigChange(LogItem.TYPE_COMMIT_CONFIG_CHANGE, null, finalFuture);
                } else if (memberNotReadyCount >= raftStatus.electQuorum ||
                        (prepareQuorum > 0 && preparedMemberNotReadyCount >= prepareQuorum)) {
                    log.error("members prepare status check failed, groupId={}, memberNotReadyCount={}, preparedMemberNotReadyCount={}",
                            groupId, memberNotReadyCount, preparedMemberNotReadyCount);
                    finalFuture.completeExceptionally(new RaftException("members prepare status check failed:memberNotReadyCount="
                            + memberNotReadyCount + ",preparedMemberNotReadyCount=" + preparedMemberNotReadyCount));
                }
                log.info("members prepare status check, groupId={}, memberReadyCount={}, memberNotReadyCount={}, " +
                        "preparedMemberReadyCount={}, preparedMemberNotReadyCount={}", groupId, memberReadyCount,
                        memberNotReadyCount, preparedMemberReadyCount, preparedMemberNotReadyCount);
            } catch (Throwable e) {
                log.error("check prepare status error", e);
                finalFuture.completeExceptionally(e);
            }
        }
    }

    private byte[] getInputData(Set<Integer> newMemberNodes, Set<Integer> newObserverNodes) {
        StringBuilder sb = new StringBuilder(64);
        appendSet(sb, raftStatus.nodeIdOfMembers);
        appendSet(sb, raftStatus.nodeIdOfObservers);
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
        gc.linearTaskRunner.submitRaftTaskInBizThread(type, input, new RaftCallback() {
            @Override
            public void success(long raftIndex, Object nullResult) {
                if (type == LogItem.TYPE_PREPARE_CONFIG_CHANGE) {
                    // When prepareIndex applied, the prepared member may still not apply to prepareIndex (the commit
                    // manager does not check prepare members since they are not active).
                    // If we call commit config change immediately after prepareIndex applied, the prepareIndex
                    // check will fail, so we issue a heartbeat and wait to prepareIndex + 1 to be applied.
                    gc.linearTaskRunner.issueHeartBeat();
                    Fiber fiber = new Fiber("finishPrepareFuture", groupConfig.fiberGroup,
                            finishPrepareFuture(f, raftIndex));
                    fiber.start();
                } else {
                    f.complete(raftIndex);
                }
            }

            @Override
            public void fail(Throwable ex) {
                f.completeExceptionally(ex);
            }
        });
    }

    private FiberFrame<Void> finishPrepareFuture(CompletableFuture<Long> f, long prepareIndex) {
        return new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                if (raftStatus.getLastApplied() < prepareIndex + 1) {
                    return gc.applyManager.applyFinishCond.await(100, this);
                }
                f.complete(prepareIndex);
                return Fiber.frameReturn();
            }
        };
    }

    private RaftMember findExistMember(int nodeId) {
        for (RaftMember m : raftStatus.members) {
            if (m.node.nodeId == nodeId) {
                return m;
            }
        }
        for (RaftMember m : raftStatus.observers) {
            if (m.node.nodeId == nodeId) {
                return m;
            }
        }
        for (RaftMember m : raftStatus.preparedMembers) {
            if (m.node.nodeId == nodeId) {
                return m;
            }
        }
        for (RaftMember m : raftStatus.preparedObservers) {
            if (m.node.nodeId == nodeId) {
                return m;
            }
        }
        return null;
    }

    private RaftMember createMember(RaftNodeEx node, RaftRole role) {
        RaftMember m = new RaftMember(node, groupConfig.fiberGroup);
        if (node.self) {
            m.ready = true;
            raftStatus.self = m;
            raftStatus.setRole(role);
            raftStatus.copyShareStatus();
        }
        return m;
    }

    public FrameCallResult doPrepare(long raftIndex, Set<Integer> newMemberIds, Set<Integer> newObserverIds) {
        ApplyConfigFrame f = new ApplyConfigFrame("(" + raftIndex + ") prepare config change",
                raftStatus.nodeIdOfMembers, raftStatus.nodeIdOfObservers, newMemberIds, newObserverIds);
        f.raftIndex = raftIndex;
        return Fiber.call(f, v -> Fiber.frameReturn());
    }

    public FrameCallResult doAbort(long raftIndex) {
        HashSet<Integer> preparedMemberIds = new HashSet<>(raftStatus.nodeIdOfPreparedMembers);
        if (preparedMemberIds.isEmpty()) {
            log.info("no pending config change, ignore abort, raftIndex={} groupId={}",
                    raftIndex, raftStatus.groupId);
            return Fiber.frameReturn();
        }
        ApplyConfigFrame f = new ApplyConfigFrame("(" + raftIndex + ") abort config change",
                raftStatus.nodeIdOfMembers, raftStatus.nodeIdOfObservers, emptySet(), emptySet());
        f.raftIndex = raftIndex;
        return Fiber.call(f, v -> Fiber.frameReturn());
    }

    public FrameCallResult doCommit(long raftIndex) {
        if (raftStatus.preparedMembers.isEmpty()) {
            log.warn("no prepared config change, ignore commit, raftIndex={}, groupId={}",
                    raftIndex, raftStatus.groupId);
            return Fiber.frameReturn();
        }
        ApplyConfigFrame f = new ApplyConfigFrame("(" + raftIndex + ") commit config change",
                raftStatus.nodeIdOfPreparedMembers, raftStatus.nodeIdOfPreparedObservers,
                emptySet(), emptySet());
        f.raftIndex = raftIndex;
        return Fiber.call(f, v -> Fiber.frameReturn());
    }

    public FiberFrame<Void> applyConfigFrame(String msg, Set<Integer> newMembers, Set<Integer> observerIds,
                                             Set<Integer> preparedMemberIds, Set<Integer> preparedObserverIds) {
        return new ApplyConfigFrame(msg, newMembers, observerIds, preparedMemberIds, preparedObserverIds);
    }

    private class ApplyConfigFrame extends FiberFrame<Void> {
        private final String msg;
        private final Set<Integer> members;
        private final Set<Integer> observers;
        private final Set<Integer> preparedMembers;
        private final Set<Integer> preparedObservers;

        private long raftIndex;

        ApplyConfigFrame(String msg, Set<Integer> members, Set<Integer> observers,
                         Set<Integer> preparedMembers, Set<Integer> preparedObservers) {
            this.msg = msg;
            this.members = members;
            this.observers = observers;
            this.preparedMembers = preparedMembers;
            this.preparedObservers = preparedObservers;
        }

        @Override
        public FrameCallResult execute(Void v) {
            VersionFactory.getInstance().fullFence();
            if (groupConfig.disableConfigChange) {
                log.warn("ignore apply config change, groupId={}", groupId);
                return Fiber.frameReturn();
            }
            if (raftStatus.nodeIdOfMembers.equals(members)
                    && raftStatus.nodeIdOfObservers.equals(observers)
                    && raftStatus.nodeIdOfPreparedMembers.equals(preparedMembers)
                    && raftStatus.nodeIdOfPreparedObservers.equals(preparedObservers)) {
                return Fiber.frameReturn();
            }
            log.info("{} begin, groupId={}, oldMember={}, oldObserver={}, oldPreparedMember={}, oldPreparedObserver={}," +
                            " newMember={}, newObserver={}, newPreparedMember={}, newPreparedObserver={}",
                    msg, groupId, raftStatus.nodeIdOfMembers, raftStatus.nodeIdOfObservers,
                    raftStatus.nodeIdOfPreparedMembers, raftStatus.nodeIdOfPreparedObservers,
                    members, observers, preparedMembers, preparedObservers);
            List<List<RaftNodeEx>> result = nodeManager.doApplyConfig(
                            raftStatus.nodeIdOfMembers, raftStatus.nodeIdOfObservers,
                            raftStatus.nodeIdOfPreparedMembers, raftStatus.nodeIdOfPreparedObservers,
                            members, observers, preparedMembers, preparedObservers);
            List<RaftNodeEx> newMemberNodes = result.get(0);
            List<RaftNodeEx> newObserverNodes = result.get(1);
            List<RaftNodeEx> newPreparedMemberNodes = result.get(2);
            List<RaftNodeEx> newPreparedObserverNodes = result.get(3);

            List<RaftMember> newMembers = createMembersInConfigChange(newMemberNodes);
            List<RaftMember> newObservers = createMembersInConfigChange(newObserverNodes);
            List<RaftMember> newPreparedMembers = createMembersInConfigChange(newPreparedMemberNodes);
            List<RaftMember> newPreparedObservers = createMembersInConfigChange(newPreparedObserverNodes);

            List<RaftMember> oldRepList = raftStatus.replicateList;

            raftStatus.members = newMembers;
            raftStatus.observers = newObservers;
            raftStatus.preparedMembers = newPreparedMembers;
            raftStatus.preparedObservers = newPreparedObservers;
            computeDuplicatedData(raftStatus);

            int selfNodeId = serverConfig.nodeId;
            int newLeaderId = -1;
            if (raftStatus.getCurrentLeader() != null) {
                newLeaderId = raftStatus.getCurrentLeader().node.nodeId;
                if (!raftStatus.nodeIdOfMembers.contains(newLeaderId)
                        && !raftStatus.nodeIdOfPreparedMembers.contains(newLeaderId)) {
                    newLeaderId = -1;
                }
            }
            if (newLeaderId == -1) {
                raftStatus.setCurrentLeader(null);
            }

            boolean selfIsMember = raftStatus.nodeIdOfMembers.contains(selfNodeId)
                    || raftStatus.nodeIdOfPreparedMembers.contains(selfNodeId);
            boolean selfIsObserver = raftStatus.nodeIdOfObservers.contains(selfNodeId)
                    || raftStatus.nodeIdOfPreparedObservers.contains(selfNodeId);
            RaftRole r = raftStatus.getRole();
            if (selfIsMember) {
                if (r != RaftRole.leader && r != RaftRole.follower) {
                    RaftUtil.changeToFollower(raftStatus, newLeaderId, "apply config change");
                }
            } else if (selfIsObserver) {
                if (r != RaftRole.observer) {
                    RaftUtil.changeToObserver(raftStatus, newLeaderId);
                }
            } else {
                if (r != RaftRole.none) {
                    RaftUtil.changeToNone(raftStatus, newLeaderId);
                }
            }
            if (r == RaftRole.leader) {
                List<RaftMember> newRepList = raftStatus.replicateList;
                for (RaftMember m : oldRepList) {
                    if (!newRepList.contains(m)) {
                        Fiber repFiber = replicateManager.replicateFibers.get(m.node.nodeId);
                        if (repFiber != null && !repFiber.isFinished()) {
                            RemoveLegacyFrame ff = new RemoveLegacyFrame(m, raftIndex, repFiber);
                            Fiber f = new Fiber("remove-legacy-" + m.node.nodeId,
                                    groupConfig.fiberGroup, ff, true);
                            f.start();
                        }
                    }
                }
            }
            raftStatus.copyShareStatus();

            gc.voteManager.cancelVote("config change");
            log.info("{} success, groupId={}", msg, groupId);
            return Fiber.frameReturn();
        }
    }

    private class RemoveLegacyFrame extends FiberFrame<Void> {

        private final RaftMember m;
        private final long raftIndex;
        private final Fiber repFiber;
        private final long startNanos;

        private RemoveLegacyFrame(RaftMember m, long raftIndex, Fiber repFiber) {
            this.m = m;
            this.raftIndex = raftIndex;
            this.repFiber = repFiber;
            this.startNanos = raftStatus.ts.nanoTime;
        }

        @Override
        public FrameCallResult execute(Void input) {
            // delay stop replicate to ensure the commit config change log is replicate to the legacy member.
            // otherwise the legacy member may start pre-vote and generate WARN logs in other members.
            // however this is not necessary.
            boolean timeout = raftStatus.ts.nanoTime - startNanos > 5000L * 1000 * 1000;
            if ((m.matchIndex >= raftIndex && m.repCommitIndexAcked >= raftIndex) || timeout) {
                return afterSleep();
            }
            return m.repDoneCondition.await(50, this);
        }

        private FrameCallResult afterSleep() {
            m.replicateEpoch++;
            return repFiber.join(this::afterJoin);
        }

        private FrameCallResult afterJoin(Void v) {
            replicateManager.replicateFibers.remove(m.node.nodeId);
            return Fiber.frameReturn();
        }
    }

    private List<RaftMember> createMembersInConfigChange(List<RaftNodeEx> nodes) {
        List<RaftMember> newMembers = new ArrayList<>();
        for (RaftNodeEx node : nodes) {
            RaftMember m = findExistMember(node.nodeId);
            if (m == null) {
                m = createMember(node, RaftRole.observer);
                m.nextIndex = raftStatus.lastLogIndex + 1;
            }
            newMembers.add(m);
        }
        return newMembers;
    }


    public boolean isValidCandidate(int nodeId) {
        RaftMember leader = raftStatus.getCurrentLeader();
        if (leader != null && leader.node.nodeId == nodeId) {
            return true;
        }
        return validCandidate(raftStatus, nodeId);
    }

    public static boolean validCandidate(RaftStatusImpl raftStatus, int nodeId) {
        return raftStatus.nodeIdOfMembers.contains(nodeId)
                || raftStatus.nodeIdOfPreparedMembers.contains(nodeId);
    }

    public void transferLeadership(int nodeId, CompletableFuture<Void> f, DtTime deadline) {
        if (!groupConfig.fiberGroup.fireFiber("transfer-leader",
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
            if (raftStatus.transferLeaderCondition != null) {
                f.completeExceptionally(new RaftException("transfer leader in progress"));
                return Fiber.frameReturn();
            }
            raftStatus.transferLeaderCondition = groupConfig.fiberGroup.newCondition("transferLeader");
            return checkBeforeTransferLeader(null);
        }

        private FrameCallResult checkBeforeTransferLeader(Void v) {
            if (raftStatus.getRole() != RaftRole.leader) {
                f.completeExceptionally(new NotLeaderException(raftStatus.getCurrentLeaderNode()));
                RaftUtil.clearTransferLeaderCondition(raftStatus);
                return Fiber.frameReturn();
            }
            RaftMember newLeader = null;
            for (RaftMember m : raftStatus.members) {
                if (m.node.nodeId == nodeId) {
                    newLeader = m;
                    break;
                }
            }
            if (newLeader == null) {
                for (RaftMember m : raftStatus.preparedMembers) {
                    if (m.node.nodeId == nodeId) {
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

            boolean lastLogCommit = raftStatus.commitIndex == raftStatus.lastLogIndex;
            boolean newLeaderHasLastLog = newLeader.matchIndex == raftStatus.lastLogIndex;

            if (newLeader.ready && lastLogCommit && newLeaderHasLastLog) {
                PbIntWritePacket req = new PbIntWritePacket(Commands.RAFT_QUERY_STATUS, groupId);
                CompletableFuture<ReadPacket<QueryStatusResp>> queryFuture = new CompletableFuture<>();
                client.sendRequest(newLeader.node.peer, req, QueryStatusResp.DECODER,
                        new DtTime(3, TimeUnit.SECONDS), RpcCallback.fromFuture(queryFuture));
                RaftNodeEx newLeaderNode = newLeader.node;
                queryFuture.whenCompleteAsync((resp, ex) -> {
                    if (ex != null) {
                        f.completeExceptionally(ex);
                    } else {
                        execTransferLeader(newLeaderNode, resp, f);
                    }
                }, groupConfig.fiberGroup.getExecutor());
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
            if (!s.members.equals(raftStatus.nodeIdOfMembers)
                    || !s.observers.equals(raftStatus.nodeIdOfObservers)
                    || !s.preparedMembers.equals(raftStatus.nodeIdOfPreparedMembers)
                    || !s.preparedObservers.equals(raftStatus.nodeIdOfPreparedObservers)) {
                log.error("config not match, groupId={}", groupId);
                finalFuture.completeExceptionally(new RaftException("config not match"));
                return;
            }
            RaftUtil.clearTransferLeaderCondition(raftStatus);
            RaftUtil.changeToFollower(raftStatus, newLeader.nodeId, "transfer leader");
            TransferLeaderReq req = new TransferLeaderReq();
            req.term = raftStatus.currentTerm;
            req.logIndex = raftStatus.lastLogIndex;
            req.oldLeaderId = serverConfig.nodeId;
            req.newLeaderId = newLeader.nodeId;
            req.groupId = groupId;
            SimpleWritePacket frame = new SimpleWritePacket(req);
            frame.command = Commands.RAFT_TRANSFER_LEADER;
            DecoderCallbackCreator<Void> dc = DecoderCallbackCreator.VOID_DECODE_CALLBACK_CREATOR;
            client.sendRequest(newLeader.peer, frame, dc, new DtTime(5, TimeUnit.SECONDS),
                    (result, ex) -> {
                        if (ex == null) {
                            log.info("transfer leader success, groupId={}", groupId);
                            finalFuture.complete(null);
                        } else {
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
