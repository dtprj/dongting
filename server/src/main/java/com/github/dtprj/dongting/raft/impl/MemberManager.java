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
import com.github.dtprj.dongting.common.MutableBool;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.common.VersionFactory;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.fiber.SimpleFrame;
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
import com.github.dtprj.dongting.raft.RaftTimeoutException;
import com.github.dtprj.dongting.raft.rpc.TransferLeaderReq;
import com.github.dtprj.dongting.raft.server.NotLeaderException;
import com.github.dtprj.dongting.raft.server.RaftCallback;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftReqData;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.store.LogHeader;

import java.util.ArrayList;
import java.util.Collections;
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
    private final Runnable syncConfigTask;
    private final int groupId;
    private final RaftGroupConfigEx groupConfig;

    private ReplicateManager replicateManager;
    private NodeManager nodeManager;

    private final CompletableFuture<Void> pingReadyFuture;
    private final int startReadyQuorum;

    int daemonSleepInterval = 1000;

    public MemberManager(NioClient client, GroupComponents gc, Runnable syncConfigTask) {
        this.client = client;
        this.gc = gc;
        this.serverConfig = gc.serverConfig;
        this.groupConfig = gc.groupConfig;
        this.raftStatus = gc.raftStatus;
        this.syncConfigTask = syncConfigTask;
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
            RaftMember m = createMember(nodeId, RaftRole.follower);
            raftStatus.members.add(m);
        }
        if (!raftStatus.nodeIdOfObservers.isEmpty()) {
            List<RaftMember> observers = new ArrayList<>();
            for (int nodeId : raftStatus.nodeIdOfObservers) {
                RaftMember m = createMember(nodeId, RaftRole.observer);
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
            createMember(serverConfig.nodeId, RaftRole.none);
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
            memberIds.add(m.nodeId);
        }
        for (RaftMember m : raftStatus.observers) {
            replicateList.add(m);
            observerIds.add(m.nodeId);
        }
        for (RaftMember m : raftStatus.preparedMembers) {
            replicateList.add(m);
            jointMemberIds.add(m.nodeId);
        }
        for (RaftMember m : raftStatus.preparedObservers) {
            jointObserverIds.add(m.nodeId);
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
        FiberFrame<Void> fiberFrame = new SimpleFrame<>("raftPing", frame -> {
            try {
                if (frame.isGroupShouldStopPlain()) {
                    return Fiber.frameReturn();
                }
                ensureRaftMemberStatus();
                replicateManager.tryStartReplicateFibers();
                return Fiber.sleep(daemonSleepInterval, frame);
            } catch (Throwable e) {
                throw Fiber.fatal(e);
            }
        });
        // daemon fiber
        return new Fiber("raftPing", groupConfig.fiberGroup, fiberFrame).setDaemon(true);
    }

    public void ensureRaftMemberStatus() {
        List<RaftMember> replicateList = raftStatus.replicateList;
        for (RaftMember member : replicateList) {
            check(member);
        }
    }

    private void check(RaftMember member) {
        if (member.self) {
            return;
        }
        RaftNodeEx node = member.node;
        if (node == null) {
            // try to resolve the node definition, it may be added after this member created
            node = nodeManager.retainNodeEx(member.nodeId);
            if (node == null) {
                log.error("node definition not exist: groupId={}, nodeId={}", groupId, member.nodeId);
                return;
            }
            member.node = node;
            log.info("node definition resolved: groupId={}, nodeId={}", groupId, member.nodeId);
        }
        NodeStatus nodeStatus = node.status;
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

            PbIntWritePacket f = new PbIntWritePacket(Commands.RAFT_QUERY_STATUS, groupId);

            Executor executor = groupConfig.fiberGroup.getExecutor();
            RpcCallback<QueryStatusResp> callback = (result, ex) -> executor.execute(
                    () -> processPingResult(raftNodeEx, member, result, ex, nodeEpochWhenStartPing));
            client.sendRequest(raftNodeEx.peer, f, QueryStatusResp.DECODER, timeout, callback);
        } catch (Exception e) {
            log.error("raft ping error, remote={}", raftNodeEx.hostPort, e);
            member.pinging = false;
        }
    }

    private void processPingResult(RaftNodeEx raftNodeEx, RaftMember member,
                                   ReadPacket<QueryStatusResp> rf, Throwable ex, int nodeEpochWhenStartPing) {
        member.pinging = false;
        try {
            if (ex != null) {
                log.warn("raft ping fail, remote={}", raftNodeEx.hostPort, ex);
                setReady(member, false);
                return;
            }
            QueryStatusResp s = rf.getBody();
            NodeStatus currentNodeStatus = raftNodeEx.status;
            if (!currentNodeStatus.isReady() || nodeEpochWhenStartPing != currentNodeStatus.getEpoch()) {
                log.warn("current node status not match. id={}, remoteHost={}, nodeReady={}, nodeEpoch={}, pingEpoch={}",
                        s.nodeId, raftNodeEx.hostPort, currentNodeStatus.isReady(),
                        currentNodeStatus.getEpoch(), nodeEpochWhenStartPing);
                setReady(member, false);
                return;
            }
            if (s.nodeId != member.nodeId) {
                log.error("raft ping fail, nodeId not match, expect={}, actual={}, remote={}",
                        member.nodeId, s.nodeId, raftNodeEx.hostPort);
                setReady(member, false);
                return;
            }
            // don't use groupReady here: it depends on election, while member readiness
            // is a prerequisite of pre-vote, so using it would deadlock group startup
            if (!s.isInitFinished() || s.isStopped()) {
                log.warn("raft ping fail, remote group not ready. remote={}, initFinished={}, " +
                                "shouldStop={}, fatalError={}, finished={}",
                        raftNodeEx.hostPort, s.isInitFinished(), s.isShouldStop(),
                        s.isFatalError(), s.isFinished());
                setReady(member, false);
                return;
            }
            log.info("raft ping success, id={}, remote={}", s.nodeId, raftNodeEx.hostPort);
            setReady(member, true);
            member.nodeEpoch = nodeEpochWhenStartPing;
            replicateManager.tryStartReplicateFibers();
        } catch (Exception e) {
            log.error("process ping result error", e);
            setReady(member, false);
        }
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
                                                        CompletableFuture<Long> f, DtTime timeout) {
        return new SimpleFrame<>("prepareJointConsensus", frame -> {
            if (!raftStatus.nodeIdOfMembers.equals(members)
                    || !raftStatus.nodeIdOfObservers.equals(observers)) {
                log.error("old members or observers not match, groupId={}", groupId);
                f.completeExceptionally(new RaftException("old members or observers not match"));
                return Fiber.frameReturn();
            }
            nodeManager.checkLeaderPrepare(newMemberNodes, newObserverNodes);
            leaderConfigChange(LogHeader.TYPE_PREPARE_CONFIG_CHANGE,
                    getInputData(newMemberNodes, newObserverNodes), f, timeout);
            return Fiber.frameReturn();
        }, ex -> {
            log.error("leader prepare joint consensus error", ex);
            f.completeExceptionally(ex);
            return Fiber.frameReturn();
        });
    }

    public FiberFrame<Void> leaderAbortJointConsensus(CompletableFuture<Long> f) {
        return new SimpleFrame<>("abortJointConsensus", frame -> {
            leaderConfigChange(LogHeader.TYPE_DROP_CONFIG_CHANGE, null, f, null);
            return Fiber.frameReturn();
        }, ex -> {
            log.error("leader abort joint consensus error", ex);
            f.completeExceptionally(ex);
            return Fiber.frameReturn();
        });
    }

    public FiberFrame<Void> leaderCommitJointConsensus(CompletableFuture<Long> finalFuture, long prepareIndex,
                                                       DtTime timeout) {
        MutableBool started = new MutableBool(false);
        return new SimpleFrame<>("leaderCommit", frame -> {
            if (frame.isGroupShouldStopPlain()) {
                finalFuture.completeExceptionally(new RaftException("raft group is stopping"));
                return Fiber.frameReturn();
            }
            // also rejects a stale commit if a concurrent abort or commit advanced the index
            // while waiting for members ready
            if (prepareIndex != raftStatus.lastConfigChangeIndex) {
                log.error("prepareIndex not match. prepareIndex={}, lastConfigChangeIndex={}",
                        prepareIndex, raftStatus.lastConfigChangeIndex);
                finalFuture.completeExceptionally(new RaftException("prepareIndex not match. prepareIndex="
                        + prepareIndex + ", lastConfigChangeIndex=" + raftStatus.lastConfigChangeIndex));
                return Fiber.frameReturn();
            }
            if (!started.value) {
                started.value = true;
                // resume re-enters execute, so the checks above run again after members ready
                return Fiber.call(new MembersReadyFrame(prepareIndex, timeout), frame);
            }
            // the commit log is generated only after rwQuorum of old config members applied and
            // persisted the prepare log, so a restarted node can't be elected with a stale config
            leaderConfigChange(LogHeader.TYPE_COMMIT_CONFIG_CHANGE, null, finalFuture, null);
            return Fiber.frameReturn();
        }, ex -> {
            log.error("leader commit joint consensus error", ex);
            finalFuture.completeExceptionally(ex);
            return Fiber.frameReturn();
        });
    }

    /**
     * Wait until rwQuorum of old config members (including self) have applied and persisted the
     * prepare log. Members may be transiently not ready because the status file flush lags the
     * apply, so not ready members are re-queried until the deadline.
     */
    private class MembersReadyFrame extends FiberFrame<Void> {
        private final long prepareIndex;
        private final DtTime timeout;
        private FiberCondition respCondition;
        private int total;
        private int ready;
        private int quorum;
        private boolean selfReady;
        private ArrayList<RaftMember> retryMembers = new ArrayList<>();

        MembersReadyFrame(long prepareIndex, DtTime timeout) {
            this.prepareIndex = prepareIndex;
            this.timeout = timeout;
        }

        @Override
        public FrameCallResult execute(Void v) {
            if (isGroupShouldStopPlain()) {
                throw new RaftException("raft group is stopping");
            }
            if (total == 0) {
                respCondition = groupConfig.fiberGroup.newCondition("membersReadyCheck-" + groupId);
                List<RaftMember> members = raftStatus.members;
                quorum = RaftUtil.getRwQuorum(members.size());
                total = members.size();
                for (RaftMember m : members) {
                    if (!m.self) {
                        retryMembers.add(m);
                    }
                }
            }
            // self readiness is checked each round: the status file flush may lag the apply.
            // self counts only if this node is a member of the old config: a prepared-only leader
            // must not dilute the rwQuorum requirement
            if (!selfReady && raftStatus.nodeIdOfMembers.contains(serverConfig.nodeId)
                    && raftStatus.persistedCommitIndex >= prepareIndex
                    && raftStatus.getLastApplied() >= prepareIndex) {
                selfReady = true;
                ready++;
            }
            if (ready >= quorum) {
                return Fiber.frameReturn();
            }
            if (timeout.isTimeout(raftStatus.ts)) {
                log.error("members not ready for prepare log, groupId={}, prepareIndex={}, ready={}, total={}",
                        groupId, prepareIndex, ready, total);
                throw new RaftTimeoutException("rwQuorum of members not persisted/applied the prepare log, try later");
            }
            if (!retryMembers.isEmpty()) {
                ArrayList<RaftMember> list = retryMembers;
                retryMembers = new ArrayList<>();
                for (RaftMember m : list) {
                    sendQuery(m);
                }
            }
            return respCondition.await(100, getFiberGroup().shouldStopCondition, this);
        }

        private void onResp(RaftMember m, boolean memberReady) {
            if (memberReady) {
                ready++;
            } else {
                retryMembers.add(m);
            }
        }

        private void sendQuery(RaftMember m) {
            if (m.node == null) {
                onResp(m, false);
                return;
            }
            PbIntWritePacket req = new PbIntWritePacket(Commands.RAFT_QUERY_STATUS, groupId);
            CompletableFuture<ReadPacket<QueryStatusResp>> f = new CompletableFuture<>();
            try {
                client.sendRequest(m.node.peer, req, QueryStatusResp.DECODER,
                        new DtTime(3, TimeUnit.SECONDS), RpcCallback.fromFuture(f));
            } catch (Throwable e) {
                log.warn("send query status fail, groupId={}, remote={}", groupId, m.nodeId, e);
                onResp(m, false);
                return;
            }
            f.whenCompleteAsync((resp, ex) -> {
                boolean memberReady = false;
                if (ex == null) {
                    QueryStatusResp s = resp.getBody();
                    if (s.isStopped()) {
                        log.error("member group is stopped, groupId={}, remote={}, shouldStop={}, fatalError={}, finished={}",
                                groupId, m.nodeId, s.isShouldStop(), s.isFatalError(), s.isFinished());
                    } else {
                        memberReady = s.persistedCommitIndex >= prepareIndex && s.lastApplied >= prepareIndex;
                        log.info("members ready check receive member status, groupId={}, remote={}, "
                                        + "memberReady={}, persistedCommitIndex={}, lastApplied={}, prepareIndex={}",
                                groupId, m.nodeId, memberReady, s.persistedCommitIndex, s.lastApplied, prepareIndex);
                    }
                } else {
                    log.warn("query status fail, groupId={}, remote={}", groupId, m.nodeId, ex);
                }
                onResp(m, memberReady);
                respCondition.signal();
            }, groupConfig.fiberGroup.getExecutor());
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

    private void leaderConfigChange(int type, byte[] data, CompletableFuture<Long> f, DtTime prepareTimeout) {
        if (raftStatus.getRole() != RaftRole.leader) {
            String stageStr;
            switch (type) {
                case LogHeader.TYPE_PREPARE_CONFIG_CHANGE:
                    stageStr = "prepare";
                    break;
                case LogHeader.TYPE_COMMIT_CONFIG_CHANGE:
                    stageStr = "commit";
                    break;
                case LogHeader.TYPE_DROP_CONFIG_CHANGE:
                    stageStr = "abort";
                    break;
                default:
                    throw new IllegalArgumentException(String.valueOf(type));
            }
            log.error("leader config change {}, not leader, role={}, groupId={}",
                    stageStr, raftStatus.getRole(), groupId);
            f.completeExceptionally(new NotLeaderException(raftStatus.getCurrentLeaderNode()));
            return;
        }
        RaftCallback c = new RaftCallback() {
            @Override
            public void success(long raftIndex, Object nullResult) {
                if (type == LogHeader.TYPE_PREPARE_CONFIG_CHANGE) {
                    // When prepareIndex applied, the prepared member may still not replicate to prepareIndex
                    // (the commit manager does not check prepare members since they are not active).
                    // Issue a heartbeat and wait to prepareIndex + 1 to be applied, so we can be sure that
                    // the prepared members are ready.
                    gc.linearTaskRunner.issueHeartBeat();
                    Fiber fiber = new Fiber("finishPrepareFuture", groupConfig.fiberGroup,
                            finishPrepareFuture(f, raftIndex, prepareTimeout)).setDaemon(true);
                    fiber.start();
                } else {
                    f.complete(raftIndex);
                }
            }

            @Override
            public void fail(Throwable ex) {
                f.completeExceptionally(ex);
            }
        };
        ByteArray ba = data == null ? null : new ByteArray(data);
        RaftReqData reqData = RaftReqData.build(type, 0, ba);
        RaftTask task = new RaftTask(reqData, null, data, null, false, c);
        // use runner fiber to execute to avoid race condition
        gc.linearTaskRunner.submitRaftTaskInBizThread(task);
    }

    private FiberFrame<Void> finishPrepareFuture(CompletableFuture<Long> f, long prepareIndex, DtTime timeout) {
        return new SimpleFrame<>("finishPrepareFuture", frame -> {
            if (frame.isGroupShouldStopPlain()) {
                f.completeExceptionally(new RaftException("raft group is stopping"));
                return Fiber.frameReturn();
            }
            if (raftStatus.getLastApplied() < prepareIndex + 1) {
                if (timeout.isTimeout(raftStatus.ts)) {
                    log.error("prepare log not applied before timeout, groupId={}, prepareIndex={}",
                            groupId, prepareIndex);
                    throw new RaftTimeoutException("prepare log not applied before timeout, try later");
                }
                return gc.applyManager.applyFinishCond.await(100,
                        frame.getFiberGroup().shouldStopCondition, frame);
            }
            // wait members ready before the prepare request returns, so a following commit
            // request passes the ready check immediately: readiness is monotonic
            return Fiber.call(new MembersReadyFrame(prepareIndex, timeout), v -> {
                f.complete(prepareIndex);
                return Fiber.frameReturn();
            });
        }, ex -> {
            f.completeExceptionally(ex);
            return Fiber.frameReturn();
        });
    }

    private RaftMember findExistMember(int nodeId) {
        for (RaftMember m : raftStatus.members) {
            if (m.nodeId == nodeId) {
                return m;
            }
        }
        for (RaftMember m : raftStatus.observers) {
            if (m.nodeId == nodeId) {
                return m;
            }
        }
        for (RaftMember m : raftStatus.preparedMembers) {
            if (m.nodeId == nodeId) {
                return m;
            }
        }
        for (RaftMember m : raftStatus.preparedObservers) {
            if (m.nodeId == nodeId) {
                return m;
            }
        }
        return null;
    }

    private RaftMember createMember(int nodeId, RaftRole role) {
        boolean self = nodeId == serverConfig.nodeId;
        RaftNodeEx node = nodeManager.retainNodeEx(nodeId);
        if (node == null) {
            log.error("node definition not exist: groupId={}, nodeId={}", groupId, nodeId);
        }
        RaftMember m = new RaftMember(nodeId, self, node, groupConfig.fiberGroup);
        if (self) {
            m.ready = true;
            raftStatus.self = m;
            raftStatus.setRole(role);
            raftStatus.copyShareStatus();
        }
        return m;
    }

    // invoked when the raft group is shutting down
    public void releaseAllNodes() {
        HashSet<RaftMember> all = new HashSet<>();
        if (raftStatus.members != null) {
            all.addAll(raftStatus.members);
        }
        if (raftStatus.observers != null) {
            all.addAll(raftStatus.observers);
        }
        if (raftStatus.preparedMembers != null) {
            all.addAll(raftStatus.preparedMembers);
        }
        if (raftStatus.preparedObservers != null) {
            all.addAll(raftStatus.preparedObservers);
        }
        ArrayList<RaftNodeEx> nodes = new ArrayList<>(all.size());
        for (RaftMember m : all) {
            if (m.node != null) {
                nodes.add(m.node);
            }
        }
        nodeManager.releaseNodeEx(nodes);
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
        return Fiber.call(f, this::afterCommit);
    }

    private FrameCallResult afterCommit(Void v) {
        syncConfigTask.run();
        return Fiber.frameReturn();
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

            HashSet<RaftMember> oldMembers = new HashSet<>();
            oldMembers.addAll(raftStatus.members);
            oldMembers.addAll(raftStatus.observers);
            oldMembers.addAll(raftStatus.preparedMembers);
            oldMembers.addAll(raftStatus.preparedObservers);

            List<RaftMember> newMembers = createMembersInConfigChange(members);
            List<RaftMember> newObservers = createMembersInConfigChange(observers);
            List<RaftMember> newPreparedMembers = createMembersInConfigChange(preparedMembers);
            List<RaftMember> newPreparedObservers = createMembersInConfigChange(preparedObservers);

            List<RaftMember> oldRepList = raftStatus.replicateList;

            raftStatus.members = newMembers;
            raftStatus.observers = newObservers;
            raftStatus.preparedMembers = newPreparedMembers;
            raftStatus.preparedObservers = newPreparedObservers;
            computeDuplicatedData(raftStatus);

            // release nodes of members which are dropped by this config change
            HashSet<RaftMember> retainedMembers = new HashSet<>();
            retainedMembers.addAll(newMembers);
            retainedMembers.addAll(newObservers);
            retainedMembers.addAll(newPreparedMembers);
            retainedMembers.addAll(newPreparedObservers);
            ArrayList<RaftNodeEx> droppedNodes = new ArrayList<>();
            for (RaftMember m : oldMembers) {
                if (!retainedMembers.contains(m) && m.node != null) {
                    droppedNodes.add(m.node);
                }
            }
            nodeManager.releaseNodeEx(droppedNodes);

            int selfNodeId = serverConfig.nodeId;
            int newLeaderId = -1;
            if (raftStatus.getCurrentLeader() != null) {
                newLeaderId = raftStatus.getCurrentLeader().nodeId;
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
                        Pair<RaftMember, Fiber> repTask = raftStatus.replicateTasks.get(m.nodeId);
                        if (repTask != null && !repTask.getRight().isFinished()) {
                            FiberFrame<Void> ff = createRemoveLegacyFrame(raftIndex, repTask);
                            Fiber f = new Fiber("remove-legacy-" + m.nodeId,
                                    groupConfig.fiberGroup, ff).setDaemon(true);
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

    private FiberFrame<Void> createRemoveLegacyFrame(long raftIndex, Pair<RaftMember, Fiber> repTask) {
        RaftMember m = repTask.getLeft();
        Fiber repFiber = repTask.getRight();
        long startNanos = raftStatus.ts.nanoTime;
        return new SimpleFrame<>("removeLegacy", frame -> {
            // delay stop replicate to ensure the commit config change log is replicate to the legacy member.
            // otherwise the legacy member may start pre-vote and generate WARN logs in other members.
            // however this is not necessary.
            if (m.matchIndex >= raftIndex && m.repCommitIndexAcked >= raftIndex) {
                return tryStopRepFiber(m, repFiber, "finished");
            } else if (raftStatus.ts.nanoTime - startNanos > 5000L * 1000 * 1000) {
                return tryStopRepFiber(m, repFiber, "timeout");
            }
            return m.repDoneCondition.await(50, frame);
        });
    }

    private FrameCallResult tryStopRepFiber(RaftMember m, Fiber repFiber, String status) {
        m.replicateEpoch++;
        log.info("legacy task {}, wait it stop. node={}", status, m.nodeId);
        return repFiber.join(v -> {
            int n = m.nodeId;
            Pair<RaftMember, Fiber> existTask = raftStatus.replicateTasks.get(n);
            if (existTask == null) {
                log.error("legacy task not exists. node={} ", n);
            } else if (existTask.getLeft() == m) {
                log.info("legacy task removed, node={} ", n);
                raftStatus.replicateTasks.remove(n);
            } else {
                log.error("legacy task not match. node={}", n);
            }
            return Fiber.frameReturn();
        });
    }

    private List<RaftMember> createMembersInConfigChange(Set<Integer> nodeIds) {
        List<RaftMember> newMembers = new ArrayList<>(nodeIds.size());
        for (int nodeId : nodeIds) {
            RaftMember m = findExistMember(nodeId);
            if (m == null) {
                m = createMember(nodeId, RaftRole.observer);
                m.nextIndex = raftStatus.lastLogIndex + 1;
            }
            newMembers.add(m);
        }
        return newMembers;
    }


    public boolean isValidCandidate(int nodeId) {
        RaftMember leader = raftStatus.getCurrentLeader();
        if (leader != null && leader.nodeId == nodeId) {
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
                new TranferLeaderFiberFrame(nodeId, f, deadline, false, 0, null))) {
            f.completeExceptionally(new RaftException("fire transfer leader fiber failed"));
        }
    }

    private class TranferLeaderFiberFrame extends FiberFrame<Void> {

        private final int nodeId;
        private final CompletableFuture<Void> f;
        private final DtTime deadline;
        private final boolean reentry;
        private long lastLogMillis;
        private FiberCondition condition;

        TranferLeaderFiberFrame(int nodeId, CompletableFuture<Void> f, DtTime deadline, boolean reentry,
                                long lastLogMillis, FiberCondition condition) {
            this.nodeId = nodeId;
            this.f = f;
            this.deadline = deadline;
            this.reentry = reentry;
            this.lastLogMillis = lastLogMillis;
            this.condition = condition;
        }

        @Override
        protected FrameCallResult handle(Throwable ex) {
            clearMyCondition();
            f.completeExceptionally(ex);
            return Fiber.frameReturn();
        }

        // the condition may be cleared by resetStatus on role change, or taken by another transfer request
        private void clearMyCondition() {
            if (raftStatus.transferLeaderCondition == condition) {
                RaftUtil.clearTransferLeaderCondition(raftStatus);
            }
        }

        @Override
        public FrameCallResult execute(Void input) {
            if (reentry) {
                // retry after target apply lag, wait a while before re-check
                return Fiber.sleep(50, this::checkBeforeTransferLeader);
            }
            if (raftStatus.transferLeaderCondition != null) {
                f.completeExceptionally(new RaftException("transfer leader in progress"));
                return Fiber.frameReturn();
            }
            condition = groupConfig.fiberGroup.newCondition("transferLeader");
            raftStatus.transferLeaderCondition = condition;
            return checkBeforeTransferLeader(null);
        }

        // returns true if the transfer is terminated: the future is completed,
        // and the condition is cleared if it is still owned by this chain
        private boolean checkTerminated() {
            if (raftStatus.transferLeaderCondition != condition) {
                f.completeExceptionally(new RaftException("transfer leader interrupted"));
                return true;
            }
            if (isGroupShouldStopPlain()) {
                f.completeExceptionally(new RaftException("raft group is stopping"));
                clearMyCondition();
                return true;
            }
            if (f.isCancelled()) {
                clearMyCondition();
                return true;
            }
            if (deadline.isTimeout()) {
                f.completeExceptionally(new RaftException("transfer leader timeout"));
                clearMyCondition();
                return true;
            }
            if (raftStatus.getRole() != RaftRole.leader) {
                f.completeExceptionally(new NotLeaderException(raftStatus.getCurrentLeaderNode()));
                clearMyCondition();
                return true;
            }
            return false;
        }

        private FrameCallResult checkBeforeTransferLeader(Void v) {
            if (checkTerminated()) {
                return Fiber.frameReturn();
            }
            RaftMember newLeader = null;
            for (RaftMember m : raftStatus.members) {
                if (m.nodeId == nodeId) {
                    newLeader = m;
                    break;
                }
            }
            if (newLeader == null) {
                for (RaftMember m : raftStatus.preparedMembers) {
                    if (m.nodeId == nodeId) {
                        newLeader = m;
                        break;
                    }
                }
            }
            if (newLeader == null) {
                f.completeExceptionally(new RaftException("nodeId not found: " + nodeId));
                clearMyCondition();
                return Fiber.frameReturn();
            }

            boolean lastLogCommit = raftStatus.commitIndex == raftStatus.lastLogIndex;
            boolean newLeaderHasLastLog = newLeader.matchIndex == raftStatus.lastLogIndex;

            if (newLeader.ready && lastLogCommit && newLeaderHasLastLog) {
                RaftNodeEx node = newLeader.node;
                if (node == null) {
                    f.completeExceptionally(new RaftException("node definition not exist: " + nodeId));
                    clearMyCondition();
                    return Fiber.frameReturn();
                }
                PbIntWritePacket req = new PbIntWritePacket(Commands.RAFT_QUERY_STATUS, groupId);
                CompletableFuture<ReadPacket<QueryStatusResp>> queryFuture = new CompletableFuture<>();
                client.sendRequest(node.peer, req, QueryStatusResp.DECODER,
                        new DtTime(3, TimeUnit.SECONDS), RpcCallback.fromFuture(queryFuture));
                queryFuture.whenCompleteAsync((resp, ex) -> afterQuery(node, resp, ex),
                        groupConfig.fiberGroup.getExecutor());
                return Fiber.frameReturn();
            } else {
                return Fiber.sleep(1, this::checkBeforeTransferLeader);
            }
        }

        // run in fiber group thread from the executor callback, not in the transfer fiber.
        // the rpc timeout guarantees this method is invoked
        private void afterQuery(RaftNodeEx newLeaderNode, ReadPacket<QueryStatusResp> resp, Throwable ex) {
            try {
                if (checkTerminated()) {
                    return;
                }
                if (ex != null) {
                    clearMyCondition();
                    f.completeExceptionally(ex);
                    return;
                }
                QueryStatusResp s = resp.getBody();
                if (s.isStopped()) {
                    log.error("target group is stopped, groupId={}, nodeId={}, shouldStop={}, fatalError={}, finished={}",
                            groupId, nodeId, s.isShouldStop(), s.isFatalError(), s.isFinished());
                    f.completeExceptionally(new RaftException("target group is stopped: " + nodeId));
                    clearMyCondition();
                    return;
                }
                if (!s.members.equals(raftStatus.nodeIdOfMembers)
                        || !s.observers.equals(raftStatus.nodeIdOfObservers)
                        || !s.preparedMembers.equals(raftStatus.nodeIdOfPreparedMembers)
                        || !s.preparedObservers.equals(raftStatus.nodeIdOfPreparedObservers)) {
                    log.error("config not match, groupId={}", groupId);
                    f.completeExceptionally(new RaftException("config not match"));
                    clearMyCondition();
                    return;
                }
                if (s.lastApplied < raftStatus.lastLogIndex) {
                    if (raftStatus.ts.wallClockMillis - lastLogMillis > 1000L) {
                        log.info("new leader apply lag, wait and retry. groupId={}, nodeId={}, "
                                        + "itsLastApplied={}, lastLogIndex={}",
                                groupId, nodeId, s.lastApplied, raftStatus.lastLogIndex);
                        lastLogMillis = raftStatus.ts.wallClockMillis;
                    }
                    if (!groupConfig.fiberGroup.fireFiber("transfer-leader-retry",
                            new TranferLeaderFiberFrame(nodeId, f, deadline, true, lastLogMillis, condition))) {
                        clearMyCondition();
                        f.completeExceptionally(new RaftException("fire retry fiber failed"));
                    }
                    return;
                }
                execTransferLeader(newLeaderNode, f);
            } catch (Throwable e) {
                log.error("transfer leader process query resp fail, groupId={}", groupId, e);
                clearMyCondition();
                f.completeExceptionally(e);
            }
        }
    }

    private void execTransferLeader(RaftNodeEx newLeader, CompletableFuture<Void> finalFuture) {
        try {
            // the condition is cleared by changeToFollower -> resetStatus
            RaftUtil.changeToFollower(raftStatus, newLeader.nodeId, "transfer leader");
            TransferLeaderReq req = new TransferLeaderReq();
            req.term = raftStatus.currentTerm;
            req.logIndex = raftStatus.lastLogIndex;
            req.oldLeaderId = serverConfig.nodeId;
            req.newLeaderId = newLeader.nodeId;
            req.groupId = groupId;
            req.raftClusterId = raftStatus.raftClusterId;
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
