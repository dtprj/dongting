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
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.client.RaftException;
import com.github.dtprj.dongting.raft.server.NotLeaderException;
import com.github.dtprj.dongting.raft.server.RaftGroup;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftOutput;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.sm.SnapshotManager;
import com.github.dtprj.dongting.raft.sm.StateMachine;
import com.github.dtprj.dongting.raft.store.RaftLog;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author huangli
 */
public class RaftGroupImpl extends RaftGroup {
    private static final DtLog log = DtLogs.getLogger(RaftGroupImpl.class);
    private final Timestamp readTimestamp = new Timestamp();

    private RaftServerConfig serverConfig;
    private RaftGroupConfig groupConfig;
    private RaftGroupThread raftGroupThread;
    private RaftStatusImpl raftStatus;
    private MemberManager memberManager;
    private VoteManager voteManager;
    private Raft raft;
    private RaftExecutor raftExecutor;
    private ApplyManager applyManager;
    private EventBus eventBus;
    private SnapshotManager snapshotManager;

    private NodeManager nodeManager;
    private PendingStat serverStat;

    public RaftGroupImpl() {
        super();
    }

    private void checkStatus() {
        if (raftStatus.isStop()) {
            throw new RaftException("raft server is not running");
        }
    }

    @Override
    public int getGroupId() {
        return groupConfig.getGroupId();
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public CompletableFuture<RaftOutput> submitLinearTask(RaftInput input) throws RaftException {
        Objects.requireNonNull(input);
        Objects.requireNonNull(input.getBody());
        RaftStatusImpl raftStatus = this.raftStatus;
        if (raftStatus.isError()) {
            throw new RaftException("raft status is error");
        }
        if (raftStatus.isStop()) {
            throw new RaftException("raft group thread is stop");
        }
        int currentPendingWrites = (int) PendingStat.PENDING_REQUESTS.getAndAddRelease(serverStat, 1);
        if (currentPendingWrites >= serverConfig.getMaxPendingWrites()) {
            String msg = "submitRaftTask failed: too many pending writes, currentPendingWrites=" + currentPendingWrites;
            log.warn(msg);
            PendingStat.PENDING_REQUESTS.getAndAddRelease(serverStat, -1);
            throw new RaftException(msg);
        }
        int size = input.getFlowControlSize();
        long currentPendingWriteBytes = (long) PendingStat.PENDING_BYTES.getAndAddRelease(serverStat, size);
        if (currentPendingWriteBytes >= serverConfig.getMaxPendingWriteBytes()) {
            String msg = "too many pending write bytes,currentPendingWriteBytes="
                    + currentPendingWriteBytes + ", currentRequestBytes=" + size;
            log.warn(msg);
            PendingStat.PENDING_BYTES.getAndAddRelease(serverStat, -size);
            throw new RaftException(msg);
        }
        CompletableFuture f = raftGroupThread.submitRaftTask(input);
        registerCallback(f, size);
        return f;
    }

    private void registerCallback(CompletableFuture<?> f, int size) {
        f.whenComplete((o, ex) -> {
            PendingStat.PENDING_REQUESTS.getAndAddRelease(serverStat, -1);
            PendingStat.PENDING_BYTES.getAndAddRelease(serverStat, -size);
        });
    }

    @Override
    public long getLogIndexForRead(DtTime deadline)
            throws RaftException, InterruptedException, TimeoutException {
        RaftStatusImpl raftStatus = this.raftStatus;
        if (raftStatus.isError()) {
            throw new RaftException("raft status error");
        }
        if (raftStatus.isStop()) {
            throw new RaftException("raft group thread is stop");
        }
        ShareStatus ss = raftStatus.getShareStatus();
        // NOTICE : timestamp is not thread safe
        readTimestamp.refresh(1);
        if (ss.role != RaftRole.leader) {
            throw new NotLeaderException(RaftUtil.getLeader(ss.currentLeader));
        }
        long t = readTimestamp.getNanoTime();
        if (ss.leaseEndNanos - t < 0) {
            throw new NotLeaderException(null);
        }
        if (ss.firstCommitOfApplied != null) {
            try {
                ss.firstCommitOfApplied.get(deadline.rest(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);
            } catch (ExecutionException e) {
                BugLog.log(e);
                throw new RaftException(e);
            }
        }
        return ss.lastApplied;
    }

    @Override
    public void markTruncateByIndex(long index, long delayMillis) {
        long finalIndex = Math.min(index, raftStatus.getLastApplied());
        if (finalIndex <= 0) {
            return;
        }
        raftExecutor.execute(() -> raftLog.markTruncateByIndex(finalIndex, delayMillis));
    }

    @Override
    public void markTruncateByTimestamp(long timestampMillis, long delayMillis) {
        raftExecutor.execute(() -> raftLog.markTruncateByTimestamp(timestampMillis, delayMillis));
    }

    @Override
    public CompletableFuture<Void> transferLeadership(int nodeId, long timeoutMillis) {
        checkStatus();
        CompletableFuture<Void> f = new CompletableFuture<>();
        DtTime deadline = new DtTime(timeoutMillis, TimeUnit.MILLISECONDS);
        raftStatus.setHoldRequest(true);
        memberManager.transferLeadership(nodeId, f, deadline);
        return f;
    }

    @Override
    public CompletableFuture<Void> leaderPrepareJointConsensus(Set<Integer> members, Set<Integer> observers) {
        Objects.requireNonNull(members);
        Objects.requireNonNull(observers);
        checkStatus();
        // node state change in scheduler thread, member state change in raft thread
        CompletableFuture<Void> f = new CompletableFuture<>();
        RaftUtil.SCHEDULED_SERVICE.execute(() -> nodeManager.leaderPrepareJointConsensus(f, this, members, observers));
        return f;
    }

    @Override
    public CompletableFuture<Void> leaderAbortJointConsensus() {
        checkStatus();
        CompletableFuture<Void> f = new CompletableFuture<>();
        raftExecutor.execute(() -> memberManager.leaderAbortJointConsensus(f));
        return f;
    }

    @Override
    public CompletableFuture<Void> leaderCommitJointConsensus() {
        checkStatus();
        CompletableFuture<Void> f = new CompletableFuture<>();
        raftExecutor.execute(() -> memberManager.leaderCommitJointConsensus(f));
        return f;
    }

    @Override
    public CompletableFuture<Long> saveSnapshot() {
        checkStatus();
        return snapshotManager.saveSnapshot(stateMachine, () -> raftStatus.isStop());
    }

    public RaftGroupThread getRaftGroupThread() {
        return raftGroupThread;
    }

    public RaftStatusImpl getRaftStatus() {
        return raftStatus;
    }

    public MemberManager getMemberManager() {
        return memberManager;
    }

    public VoteManager getVoteManager() {
        return voteManager;
    }

    public RaftServerConfig getServerConfig() {
        return serverConfig;
    }

    public RaftGroupConfig getGroupConfig() {
        return groupConfig;
    }

    public void setServerConfig(RaftServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }

    public void setGroupConfig(RaftGroupConfig groupConfig) {
        this.groupConfig = groupConfig;
    }

    public void setRaftGroupThread(RaftGroupThread raftGroupThread) {
        this.raftGroupThread = raftGroupThread;
    }

    public void setRaftStatus(RaftStatusImpl raftStatus) {
        this.raftStatus = raftStatus;
    }

    public void setMemberManager(MemberManager memberManager) {
        this.memberManager = memberManager;
    }

    public void setVoteManager(VoteManager voteManager) {
        this.voteManager = voteManager;
    }

    public void setRaftLog(RaftLog raftLog) {
        this.raftLog = raftLog;
    }

    public void setStateMachine(StateMachine stateMachine) {
        this.stateMachine = stateMachine;
    }

    public RaftExecutor getRaftExecutor() {
        return raftExecutor;
    }

    public void setRaftExecutor(RaftExecutor raftExecutor) {
        this.raftExecutor = raftExecutor;
    }

    public Raft getRaft() {
        return raft;
    }

    public void setRaft(Raft raft) {
        this.raft = raft;
    }

    public ApplyManager getApplyManager() {
        return applyManager;
    }

    public void setApplyManager(ApplyManager applyManager) {
        this.applyManager = applyManager;
    }

    public EventBus getEventBus() {
        return eventBus;
    }

    public void setEventBus(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    public void setNodeManager(NodeManager nodeManager) {
        this.nodeManager = nodeManager;
    }

    public void setServerStat(PendingStat serverStat) {
        this.serverStat = serverStat;
    }

    public SnapshotManager getSnapshotManager() {
        return snapshotManager;
    }

    public void setSnapshotManager(SnapshotManager snapshotManager) {
        this.snapshotManager = snapshotManager;
    }
}
