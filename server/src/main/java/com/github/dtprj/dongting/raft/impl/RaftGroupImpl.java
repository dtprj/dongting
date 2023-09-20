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
import com.github.dtprj.dongting.common.FlowControlException;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.server.NotLeaderException;
import com.github.dtprj.dongting.raft.server.RaftExecTimeoutException;
import com.github.dtprj.dongting.raft.server.RaftGroup;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftOutput;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.sm.StateMachine;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class RaftGroupImpl extends RaftGroup {
    private static final DtLog log = DtLogs.getLogger(RaftGroupImpl.class);
    private final Timestamp readTimestamp = new Timestamp();

    private final GroupComponents gc;
    private final RaftStatusImpl raftStatus;
    private final RaftGroupThread raftGroupThread;
    private final RaftServerConfig serverConfig;
    private final PendingStat serverStat;
    private final StateMachine stateMachine;

    public RaftGroupImpl(GroupComponents gc) {
        this.gc = gc;
        this.raftStatus = gc.getRaftStatus();
        this.raftGroupThread = gc.getRaftGroupThread();
        this.serverConfig = gc.getServerConfig();
        this.serverStat = gc.getServerStat();
        this.stateMachine = gc.getStateMachine();
    }

    private void checkStatus() {
        if (!raftGroupThread.isPrepared()) {
            throw new RaftException("not initialized");
        }
        if (raftStatus.isStop()) {
            throw new RaftException("raft group is not running");
        }
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public CompletableFuture<RaftOutput> submitLinearTask(RaftInput input) {
        Objects.requireNonNull(input);
        RaftStatusImpl raftStatus = this.raftStatus;
        if (raftStatus.isError()) {
            return CompletableFuture.failedFuture(new RaftException("raft status is error"));
        }
        if (raftStatus.isStop()) {
            return CompletableFuture.failedFuture(new RaftException("raft group thread is stop"));
        }
        int currentPendingWrites = (int) PendingStat.PENDING_REQUESTS.getAndAddRelease(serverStat, 1);
        if (currentPendingWrites >= serverConfig.getMaxPendingWrites()) {
            String msg = "submitRaftTask failed: too many pending writes, currentPendingWrites=" + currentPendingWrites;
            log.warn(msg);
            PendingStat.PENDING_REQUESTS.getAndAddRelease(serverStat, -1);
            return CompletableFuture.failedFuture(new FlowControlException(msg));
        }
        long size = input.getFlowControlSize();
        long currentPendingWriteBytes = (long) PendingStat.PENDING_BYTES.getAndAddRelease(serverStat, size);
        if (currentPendingWriteBytes >= serverConfig.getMaxPendingWriteBytes()) {
            String msg = "too many pending write bytes,currentPendingWriteBytes="
                    + currentPendingWriteBytes + ", currentRequestBytes=" + size;
            log.warn(msg);
            PendingStat.PENDING_BYTES.getAndAddRelease(serverStat, -size);
            return CompletableFuture.failedFuture(new FlowControlException(msg));
        }
        CompletableFuture f = raftGroupThread.submitRaftTask(input);
        registerCallback(f, size);
        return f;
    }

    private void registerCallback(CompletableFuture<?> f, long size) {
        f.whenComplete((o, ex) -> {
            PendingStat.PENDING_REQUESTS.getAndAddRelease(serverStat, -1);
            PendingStat.PENDING_BYTES.getAndAddRelease(serverStat, -size);
        });
    }

    @Override
    public CompletableFuture<Long> getLogIndexForRead(DtTime deadline) {
        RaftStatusImpl raftStatus = this.raftStatus;
        if (raftStatus.isError()) {
            return CompletableFuture.failedFuture(new RaftException("raft status error"));
        }
        if (raftStatus.isStop()) {
            return CompletableFuture.failedFuture(new RaftException("raft group thread is stop"));
        }
        ShareStatus ss = raftStatus.getShareStatus();
        // NOTICE : timestamp is not thread safe
        readTimestamp.refresh(1);
        if (ss.role != RaftRole.leader) {
            return CompletableFuture.failedFuture(new NotLeaderException(
                    ss.currentLeader == null ? null : ss.currentLeader.getNode()));
        }
        long t = readTimestamp.getNanoTime();
        if (ss.leaseEndNanos - t < 0) {
            return CompletableFuture.failedFuture(new NotLeaderException(null));
        }
        if (ss.firstCommitOfApplied == null) {
            return CompletableFuture.completedFuture(ss.lastApplied);
        }

        // wait fist commit of applied
        return ss.firstCommitOfApplied.thenCompose(v -> {
            if (deadline.isTimeout()) {
                return CompletableFuture.failedFuture(new RaftExecTimeoutException());
            }
            // ss should re-read
            return getLogIndexForRead(deadline);
        });
    }

    @Override
    public void markTruncateByIndex(long index, long delayMillis) {
        long finalIndex = Math.min(index, raftStatus.getLastApplied());
        if (finalIndex <= 0) {
            return;
        }
        gc.getRaftExecutor().execute(() -> gc.getRaftLog().markTruncateByIndex(finalIndex, delayMillis));
    }

    @Override
    public void markTruncateByTimestamp(long timestampMillis, long delayMillis) {
        gc.getRaftExecutor().execute(() -> gc.getRaftLog().markTruncateByTimestamp(timestampMillis, delayMillis));
    }

    @Override
    public CompletableFuture<Void> transferLeadership(int nodeId, long timeoutMillis) {
        checkStatus();
        CompletableFuture<Void> f = new CompletableFuture<>();
        DtTime deadline = new DtTime(timeoutMillis, TimeUnit.MILLISECONDS);
        raftStatus.setHoldRequest(true);
        gc.getMemberManager().transferLeadership(nodeId, f, deadline);
        return f;
    }

    @Override
    public CompletableFuture<Long> leaderPrepareJointConsensus(Set<Integer> members, Set<Integer> observers) {
        Objects.requireNonNull(members);
        Objects.requireNonNull(observers);
        checkStatus();
        // node state change in scheduler thread, member state change in raft thread
        CompletableFuture<Long> f = new CompletableFuture<>();
        RaftUtil.SCHEDULED_SERVICE.execute(() -> gc.getNodeManager().leaderPrepareJointConsensus(f, this, members, observers));
        return f;
    }

    @Override
    public CompletableFuture<Void> leaderAbortJointConsensus() {
        checkStatus();
        CompletableFuture<Void> f = new CompletableFuture<>();
        gc.getRaftExecutor().execute(() -> gc.getMemberManager().leaderAbortJointConsensus(f));
        return f;
    }

    @Override
    public CompletableFuture<Void> leaderCommitJointConsensus(long prepareIndex) {
        checkStatus();
        CompletableFuture<Void> f = new CompletableFuture<>();
        gc.getRaftExecutor().execute(() -> gc.getMemberManager().leaderCommitJointConsensus(f, prepareIndex));
        return f;
    }

    @Override
    public CompletableFuture<Long> saveSnapshot() {
        checkStatus();
        return gc.getSnapshotManager().saveSnapshot(gc.getStateMachine(), raftStatus::isStop);
    }

    public GroupComponents getGroupComponents() {
        return gc;
    }

    @Override
    public int getGroupId() {
        return gc.getGroupConfig().getGroupId();
    }

    @Override
    public StateMachine getStateMachine() {
        return stateMachine;
    }
}
