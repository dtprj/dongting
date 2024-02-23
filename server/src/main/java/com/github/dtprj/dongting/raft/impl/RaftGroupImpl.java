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
import com.github.dtprj.dongting.common.IntObjMap;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.fiber.FiberChannel;
import com.github.dtprj.dongting.fiber.FiberGroup;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class RaftGroupImpl extends RaftGroup {
    private static final DtLog log = DtLogs.getLogger(RaftGroupImpl.class);
    private final int groupId;
    private final RaftStatusImpl raftStatus;
    private final RaftServerConfig serverConfig;
    private final PendingStat serverStat;
    private final StateMachine stateMachine;
    private final FiberGroup fiberGroup;

    private final Timestamp readTimestamp = new Timestamp();
    private final GroupComponents gc;
    private final IntObjMap<FiberChannel<Object>> processorChannels = new IntObjMap<>();
    private boolean requestShutdown;

    public RaftGroupImpl(GroupComponents gc) {
        this.gc = gc;
        this.groupId = gc.getGroupConfig().getGroupId();

        this.raftStatus = gc.getRaftStatus();
        this.serverConfig = gc.getServerConfig();
        this.serverStat = gc.getServerStat();
        this.stateMachine = gc.getStateMachine();
        this.fiberGroup = gc.getFiberGroup();
    }

    public IntObjMap<FiberChannel<Object>> getProcessorChannels() {
        return processorChannels;
    }

    @Override
    public int getGroupId() {
        return groupId;
    }

    @Override
    public StateMachine getStateMachine() {
        return stateMachine;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public CompletableFuture<RaftOutput> submitLinearTask(RaftInput input) {
        Objects.requireNonNull(input);
        if (fiberGroup.isShouldStop()) {
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
        CompletableFuture f = gc.getLinearTaskRunner().submitRaftTaskInBizThread(input);
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
        if (fiberGroup.isShouldStop()) {
            return CompletableFuture.failedFuture(new RaftException("raft group thread is stop"));
        }
        RaftStatusImpl raftStatus = this.raftStatus;
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
        ExecutorService executor = gc.getFiberGroup().getExecutor();
        executor.execute(() -> gc.getRaftLog().markTruncateByIndex(index, delayMillis));
    }

    @Override
    public void markTruncateByTimestamp(long timestampMillis, long delayMillis) {
        ExecutorService executor = gc.getFiberGroup().getExecutor();
        executor.execute(() -> gc.getRaftLog().markTruncateByTimestamp(timestampMillis, delayMillis));
    }

    @Override
    public CompletableFuture<Long> saveSnapshot() {
        checkStatus();
        CompletableFuture<Long> fu = new CompletableFuture<>();
        ExecutorService executor = gc.getFiberGroup().getExecutor();
        executor.submit(() -> gc.getSnapshotManager().saveSnapshot(stateMachine, fu));
        return fu;
    }

    @Override
    public CompletableFuture<Void> transferLeadership(int nodeId, long timeoutMillis) {
        checkStatus();
        CompletableFuture<Void> f = new CompletableFuture<>();
        DtTime deadline = new DtTime(timeoutMillis, TimeUnit.MILLISECONDS);
        gc.getMemberManager().transferLeadership(nodeId, f, deadline);
        return f;
    }

    private void checkStatus() {
        CompletableFuture<Void> f = gc.getMemberManager().getStartReadyFuture();
        if (!f.isDone() || f.isCompletedExceptionally()) {
            throw new RaftException("not initialized");
        }
        if (raftStatus.getFiberGroup().isShouldStop()) {
            throw new RaftException("raft group is not running");
        }
    }

    @Override
    public CompletableFuture<Long> leaderPrepareJointConsensus(Set<Integer> members, Set<Integer> observers) {
        Objects.requireNonNull(members);
        Objects.requireNonNull(observers);
        checkStatus();
        for (int nodeId : members) {
            if (observers.contains(nodeId)) {
                log.error("node is both member and observer: nodeId={}, groupId={}", nodeId, groupId);
                throw new RaftException("node is both member and observer: " + nodeId);
            }
        }
        gc.getNodeManager().checkLeaderPrepare(members, observers);
        CompletableFuture<Long> f = new CompletableFuture<>();
        ExecutorService executor = gc.getFiberGroup().getExecutor();
        executor.execute(() -> gc.getMemberManager().leaderPrepareJointConsensus(members, observers, f));
        return f;
    }

    @Override
    public CompletableFuture<Void> leaderAbortJointConsensus() {
        checkStatus();
        CompletableFuture<Void> f = new CompletableFuture<>();
        ExecutorService executor = gc.getFiberGroup().getExecutor();
        executor.execute(() -> gc.getMemberManager().leaderAbortJointConsensus(f));
        return f;
    }

    @Override
    public CompletableFuture<Void> leaderCommitJointConsensus(long prepareIndex) {
        checkStatus();
        CompletableFuture<Void> f = new CompletableFuture<>();
        ExecutorService executor = gc.getFiberGroup().getExecutor();
        executor.execute(() -> gc.getMemberManager().leaderCommitJointConsensus(f, prepareIndex));
        return f;
    }

    public GroupComponents getGroupComponents() {
        return gc;
    }

    public FiberGroup getFiberGroup() {
        return fiberGroup;
    }

    public boolean isRequestShutdown() {
        return requestShutdown;
    }

    public void setRequestShutdown(boolean requestShutdown) {
        this.requestShutdown = requestShutdown;
    }
}
