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
import com.github.dtprj.dongting.common.FutureCallback;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.NotLeaderException;
import com.github.dtprj.dongting.raft.server.RaftCallback;
import com.github.dtprj.dongting.raft.server.RaftGroup;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.sm.StateMachine;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public final class RaftGroupImpl extends RaftGroup {
    private static final DtLog log = DtLogs.getLogger(RaftGroupImpl.class);

    public final GroupComponents groupComponents;
    public final FiberGroup fiberGroup;
    public CompletableFuture<Void> shutdownFuture;

    private final int groupId;
    private final RaftStatusImpl raftStatus;
    private final StateMachine stateMachine;

    public RaftGroupImpl(GroupComponents groupComponents) {
        this.groupComponents = groupComponents;
        this.groupId = groupComponents.groupConfig.groupId;

        this.raftStatus = groupComponents.raftStatus;
        this.stateMachine = groupComponents.stateMachine;
        this.fiberGroup = groupComponents.fiberGroup;
    }

    @Override
    public int getGroupId() {
        return groupId;
    }

    @Override
    public boolean isLeader() {
        RaftMember leader = raftStatus.getShareStatus().currentLeader;
        return leader != null && leader.node.self;
    }

    @Override
    public StateMachine getStateMachine() {
        return stateMachine;
    }

    @Override
    public void submitLinearTask(RaftInput input, RaftCallback callback) {
        Objects.requireNonNull(input);
        if (fiberGroup.shareStatusSource.getShareStatus(false).shouldStop) {
            RaftUtil.release(input);
            throw new RaftException("raft group thread is stop");
        }
        int type = input.isReadOnly() ? LogItem.TYPE_LOG_READ : LogItem.TYPE_NORMAL;
        groupComponents.linearTaskRunner.submitRaftTaskInBizThread(type, input, callback);
    }

    private long lastLeaseTimeoutLogNanoTime;

    @Override
    public void leaseRead(Timestamp ts, DtTime deadline, FutureCallback<Long> callback) {
        if (fiberGroup.shareStatusSource.getShareStatus(true).shouldStop) {
            FutureCallback.callFail(callback, new RaftException("raft group thread is stop"));
            return;
        }
        RaftShareStatus ss = raftStatus.getShareStatus();
        if (ss.role != RaftRole.leader) {
            FutureCallback.callFail(callback, new NotLeaderException(
                    ss.currentLeader == null ? null : ss.currentLeader.node));
            return;
        }

        if (ss.groupReady) {
            ts.refresh(1);
            long t = ts.nanoTime;
            if (ss.leaseEndNanos - t < 0) {
                long x = (t - ss.leaseEndNanos) / 1_000_000;
                if (lastLeaseTimeoutLogNanoTime == 0 || t - lastLeaseTimeoutLogNanoTime > 1_000_000_000L) {
                    log.error("lease expired for {} ms. lastApplied={}", x, ss.lastApplied);
                    lastLeaseTimeoutLogNanoTime = t;
                }
                FutureCallback.callFail(callback, new NotLeaderException(null, "lease expired for " + x + "ms"));
            } else {
                FutureCallback.callSuccess(callback, ss.lastApplied);
            }
        } else {
            // wait group ready
            CompletableFuture<Long> f = groupComponents.applyManager.addToWaitReadyQueue(deadline);
            f.whenComplete((idx, ex) -> {
                if (ex != null) {
                    FutureCallback.callFail(callback, ex);
                } else {
                    FutureCallback.callSuccess(callback, idx);
                }
            });
        }
    }

    @Override
    public void markTruncateByIndex(long index, long delayMillis) {
        ExecutorService executor = groupComponents.fiberGroup.getExecutor();
        executor.execute(() -> groupComponents.raftLog.markTruncateByIndex(index, delayMillis));
    }

    @Override
    public void markTruncateByTimestamp(long timestampMillis, long delayMillis) {
        ExecutorService executor = groupComponents.fiberGroup.getExecutor();
        executor.execute(() -> groupComponents.raftLog.markTruncateByTimestamp(timestampMillis, delayMillis));
    }

    @Override
    public CompletableFuture<Long> fireSaveSnapshot() {
        checkStatus();
        CompletableFuture<Long> f = new CompletableFuture<>();
        groupComponents.fiberGroup.getExecutor().execute(() -> {
            try {
                FiberFuture<Long> ff = groupComponents.snapshotManager.saveSnapshot();
                ff.registerCallback((idx, ex) -> {
                    if (ex != null) {
                        f.completeExceptionally(ex);
                    } else {
                        f.complete(idx);
                    }
                });
            } catch (Exception e) {
                f.completeExceptionally(e);
            }
        });
        return f;
    }

    @Override
    public CompletableFuture<Void> transferLeadership(int nodeId, long timeoutMillis) {
        checkStatus();
        CompletableFuture<Void> f = new CompletableFuture<>();
        DtTime deadline = new DtTime(timeoutMillis, TimeUnit.MILLISECONDS);
        groupComponents.memberManager.transferLeadership(nodeId, f, deadline);
        return f;
    }

    private void checkStatus() {
        CompletableFuture<Void> f = groupComponents.memberManager.getPingReadyFuture();
        if (!f.isDone() || f.isCompletedExceptionally()) {
            throw new RaftException("not initialized");
        }
        if (raftStatus.fiberGroup.shareStatusSource.getShareStatus(true).shouldStop) {
            throw new RaftException("raft group is not running");
        }
    }

    @Override
    public CompletableFuture<Long> leaderPrepareJointConsensus(Set<Integer> members, Set<Integer> observers,
                                                               Set<Integer> preparedMembers, Set<Integer> prepareObservers) {
        if (preparedMembers.isEmpty()) {
            throw new RaftException("preparedMembers are empty");
        }
        checkStatus();
        for (int nodeId : preparedMembers) {
            if (prepareObservers.contains(nodeId)) {
                log.error("node is both member and observer: nodeId={}, groupId={}", nodeId, groupId);
                throw new RaftException("node is both member and observer: " + nodeId);
            }
        }
        CompletableFuture<Long> f = new CompletableFuture<>();
        FiberFrame<Void> ff = groupComponents.memberManager.leaderPrepareJointConsensus(members, observers, preparedMembers, prepareObservers, f);
        groupComponents.fiberGroup.fireFiber("leaderPrepareJointConsensus", ff);
        return f;
    }

    @Override
    public CompletableFuture<Long> leaderAbortJointConsensus() {
        checkStatus();
        CompletableFuture<Long> f = new CompletableFuture<>();
        FiberFrame<Void> ff = groupComponents.memberManager.leaderAbortJointConsensus(f);
        groupComponents.fiberGroup.fireFiber("leaderAbortJointConsensus", ff);
        return f;
    }

    @Override
    public CompletableFuture<Long> leaderCommitJointConsensus(long prepareIndex) {
        checkStatus();
        CompletableFuture<Long> f = new CompletableFuture<>();
        FiberFrame<Void> ff = groupComponents.memberManager.leaderCommitJointConsensus(f, prepareIndex);
        groupComponents.fiberGroup.fireFiber("leaderCommitJointConsensus", ff);
        return f;
    }

}
