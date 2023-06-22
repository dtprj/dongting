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

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.client.RaftException;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftOutput;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.sm.Snapshot;
import com.github.dtprj.dongting.raft.sm.SnapshotManager;
import com.github.dtprj.dongting.raft.sm.StateMachine;
import com.github.dtprj.dongting.raft.store.RaftLog;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class RaftGroupThread extends Thread {
    private static final DtLog log = DtLogs.getLogger(RaftGroupThread.class);

    private final Random random = new Random();
    private final Supplier<Boolean> cancelInit;

    private RaftServerConfig config;
    private RaftStatusImpl raftStatus;
    private Raft raft;
    private MemberManager memberManager;
    private VoteManager voteManager;
    private StateMachine stateMachine;
    private RaftLog raftLog;
    private SnapshotManager snapshotManager;

    private long heartbeatIntervalNanos;
    private long electTimeoutNanos;

    // TODO optimise blocking queue
    private LinkedBlockingQueue<Object> queue;

    public RaftGroupThread(Supplier<Boolean> cancelInit) {
        this.cancelInit = cancelInit;
    }

    public void init(RaftGroupImpl gc) {
        this.config = gc.getServerConfig();
        this.raftStatus = gc.getRaftStatus();
        this.queue = gc.getRaftExecutor().getQueue();
        this.raft = gc.getRaft();
        this.memberManager = gc.getMemberManager();
        this.stateMachine = gc.getStateMachine();
        this.raftLog = gc.getRaftLog();
        this.voteManager = gc.getVoteManager();
        this.snapshotManager = gc.getSnapshotManager();

        electTimeoutNanos = Duration.ofMillis(config.getElectTimeout()).toNanos();
        raftStatus.setElectTimeoutNanos(electTimeoutNanos);
        heartbeatIntervalNanos = Duration.ofMillis(config.getHeartbeatInterval()).toNanos();

        RaftGroupConfig groupConfig = gc.getGroupConfig();
        setName("raft-" + groupConfig.getGroupId());

        try {
            StatusUtil.initStatusFileChannel(groupConfig.getDataDir(), groupConfig.getStatusFile(), raftStatus);
            Pair<Integer, Long> snapshotResult = recoverStateMachine();
            int snapshotTerm = snapshotResult == null ? 0 : snapshotResult.getLeft();
            long snapshotIndex = snapshotResult == null ? 0 : snapshotResult.getRight();
            log.info("load snapshot to term={}, index={}, groupId={}", snapshotTerm, snapshotIndex, groupConfig.getGroupId());
            raftStatus.setLastApplied(snapshotIndex);
            if (snapshotIndex > raftStatus.getCommitIndex()) {
                raftStatus.setCommitIndex(snapshotIndex);
            }
            RaftUtil.checkCancel(cancelInit);

            Pair<Integer, Long> initResult = raftLog.init(cancelInit);
            int initResultTerm = initResult.getLeft();
            long initResultIndex = initResult.getRight();
            if (initResultIndex < snapshotIndex || initResultIndex < raftStatus.getCommitIndex()) {
                log.error("raft log last index invalid, {}, {}, {}", initResultIndex, snapshotIndex, raftStatus.getCommitIndex());
                throw new RaftException("raft log last index invalid");
            }
            if (initResultTerm < snapshotTerm || initResultTerm < raftStatus.getCurrentTerm()) {
                log.error("raft log last term invalid, {}, {}, {}", initResultTerm, snapshotTerm, raftStatus.getCurrentTerm());
                throw new RaftException("raft log last term invalid");
            }
            RaftUtil.checkCancel(cancelInit);

            log.info("init raft log, maxTerm={}, maxIndex={}, groupId={}",
                    initResult.getLeft(), initResult.getRight(), groupConfig.getGroupId());
            raftStatus.setLastLogTerm(initResultTerm);
            raftStatus.setLastLogIndex(initResultIndex);

            gc.getApplyManager().apply(raftStatus);
        } catch (RuntimeException e) {
            clean();
            throw e;
        } catch (Exception e) {
            clean();
            throw new RaftException(e);
        }
    }

    private Pair<Integer, Long> recoverStateMachine() throws Exception {
        if (snapshotManager == null) {
            return null;
        }
        try (Snapshot snapshot = snapshotManager.init(cancelInit)) {
            if (snapshot == null) {
                return null;
            }
            long offset = 0;
            while (true) {
                RaftUtil.checkCancel(cancelInit);
                CompletableFuture<RefBuffer> f = snapshot.readNext();
                RefBuffer rb = f.get();
                try {
                    long count = rb == null ? 0 : rb.getBuffer() == null ? 0 : rb.getBuffer().remaining();
                    if (count == 0) {
                        stateMachine.installSnapshot(snapshot.getLastIncludedIndex(), snapshot.getLastIncludedTerm(),
                                offset, true, rb);
                        break;
                    }
                    stateMachine.installSnapshot(snapshot.getLastIncludedIndex(), snapshot.getLastIncludedTerm(),
                            offset , false, rb);
                    offset += count;
                } finally {
                    if (rb != null) {
                        rb.release();
                    }
                }
            }
            return new Pair<>(snapshot.getLastIncludedTerm(), snapshot.getLastIncludedIndex());
        }
    }

    public void waitReady() {
        int electQuorum = raftStatus.getElectQuorum();
        if (electQuorum <= 1) {
            return;
        }
        try {
            memberManager.createReadyFuture(electQuorum).get();
        } catch (Exception e) {
            throw new RaftException(e);
        }
    }

    private void clean() {
        if (raftStatus != null) {
            DtUtil.close(raftStatus.getStatusFile());
        }
        DtUtil.close(stateMachine);
        DtUtil.close(raftLog);
    }

    @Override
    public void run() {
        try {
            RaftUtil.checkCancel(cancelInit);
            if (raftStatus.getElectQuorum() == 1 && raftStatus.getNodeIdOfMembers().contains(config.getNodeId())) {
                RaftUtil.changeToLeader(raftStatus);
                raft.sendHeartBeat();
            }
            run0();
        } catch (Throwable e) {
            BugLog.getLog().error("raft thread error", e);
        } finally {
            clean();
        }
    }

    private void run0() {
        Timestamp ts = raftStatus.getTs();
        long lastCleanTime = ts.getNanoTime();
        ArrayList<RaftTask> rwTasks = new ArrayList<>(32);
        ArrayList<Runnable> runnables = new ArrayList<>(32);
        ArrayList<Object> queueData = new ArrayList<>(32);
        boolean poll = true;
        while (!raftStatus.isStop()) {
            if (raftStatus.getRole() != RaftRole.observer) {
                memberManager.ensureRaftMemberStatus();
            }

            try {
                poll = pollAndRefreshTs(ts, queueData, poll);
            } catch (InterruptedException e) {
                return;
            }
            if (!process(rwTasks, runnables, queueData)) {
                return;
            }
            if (queueData.size() > 0) {
                ts.refresh(1);
                queueData.clear();
            }
            if (ts.getNanoTime() - lastCleanTime > 5 * 1000 * 1000) {
                idle(ts);
                lastCleanTime = ts.getNanoTime();
            }
        }
    }

    private boolean process(ArrayList<RaftTask> rwTasks, ArrayList<Runnable> runnables, ArrayList<Object> queueData) {
        RaftStatusImpl raftStatus = this.raftStatus;
        int len = queueData.size();
        for (int i = 0; i < len; i++) {
            Object o = queueData.get(i);
            if (o instanceof RaftTask) {
                rwTasks.add((RaftTask) o);
            } else {
                runnables.add((Runnable) o);
            }
        }

        // the sequence of RaftTask and Runnable is reordered, but it will not affect the linearizability
        if (rwTasks.size() > 0) {
            if (!raftStatus.isHoldRequest()) {
                raft.raftExec(rwTasks);
                rwTasks.clear();
                raftStatus.copyShareStatus();
            }
        }
        len = runnables.size();
        if (len > 0) {
            for (int i = 0; i < len; i++) {
                runnables.get(i).run();
            }
            runnables.clear();
            raftStatus.copyShareStatus();
        }
        return true;
    }

    private boolean pollAndRefreshTs(Timestamp ts, ArrayList<Object> queueData, boolean poll) throws InterruptedException {
        long oldNanos = ts.getNanoTime();
        if (poll) {
            Object o = queue.poll(50, TimeUnit.MILLISECONDS);
            if (o != null) {
                queueData.add(o);
            }
        } else {
            queue.drainTo(queueData);
        }

        ts.refresh(1);
        return ts.getNanoTime() - oldNanos > 2 * 1000 * 1000 || queueData.size() == 0;
    }

    public void requestShutdown() {
        raftStatus.setStop(true);
        log.info("request raft thread shutdown");
    }

    private void idle(Timestamp ts) {
        RaftStatusImpl raftStatus = this.raftStatus;
        if (raftStatus.isError()) {
            return;
        }

        raftStatus.getPendingRequests().cleanPending(raftStatus, config.getMaxPendingWrites(),
                config.getMaxPendingWriteBytes());

        raftLog.doDelete();

        if (raftStatus.getElectQuorum() <= 1) {
            return;
        }
        long roundTimeNanos = ts.getNanoTime();

        RaftRole role = raftStatus.getRole();
        if (roundTimeNanos - raftStatus.getHeartbeatTime() > heartbeatIntervalNanos) {
            if (role == RaftRole.leader) {
                raftStatus.setHeartbeatTime(roundTimeNanos);
                raft.sendHeartBeat();
                raftStatus.copyShareStatus();
            }
        }
        if (role == RaftRole.follower || role == RaftRole.candidate) {
            if (roundTimeNanos - raftStatus.getLastElectTime() > electTimeoutNanos + random.nextInt(200)) {
                voteManager.tryStartPreVote();
                raftStatus.copyShareStatus();
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public CompletableFuture<RaftOutput> submitRaftTask(RaftInput input) {
        CompletableFuture f = new CompletableFuture<>();
        RaftTask t = new RaftTask(raftStatus.getTs(), LogItem.TYPE_NORMAL, input, f);
        queue.offer(t);
        return f;
    }
}
