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

import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberChannel;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.rpc.RaftSequenceProcessor;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.sm.Snapshot;
import com.github.dtprj.dongting.raft.sm.SnapshotInfo;

import java.time.Duration;
import java.util.Set;

/**
 * @author huangli
 */
public class InitFiberFrame extends FiberFrame<Void> {
    private static final DtLog log = DtLogs.getLogger(InitFiberFrame.class);

    private final GroupComponents gc;
    private final RaftStatusImpl raftStatus;
    private final RaftGroupConfigEx groupConfig;
    private final Set<RaftSequenceProcessor<?>> raftSequenceProcessors;

    public InitFiberFrame(GroupComponents gc, Set<RaftSequenceProcessor<?>> raftSequenceProcessors) {
        this.gc = gc;
        this.raftStatus = gc.raftStatus;
        this.groupConfig = gc.groupConfig;
        this.raftSequenceProcessors = raftSequenceProcessors;
    }

    @Override
    protected FrameCallResult handle(Throwable ex) {
        log.error("raft group init failed, groupId={}", groupConfig.groupId, ex);
        raftStatus.markInit(true);
        raftStatus.copyShareStatus();
        raftStatus.initFuture.completeExceptionally(ex);
        getFiberGroup().requestShutdown();
        return Fiber.frameReturn();
    }

    private boolean cancelInit() {
        if (isGroupShouldStopPlain() && !raftStatus.initFinished) {
            raftStatus.markInit(true);
            raftStatus.copyShareStatus();
            raftStatus.initFuture.completeExceptionally(new RaftException("group should stop"));
            return true;
        }
        return false;
    }

    @Override
    public FrameCallResult execute(Void input) throws Throwable {
        gc.stateMachine.start(); // stop in apply manager
        FiberGroup fg = getFiberGroup();
        initRaftStatus(raftStatus, fg, gc.serverConfig);

        for (RaftSequenceProcessor<?> processor : raftSequenceProcessors) {
            @SuppressWarnings("rawtypes")
            FiberChannel channel = gc.processorChannels.get(processor.getTypeId());
            //noinspection unchecked
            processor.startProcessFiber(channel);
        }

        gc.linearTaskRunner.init(fg.newChannel());

        return Fiber.call(gc.statusManager.initStatusFile(), this::afterInitStatusFile);
    }

    public static void initRaftStatus(RaftStatusImpl raftStatus, FiberGroup fg, RaftServerConfig serverConfig) {
        raftStatus.setElectTimeoutNanos(Duration.ofMillis(serverConfig.electTimeout).toNanos());
        raftStatus.fiberGroup = fg;
        int groupId = raftStatus.groupId;
        raftStatus.needRepCondition = fg.newCondition("needRep" + groupId);
        raftStatus.logForceFinishCondition = fg.newCondition("logSyncFinish" + groupId);
        raftStatus.logWriteFinishCondition = fg.newCondition("logWriteFinish" + groupId);
    }

    private FrameCallResult afterInitStatusFile(Void unused) {
        if (cancelInit()) {
            return Fiber.frameReturn();
        }
        if (raftStatus.installSnapshot || gc.snapshotManager == null) {
            if (raftStatus.installSnapshot) {
                log.info("install snapshot, skip recover, groupId={}", groupConfig.groupId);
            } else {
                raftStatus.installSnapshot = true;
                log.info("no snapshot manager, mark install snapshot, groupId={}", groupConfig.groupId);
            }
            return afterRecoverStateMachine(null);
        } else {
            FiberFrame<Snapshot> f = gc.snapshotManager.init();
            return Fiber.call(f, this::afterSnapshotManagerInit);
        }
    }

    private FrameCallResult afterSnapshotManagerInit(Snapshot snapshot) {
        if (cancelInit()) {
            return Fiber.frameReturn();
        }
        if (snapshot == null) {
            if (raftStatus.firstValidIndex > 1) {
                raftStatus.installSnapshot = true;
                log.warn("no snapshot and firstValidIndex>1, mark install snapshot");
            }
            return afterRecoverStateMachine(null);
        } else if (snapshot.getSnapshotInfo().getLastIncludedIndex() < raftStatus.firstValidIndex) {
            raftStatus.installSnapshot = true;
            log.warn("snapshot lastIncludedIndex({}) less than firstValidIndex({}), mark install snapshot",
                    snapshot.getSnapshotInfo().getLastIncludedIndex(), raftStatus.firstValidIndex);
            return afterRecoverStateMachine(null);
        }
        SnapshotInfo si = snapshot.getSnapshotInfo();
        if (si.getLastIncludedTerm() > raftStatus.currentTerm) {
            log.error("snapshot term greater than current term, snapshot={}, current={}",
                    si.getLastIncludedTerm(), raftStatus.currentTerm);
            throw new RaftException("snapshot term greater than current term");
        }
        gc.raftStatus.lastConfigChangeIndex = si.getLastConfigChangeIndex();
        gc.raftStatus.lastSavedSnapshotIndex = si.getLastIncludedIndex();

        FiberFrame<Void> f = gc.memberManager.applyConfigFrame(
                "state machine recover apply config change",
                si.getMembers(), si.getObservers(), si.getPreparedMembers(), si.getPreparedObservers());
        return Fiber.call(f, v -> afterApplyConfigChange(snapshot));
    }

    private FrameCallResult afterApplyConfigChange(Snapshot snapshot) {
        if (cancelInit()) {
            return Fiber.frameReturn();
        }
        FiberFrame<Void> f = gc.snapshotManager.recover(snapshot);
        return Fiber.call(f, v -> afterRecoverStateMachine(snapshot));
    }

    private FrameCallResult afterRecoverStateMachine(Snapshot snapshot) {
        if (cancelInit()) {
            return Fiber.frameReturn();
        }

        int snapshotTerm = snapshot == null ? 0 : snapshot.getSnapshotInfo().getLastIncludedTerm();
        long snapshotIndex = snapshot == null ? 0 : snapshot.getSnapshotInfo().getLastIncludedIndex();
        log.info("load snapshot to term={}, index={}, groupId={}", snapshotTerm, snapshotIndex, groupConfig.groupId);
        raftStatus.setLastApplied(snapshotIndex);
        raftStatus.lastAppliedTerm = snapshotTerm;
        raftStatus.lastApplying = snapshotIndex;
        if (snapshotIndex > raftStatus.commitIndex) {
            raftStatus.commitIndex = snapshotIndex;
        }

        raftStatus.copyShareStatus();
        return Fiber.call(gc.raftLog.init(),
                initResult -> afterRaftLogInit(initResult, snapshotTerm, snapshotIndex));
    }

    private FrameCallResult afterRaftLogInit(Pair<Integer, Long> logInitResult, int snapshotTerm, long snapshotIndex) {
        if (cancelInit()) {
            return Fiber.frameReturn();
        }
        if (logInitResult != null) {
            int logInitResultTerm = logInitResult.getLeft();
            long logInitResultIndex = logInitResult.getRight();
            if (logInitResultIndex < snapshotIndex || logInitResultIndex < raftStatus.commitIndex) {
                log.error("raft log last index invalid, {}, {}, {}", logInitResultIndex, snapshotIndex, raftStatus.commitIndex);
                throw new RaftException("raft log last index invalid");
            }
            if (logInitResultTerm < snapshotTerm) {
                log.error("raft log last term invalid, {}, {}", logInitResultTerm, snapshotTerm);
                throw new RaftException("raft log last term invalid");
            }
            if (logInitResultTerm > raftStatus.currentTerm) {
                log.error("raft log last term({}) greater than current term({})",
                        logInitResultTerm, raftStatus.currentTerm);
                throw new RaftException("raft log last term greater than current term");
            }

            raftStatus.lastLogTerm = logInitResultTerm;

            raftStatus.lastLogIndex = logInitResultIndex;
            raftStatus.lastWriteLogIndex = logInitResultIndex;
            raftStatus.lastForceLogIndex = logInitResultIndex;

            log.info("raft group log init complete, maxTerm={}, maxIndex={}, groupId={}",
                    logInitResult.getLeft(), logInitResult.getRight(), groupConfig.groupId);
        } else {
            raftStatus.installSnapshot = true;
        }

        raftStatus.copyShareStatus();
        gc.commitManager.startCommitFiber();
        gc.voteManager.startVoteFiber();
        gc.applyManager.init(getFiberGroup());
        gc.snapshotManager.startFiber();
        return Fiber.frameReturn();
    }

}
