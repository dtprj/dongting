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
        this.raftStatus = gc.getRaftStatus();
        this.groupConfig = gc.getGroupConfig();
        this.raftSequenceProcessors = raftSequenceProcessors;
    }

    @Override
    protected FrameCallResult handle(Throwable ex) {
        log.error("raft group init failed, groupId={}", groupConfig.getGroupId(), ex);
        raftStatus.getInitFuture().completeExceptionally(ex);
        getFiberGroup().requestShutdown();
        return Fiber.frameReturn();
    }

    @Override
    public FrameCallResult execute(Void input) throws Throwable {
        gc.getStateMachine().start(); // stop in apply manager
        FiberGroup fg = getFiberGroup();
        initRaftStatus(raftStatus, fg, gc.getServerConfig());

        for (RaftSequenceProcessor<?> processor : raftSequenceProcessors) {
            @SuppressWarnings("rawtypes")
            FiberChannel channel = gc.getProcessorChannels().get(processor.getTypeId());
            //noinspection unchecked
            processor.startProcessFiber(channel);
        }

        gc.getLinearTaskRunner().init(fg.newChannel());

        return Fiber.call(gc.getStatusManager().initStatusFile(), this::afterInitStatusFile);
    }

    public static void initRaftStatus(RaftStatusImpl raftStatus, FiberGroup fg, RaftServerConfig serverConfig) {
        raftStatus.setElectTimeoutNanos(Duration.ofMillis(serverConfig.getElectTimeout()).toNanos());
        raftStatus.setFiberGroup(fg);
        raftStatus.setDataArrivedCondition(fg.newCondition("dataArrived"));
        raftStatus.setLogForceFinishCondition(fg.newCondition("logSyncFinish"));
        raftStatus.setLogWriteFinishCondition(fg.newCondition("logWriteFinish"));
    }

    private FrameCallResult afterInitStatusFile(Void unused) {
        if (raftStatus.isInstallSnapshot()) {
            log.info("install snapshot, skip recover, groupId={}", groupConfig.getGroupId());
            return initRaftFibers();
        } else {
            if (gc.getSnapshotManager() == null) {
                return afterRecoverStateMachine(null);
            }
            FiberFrame<Snapshot> f = gc.getSnapshotManager().init();
            return Fiber.call(f, this::afterSnapshotManagerInit);
        }
    }

    private FrameCallResult afterSnapshotManagerInit(Snapshot snapshot) {
        if (snapshot == null) {
            return afterRecoverStateMachine(null);
        }
        SnapshotInfo si = snapshot.getSnapshotInfo();
        if (si.getLastIncludedTerm() > raftStatus.getCurrentTerm()) {
            log.error("snapshot term greater than current term, snapshot={}, current={}",
                    si.getLastIncludedTerm(), raftStatus.getCurrentTerm());
            throw new RaftException("snapshot term greater than current term");
        }
        gc.getRaftStatus().setLastConfigChangeIndex(si.getLastConfigChangeIndex());

        FiberFrame<Void> f = gc.getMemberManager().applyConfigFrame(
                "state machine recover apply config change",
                si.getMembers(), si.getObservers(), si.getPreparedMembers(), si.getPreparedObservers());
        return Fiber.call(f, v -> afterApplyConfigChange(snapshot));
    }

    private FrameCallResult afterApplyConfigChange(Snapshot snapshot) {
        FiberFrame<Void> f = gc.getSnapshotManager().recover(snapshot);
        return Fiber.call(f, v -> afterRecoverStateMachine(snapshot));
    }

    private FrameCallResult afterRecoverStateMachine(Snapshot snapshot) {
        if (isGroupShouldStopPlain()) {
            raftStatus.getInitFuture().completeExceptionally(new RaftException("group should stop"));
            return Fiber.frameReturn();
        }

        int snapshotTerm = snapshot == null ? 0 : snapshot.getSnapshotInfo().getLastIncludedTerm();
        long snapshotIndex = snapshot == null ? 0 : snapshot.getSnapshotInfo().getLastIncludedIndex();
        log.info("load snapshot to term={}, index={}, groupId={}", snapshotTerm, snapshotIndex, groupConfig.getGroupId());
        raftStatus.setLastApplied(snapshotIndex);
        raftStatus.setLastAppliedTerm(snapshotTerm);
        raftStatus.setLastApplying(snapshotIndex);
        if (snapshotIndex > raftStatus.getCommitIndex()) {
            raftStatus.setCommitIndex(snapshotIndex);
        }

        return Fiber.call(gc.getRaftLog().init(),
                initResult -> afterRaftLogInit(initResult, snapshotTerm, snapshotIndex));
    }

    private FrameCallResult afterRaftLogInit(Pair<Integer, Long> logInitResult, int snapshotTerm, long snapshotIndex) {
        if (isGroupShouldStopPlain()) {
            raftStatus.getInitFuture().completeExceptionally(new RaftException("group should stop"));
            return Fiber.frameReturn();
        }
        int logInitResultTerm = logInitResult.getLeft();
        long logInitResultIndex = logInitResult.getRight();
        if (logInitResultIndex < snapshotIndex || logInitResultIndex < raftStatus.getCommitIndex()) {
            log.error("raft log last index invalid, {}, {}, {}", logInitResultIndex, snapshotIndex, raftStatus.getCommitIndex());
            throw new RaftException("raft log last index invalid");
        }
        if (logInitResultTerm < snapshotTerm) {
            log.error("raft log last term invalid, {}, {}", logInitResultTerm, snapshotTerm);
            throw new RaftException("raft log last term invalid");
        }
        if(logInitResultTerm > raftStatus.getCurrentTerm()) {
            log.error("raft log last term({}) greater than current term({})",
                    logInitResultTerm, raftStatus.getCurrentTerm());
            throw new RaftException("raft log last term greater than current term");
        }

        raftStatus.setLastLogTerm(logInitResultTerm);

        raftStatus.setLastLogIndex(logInitResultIndex);
        raftStatus.setLastWriteLogIndex(logInitResultIndex);
        raftStatus.setLastForceLogIndex(logInitResultIndex);

        log.info("raft group log init complete, maxTerm={}, maxIndex={}, groupId={}",
                logInitResult.getLeft(), logInitResult.getRight(), groupConfig.getGroupId());

        return initRaftFibers();
    }

    private FrameCallResult initRaftFibers() {
        raftStatus.copyShareStatus();
        gc.getCommitManager().startCommitFiber();
        gc.getVoteManager().startFiber();
        gc.getApplyManager().init(getFiberGroup());
        gc.getSnapshotManager().startFiber();
        return Fiber.frameReturn();
    }

}
