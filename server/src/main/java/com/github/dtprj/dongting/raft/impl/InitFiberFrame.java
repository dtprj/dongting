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
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberChannel;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.rpc.RaftGroupProcessor;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.sm.Snapshot;
import com.github.dtprj.dongting.raft.sm.SnapshotManager;
import com.github.dtprj.dongting.raft.sm.StateMachine;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
public class InitFiberFrame extends FiberFrame<Void> {
    private static final DtLog log = DtLogs.getLogger(InitFiberFrame.class);

    private final CompletableFuture<Void> prepareFuture = new CompletableFuture<>();
    private final GroupComponents gc;
    private final RaftStatusImpl raftStatus;
    private final RaftGroupConfigEx groupConfig;
    private final List<RaftGroupProcessor<?>> raftGroupProcessors;
    private final NioClient replicateClient;

    public InitFiberFrame(GroupComponents gc, List<RaftGroupProcessor<?>> raftGroupProcessors,
                          NioClient replicateClient) {
        this.gc = gc;
        this.raftStatus = gc.getRaftStatus();
        this.groupConfig = gc.getGroupConfig();
        this.raftGroupProcessors = raftGroupProcessors;
        this.replicateClient = replicateClient;
    }

    @Override
    protected FrameCallResult handle(Throwable ex) {
        log.error("raft group init failed, groupId={}", groupConfig.getGroupId(), ex);
        prepareFuture.completeExceptionally(ex);
        return Fiber.frameReturn();
    }

    @Override
    public FrameCallResult execute(Void input) throws Throwable {
        FiberGroup fg = getFiberGroup();
        initRaftStatus(raftStatus, fg, gc.getServerConfig());

        for (RaftGroupProcessor<?> processor : raftGroupProcessors) {
            @SuppressWarnings("rawtypes")
            FiberChannel channel = getFiberGroup().newChannel();
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
        raftStatus.setLogSyncFinishCondition(fg.newCondition("logSyncFinish"));
        raftStatus.setLogWriteFinishCondition(fg.newCondition("logWriteFinish"));
    }

    private FrameCallResult afterInitStatusFile(Void unused) {
        return Fiber.call(new RecoverStateMachineFiberFrame(gc), this::afterRecoverStateMachine);
    }

    private FrameCallResult afterRecoverStateMachine(Pair<Integer, Long> snapshotResult) {
        if (isGroupShouldStopPlain()) {
            prepareFuture.completeExceptionally(new RaftException("group should stop"));
            return Fiber.frameReturn();
        }

        int snapshotTerm = snapshotResult == null ? 0 : snapshotResult.getLeft();
        long snapshotIndex = snapshotResult == null ? 0 : snapshotResult.getRight();
        log.info("load snapshot to term={}, index={}, groupId={}", snapshotTerm, snapshotIndex, groupConfig.getGroupId());
        raftStatus.setLastApplied(snapshotIndex);
        if (snapshotIndex > raftStatus.getCommitIndex()) {
            raftStatus.setCommitIndex(snapshotIndex);
        }

        return Fiber.call(gc.getRaftLog().init(),
                initResult -> afterRaftLogInit(initResult, snapshotTerm, snapshotIndex));
    }

    private FrameCallResult afterRaftLogInit(Pair<Integer, Long> initResult, int snapshotTerm, long snapshotIndex) {
        if (isGroupShouldStopPlain()) {
            prepareFuture.completeExceptionally(new RaftException("group should stop"));
            return Fiber.frameReturn();
        }
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

        raftStatus.setLastLogTerm(initResultTerm);

        raftStatus.setLastLogIndex(initResultIndex);
        raftStatus.setLastWriteLogIndex(initResultIndex);
        raftStatus.setLastSyncLogIndex(initResultIndex);

        log.info("raft group log init complete, maxTerm={}, maxIndex={}, groupId={}",
                initResult.getLeft(), initResult.getRight(), groupConfig.getGroupId());

        gc.getCommitManager().startCommitFiber();
        gc.getVoteManager().startFiber();
        gc.getApplyManager().init(getFiberGroup(), () -> {
            raftStatus.copyShareStatus();
            prepareFuture.complete(null);
        });
        return Fiber.frameReturn();
    }

    public CompletableFuture<Void> getPrepareFuture() {
        return prepareFuture;
    }
}

class RecoverStateMachineFiberFrame extends FiberFrame<Pair<Integer, Long>> {

    private final SnapshotManager snapshotManager;
    private final StateMachine stateMachine;

    private Snapshot snapshot;
    private long offset;
    private RefBuffer rb;

    public RecoverStateMachineFiberFrame(GroupComponents gc) {
        this.snapshotManager = gc.getSnapshotManager();
        this.stateMachine = gc.getStateMachine();
    }

    @Override
    protected FrameCallResult doFinally() {
        if (rb != null) {
            rb.release();
        }
        if (snapshot != null) {
            snapshot.close();
        }
        return Fiber.frameReturn();
    }

    @Override
    public FrameCallResult execute(Void input) throws Throwable {
        if (snapshotManager == null) {
            setResult(null);
            return Fiber.frameReturn();
        }
        return Fiber.call(snapshotManager.init(), this::afterSnapshotInit);
    }

    private FrameCallResult afterSnapshotInit(Snapshot snapshot) {
        if (snapshot == null) {
            setResult(null);
            return Fiber.frameReturn();
        }
        this.snapshot = snapshot;

        return read(null);
    }

    private FrameCallResult read(Void v) {
        RaftUtil.checkStop(getFiberGroup());
        if (rb != null) {
            rb.release();
        }
        FiberFuture<RefBuffer> f = snapshot.readNext();
        return f.await(this::afterRead);
    }

    private FrameCallResult afterRead(RefBuffer rb) {
        this.rb = rb;
        RaftUtil.checkStop(getFiberGroup());
        long count = rb == null ? 0 : rb.getBuffer() == null ? 0 : rb.getBuffer().remaining();
        if (count == 0) {
            FiberFuture<Void> fu = stateMachine.installSnapshot(snapshot.getLastIncludedIndex(),
                    snapshot.getLastIncludedTerm(), offset, true, rb);
            return fu.await(this::finish);
        } else {
            FiberFuture<Void> fu = stateMachine.installSnapshot(snapshot.getLastIncludedIndex(),
                    snapshot.getLastIncludedTerm(), offset, false, rb);
            offset += count;
            return fu.await(this::read);
        }
    }

    private FrameCallResult finish(Void v) {
        setResult(new Pair<>(snapshot.getLastIncludedTerm(), snapshot.getLastIncludedIndex()));
        return Fiber.frameReturn();
    }
}
