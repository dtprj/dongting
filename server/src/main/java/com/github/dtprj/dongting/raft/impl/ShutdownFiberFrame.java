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
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.server.RaftFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class ShutdownFiberFrame extends FiberFrame<Void> {

    private static final DtLog log = DtLogs.getLogger(ShutdownFiberFrame.class);

    private final RaftGroupImpl g;
    private final FiberGroup fiberGroup;
    private final GroupComponents gc;

    public DtTime timeout = new DtTime(30, TimeUnit.SECONDS);
    public boolean saveSnapshot;

    private boolean error;

    public ShutdownFiberFrame(RaftGroupImpl g) {
        this.g = g;
        this.fiberGroup = g.fiberGroup;
        this.gc = g.groupComponents;
    }

    @Override
    protected FrameCallResult doFinally() {
        gc.groupConfig.perfCallback.shutdown();

        RaftFactory raftFactory = gc.raftFactory;
        if (!raftFactory.useSharedIoExecutor()) {
            raftFactory.shutdownBlockIoExecutor(gc.serverConfig, gc.groupConfig,
                    gc.groupConfig.blockIoExecutor);
        }
        gc.raftFactory.stopDispatcher(fiberGroup.dispatcher, timeout);
        return Fiber.frameReturn();
    }

    @Override
    protected FrameCallResult handle(Throwable ex) {
        log.error("shutdown step failed, groupId={}", g.getGroupId(), ex);
        return Fiber.frameReturn();
    }

    @Override
    public FrameCallResult execute(Void input) {
        if (gc.raftStatus.needRepCondition != null) {
            gc.raftStatus.needRepCondition.signalAll();
        }
        return Fiber.call(new AwaitFutureFrame<>("saveSnapshot", () -> {
            if (saveSnapshot && gc.raftStatus.isInitFinished() && !gc.raftStatus.isInitFailed()) {
                return gc.snapshotManager.saveSnapshot();
            } else {
                return FiberFuture.completedFuture(getFiberGroup(), 0L);
            }
        }), this::afterSaveSnapshot);
    }

    private void markError(Throwable e, String step) {
        error = true;
        log.error("{} failed during shutdown, groupId={}", step, g.getGroupId(), e);
    }

    private FrameCallResult afterSaveSnapshot(Long ignored) {
        try {
            gc.snapshotManager.stopFiber();
        } catch (Throwable e) {
            markError(e, "stopSnapshotFiber");
        }
        return Fiber.call(new AwaitFutureFrame<>("applyManagerShutdown",
                () -> gc.applyManager.shutdown()), this::afterApplyManagerShutdown);
    }

    private FrameCallResult afterApplyManagerShutdown(Void unused) {
        return Fiber.call(new AwaitFutureFrame<>("raftLogClose", () -> gc.raftLog.close()),
                this::afterRaftLogClose);
    }

    private FrameCallResult afterRaftLogClose(Void unused) {
        try {
            gc.raftStatus.tailCache.cleanAll();
        } catch (Throwable e) {
            markError(e, "cleanTailCache");
        }
        // if any error occurred, skip the final status persist; stale status in the file is always safe
        return Fiber.call(new AwaitFutureFrame<>("statusManagerClose",
                () -> gc.statusManager.close(!error)), this::justReturn);
    }

    // a frame's handle() only catches the first exception, so each step uses a new AwaitFutureFrame
    private class AwaitFutureFrame<O> extends FiberFrame<O> {
        private final String step;
        private final Supplier<FiberFuture<O>> futureSupplier;

        AwaitFutureFrame(String step, Supplier<FiberFuture<O>> futureSupplier) {
            super(step);
            this.step = step;
            this.futureSupplier = futureSupplier;
        }

        @Override
        public FrameCallResult execute(Void input) {
            return futureSupplier.get().await(this::justReturn);
        }

        @Override
        protected FrameCallResult handle(Throwable ex) {
            markError(ex, step);
            return Fiber.frameReturn();
        }
    }
}
