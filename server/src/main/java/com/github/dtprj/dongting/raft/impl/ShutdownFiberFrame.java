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

    public ShutdownFiberFrame(RaftGroupImpl g) {
        this.g = g;
        this.fiberGroup = g.fiberGroup;
        this.gc = g.groupComponents;
    }

    @Override
    protected FrameCallResult doFinally() {
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
        gc.raftStatus.needRepCondition.signalAll();
        FiberFuture<Long> f;
        if (gc.snapshotManager != null && saveSnapshot
                && gc.raftStatus.isInitFinished() && !gc.raftStatus.isInitFailed()) {
            f = gc.snapshotManager.saveSnapshot();
        } else {
            f = FiberFuture.completedFuture(getFiberGroup(), 0L);
        }
        return f.await(this::afterSaveSnapshot);
    }

    private FrameCallResult afterSaveSnapshot(Long notUsed) {
        if (gc.snapshotManager != null) {
            gc.snapshotManager.stopFiber();
        }
        gc.applyManager.shutdown(timeout);
        return gc.raftLog.close().await(this::afterRaftLogClose);
    }

    private FrameCallResult afterRaftLogClose(Void unused) {
        g.groupComponents.raftStatus.tailCache.cleanAll();
        return g.groupComponents.statusManager.close().await(this::afterStatusManagerClose);
    }

    private FrameCallResult afterStatusManagerClose(Void unused) {
        RaftFactory raftFactory = gc.raftFactory;
        if (!raftFactory.useSharedIoExecutor()) {
            raftFactory.shutdownBlockIoExecutor(gc.serverConfig, gc.groupConfig,
                    gc.groupConfig.blockIoExecutor);
        }
        gc.groupConfig.perfCallback.shutdown();

        return Fiber.frameReturn();
    }
}
