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
package com.github.dtprj.dongting.raft.store;

import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCall;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.fiber.PostFiberFrame;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.impl.FileUtil;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;

import java.io.File;
import java.util.Map;

/**
 * @author huangli
 */
public class StatusManager {
    private static final DtLog log = DtLogs.getLogger(StatusManager.class);

    public static final String CURRENT_TERM = "currentTerm";
    public static final String VOTED_FOR = "votedFor";
    public static final String COMMIT_INDEX = "commitIndex";
    public static final String INSTALL_SNAPSHOT = "installSnapshot";

    public static final String FIRST_VALID_IDX = "firstValidIndex";

    private final RaftGroupConfigEx groupConfig;
    private final RaftStatusImpl raftStatus;
    private final StatusFile statusFile;

    private boolean closed;

    private long lastNeedForceVersion;
    private long requestUpdateVersion;
    private long finishedUpdateVersion;

    private final FiberCondition needUpdateCondition;
    private final FiberCondition updateDoneCondition;
    private final Fiber updateFiber;

    public StatusManager(RaftGroupConfigEx groupConfig) {
        this.groupConfig = groupConfig;
        this.raftStatus = (RaftStatusImpl) groupConfig.getRaftStatus();
        File dir = FileUtil.ensureDir(groupConfig.getDataDir());
        File file = new File(dir, groupConfig.getStatusFile());
        FiberGroup fg = groupConfig.getFiberGroup();
        this.statusFile = new StatusFile(file, groupConfig);
        this.updateFiber = new Fiber("status-update-" + groupConfig.getGroupId(), fg, new UpdateFiberFrame());
        this.needUpdateCondition = fg.newCondition("StatusNeedUpdate" + groupConfig.getGroupId());
        this.updateDoneCondition = fg.newCondition("StatusUpdateDone" + groupConfig.getGroupId());
    }

    public FiberFrame<Void> initStatusFile() {
        FiberFrame<Void> subFrame = statusFile.init();
        return new PostFiberFrame<>(subFrame) {
            @Override
            protected FrameCallResult postProcess(Void result) {
                Map<String, String> loadedProps = statusFile.getProperties();

                raftStatus.setCurrentTerm(RaftUtil.parseInt(loadedProps, CURRENT_TERM, 0));
                raftStatus.setVotedFor(RaftUtil.parseInt(loadedProps, VOTED_FOR, 0));
                raftStatus.setCommitIndex(RaftUtil.parseInt(loadedProps, COMMIT_INDEX, 0));
                raftStatus.setInstallSnapshot(RaftUtil.parseBoolean(loadedProps, INSTALL_SNAPSHOT, false));
                raftStatus.setFirstValidIndex(RaftUtil.parseLong(loadedProps, FIRST_VALID_IDX, 1));

                updateFiber.start();
                return Fiber.frameReturn();
            }
        };
    }

    public FiberFuture<Void> close() {
        // TODO wait last update
        closed = true;
        // wake up update fiber
        needUpdateCondition.signalAll();
        if (updateFiber.isStarted()) {
            return updateFiber.join();
        } else {
            return FiberFuture.completedFuture(groupConfig.getFiberGroup(), null);
        }
    }

    private class UpdateFiberFrame extends FiberFrame<Void> {
        private long version;
        private boolean force;

        @Override
        public FrameCallResult execute(Void input) {
            if (requestUpdateVersion > finishedUpdateVersion) {
                return doUpdate(null);
            } else {
                if (closed) {
                    log.info("status update fiber exit, groupId={}", groupConfig.getGroupId());
                    updateDoneCondition.signalAll();
                    return Fiber.frameReturn();
                }
                return needUpdateCondition.await(this::doUpdate);
            }
        }

        private FrameCallResult doUpdate(Void v) {
            FiberFrame<Void> updateFrame = new FiberFrame<>() {
                @Override
                public FrameCallResult execute(Void input) {
                    copyWriteData();
                    version = requestUpdateVersion;
                    force = lastNeedForceVersion > finishedUpdateVersion;
                    FiberFuture<Void> f = statusFile.update(force);
                    return f.await(this::justReturn);
                }
            };
            RetryFrame<Void> retryFrame = new RetryFrame<>(updateFrame, groupConfig.getIoRetryInterval(),
                    true, raftStatus::isInstallSnapshot);
            return Fiber.call(retryFrame, this::resumeOnUpdateDone);
        }

        private FrameCallResult resumeOnUpdateDone(Void v) {
            // log.info("status update done, version={}, flush={}", version, flush);
            finishedUpdateVersion = version;
            updateDoneCondition.signalAll();
            // loop
            return Fiber.yield(this);
        }

        @Override
        protected FrameCallResult handle(Throwable ex) {
            updateDoneCondition.signalAll();
            log.error("update status file error, groupId={}", groupConfig.getGroupId(), ex);
            throw Fiber.fatal(ex);
        }

        @Override
        protected FrameCallResult doFinally() {
            DtUtil.close(statusFile);
            return Fiber.frameReturn();
        }

        private void copyWriteData() {
            Map<String, String> destMap = statusFile.getProperties();

            destMap.put(CURRENT_TERM, String.valueOf(raftStatus.getCurrentTerm()));
            destMap.put(VOTED_FOR, String.valueOf(raftStatus.getVotedFor()));
            destMap.put(COMMIT_INDEX, String.valueOf(raftStatus.getCommitIndex()));
            destMap.put(INSTALL_SNAPSHOT, String.valueOf(raftStatus.isInstallSnapshot()));
            destMap.put(FIRST_VALID_IDX, String.valueOf(raftStatus.getFirstValidIndex()));
        }
    }

    public void persistAsync(boolean force) {
        requestUpdateVersion++;
        if (force) {
            lastNeedForceVersion = requestUpdateVersion;
        }
        needUpdateCondition.signal();
    }

    public FrameCallResult waitUpdateFinish(FrameCall<Void> resumePoint) {
        return waitUpdateFinish(requestUpdateVersion, resumePoint);
    }

    private FrameCallResult waitUpdateFinish(long version, FrameCall<Void> resumePoint) {
        if (finishedUpdateVersion >= version) {
            return Fiber.resume(null, resumePoint);
        }
        return updateDoneCondition.await(1000, v -> waitUpdateFinish(version, resumePoint));
    }

    public Map<String, String> getProperties() {
        return statusFile.getProperties();
    }
}
