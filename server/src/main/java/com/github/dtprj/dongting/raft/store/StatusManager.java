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

import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCall;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.fiber.HandlerFrame;
import com.github.dtprj.dongting.fiber.PostFiberFrame;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
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

    private long requestUpdateVersion;
    private long finishedUpdateVersion;
    // updates with version <= failedUpdateVersion failed or were cancelled, waiters of them
    // receive an exception. Reset when a later update succeeds.
    private long failedUpdateVersion;
    // the value of IdxFileQueue.KEY_PERSIST_IDX_INDEX that is known to be durable in the status
    // file. The restore process starts from the value in the status file, so log/idx file deletion
    // must not remove data that the next restore still depends on.
    long lastPersistedIdxIndex;

    private final FiberCondition needUpdateCondition;
    private final FiberCondition updateDoneCondition;
    private final Fiber updateFiber;

    public StatusManager(RaftGroupConfigEx groupConfig) {
        this.groupConfig = groupConfig;
        this.raftStatus = (RaftStatusImpl) groupConfig.raftStatus;
        File dir = FileUtil.ensureDir(groupConfig.dataDir);
        File file = new File(dir, groupConfig.statusFile);
        FiberGroup fg = groupConfig.fiberGroup;
        this.statusFile = new StatusFile(file, groupConfig);
        this.updateFiber = new Fiber("status-update-" + groupConfig.groupId, fg, new UpdateFiberFrame());
        this.needUpdateCondition = fg.newCondition("StatusNeedUpdate" + groupConfig.groupId);
        this.updateDoneCondition = fg.newCondition("StatusUpdateDone" + groupConfig.groupId);
    }

    public FiberFrame<Void> initStatusFile() {
        FiberFrame<Void> subFrame = statusFile.init();
        return new PostFiberFrame<>(subFrame) {
            @Override
            protected FrameCallResult postProcess(Void result) {
                Map<String, String> loadedProps = statusFile.getProperties();

                raftStatus.currentTerm = RaftUtil.parseInt(loadedProps, CURRENT_TERM, 0);
                raftStatus.votedFor = RaftUtil.parseInt(loadedProps, VOTED_FOR, 0);
                raftStatus.commitIndex = RaftUtil.parseInt(loadedProps, COMMIT_INDEX, 0);
                raftStatus.installSnapshot = RaftUtil.parseBoolean(loadedProps, INSTALL_SNAPSHOT, false);
                raftStatus.firstValidIndex = RaftUtil.parseLong(loadedProps, FIRST_VALID_IDX, 1);
                lastPersistedIdxIndex = RaftUtil.parseLong(loadedProps, RaftIdxFileQueue.KEY_PERSIST_IDX_INDEX, 0);

                updateFiber.start();
                return Fiber.frameReturn();
            }
        };
    }

    public FiberFuture<Void> close() {
        closed = true;
        persistAsync();
        if (updateFiber.isStarted()) {
            return updateFiber.join();
        } else {
            return FiberFuture.completedFuture(groupConfig.fiberGroup, null);
        }
    }

    private class UpdateFiberFrame extends FiberFrame<Void> {
        private long version;
        private long writingIdxIndex;

        @Override
        public FrameCallResult execute(Void input) {
            if (requestUpdateVersion > finishedUpdateVersion && requestUpdateVersion > failedUpdateVersion) {
                return doUpdate();
            } else {
                if (closed) {
                    log.info("status update fiber exit, groupId={}", groupConfig.groupId);
                    updateDoneCondition.signalAll();
                    return Fiber.frameReturn();
                }
                return needUpdateCondition.await(this);
            }
        }

        private FrameCallResult doUpdate() {
            FiberFrame<Void> updateFrame = new FiberFrame<>() {
                @Override
                public FrameCallResult execute(Void input) {
                    copyWriteData();
                    version = requestUpdateVersion;
                    writingIdxIndex = RaftUtil.parseLong(statusFile.getProperties(),
                            RaftIdxFileQueue.KEY_PERSIST_IDX_INDEX, 0);
                    FiberFuture<Void> f = statusFile.update();
                    return f.await(this::justReturn);
                }
            };
            RetryFrame<Void> retryFrame = new RetryFrame<>(updateFrame, groupConfig.ioRetryInterval,
                    true, () -> raftStatus.installSnapshot);
            return Fiber.call(new HandlerFrame<>(retryFrame), this::afterUpdate);
        }

        private FrameCallResult afterUpdate(Pair<Void, Throwable> result) {
            Throwable ex = result.getRight();
            if (ex == null) {
                finishedUpdateVersion = version;
                lastPersistedIdxIndex = writingIdxIndex;
                failedUpdateVersion = 0;
            } else {
                failedUpdateVersion = version;
                log.error("save status failed, groupId={}", groupConfig.groupId, ex);
            }
            updateDoneCondition.signalAll();
            // loop
            return Fiber.yield(this);
        }

        @Override
        protected FrameCallResult handle(Throwable ex) {
            failedUpdateVersion = requestUpdateVersion;
            updateDoneCondition.signalAll();
            log.error("update status file error, groupId={}", groupConfig.groupId, ex);
            throw Fiber.fatal(ex);
        }

        private void copyWriteData() {
            Map<String, String> destMap = statusFile.getProperties();

            destMap.put(CURRENT_TERM, String.valueOf(raftStatus.currentTerm));
            destMap.put(VOTED_FOR, String.valueOf(raftStatus.votedFor));
            destMap.put(COMMIT_INDEX, String.valueOf(raftStatus.commitIndex));
            destMap.put(INSTALL_SNAPSHOT, String.valueOf(raftStatus.installSnapshot));
            destMap.put(FIRST_VALID_IDX, String.valueOf(raftStatus.firstValidIndex));
        }
    }

    public void persistAsync() {
        requestUpdateVersion++;
        needUpdateCondition.signalAll();
    }

    public FrameCallResult waitUpdateFinish(FrameCall<Void> resumePoint) throws RaftException {
        return waitUpdateFinish(requestUpdateVersion, resumePoint);
    }

    private FrameCallResult waitUpdateFinish(long version, FrameCall<Void> resumePoint) throws RaftException {
        if (finishedUpdateVersion >= version) {
            return Fiber.resume(null, resumePoint);
        }
        if (failedUpdateVersion >= version) {
            throw new RaftException("status update failed, groupId=" + groupConfig.groupId
                    + ", version=" + version);
        }
        return updateDoneCondition.await(1000, v -> waitUpdateFinish(version, resumePoint));
    }

    public Map<String, String> getProperties() {
        return statusFile.getProperties();
    }
}
