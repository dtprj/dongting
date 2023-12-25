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
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.fiber.PostFiberFrame;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.FileUtil;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;
import com.github.dtprj.dongting.raft.server.RaftStatus;

import java.io.File;
import java.util.Properties;

/**
 * @author huangli
 */
public class StatusManager {
    private static final DtLog log = DtLogs.getLogger(StatusManager.class);

    static final String CURRENT_TERM_KEY = "currentTerm";
    static final String VOTED_FOR_KEY = "votedFor";
    static final String COMMIT_INDEX_KEY = "commitIndex";

    private final RaftGroupConfig groupConfig;
    private final RaftStatus raftStatus;
    private final StatusFile statusFile;

    private boolean closed;

    private long lastNeedFlushVersion;
    private long requestUpdateVersion;
    private long finishedUpdateVersion;

    private final FiberCondition needUpdateCondition;
    final FiberCondition updateDoneCondition;
    final Fiber updateFiber;

    public StatusManager(RaftGroupConfig groupConfig) {
        this.groupConfig = groupConfig;
        this.raftStatus = groupConfig.getRaftStatus();
        File dir = FileUtil.ensureDir(groupConfig.getDataDir());
        File file = new File(dir, groupConfig.getStatusFile());
        FiberGroup fg = groupConfig.getFiberGroup();
        this.statusFile = new StatusFile(file, groupConfig.getIoExecutor(), fg);
        this.updateFiber = new Fiber("status-update-" + groupConfig.getGroupId(), fg, new UpdateFiberFrame());
        this.needUpdateCondition = fg.newCondition("StatusNeedUpdate" + groupConfig.getGroupId());
        this.updateDoneCondition = fg.newCondition("StatusUpdateDone" + groupConfig.getGroupId());
    }

    public FiberFrame<Void> initStatusFile() {
        FiberFrame<Void> subFrame = statusFile.init(groupConfig.getIoTimeout());
        return new PostFiberFrame<>(subFrame) {
            @Override
            protected FrameCallResult postProcess(Void result) {
                Properties loadedProps = statusFile.getProperties();

                raftStatus.setCurrentTerm(Integer.parseInt(loadedProps.getProperty(CURRENT_TERM_KEY, "0")));
                raftStatus.setVotedFor(Integer.parseInt(loadedProps.getProperty(VOTED_FOR_KEY, "0")));
                raftStatus.setCommitIndex(Integer.parseInt(loadedProps.getProperty(COMMIT_INDEX_KEY, "0")));

                updateFiber.start();
                return Fiber.frameReturn();
            }
        };
    }

    public FiberFuture<Void> close() {
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
        private boolean flush;

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
            FiberFrame<Void> updateFrame = new FiberFrame<>(){
                @Override
                public FrameCallResult execute(Void input) {
                    copyWriteData();
                    version = requestUpdateVersion;
                    flush = lastNeedFlushVersion > finishedUpdateVersion;
                    FiberFuture<Void> f = statusFile.update(flush);
                    return f.await(groupConfig.getIoTimeout(), this::justReturn);
                }
            };
            RetryFrame<Void> retryFrame = new RetryFrame<>(updateFrame,
                    groupConfig.getIoRetryInterval(), true);
            return Fiber.call(retryFrame, this::resumeOnUpdateDone);
        }

        private FrameCallResult resumeOnUpdateDone(Void v) {
            log.info("status update done, version={}, flush={}", version, flush);
            finishedUpdateVersion = version;
            updateDoneCondition.signalAll();
            // loop
            return execute(v);
        }

        @Override
        protected FrameCallResult handle(Throwable ex) {
            updateDoneCondition.signalAll();
            log.error("update status file error, groupId={}", groupConfig.getGroupId(), ex);
            return Fiber.fatal(ex);
        }

        @Override
        protected FrameCallResult doFinally() {
            DtUtil.close(statusFile);
            return Fiber.frameReturn();
        }

        private void copyWriteData() {
            Properties destProps = statusFile.getProperties();

            destProps.setProperty(CURRENT_TERM_KEY, String.valueOf(raftStatus.getCurrentTerm()));
            destProps.setProperty(VOTED_FOR_KEY, String.valueOf(raftStatus.getVotedFor()));
            destProps.setProperty(COMMIT_INDEX_KEY, String.valueOf(raftStatus.getCommitIndex()));
        }
    }

    public void persistAsync() {
        requestUpdateVersion++;
        needUpdateCondition.signal();
    }

    public FiberFrame<Void> persistSync() {
        if (closed) {
            throw new RaftException("status manager is closed");
        }
        return new FiberFrame<>() {
            private long reqVersion;
            @Override
            public FrameCallResult execute(Void unused) {
                requestUpdateVersion++;
                lastNeedFlushVersion = requestUpdateVersion;
                reqVersion = requestUpdateVersion;
                return execute0();
            }

            private FrameCallResult execute0() {
                if (updateFiber.isFinished()) {
                    throw new RaftException("update fiber is finished");
                }
                needUpdateCondition.signal();
                return updateDoneCondition.await(this::resume);
            }

            private FrameCallResult resume(Void unused) {
                if (finishedUpdateVersion >= reqVersion) {
                    return Fiber.frameReturn();
                }
                return execute0();
            }
        };
    }

    public Properties getProperties() {
        return statusFile.getProperties();
    }
}
