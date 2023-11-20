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
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class StatusManager implements AutoCloseable {
    private static final DtLog log = DtLogs.getLogger(StatusManager.class);

    private static final String CURRENT_TERM_KEY = "currentTerm";
    private static final String VOTED_FOR_KEY = "votedFor";
    private static final String COMMIT_INDEX_KEY = "commitIndex";

    private final RaftGroupConfig groupConfig;
    private final RaftStatus raftStatus;
    private final StatusFile statusFile;

    private boolean closed;

    private long lastNeedFlushVersion;
    private long requestUpdateVersion;
    private long finishedUpdateVersion;

    private final FiberCondition needUpdateCondition;
    private final FiberCondition updateDoneCondition;
    private final Fiber updateFiber;

    public StatusManager(RaftGroupConfig groupConfig, RaftStatus raftStatus) {
        this.groupConfig = groupConfig;
        this.raftStatus = raftStatus;
        File dir = FileUtil.ensureDir(groupConfig.getDataDir());
        File file = new File(dir, groupConfig.getStatusFile());
        FiberGroup fg = groupConfig.getFiberGroup();
        this.statusFile = new StatusFile(file, groupConfig.getIoExecutor(), fg);
        this.updateFiber = new Fiber("status-update", fg, new UpdateFiberFrame());
        this.needUpdateCondition = fg.newCondition();
        this.updateDoneCondition = fg.newCondition();
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
                return frameReturn();
            }
        };
    }

    public void close() {
        closed = true;
        // wake up update fiber
        needUpdateCondition.signal();
    }

    private class UpdateFiberFrame extends FiberFrame<Void> {
        private long version;
        private final Supplier<FiberFuture<Void>> ioCallback = () -> {
            copyWriteData();
            version = requestUpdateVersion;
            FiberFuture<Void> f = getFiberGroup().newFuture();
            statusFile.update(lastNeedFlushVersion > finishedUpdateVersion, AsyncIoTask.wrap(f));
            return f;
        };
        // the frame is reused
        private final IoRetryFrame<Void> ioFrame = new IoRetryFrame<>(groupConfig.getIoRetryInterval(),
                groupConfig.getIoTimeout(), groupConfig.getStopCondition(), ioCallback);
        @Override
        public FrameCallResult execute(Void input) {
            if (closed) {
                log.debug("status update fiber exit");
                updateDoneCondition.signal();
                return frameReturn();
            }
            if (requestUpdateVersion > finishedUpdateVersion) {
                return doUpdate(null);
            } else {
                return awaitOn(needUpdateCondition, this::doUpdate);
            }
        }

        private FrameCallResult doUpdate(Void v) {
            return call(ioFrame, this::resumeOnUpdateDone);
        }

        private FrameCallResult resumeOnUpdateDone(Void v) {
            finishedUpdateVersion = version;
            updateDoneCondition.signal();
            // loop
            return execute(v);
        }

        @Override
        protected FrameCallResult handle(Throwable ex) throws Throwable {
            updateDoneCondition.signal();
            log.error("update status file error", ex);
            return fatal(ex);
        }

        @Override
        protected FrameCallResult doFinally() {
            DtUtil.close(statusFile);
            return frameReturn();
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
                if (closed) {
                    throw new RaftException("status manager is closed");
                }
                if (finished(updateFiber)) {
                    throw new RaftException("update fiber is finished");
                }
                needUpdateCondition.signal();
                return awaitOn(updateDoneCondition, this::resume);
            }

            private FrameCallResult resume(Void unused) {
                if (finishedUpdateVersion >= reqVersion) {
                    return frameReturn();
                }
                return execute0();
            }
        };
    }

    public Properties getProperties() {
        return statusFile.getProperties();
    }
}
