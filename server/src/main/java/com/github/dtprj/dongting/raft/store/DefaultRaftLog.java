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
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.impl.FileUtil;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.impl.TailCache;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;

import java.io.File;
import java.util.function.Supplier;

import static com.github.dtprj.dongting.raft.store.IdxFileQueue.KEY_NEXT_POS_AFTER_INSTALL_SNAPSHOT;

/**
 * @author huangli
 */
public class DefaultRaftLog implements RaftLog {
    private static final DtLog log = DtLogs.getLogger(DefaultRaftLog.class);
    private final RaftGroupConfigEx groupConfig;
    private final Timestamp ts;
    private final RaftStatusImpl raftStatus;
    private final StatusManager statusManager;
    private final FiberGroup fiberGroup;
    LogFileQueue logFiles;
    IdxFileQueue idxFiles;

    private static final long DEFAULT_DELETE_INTERVAL_MILLIS = 10 * 1000;

    int idxItemsPerFile = IdxFileQueue.DEFAULT_ITEMS_PER_FILE;
    int idxMaxCacheItems = IdxFileQueue.DEFAULT_MAX_CACHE_ITEMS;
    long logFileSize = LogFileQueue.DEFAULT_LOG_FILE_SIZE;

    private final Fiber deleteFiber;

    DefaultRaftLog(RaftGroupConfigEx groupConfig, StatusManager statusManager, long deleteIntervalMillis) {
        this.groupConfig = groupConfig;
        this.ts = groupConfig.getTs();
        this.raftStatus = (RaftStatusImpl) groupConfig.getRaftStatus();
        this.statusManager = statusManager;
        this.fiberGroup = groupConfig.getFiberGroup();

        this.deleteFiber = new Fiber("delete-" + groupConfig.getGroupId(),
                fiberGroup, new DeleteFiberFrame(deleteIntervalMillis), true);
    }

    public DefaultRaftLog(RaftGroupConfigEx groupConfig, StatusManager statusManager) {
        this(groupConfig, statusManager, DEFAULT_DELETE_INTERVAL_MILLIS);
    }

    @Override
    public FiberFrame<Pair<Integer, Long>> init() throws Exception {
        return new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) throws Exception {
                File dataDir = FileUtil.ensureDir(groupConfig.getDataDir());

                idxFiles = new IdxFileQueue(FileUtil.ensureDir(dataDir, "idx"),
                        statusManager, groupConfig, idxItemsPerFile, idxMaxCacheItems);
                logFiles = new LogFileQueue(FileUtil.ensureDir(dataDir, "log"),
                        groupConfig, idxFiles, logFileSize);
                logFiles.initQueue();
                RaftUtil.checkStop(fiberGroup);
                return Fiber.call(idxFiles.initRestorePos(), this::afterIdxFileQueueInit);
            }

            private FrameCallResult afterIdxFileQueueInit(Pair<Long, Long> p) {
                RaftUtil.checkStop(fiberGroup);
                if (p == null) {
                    raftStatus.setInstallSnapshot(true);
                    setResult(new Pair<>(0, 0L));
                    return Fiber.frameReturn();
                }
                long restoreIndex = p.getLeft();
                long restoreIndexPos = p.getRight();
                long firstValidPos = Long.parseLong(statusManager.getProperties()
                        .getProperty(KEY_NEXT_POS_AFTER_INSTALL_SNAPSHOT, "0"));
                return Fiber.call(logFiles.restore(restoreIndex, restoreIndexPos, firstValidPos),
                        this::afterLogRestore);
            }

            private FrameCallResult afterLogRestore(int lastTerm) {
                RaftUtil.checkStop(fiberGroup);
                idxFiles.setInitialized(true);
                logFiles.setInitialized(true);
                if (idxFiles.getNextIndex() == 1) {
                    setResult(new Pair<>(0, 0L));
                } else {
                    long lastIndex = idxFiles.getNextIndex() - 1;
                    setResult(new Pair<>(lastTerm, lastIndex));
                }
                deleteFiber.start();
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) throws Throwable {
                close();
                throw ex;
            }
        };
    }

    @Override
    public void truncateTail(long index) {
        TailCache tailCache = raftStatus.getTailCache();
        tailCache.truncate(index);

        // committed logs can't truncate, and we wait write finish before truncate,
        // so we can assert the index is in the cache
        long pos = idxFiles.loadLogPosInCache(index);

        idxFiles.truncateTail(index);
        logFiles.truncateTail(index, pos);
    }

    @Override
    public LogIterator openIterator(Supplier<Boolean> cancelIndicator) {
        return new FileLogLoader(idxFiles, logFiles, groupConfig, cancelIndicator);
    }

    @Override
    public FiberFrame<Pair<Integer, Long>> tryFindMatchPos(int suggestTerm, long suggestIndex,
                                                           Supplier<Boolean> cancelIndicator) {
        return logFiles.tryFindMatchPos(suggestTerm, suggestIndex, cancelIndicator);
    }

    @Override
    public void markTruncateByIndex(long index, long delayMillis) {
        long bound = Math.min(raftStatus.getLastApplied(), idxFiles.getNextPersistIndex());
        bound = Math.min(bound, index);
        logFiles.markDelete(bound, Long.MAX_VALUE, delayMillis);
    }

    @Override
    public void markTruncateByTimestamp(long timestampBound, long delayMillis) {
        long bound = Math.min(raftStatus.getLastApplied(), idxFiles.getNextPersistIndex());
        logFiles.markDelete(bound, timestampBound, delayMillis);
    }

    @Override
    public void beginInstall() throws Exception {

    }

    @Override
    public void finishInstall(long nextLogIndex, long nextLogPos) throws Exception {

    }

    @Override
    public FiberFrame<Long> loadNextItemPos(long index) {
        return null;
    }

    @Override
    public FiberFuture<Void> close() {
        FiberFuture<Void> f1 = idxFiles.close();
        FiberFuture<Void> f2 = logFiles.close();
        // delete fiber is daemon
        return FiberFuture.allOf(f1, f2);
    }

    private class DeleteFiberFrame extends FiberFrame<Void> {

        private final long deleteIntervalMillis;

        public DeleteFiberFrame(long deleteIntervalMillis) {
            this.deleteIntervalMillis = deleteIntervalMillis;
        }

        @Override
        public FrameCallResult execute(Void input) {
            if (isGroupShouldStopPlain()) {
                return Fiber.frameReturn();
            }
            return Fiber.sleepUntilShouldStop(deleteIntervalMillis, this::deleteLogs);
        }

        private FrameCallResult deleteLogs(Void unused) {
            if (isGroupShouldStopPlain()) {
                return Fiber.frameReturn();
            }
            long taskStartTimestamp = ts.getWallClockMillis();
            // ex handled by delete method
            FiberFrame<Void> f = logFiles.delete(logFile -> {
                if (isGroupShouldStopPlain()) {
                    return false;
                }
                long deleteTimestamp = logFile.deleteTimestamp;
                return deleteTimestamp > 0 && deleteTimestamp < taskStartTimestamp;
            });
            return Fiber.call(f, this::deleteIdx);
        }

        private FrameCallResult deleteIdx(Void unused) {
            if (isGroupShouldStopPlain()) {
                return Fiber.frameReturn();
            }
            // ex handled by delete method
            FiberFrame<Void> f = idxFiles.delete(logFile -> {
                if (isGroupShouldStopPlain()) {
                    return false;
                }
                long firstIndexOfNextFile = idxFiles.posToIndex(logFile.endPos);
                return firstIndexOfNextFile < logFiles.getFirstIndex()
                        && firstIndexOfNextFile < idxFiles.persistedIndex;
            });
            // loop
            return Fiber.call(f, this);
        }

        @Override
        protected FrameCallResult handle(Throwable ex) throws Throwable {
            throw Fiber.fatal(ex);
        }
    }
}
