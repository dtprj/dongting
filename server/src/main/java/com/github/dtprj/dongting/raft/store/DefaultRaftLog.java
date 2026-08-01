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

import com.github.dtprj.dongting.common.IndexedQueue;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.impl.FileUtil;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.sm.RaftCodecFactory;

import java.io.File;
import java.util.List;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public final class DefaultRaftLog implements RaftLog {
    private static final DtLog log = DtLogs.getLogger(DefaultRaftLog.class);
    private static final String KEY_FIRST_VALID_POS = "firstValidPos";
    private final RaftGroupConfigEx groupConfig;
    private final Timestamp ts;
    private final RaftStatusImpl raftStatus;
    private final StatusManager statusManager;
    private final FiberGroup fiberGroup;
    private final RaftCodecFactory raftCodecFactory;
    private final long deleteIntervalMillis;
    LogFileQueue logFiles;
    RaftIdxFileQueue idxFiles;

    private static final long DEFAULT_DELETE_INTERVAL_MILLIS = 10 * 1000;

    private QueueDeleteFiberFrame deleteFrame;

    DefaultRaftLog(RaftGroupConfigEx groupConfig, StatusManager statusManager, RaftCodecFactory raftCodecFactory,
                   long deleteIntervalMillis) {
        this.groupConfig = groupConfig;
        this.ts = groupConfig.ts;
        this.raftStatus = (RaftStatusImpl) groupConfig.raftStatus;
        this.statusManager = statusManager;
        this.fiberGroup = groupConfig.fiberGroup;
        this.raftCodecFactory = raftCodecFactory;
        this.deleteIntervalMillis = deleteIntervalMillis;
    }

    public DefaultRaftLog(RaftGroupConfigEx groupConfig, StatusManager statusManager, RaftCodecFactory raftCodecFactory) {
        this(groupConfig, statusManager, raftCodecFactory, DEFAULT_DELETE_INTERVAL_MILLIS);
    }

    private void createFiles() {
        File dataDir = FileUtil.ensureDir(groupConfig.dataDir);

        idxFiles = new RaftIdxFileQueue(FileUtil.ensureDir(dataDir, "idx"), statusManager, groupConfig);
        logFiles = new LogFileQueue(FileUtil.ensureDir(dataDir, "log"), groupConfig, idxFiles);
    }

    @Override
    public FiberFrame<Pair<Integer, Long>> init() {
        return new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                createFiles();
                logFiles.initQueue();
                idxFiles.initQueue();
                RaftUtil.checkStop(fiberGroup);

                if (raftStatus.installSnapshot) {
                    idxFiles.setInitialized(true);
                    logFiles.setInitialized(true);
                    startQueueDeleteFiber();
                    deleteFrame.requestDeleteAllAndExit = true;
                    deleteFrame.delCond.signal();
                    setResult(null);
                    return Fiber.frameReturn();
                }

                return Fiber.call(idxFiles.initRestorePos(), this::afterIdxFileQueueInit);
            }

            private FrameCallResult afterIdxFileQueueInit(Pair<Long, Long> p) {
                RaftUtil.checkStop(fiberGroup);
                if (p == null) {
                    // return null will cause install snapshot
                    setResult(null);
                    return Fiber.frameReturn();
                }
                long restoreIndex = p.getLeft();
                long restoreStartPos = p.getRight();
                long firstValidPos = RaftUtil.parseLong(statusManager.getProperties(),
                        KEY_FIRST_VALID_POS, 0);

                // restore will cause idx write, so start idx fibers
                idxFiles.startFibers();

                return Fiber.call(logFiles.restore(restoreIndex, restoreStartPos, firstValidPos),
                        this::afterLogRestore);
            }

            private FrameCallResult afterLogRestore(int lastTerm) {
                RaftUtil.checkStop(fiberGroup);

                logFiles.startFibers();
                idxFiles.setInitialized(true);
                logFiles.setInitialized(true);

                startQueueDeleteFiber();

                if (idxFiles.getNextIndex() == 1) {
                    setResult(new Pair<>(0, 0L));
                } else {
                    long lastIndex = idxFiles.getNextIndex() - 1;
                    setResult(new Pair<>(lastTerm, lastIndex));
                }
                return Fiber.frameReturn();
            }
        };
    }

    private void startQueueDeleteFiber() {
        deleteFrame = new QueueDeleteFiberFrame();
        Fiber deleteFiber = new Fiber("delete-" + groupConfig.groupId,
                fiberGroup, deleteFrame);
        deleteFiber.start();
    }

    @Override
    public FiberFrame<Void> append(List<RaftTask> inputs) {
        return logFiles.append(inputs);
    }

    @Override
    public FiberFrame<Void> truncateTail(long index) {
        return new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(idxFiles.loadLogPos(index), this::afterPosLoad);
            }

            private FrameCallResult afterPosLoad(Long pos) {
                if (logFiles.startPosOfFile(pos) == pos && index - 1 >= logFiles.getFirstIndex()) {
                    return Fiber.call(loadNextItemPos(index - 1), this::afterPosLoad2);
                }
                idxFiles.truncateTail(index);
                logFiles.truncateTail(index, pos);
                return Fiber.frameReturn();
            }

            private FrameCallResult afterPosLoad2(Long pos) {
                idxFiles.truncateTail(index);
                logFiles.truncateTail(index, pos);
                return Fiber.frameReturn();
            }
        };

    }

    @Override
    public LogIterator openIterator(Supplier<Boolean> cancelIndicator, boolean decode) {
        return new FileLogLoader(idxFiles, logFiles, groupConfig, raftCodecFactory, cancelIndicator, decode);
    }

    @Override
    public FiberFrame<Pair<Integer, Long>> tryFindMatchPos(int suggestTerm, long suggestIndex,
                                                           Supplier<Boolean> cancelIndicator) {
        return new MatchPosFinder(groupConfig, logFiles.queue, idxFiles, cancelIndicator, raftStatus.tailCache,
                logFiles.fileLenMask, suggestTerm, suggestIndex, raftStatus.lastLogIndex);
    }

    @Override
    public void markTruncateByIndex(long index, long delayMillis) {
        long bound = Math.min(raftStatus.getLastApplied(), statusManager.lastPersistedIdxIndex);
        bound = Math.min(bound, raftStatus.lastSavedSnapshotIndex);
        bound = Math.min(bound, index);
        log.info("mark truncate log files by index {}, bound={}", index, bound);
        logFiles.markDelete(bound, Long.MAX_VALUE, delayMillis);
    }

    @Override
    public void markTruncateByTimestamp(long timestampBound, long delayMillis) {
        long bound = Math.min(raftStatus.getLastApplied(), statusManager.lastPersistedIdxIndex);
        bound = Math.min(bound, raftStatus.lastSavedSnapshotIndex);
        log.info("mark truncate log files by timestamp {}, bound={}", timestampBound, bound);
        logFiles.markDelete(bound, timestampBound, delayMillis);
    }

    @Override
    public FiberFrame<Void> beginInstall() {
        return new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void unused) {
                FiberFuture<Void> f1 = idxFiles.close();
                FiberFuture<Void> f2 = logFiles.close();
                return FiberFuture.allOf("idxAndLogClose", f1, f2).await(this::afterIdxAndLogClose);
            }

            private FrameCallResult afterIdxAndLogClose(Void unused) {
                deleteFrame.requestDeleteAllAndExit = true;
                deleteFrame.delCond.signal();
                return deleteFrame.getFiber().join(this::afterDeleteFiberExit);
            }

            private FrameCallResult afterDeleteFiberExit(Void unused) {
                return Fiber.call(idxFiles.forceDeleteAll(), this::afterForceDeleteIdxFiles);
            }

            private FrameCallResult afterForceDeleteIdxFiles(Void unused) {
                return Fiber.call(logFiles.forceDeleteAll(), this::justReturn);
            }
        };
    }

    @Override
    public FiberFrame<Long> loadNextItemPos(long index) {
        return new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                if (index == 0) {
                    setResult(0L);
                    return Fiber.frameReturn();
                }
                return Fiber.call(idxFiles.loadRaftIdxInfo(index), this::afterLoadPos);
            }

            private FrameCallResult afterLoadPos(IdxItem idxInfo) {
                setResult(idxInfo.position + idxInfo.size);
                return Fiber.frameReturn();
            }
        };
    }

    @Override
    public FiberFrame<Void> finishInstall(long nextLogIndex, long nextLogPos) {
        createFiles();
        return new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(idxFiles.finishInstall(nextLogIndex), this::afterIdxFinishInstall);
            }

            private FrameCallResult afterIdxFinishInstall(Void unused) {
                return Fiber.call(logFiles.finishInstall(nextLogIndex, nextLogPos), this::afterLogFinishInstall);
            }

            private FrameCallResult afterLogFinishInstall(Void unused) {
                idxFiles.setInitialized(true);
                logFiles.setInitialized(true);
                startQueueDeleteFiber();
                statusManager.getProperties().put(KEY_FIRST_VALID_POS, String.valueOf(nextLogPos));
                return Fiber.frameReturn();
            }
        };
    }

    @Override
    public FiberFuture<Void> close() {
        if (logFiles == null || idxFiles == null) {
            // init() was not called or failed before createFiles()
            return FiberFuture.completedFuture(fiberGroup, null);
        }
        FiberFuture<Void> f1 = logFiles.close();
        FiberFuture<Void> f2 = idxFiles.close();
        if (deleteFrame != null) {
            return FiberFuture.allOf("logClose", f1, f2).compose("deleteFiberJoin", v -> {
                deleteFrame.stopRequested = true;
                deleteFrame.delCond.signal();
                return deleteFrame.getFiber().join();
            });
        } else {
            return FiberFuture.allOf("logClose", f1, f2);
        }
    }

    private class QueueDeleteFiberFrame extends FiberFrame<Void> {

        boolean requestDeleteAllAndExit;
        boolean deleteAndExit;
        boolean stopRequested;
        final FiberCondition delCond = FiberGroup.currentGroup().newCondition("delCond");

        public QueueDeleteFiberFrame() {
        }

        @Override
        public Fiber getFiber() {
            return super.getFiber();
        }

        @Override
        protected FrameCallResult handle(Throwable ex) {
            throw Fiber.fatal(ex);
        }

        @Override
        public FrameCallResult execute(Void input) {
            if (stopRequested) {
                return Fiber.frameReturn();
            }
            logFiles.closeIdleFiles();
            idxFiles.closeIdleFiles();
            if (requestDeleteAllAndExit) {
                deleteAndExit = true;
                return deleteLogs(null);
            }
            return delCond.await(deleteIntervalMillis, this::deleteLogs);
        }

        private boolean shouldDeleteFirstLog() {
            long taskStartTimestamp = ts.wallClockMillis;
            IndexedQueue<LogFile> q = logFiles.queue;
            if (q.size() <= 1) {
                return false;
            }
            LogFile first = q.get(0);
            long deleteTimestamp = first.deleteTimestamp;
            if (deleteTimestamp <= 0 || deleteTimestamp >= taskStartTimestamp) {
                return false;
            }
            LogFile second = q.get(1);
            if (second.firstIndex == 0) {
                return false;
            }
            if (raftStatus.getLastApplied() < second.firstIndex ||
                    raftStatus.lastForceLogIndex < second.firstIndex) {
                return false;
            }
            // the restore process starts from KEY_PERSIST_IDX_INDEX in the status file,
            // so the log entry at that index must not be deleted
            if (statusManager.lastPersistedIdxIndex < second.firstIndex) {
                return false;
            }
            return !first.inUse();
        }

        private FrameCallResult deleteLogs(Void unused) {
            if (stopRequested) {
                return Fiber.frameReturn();
            }
            if (deleteAndExit) {
                if (logFiles.queue.size() > 0) {
                    return Fiber.call(logFiles.deleteFirstFile(), this::deleteLogs);
                } else {
                    log.info("delete all log files done");
                    return deleteIdx(null);
                }
            } else {
                if (shouldDeleteFirstLog()) {
                    return Fiber.call(logFiles.deleteFirstFile(), this::deleteLogs);
                } else {
                    return deleteIdx(null);
                }
            }
        }

        private boolean shouldDeleteFirstIdx() {
            IndexedQueue<LogFile> q = idxFiles.queue;
            if (q.size() <= 1) {
                // don't delete last file
                return false;
            }
            LogFile first = q.get(0);
            long firstIndexOfNextFile = idxFiles.posToIndex(first.endPos);
            if (logFiles.getFirstIndex() < firstIndexOfNextFile) {
                return false;
            }
            // use the durable value in the status file
            if (statusManager.lastPersistedIdxIndex < firstIndexOfNextFile) {
                return false;
            }
            return !first.inUse();
        }

        private FrameCallResult deleteIdx(Void unused) {
            if (stopRequested) {
                return Fiber.frameReturn();
            }
            if (deleteAndExit) {
                if (idxFiles.queue.size() > 0) {
                    return Fiber.call(idxFiles.deleteFirstFile(), this::deleteIdx);
                } else {
                    log.info("delete all idx files done");
                    // fiber exit
                    return Fiber.frameReturn();
                }
            } else {
                if (shouldDeleteFirstIdx()) {
                    return Fiber.call(idxFiles.deleteFirstFile(), this::deleteIdx);
                } else {
                    // loop
                    return Fiber.resume(null, this);
                }
            }
        }
    }
}
