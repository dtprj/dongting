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
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.FileUtil;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.ChecksumException;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.sm.RaftCodecFactory;

import java.io.File;
import java.util.List;
import java.util.function.Supplier;

import static com.github.dtprj.dongting.raft.store.IdxFileQueue.KEY_NEXT_IDX_AFTER_INSTALL_SNAPSHOT;
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
    private final RaftCodecFactory raftCodecFactory;
    LogFileQueue logFiles;
    IdxFileQueue idxFiles;

    private static final long DEFAULT_DELETE_INTERVAL_MILLIS = 10 * 1000;

    int idxItemsPerFile = IdxFileQueue.DEFAULT_ITEMS_PER_FILE;
    long logFileSize = LogFileQueue.DEFAULT_LOG_FILE_SIZE;

    private final Fiber deleteFiber;

    DefaultRaftLog(RaftGroupConfigEx groupConfig, StatusManager statusManager, RaftCodecFactory raftCodecFactory,
                   long deleteIntervalMillis) {
        this.groupConfig = groupConfig;
        this.ts = groupConfig.getTs();
        this.raftStatus = (RaftStatusImpl) groupConfig.getRaftStatus();
        this.statusManager = statusManager;
        this.fiberGroup = groupConfig.getFiberGroup();
        this.raftCodecFactory = raftCodecFactory;

        this.deleteFiber = new Fiber("delete-" + groupConfig.getGroupId(),
                fiberGroup, new QueueDeleteFiberFrame(deleteIntervalMillis), true);
    }

    public DefaultRaftLog(RaftGroupConfigEx groupConfig, StatusManager statusManager, RaftCodecFactory raftCodecFactory) {
        this(groupConfig, statusManager, raftCodecFactory, DEFAULT_DELETE_INTERVAL_MILLIS);
    }

    @Override
    public FiberFrame<Pair<Integer, Long>> init() {
        return new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) throws Exception {
                File dataDir = FileUtil.ensureDir(groupConfig.getDataDir());

                idxFiles = new IdxFileQueue(FileUtil.ensureDir(dataDir, "idx"),
                        statusManager, groupConfig, idxItemsPerFile);
                logFiles = new LogFileQueue(FileUtil.ensureDir(dataDir, "log"),
                        groupConfig, idxFiles, logFileSize);
                logFiles.initQueue();
                RaftUtil.checkStop(fiberGroup);
                return Fiber.call(idxFiles.initRestorePos(), this::afterIdxFileQueueInit);
            }

            private FrameCallResult afterIdxFileQueueInit(Pair<Long, Long> p) {
                RaftUtil.checkStop(fiberGroup);
                long restoreIndex = p.getLeft();
                long restoreStartPos = p.getRight();
                long firstValidPos = RaftUtil.parseLong(statusManager.getProperties(),
                        KEY_NEXT_POS_AFTER_INSTALL_SNAPSHOT, 0);
                return Fiber.call(logFiles.restore(restoreIndex, restoreStartPos, firstValidPos),
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
    public void append(List<LogItem> inputs) {
        logFiles.submit(inputs);
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
    public LogIterator openIterator(Supplier<Boolean> cancelIndicator) {
        return new FileLogLoader(idxFiles, logFiles, groupConfig, raftCodecFactory, cancelIndicator);
    }

    @Override
    public FiberFrame<Pair<Integer, Long>> tryFindMatchPos(int suggestTerm, long suggestIndex,
                                                           Supplier<Boolean> cancelIndicator) {
        return new MatchPosFinder(groupConfig, logFiles.queue, idxFiles, cancelIndicator, raftStatus.getTailCache(),
                logFiles.fileLenMask, suggestTerm, suggestIndex, raftStatus.getLastLogIndex());
    }

    @Override
    public void markTruncateByIndex(long index, long delayMillis) {
        long bound = Math.min(raftStatus.getLastApplied(), idxFiles.getPersistedIndex());
        bound = Math.min(bound, index);
        logFiles.markDelete(bound, Long.MAX_VALUE, delayMillis);
    }

    @Override
    public void markTruncateByTimestamp(long timestampBound, long delayMillis) {
        long bound = Math.min(raftStatus.getLastApplied(), idxFiles.getPersistedIndex());
        logFiles.markDelete(bound, timestampBound, delayMillis);
    }

    @Override
    public FiberFrame<Void> beginInstall() {
        return new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                raftStatus.setInstallSnapshot(true);
                statusManager.persistAsync(true);
                return statusManager.waitUpdateFinish(this::afterPersist);
            }

            private FrameCallResult afterPersist(Void unused) {
                return Fiber.call(logFiles.beginInstall(), this::afterLogBeginInstall);
            }

            private FrameCallResult afterLogBeginInstall(Void unused) {
                return Fiber.call(idxFiles.beginInstall(), this::justReturn);
            }
        };
    }

    @Override
    public FiberFrame<Long> loadNextItemPos(long index) {
        return new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(idxFiles.loadLogPos(index), this::afterLoadPos);
            }

            private FrameCallResult afterLoadPos(Long pos) {
                return Fiber.call(logFiles.loadHeader(pos), h -> afterLoadHeader(h, pos));
            }

            private FrameCallResult afterLoadHeader(LogHeader header, long pos) {
                if (!header.crcMatch()) {
                    throw new ChecksumException("log header crc mismatch: " + pos);
                }
                if (header.isEndMagic()) {
                    throw new RaftException("unexpected end magic: " + pos);
                }
                long nextPos = pos + header.totalLen;
                setResult(nextPos);
                return Fiber.frameReturn();
            }
        };
    }

    @Override
    public FiberFrame<Void> finishInstall(long nextLogIndex, long nextLogPos) {
        logFiles.finishInstall(nextLogIndex, nextLogPos);
        return new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(idxFiles.finishInstall(nextLogIndex), this::afterIdxFinishInstall);
            }

            private FrameCallResult afterIdxFinishInstall(Void unused) {
                statusManager.getProperties().put(KEY_NEXT_IDX_AFTER_INSTALL_SNAPSHOT, String.valueOf(nextLogIndex));
                statusManager.getProperties().put(KEY_NEXT_POS_AFTER_INSTALL_SNAPSHOT, String.valueOf(nextLogPos));
                statusManager.persistAsync(true);
                return statusManager.waitUpdateFinish(this::justReturn);
            }
        };
    }

    @Override
    public FiberFuture<Void> close() {
        FiberFuture<Void> closeFuture = fiberGroup.newFuture("logClose");
        logFiles.close().registerCallback((v, ex) -> {
            if (ex != null) {
                closeFuture.completeExceptionally(ex);
            } else {
                idxFiles.close().registerCallback((v2, ex2) -> {
                    if (ex2 != null) {
                        closeFuture.completeExceptionally(ex2);
                    } else {
                        closeFuture.complete(null);
                    }
                });
            }
        });
        // delete fiber is daemon
        return closeFuture;
    }

    private class QueueDeleteFiberFrame extends FiberFrame<Void> {

        private final long deleteIntervalMillis;

        public QueueDeleteFiberFrame(long deleteIntervalMillis) {
            this.deleteIntervalMillis = deleteIntervalMillis;
        }

        @Override
        protected FrameCallResult handle(Throwable ex) {
            throw Fiber.fatal(ex);
        }

        @Override
        public FrameCallResult execute(Void input) {
            return Fiber.sleep(deleteIntervalMillis, this::deleteLogs);
        }

        @SuppressWarnings("BooleanMethodIsAlwaysInverted")
        private boolean inUse(LogFile logFile) {
            if (logFile.getReaders() > 0 || logFile.getWriters() > 0) {
                log.info("file still in use: {}, readers={}, writes={}",
                        logFile.getFile().getPath(), logFile.getReaders(), logFile.getWriters());
                return true;
            }
            return false;
        }

        private FrameCallResult deleteLogs(Void unused) {
            long taskStartTimestamp = ts.getWallClockMillis();
            // ex handled by deleteByPredicate method
            FiberFrame<Void> f = logFiles.deleteByPredicate(() -> {
                IndexedQueue<LogFile> q = logFiles.queue;
                if (q.size() <= 1) {
                    // don't delete last file
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
                        raftStatus.getLastForceLogIndex() < second.firstIndex) {
                    return false;
                }
                return !inUse(first);
            });
            return Fiber.call(f, this::deleteIdx);
        }

        private FrameCallResult deleteIdx(Void unused) {
            // ex handled by deleteByPredicate method
            FiberFrame<Void> f = idxFiles.deleteByPredicate(() -> {
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
                if (idxFiles.getPersistedIndex() < firstIndexOfNextFile) {
                    return false;
                }
                return !inUse(first);
            });
            // loop
            return Fiber.call(f, this);
        }
    }
}
