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

import com.github.dtprj.dongting.buf.ByteBufferPool;
import com.github.dtprj.dongting.buf.RefBufferFactory;
import com.github.dtprj.dongting.common.BitUtil;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.impl.TailCache;
import com.github.dtprj.dongting.raft.server.ChecksumException;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * @author huangli
 */
class LogFileQueue extends FileQueue {
    private static final DtLog log = DtLogs.getLogger(LogFileQueue.class);

    public static final long DEFAULT_LOG_FILE_SIZE = 1024 * 1024 * 1024;
    public static final int DEFAULT_WRITE_BUFFER_SIZE = 128 * 1024;

    private final long logFileSize;
    private final long fileLenMask;
    private final int fileLenShiftBits;

    protected final RefBufferFactory heapPool;
    protected final ByteBufferPool directPool;

    private final IdxOps idxOps;

    private final Timestamp ts;
    private final ByteBuffer writeBuffer;

    private final LogAppender logAppender;

    public LogFileQueue(File dir, RaftGroupConfig groupConfig, IdxOps idxOps, RaftLog.AppendCallback callback,
                        long logFileSize, int writeBufferSize) {
        super(dir, groupConfig);
        this.idxOps = idxOps;
        this.ts = groupConfig.getTs();

        this.heapPool = groupConfig.getHeapPool();
        this.directPool = groupConfig.getDirectPool();
        this.logFileSize = logFileSize;
        this.fileLenMask = logFileSize - 1;
        this.fileLenShiftBits = BitUtil.zeroCountOfBinary(logFileSize);
        this.writeBuffer = ByteBuffer.allocateDirect(writeBufferSize);
        this.logAppender = new LogAppender(idxOps, this, groupConfig, writeBuffer, callback);
    }

    @Override
    protected long getFileSize() {
        return logFileSize;
    }

    @Override
    public int getFileLenShiftBits() {
        return fileLenShiftBits;
    }

    public int restore(long restoreIndex, long restoreIndexPos, Supplier<Boolean> stopIndicator)
            throws IOException, InterruptedException {
        log.info("start restore from {}, {}", restoreIndex, restoreIndexPos);
        Restorer restorer = new Restorer(idxOps, this, restoreIndex, restoreIndexPos);
        if (queue.size() == 0) {
            tryAllocate();
            logAppender.setNextPersistIndex(1);
            logAppender.setNextPersistPos(0);
            return 0;
        }
        if (restoreIndexPos < queue.get(0).startPos) {
            throw new RaftException("restoreIndexPos is illegal. " + restoreIndexPos);
        }
        if (restoreIndexPos >= queue.get(queue.size() - 1).endPos) {
            throw new RaftException("restoreIndexPos is illegal. " + restoreIndexPos);
        }
        long writePos = 0;
        for (int i = 0; i < queue.size(); i++) {
            RaftUtil.checkStop(stopIndicator);
            LogFile lf = queue.get(i);
            Pair<Boolean, Long> result = restorer.restoreFile(writeBuffer, lf, stopIndicator);
            writePos = result.getRight();
            if (result.getLeft()) {
                break;
            }
        }

        log.info("restore finished. lastTerm={}, lastIndex={}, lastPos={}, lastFile={}",
                restorer.previousTerm, restorer.previousIndex, writePos, queue.get(queue.size() - 1).file.getPath());
        logAppender.setNextPersistIndex(restorer.previousIndex + 1);
        logAppender.setNextPersistPos(writePos);
        return restorer.previousTerm;
    }

    public void append(TailCache tailCache) throws InterruptedException {
        logAppender.append(tailCache);
    }

    public long nextFilePos(long absolutePos) {
        return ((absolutePos >>> fileLenShiftBits) + 1) << fileLenShiftBits;
    }

    public void markDelete(long boundIndex, long timestampBound, long delayMills) {
        long deleteTimestamp = ts.getWallClockMillis() + delayMills;
        int queueSize = queue.size();
        for (int i = 0; i < queueSize - 1; i++) {
            LogFile logFile = queue.get(i);
            LogFile nextFile = queue.get(i + 1);
            boolean result = nextFile.firstTimestamp > 0
                    && timestampBound > nextFile.firstTimestamp
                    && boundIndex >= nextFile.firstIndex;
            if (log.isDebugEnabled()) {
                log.debug("mark {} delete: {}. timestampBound={}, nextFileFirstTimeStamp={}, boundIndex={}, nextFileFirstIndex={}",
                        logFile.file.getName(), result, timestampBound, nextFile.firstTimestamp, boundIndex, nextFile.firstIndex);
            }
            if (result) {
                if (logFile.deleteTimestamp == 0) {
                    logFile.deleteTimestamp = deleteTimestamp;
                } else {
                    logFile.deleteTimestamp = Math.min(deleteTimestamp, logFile.deleteTimestamp);
                }
            } else {
                return;
            }
        }
    }

    public void submitDeleteTask(long taskStartTimestamp) {
        submitDeleteTask(logFile -> {
            long deleteTimestamp = logFile.deleteTimestamp;
            return deleteTimestamp > 0 && deleteTimestamp < taskStartTimestamp && logFile.use <= 0;
        });
    }

    public long getFirstIndex() {
        if (queue.size() > 0) {
            return queue.get(0).firstIndex;
        }
        return 0;
    }

    public CompletableFuture<Pair<Integer, Long>> tryFindMatchPos(
            int suggestTerm, long suggestIndex, Supplier<Boolean> cancelIndicator) {
        Pair<LogFile, Long> p = findMatchLogFile(suggestTerm, suggestIndex);
        if (p == null) {
            return CompletableFuture.completedFuture(null);
        }
        long rightBound = p.getRight();
        MatchPosFinder finder = new MatchPosFinder(cancelIndicator, p.getLeft(),
                suggestTerm, Math.min(suggestIndex, rightBound));
        finder.exec();
        return finder.future;
    }

    private Pair<LogFile, Long> findMatchLogFile(int suggestTerm, long suggestIndex) {
        if (queue.size() == 0) {
            return null;
        }
        int left = 0;
        int right = queue.size() - 1;
        while (left <= right) {
            int mid = (left + right + 1) >>> 1;
            LogFile logFile = queue.get(mid);
            if (logFile.deleteTimestamp > 0) {
                left = mid + 1;
                continue;
            }
            if (logFile.firstIndex == 0) {
                right = mid - 1;
                continue;
            }
            if (logFile.firstIndex > suggestIndex) {
                right = mid - 1;
                continue;
            }
            if (logFile.firstIndex == suggestIndex && logFile.firstTerm == suggestTerm) {
                return new Pair<>(logFile, logFile.firstIndex);
            } else if (logFile.firstIndex < suggestIndex && logFile.firstTerm <= suggestTerm) {
                if (left == right) {
                    return new Pair<>(logFile, Math.min(tryFindEndIndex(mid), suggestIndex));
                } else {
                    left = mid;
                }
            } else {
                right = mid - 1;
            }
        }
        return null;
    }

    private long tryFindEndIndex(int fileIndex) {
        if (fileIndex == queue.size() - 1) {
            return Long.MAX_VALUE;
        } else {
            LogFile nextFile = queue.get(fileIndex + 1);
            if (nextFile.firstIndex == 0) {
                return Long.MAX_VALUE;
            } else {
                return nextFile.firstIndex - 1;
            }
        }
    }

    private class MatchPosFinder {
        private final Supplier<Boolean> cancel;
        private final LogFile logFile;
        private final int suggestTerm;
        private final long suggestRightBoundIndex;
        private final CompletableFuture<Pair<Integer, Long>> future = new CompletableFuture<>();

        private long leftIndex;
        private int leftTerm;
        private long rightIndex;
        private long midIndex;

        MatchPosFinder(Supplier<Boolean> cancel, LogFile logFile, int suggestTerm, long suggestRightBoundIndex) {
            this.cancel = cancel;
            this.logFile = logFile;
            this.suggestTerm = suggestTerm;
            this.suggestRightBoundIndex = suggestRightBoundIndex;

            this.leftIndex = logFile.firstIndex;
            this.leftTerm = logFile.firstTerm;
            this.rightIndex = suggestRightBoundIndex;
        }

        void exec() {
            try {
                if (cancel.get()) {
                    future.cancel(false);
                    return;
                }
                if (leftIndex < rightIndex) {
                    midIndex = (leftIndex + rightIndex + 1) >>> 1;
                    CompletableFuture<Long> posFuture = idxOps.loadLogPos(midIndex);
                    posFuture.whenCompleteAsync((r, ex) -> posLoadComplete(ex, r), raftExecutor);
                } else {
                    future.complete(new Pair<>(leftTerm, leftIndex));
                }
            } catch (Throwable e) {
                future.completeExceptionally(e);
            }
        }

        private boolean failOrCancel(Throwable ex) {
            if (ex != null) {
                future.completeExceptionally(ex);
                return true;
            }
            if (cancel.get()) {
                future.cancel(false);
                return true;
            }
            return false;
        }

        private void posLoadComplete(Throwable ex, Long pos) {
            try {
                if (failOrCancel(ex)) {
                    return;
                }
                if (pos >= logFile.endPos) {
                    BugLog.getLog().error("pos >= logFile.endPos, pos={}, logFile={}", pos, logFile);
                    rightIndex = midIndex - 1;
                    exec();
                    return;
                }

                AsyncIoTask task = new AsyncIoTask(logFile.channel, stopIndicator, cancel);
                ByteBuffer buf = ByteBuffer.allocate(LogHeader.ITEM_HEADER_SIZE);
                CompletableFuture<Void> f = task.read(buf, pos & fileLenMask);
                f.whenCompleteAsync((v, loadHeaderEx) -> headerLoadComplete(loadHeaderEx, buf), raftExecutor);
            } catch (Throwable e) {
                future.completeExceptionally(e);
            }
        }

        private void headerLoadComplete(Throwable ex, ByteBuffer buf) {
            try {
                if (failOrCancel(ex)) {
                    return;
                }

                buf.flip();
                LogHeader header = new LogHeader();
                header.read(buf);
                if (!header.crcMatch()) {
                    future.completeExceptionally(new ChecksumException());
                    return;
                }
                if (header.index != midIndex) {
                    future.completeExceptionally(new RaftException("index not match"));
                    return;
                }
                if (midIndex == suggestRightBoundIndex && header.term == suggestTerm) {
                    future.complete(new Pair<>(header.term, midIndex));
                    return;
                } else if (midIndex < suggestRightBoundIndex && header.term <= suggestTerm) {
                    leftIndex = midIndex;
                    leftTerm = header.term;
                } else {
                    rightIndex = midIndex - 1;
                }
                exec();
            } catch (Throwable e) {
                future.completeExceptionally(e);
            }
        }
    }

    public long filePos(long absolutePos) {
        return absolutePos & fileLenMask;
    }

    public long fileLength() {
        return logFileSize;
    }

    public LogAppender getLogAppender() {
        return logAppender;
    }
}
