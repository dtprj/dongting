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
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;

import java.io.File;
import java.nio.ByteBuffer;

/**
 * @author huangli
 */
class LogFileQueue extends FileQueue {
    private static final DtLog log = DtLogs.getLogger(LogFileQueue.class);

    public static final long DEFAULT_LOG_FILE_SIZE = 1024 * 1024 * 1024;
    public static final int MAX_WRITE_BUFFER_SIZE = 128 * 1024;

    private final FiberGroup fiberGroup;

    protected final RefBufferFactory heapPool;
    protected final ByteBufferPool directPool;

    private final IdxOps idxOps;

    private final Timestamp ts;

    final LogAppender logAppender;

    // read in io thread
    private volatile boolean closed;

    int maxWriteBufferSize = MAX_WRITE_BUFFER_SIZE;

    public LogFileQueue(File dir, RaftGroupConfig groupConfig, IdxOps idxOps, RaftLog.AppendCallback callback,
                        long fileSize) {
        super(dir, groupConfig, fileSize);
        this.idxOps = idxOps;
        this.ts = groupConfig.getTs();
        this.fiberGroup = groupConfig.getFiberGroup();
        this.heapPool = groupConfig.getHeapPool();
        this.directPool = groupConfig.getDirectPool();

        this.logAppender = new LogAppender(idxOps, this, groupConfig, callback);
    }

    public long fileLength() {
        return fileSize;
    }

    public FiberFrame<Integer> restore(long restoreIndex, long restoreIndexPos, long firstValidPos) {
        log.info("start restore from {}, {}", restoreIndex, restoreIndexPos);
        Restorer restorer = new Restorer(idxOps, this,
                restoreIndex, restoreIndexPos, firstValidPos);
        if (queue.size() == 0) {
            tryAllocateAsync(0);
            logAppender.setNext(1, 0);
            logAppender.startFiber();
            return FiberFrame.completedFrame(0);
        }
        if (restoreIndexPos < queue.get(0).startPos) {
            throw new RaftException("restoreIndexPos is illegal. " + restoreIndexPos);
        }
        if (restoreIndexPos >= queue.get(queue.size() - 1).endPos) {
            throw new RaftException("restoreIndexPos is illegal. " + restoreIndexPos);
        }
        return new FiberFrame<>() {
            long writePos = 0;
            int i = 0;
            final ByteBuffer buffer = directPool.borrow(maxWriteBufferSize);

            @Override
            public FrameCallResult execute(Void input) {
                RaftUtil.checkStop(fiberGroup);
                if (i >= queue.size()) {
                    // finish loop
                    return finish();
                }
                LogFile lf = queue.get(i);
                return Fiber.call(restorer.restoreFile(buffer, lf),
                        this::afterRestoreSingleFile);
            }

            private FrameCallResult afterRestoreSingleFile(Pair<Boolean, Long> singleResult) {
                writePos = singleResult.getRight();
                if (singleResult.getLeft()) {
                    // break for loop
                    return finish();
                }
                // loop
                i++;
                return execute(null);
            }

            private FrameCallResult finish() {
                if (queue.size() > 1) {
                    LogFile first = queue.get(0);
                    if (firstValidPos > first.startPos && firstValidPos < first.endPos && first.firstIndex == 0) {
                        // after install snapshot, the firstValidPos is too large in file, so this file has no items
                        queue.removeFirst();
                        delete(first);
                    }
                }

                log.info("restore finished. lastTerm={}, lastIndex={}, lastPos={}, lastFile={}",
                        restorer.previousTerm, restorer.previousIndex, writePos,
                        queue.get(queue.size() - 1).getFile().getPath());
                logAppender.setNext(restorer.previousIndex + 1, writePos);
                logAppender.startFiber();
                setResult(restorer.previousTerm);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult doFinally() {
                directPool.release(buffer);
                return super.doFinally();
            }
        };
    }

    public void append() {
        logAppender.append();
    }

    public long nextFilePos(long absolutePos) {
        return ((absolutePos >>> fileLenShiftBits) + 1) << fileLenShiftBits;
    }

    public long filePos(long absolutePos) {
        return absolutePos & fileLenMask;
    }

    public long getFirstIndex() {
        if (queue.size() > 0) {
            return queue.get(0).firstIndex;
        }
        return 0;
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
                        logFile.getFile().getName(), result, timestampBound, nextFile.firstTimestamp, boundIndex, nextFile.firstIndex);
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

    public FiberFuture<Void> close() {
        closed = true;
        logAppender.needAppendCondition.signal();
        FiberFuture<Void> f;
        if (logAppender.appendFiber.isStarted()) {
            f = logAppender.appendFiber.join();
        } else {
            f = FiberFuture.completedFuture(groupConfig.getFiberGroup(), null);
        }
        return f.convertWithHandle((v, ex) -> {
            if (ex != null) {
                log.error("close log file queue failed", ex);
            }
            closeChannel();
            return null;
        });
    }

    public boolean isClosed() {
        return closed;
    }
}
