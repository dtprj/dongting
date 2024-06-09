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

import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.common.PerfCallback;
import com.github.dtprj.dongting.fiber.DoInLockFrame;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FiberInterruptException;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.PerfConsts;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.impl.TailCache;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.function.Supplier;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
class LogAppender {
    private static final DtLog log = DtLogs.getLogger(LogAppender.class);
    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocateDirect(0);

    private final TailCache cache;

    private final IdxOps idxOps;
    private final LogFileQueue logFileQueue;
    private final RaftGroupConfigEx groupConfig;
    private final CRC32C crc32c = new CRC32C();
    private final EncodeContext encodeContext;
    private final long fileLenMask;

    // update before write operation issued
    long nextPersistIndex = -1;
    long nextPersistPos = -1;

    private WriteTask writeTaskQueueHead;
    private WriteTask syncTaskQueueHead;

    private final Fiber appendFiber;

    private final Fiber fsyncFiber;
    private final FiberCondition needFsyncCondition;

    private final Supplier<Boolean> writeStopIndicator;

    private final RaftStatusImpl raftStatus;

    private final PerfCallback perfCallback;
    private long roundStartTime;

    LogAppender(IdxOps idxOps, LogFileQueue logFileQueue, RaftGroupConfigEx groupConfig) {
        this.idxOps = idxOps;
        this.logFileQueue = logFileQueue;
        this.encodeContext = new EncodeContext(groupConfig.getHeapPool());
        this.fileLenMask = logFileQueue.fileLength() - 1;
        this.groupConfig = groupConfig;
        this.raftStatus = (RaftStatusImpl) groupConfig.getRaftStatus();
        this.cache = raftStatus.getTailCache();
        FiberGroup fiberGroup = groupConfig.getFiberGroup();
        WriteFiberFrame writeFiberFrame = new WriteFiberFrame();
        this.appendFiber = new Fiber("append-" + groupConfig.getGroupId(), fiberGroup, writeFiberFrame);
        this.writeStopIndicator = logFileQueue::isClosed;
        this.fsyncFiber = new Fiber("fsync-" + groupConfig.getGroupId(), fiberGroup, new SyncLoopFrame());
        this.needFsyncCondition = fiberGroup.newCondition("NeedFsync-" + groupConfig.getGroupId());
        this.perfCallback = groupConfig.getPerfCallback();
    }

    public void startFiber() {
        appendFiber.start();
        fsyncFiber.start();
    }

    public FiberFuture<Void> close() {
        appendFiber.interrupt();
        needFsyncCondition.signal();
        raftStatus.getLogForceFinishCondition().signalAll();
        FiberFuture<Void> f1, f2;
        if (appendFiber.isStarted()) {
            f1 = appendFiber.join();
        } else {
            f1 = FiberFuture.completedFuture(groupConfig.getFiberGroup(), null);
        }
        if (fsyncFiber.isStarted()) {
            f2 = fsyncFiber.join();
        } else {
            f2 = FiberFuture.completedFuture(groupConfig.getFiberGroup(), null);
        }
        return FiberFuture.allOf("closeLogAppender", f1, f2);
    }

    private class WriteFiberFrame extends FiberFrame<Void> {

        // 4 temp status fields, should reset in writeData()
        private final ArrayList<LogItem> items = new ArrayList<>(32);
        private LogItem lastItem;
        private int perfCount;
        private long writeStartPosInFile;
        private int bytesToWrite;

        @Override
        protected FrameCallResult handle(Throwable ex) {
            if (ex instanceof FiberInterruptException) {
                return Fiber.frameReturn();
            }
            throw Fiber.fatal(ex);
        }

        @Override
        public FrameCallResult execute(Void input) {
            if (logFileQueue.isClosed()) {
                return Fiber.frameReturn();
            }
            roundStartTime = perfCallback.takeTime(PerfConsts.RAFT_D_LOG_WRITE_FIBER_ROUND);
            processWriteResult();
            TailCache tailCache = LogAppender.this.cache;
            long nextPersistIndex = LogAppender.this.nextPersistIndex;
            if (tailCache.size() > 0 && tailCache.getLastIndex() >= nextPersistIndex) {
                if (nextPersistIndex < tailCache.getFirstIndex()) {
                    BugLog.getLog().error("nextPersistIndex {} < tailCache.getFirstIndex() {}",
                            nextPersistIndex, tailCache.getFirstIndex());
                    throw Fiber.fatal(new RaftException("nextPersistIndex<tailCache.getFirstIndex()"));
                }
                if (idxOps.needWaitFlush()) {
                    long start = perfCallback.takeTime(PerfConsts.RAFT_D_IDX_BLOCK);
                    return Fiber.call(idxOps.waitFlush(), v -> afterIdxReady(start));
                }
                if (logFileQueue.isClosed()) {
                    return Fiber.frameReturn();
                }
                return Fiber.call(logFileQueue.ensureWritePosReady(nextPersistPos), this::afterPosReady);
            } else {
                return raftStatus.getDataArrivedCondition().await(this);
            }
        }

        private FrameCallResult afterIdxReady(long perfStartTime) {
            perfCallback.fireTime(PerfConsts.RAFT_D_IDX_BLOCK, perfStartTime);
            return Fiber.resume(null, this);
        }

        private FrameCallResult afterPosReady(Void unused) {
            if (logFileQueue.isClosed()) {
                return Fiber.frameReturn();
            }
            LogFile lf = logFileQueue.getLogFile(nextPersistPos);
            if (lf.isDeleted()) {
                BugLog.getLog().error("file is deleted or mark deleted: {}", lf.getFile().getPath());
                throw new RaftException("file is deleted or mark deleted: " + lf.getFile().getPath());
            }
            // use read lock, so not block read operation.
            // because we never read file block that is being written.
            // unlock in encodeAndWriteItems()
            return lf.getLock().readLock().lock(v -> encodeAndWriteItems(lf));
        }

        private FrameCallResult encodeAndWriteItems(LogFile file) {
            // reset 4 status fields
            writeStartPosInFile = nextPersistPos & fileLenMask;
            bytesToWrite = 0;
            ArrayList<LogItem> items = this.items;
            items.clear();
            lastItem = null;
            perfCount = 0;

            boolean writeEndHeader = false;
            boolean rollNextFile = false;
            for (long lastIndex = cache.getLastIndex(), fileRestBytes = file.endPos - nextPersistPos;
                 nextPersistIndex <= lastIndex; ) {
                RaftTask rt = cache.get(nextPersistIndex);
                LogItem li = rt.getItem();
                int len = LogHeader.computeTotalLen(0, li.getActualHeaderSize(),
                        li.getActualBodySize());
                if (len <= fileRestBytes) {
                    items.add(li);
                    bytesToWrite += len;
                    fileRestBytes -= len;
                    nextPersistIndex++;
                    nextPersistPos += len;
                } else {
                    rollNextFile = true;
                    // file rest bytes not enough
                    if (fileRestBytes >= LogHeader.ITEM_HEADER_SIZE) {
                        writeEndHeader = true;
                        bytesToWrite += LogHeader.ITEM_HEADER_SIZE;
                    }
                    break;
                }
            }

            ByteBuffer buffer = borrowBuffer(bytesToWrite);
            buffer = encodeItems(items, file, buffer);

            if (writeEndHeader) {
                if (buffer.remaining() < LogHeader.ITEM_HEADER_SIZE) {
                    buffer = doWrite(file, buffer);
                }
                LogHeader.writeEndHeader(crc32c, buffer);
            }
            if (buffer.position() > 0) {
                doWrite(file, buffer);
            } else {
                if (buffer.capacity() > 0) {
                    BugLog.getLog().error("buffer capacity > 0", buffer.capacity());
                }
            }

            items.clear();
            if (nextPersistPos == file.endPos) {
                log.info("current file {} has no enough space, nextPersistPos is {}, next file start pos is {}",
                        file.getFile().getName(), nextPersistPos, nextPersistPos);
            } else if (rollNextFile) {
                // prepare to write new file
                long next = logFileQueue.nextFilePos(nextPersistPos);
                log.info("current file {} has no enough space, nextPersistPos is {}, next file start pos is {}",
                        file.getFile().getName(), nextPersistPos, next);
                nextPersistPos = next;
            }
            file.getLock().readLock().unlock();
            perfCallback.fireTime(PerfConsts.RAFT_D_LOG_WRITE_FIBER_ROUND, roundStartTime);
            // continue loop
            return Fiber.resume(null, this);
        }

        private ByteBuffer encodeItems(ArrayList<LogItem> items, LogFile file, ByteBuffer buffer) {
            long dataPos = file.startPos + writeStartPosInFile;
            for (int count = items.size(), i = 0; i < count; i++) {
                LogItem li = items.get(i);
                if (file.firstIndex == 0) {
                    file.firstIndex = li.getIndex();
                    file.firstTerm = li.getTerm();
                    file.firstTimestamp = li.getTimestamp();
                }
                if (buffer.remaining() < LogHeader.ITEM_HEADER_SIZE) {
                    buffer = doWrite(file, buffer);
                }
                int len = LogHeader.writeHeader(crc32c, buffer, li);

                if (li.getActualHeaderSize() > 0) {
                    if (!buffer.hasRemaining()) {
                        buffer = doWrite(file, buffer);
                    }
                    buffer = encodeData(li.getActualHeaderSize(), li.getHeader(), buffer, file);
                }
                if (li.getActualBodySize() > 0) {
                    if (!buffer.hasRemaining()) {
                        buffer = doWrite(file, buffer);
                    }
                    buffer = encodeData(li.getActualBodySize(), li.getBody(), buffer, file);
                }

                idxOps.put(li.getIndex(), dataPos);
                dataPos += len;
                lastItem = li;
                perfCount++;
            }
            return buffer;
        }

        private ByteBuffer encodeData(int actualSize, Encodable src, ByteBuffer dest, LogFile file) {
            crc32c.reset();
            try {
                int totalEncodeLen = 0;
                while (true) {
                    int startPos = dest.position();
                    boolean finish = src.encode(encodeContext, dest);
                    totalEncodeLen += dest.position() - startPos;
                    RaftUtil.updateCrc(crc32c, dest, startPos, dest.position() - startPos);
                    if (finish) {
                        if (totalEncodeLen != actualSize) {
                            throw new RaftException("encode problem, totalEncodeLen != actualSize");
                        }
                        break;
                    } else {
                        dest = doWrite(file, dest);
                    }
                }
            } finally {
                encodeContext.reset();
            }
            if (dest.remaining() < 4) {
                dest = doWrite(file, dest);
            }
            dest.putInt((int) crc32c.getValue());
            return dest;
        }

        private ByteBuffer doWrite(LogFile file, ByteBuffer buffer) {
            buffer.flip();
            int bytes = buffer.remaining();
            boolean retry = logFileQueue.initialized && !logFileQueue.isClosed();
            WriteTask task = new WriteTask(groupConfig, file, retry, true, writeStopIndicator);
            task.bytes = bytes;
            if (lastItem != null) {
                task.lastTerm = lastItem.getTerm();
                task.lastIndex = lastItem.getIndex();
                task.perfItemCount = perfCount;
            }

            long startTime = perfCallback.takeTime(PerfConsts.RAFT_D_LOG_WRITE1);
            // no sync
            task.write(buffer, writeStartPosInFile);
            perfCallback.fireTime(PerfConsts.RAFT_D_LOG_WRITE1, startTime, task.perfItemCount, bytes);

            if (writeTaskQueueHead == null) {
                writeTaskQueueHead = task;
            } else {
                writeTaskQueueHead.nextNeedWriteTask = task;
            }

            // tryLock() will success immediately since we lock the file in afterPosReady()
            file.getLock().readLock().tryLock();
            task.getFuture().registerCallback((r, ex) -> {
                // release lock in processWriteResult() since we should unlock in same fiber.
                // unlock in processWriteResult
                perfCallback.fireTime(PerfConsts.RAFT_D_LOG_WRITE2, startTime, task.perfItemCount, bytes);
                groupConfig.getDirectPool().release(task.getIoBuffer());
                raftStatus.getDataArrivedCondition().signal(appendFiber);
            });

            writeStartPosInFile += bytes;
            bytesToWrite -= bytes;
            lastItem = null;
            perfCount = 0;

            return borrowBuffer(bytesToWrite);
        }

        private ByteBuffer borrowBuffer(int size) {
            if (size == 0) {
                return EMPTY_BUFFER;
            } else {
                size = Math.min(size, logFileQueue.maxWriteBufferSize);
                return groupConfig.getDirectPool().borrow(size);
            }
        }

        private void processWriteResult() {
            boolean needSignal = false;
            while (writeTaskQueueHead != null && writeTaskQueueHead.getFuture().isDone()) {
                WriteTask wt = writeTaskQueueHead;
                writeTaskQueueHead = wt.nextNeedWriteTask;
                wt.getDtFile().getLock().readLock().unlock();
                if (wt.getFuture().getEx() != null) {
                    throw Fiber.fatal(new RaftException("write error", wt.getFuture().getEx()));
                }
                if (wt.lastTerm > 0) {
                    raftStatus.setLastWriteLogIndex(wt.lastIndex);
                    needSignal = true;
                    if (syncTaskQueueHead == null) {
                        syncTaskQueueHead = wt;
                    } else {
                        syncTaskQueueHead.nextNeedSyncTask = wt;
                    }
                }
            }
            if (needSignal) {
                raftStatus.getLogWriteFinishCondition().signalAll();
                needFsyncCondition.signalLater();
            }
        }
    }


    static class WriteTask extends AsyncIoTask {
        int lastTerm;
        long lastIndex;
        int perfItemCount;

        int bytes;

        WriteTask nextNeedWriteTask;
        WriteTask nextNeedSyncTask;

        public WriteTask(RaftGroupConfigEx groupConfig, DtFile dtFile,
                         boolean retry, boolean retryForever, Supplier<Boolean> cancelIndicator) {
            super(groupConfig, dtFile, retry, retryForever, cancelIndicator);
        }
    }

    private class SyncLoopFrame extends FiberFrame<Void> {
        @Override
        protected FrameCallResult handle(Throwable ex) {
            throw Fiber.fatal(ex);
        }

        @Override
        public FrameCallResult execute(Void input) {
            if (logFileQueue.isClosed()) {
                return Fiber.frameReturn();
            }
            if (syncTaskQueueHead == null) {
                return needFsyncCondition.await(this);
            } else {
                long perfStartTime = perfCallback.takeTime(PerfConsts.RAFT_D_LOG_SYNC);
                WriteTask task = syncTaskQueueHead;
                long bytes = task.bytes;
                int count = task.perfItemCount;
                while (task.nextNeedSyncTask != null) {
                    if (task.getDtFile() == task.nextNeedSyncTask.getDtFile()) {
                        task = task.nextNeedSyncTask;
                        bytes += task.bytes;
                        count += task.perfItemCount;
                    } else {
                        break;
                    }
                }
                return Fiber.call(new LockThenSyncFrame(task, perfStartTime, count, bytes), this);
            }
        }
    }

    private class LockThenSyncFrame extends DoInLockFrame<Void> {
        private final WriteTask task;
        private final long perfStartTime;
        private final int count;
        private final long bytes;

        public LockThenSyncFrame(WriteTask task, long perfStartTime, int count, long bytes) {
            // use read lock, no block read operations
            super(task.getDtFile().getLock().readLock());
            this.task = task;
            this.perfStartTime = perfStartTime;
            this.count = count;
            this.bytes = bytes;
        }

        @Override
        protected FrameCallResult afterGetLock() {
            RetryFrame<Void> rf = new RetryFrame<>(new SyncFrame(task, perfStartTime, count, bytes),
                    groupConfig.getIoRetryInterval(), false);
            return Fiber.call(rf, this::justReturn);
        }
    }

    private class SyncFrame extends ForceFrame {

        private final WriteTask task;
        private final long perfStartTime;
        private final int count;
        private final long bytes;

        public SyncFrame(WriteTask task, long perfStartTime, int count, long bytes) {
            super(task.getDtFile().getChannel(), groupConfig.getIoExecutor(), false);
            this.task = task;
            this.perfStartTime = perfStartTime;
            this.count = count;
            this.bytes = bytes;
        }

        @Override
        protected FrameCallResult afterForce(Void unused) {
            perfCallback.fireTime(PerfConsts.RAFT_D_LOG_SYNC, perfStartTime, count, bytes);
            WriteTask head = syncTaskQueueHead;
            if (head != null && head.lastIndex <= task.lastIndex) {
                syncTaskQueueHead = head.nextNeedSyncTask;
                raftStatus.setLastForceLogIndex(head.lastIndex);
                raftStatus.getLogForceFinishCondition().signalAll();
            }
            return Fiber.frameReturn();
        }
    }

    public void setNext(long nextPersistIndex, long nextPersistPos) {
        this.nextPersistIndex = nextPersistIndex;
        this.nextPersistPos = nextPersistPos;
    }

    public boolean writeNotFinish() {
        return nextPersistIndex <= cache.getLastIndex()
                || writeTaskQueueHead != null
                || syncTaskQueueHead != null;
    }

}
