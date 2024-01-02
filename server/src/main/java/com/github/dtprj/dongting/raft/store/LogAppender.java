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

import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.Encoder;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.IndexedQueue;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCancelException;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.impl.TailCache;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;
import com.github.dtprj.dongting.raft.sm.RaftCodecFactory;

import java.io.IOException;
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
    private final RaftCodecFactory codecFactory;
    private final RaftGroupConfig groupConfig;
    private final CRC32C crc32c = new CRC32C();
    private final EncodeContext encodeContext;
    private final long fileLenMask;
    private final RaftLog.AppendCallback appendCallback;
    private final FiberGroup fiberGroup;

    long nextPersistIndex = -1;
    long nextPersistPos = -1;

    private final IndexedQueue<WriteTask> writeTaskQueue = new IndexedQueue<>(32);
    private WriteTask syncWriteTaskQueueHead;

    private final Fiber appendFiber;
    private final AppendFiberFrame appendFiberFrame = new AppendFiberFrame();
    private final FiberCondition needAppendCondition;

    private final Fiber fsyncFiber;
    private final FiberCondition needFsyncCondition;

    // 4 temp status fields, should reset in writeData()
    private final ArrayList<LogItem> items = new ArrayList<>(32);
    private LogItem lastItem;
    private long writeStartPosInFile;
    private int bytesToWrite;

    private final Supplier<Boolean> writeStopIndicator;

    private final FiberCondition noPendingCondition;

    LogAppender(IdxOps idxOps, LogFileQueue logFileQueue, RaftGroupConfig groupConfig,
                RaftLog.AppendCallback appendCallback) {
        this.idxOps = idxOps;
        this.logFileQueue = logFileQueue;
        this.codecFactory = groupConfig.getCodecFactory();
        this.encodeContext = new EncodeContext(groupConfig.getHeapPool());
        this.fileLenMask = logFileQueue.fileLength() - 1;
        this.groupConfig = groupConfig;
        this.appendCallback = appendCallback;
        RaftStatusImpl raftStatus = (RaftStatusImpl) groupConfig.getRaftStatus();
        this.cache = raftStatus.getTailCache();
        this.fiberGroup = groupConfig.getFiberGroup();
        this.appendFiber = new Fiber("append-" + groupConfig.getGroupId(), fiberGroup, appendFiberFrame);
        this.needAppendCondition = fiberGroup.newCondition("NeedAppend-" + groupConfig.getGroupId());
        this.writeStopIndicator = logFileQueue::isClosed;
        this.noPendingCondition = fiberGroup.newCondition("NoPending-" + groupConfig.getGroupId());
        this.fsyncFiber = new Fiber("fsync-" + groupConfig.getGroupId(), fiberGroup, new FsyncFiberFrame());
        this.needFsyncCondition = fiberGroup.newCondition("NeedFsync-" + groupConfig.getGroupId());
    }

    public void startFiber() {
        appendFiber.start();
        fsyncFiber.start();
    }

    public FiberFuture<Void> close() {
        needAppendCondition.signal();
        needFsyncCondition.signal();
        noPendingCondition.signalAll();
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
        return FiberFuture.allOf(f1, f2);
    }

    private class AppendFiberFrame extends FiberFrame<Void> {

        @Override
        public FrameCallResult execute(Void input) {
            if (logFileQueue.isClosed()) {
                return Fiber.frameReturn();
            }
            if (idxOps.needWaitFlush()) {
                return Fiber.call(idxOps.waitFlush(), this);
            }
            if (logFileQueue.isClosed()) {
                return Fiber.frameReturn();
            }
            TailCache tailCache = LogAppender.this.cache;
            long nextPersistIndex = LogAppender.this.nextPersistIndex;
            if (tailCache.size() > 0 && tailCache.getLastIndex() >= nextPersistIndex) {
                if (nextPersistIndex < tailCache.getFirstIndex()) {
                    BugLog.getLog().error("nextPersistIndex {} < tailCache.getFirstIndex() {}",
                            nextPersistIndex, tailCache.getFirstIndex());
                    return Fiber.fatal(new RaftException("nextPersistIndex<tailCache.getFirstIndex()"));
                }
                return Fiber.call(logFileQueue.ensureWritePosReady(nextPersistPos), this::afterPosReady);
            } else {
                return needAppendCondition.await(this);
            }
        }

        private FrameCallResult afterPosReady(Void unused) {
            if (logFileQueue.isClosed()) {
                return Fiber.frameReturn();
            }
            return writeData();
        }

        @Override
        protected FrameCallResult handle(Throwable ex) {
            return Fiber.fatal(ex);
        }
    }


    @SuppressWarnings("FieldMayBeFinal")
    static class WriteTask extends AsyncIoTask {
        int lastTerm;
        long lastIndex;

        WriteTask nextNeedSyncTask;

        public WriteTask(FiberGroup fiberGroup, DtFile dtFile,
                         long[] retryInterval, boolean retryForever, Supplier<Boolean> cancelIndicator) {
            super(fiberGroup, dtFile, retryInterval, retryForever, cancelIndicator);
        }

    }

    public void append() {
        needAppendCondition.signalLater();
    }

    private ByteBuffer borrowBuffer(int size) {
        if (size == 0) {
            return EMPTY_BUFFER;
        } else {
            size = Math.min(size, logFileQueue.maxWriteBufferSize);
            return groupConfig.getDirectPool().borrow(size);
        }
    }

    private FrameCallResult writeData() {
        // reset 4 status fields
        writeStartPosInFile = nextPersistPos & fileLenMask;
        bytesToWrite = 0;
        ArrayList<LogItem> items = this.items;
        items.clear();
        lastItem = null;

        long calculatedItemIndex = -1;
        LogFile file = logFileQueue.getLogFile(nextPersistPos);
        boolean writeEndHeader = false;
        boolean rollNextFile = false;
        for (long lastIndex = cache.getLastIndex(), fileRestBytes = file.endPos - nextPersistPos;
             this.nextPersistIndex <= lastIndex; ) {
            RaftTask rt = cache.get(nextPersistIndex);
            LogItem li = rt.getItem();
            calculatedItemIndex = initItemSize(li, calculatedItemIndex);
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
        buffer = writeItems(items, file, buffer);

        if (writeEndHeader) {
            if (buffer.remaining() < LogHeader.ITEM_HEADER_SIZE) {
                buffer = write(file, buffer);
            }
            LogHeader.writeEndHeader(crc32c, buffer);
        }
        if (buffer.position() > 0) {
            write(file, buffer);
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
        // continue loop
        return Fiber.resume(null, appendFiberFrame);
    }

    private ByteBuffer writeItems(ArrayList<LogItem> items, LogFile file, ByteBuffer buffer) {
        long dataPos = file.startPos + writeStartPosInFile;
        for (int count = items.size(), i = 0; i < count; i++) {
            LogItem li = items.get(i);
            if (file.firstIndex == 0) {
                file.firstIndex = li.getIndex();
                file.firstTerm = li.getTerm();
                file.firstTimestamp = li.getTimestamp();
            }
            if (buffer.remaining() < LogHeader.ITEM_HEADER_SIZE) {
                buffer = write(file, buffer);
            }
            int len = LogHeader.writeHeader(crc32c, buffer, li);
            buffer = writeBizHeader(li, buffer, file);
            buffer = writeBizBody(li, buffer, file);
            idxOps.put(li.getIndex(), dataPos);
            dataPos += len;
            lastItem = li;
        }
        return buffer;
    }

    private ByteBuffer writeBizHeader(LogItem li, ByteBuffer buffer, LogFile file) {
        if (li.getActualHeaderSize() > 0) {
            crc32c.reset();
            try {
                while (true) {
                    int startPos = buffer.position();
                    boolean finish;
                    if (li.getHeaderBuffer() != null) {
                        finish = ByteBufferEncoder.INSTANCE.encode(encodeContext, buffer, li.getHeaderBuffer());
                    } else {
                        //noinspection rawtypes
                        Encoder encoder = codecFactory.createHeaderEncoder(li.getBizType());
                        //noinspection unchecked
                        finish = encoder.encode(encodeContext, buffer, li.getHeader());
                    }
                    RaftUtil.updateCrc(crc32c, buffer, startPos, buffer.position() - startPos);
                    if (finish) {
                        break;
                    } else {
                        buffer = write(file, buffer);
                    }
                }
            } finally {
                encodeContext.setStatus(null);
            }
            if (buffer.remaining() < 4) {
                buffer = write(file, buffer);
            }
            buffer.putInt((int) crc32c.getValue());
        }
        return buffer;
    }

    private ByteBuffer writeBizBody(LogItem li, ByteBuffer buffer, LogFile file) {
        if (li.getActualBodySize() > 0) {
            crc32c.reset();
            try {
                while (true) {
                    int startPos = buffer.position();
                    boolean finish;
                    if (li.getBodyBuffer() != null) {
                        finish = ByteBufferEncoder.INSTANCE.encode(encodeContext, buffer, li.getBodyBuffer());
                    } else {
                        //noinspection rawtypes
                        Encoder encoder = codecFactory.createBodyEncoder(li.getBizType());
                        //noinspection unchecked
                        finish = encoder.encode(encodeContext, buffer, li.getBody());
                    }
                    RaftUtil.updateCrc(crc32c, buffer, startPos, buffer.position() - startPos);
                    if (finish) {
                        break;
                    } else {
                        buffer = write(file, buffer);
                    }
                }
            } finally {
                encodeContext.setStatus(null);
            }
            if (buffer.remaining() < 4) {
                buffer = write(file, buffer);
            }
            buffer.putInt((int) crc32c.getValue());
        }
        return buffer;
    }

    private ByteBuffer write(LogFile file, ByteBuffer buffer) {
        buffer.flip();
        int bytes = buffer.remaining();
        long[] retry = (logFileQueue.initialized && !logFileQueue.isClosed()) ? groupConfig.getIoRetryInterval() : null;
        WriteTask task = new WriteTask(fiberGroup, file, retry, true, writeStopIndicator);
        if (lastItem != null) {
            task.lastTerm = lastItem.getTerm();
            task.lastIndex = lastItem.getIndex();
        }

        // no flush
        task.write(buffer, writeStartPosInFile);

        writeTaskQueue.addLast(task);

        task.getFuture().registerCallback(new FiberFuture.FutureCallback<>() {
            @Override
            protected FrameCallResult onCompleted(Void unused, Throwable ex) {
                processWriteResult(task, ex);
                return Fiber.frameReturn();
            }
        });

        writeStartPosInFile += bytes;
        bytesToWrite -= bytes;
        lastItem = null;

        return borrowBuffer(bytesToWrite);
    }

    private void processWriteResult(WriteTask wt, Throwable ex) {
        try {
            if (logFileQueue.isClosed()) {
                return;
            }
            if (ex != null) {
                //noinspection StatementWithEmptyBody
                if (DtUtil.rootCause(ex) instanceof FiberCancelException) {
                    // no ops
                } else {
                    log.error("log append fail", ex);
                    fiberGroup.requestShutdown();
                }
            } else {
                while (writeTaskQueue.size() > 0) {
                    if (!writeTaskQueue.get(0).getFuture().isDone()) {
                        break;
                    }
                    WriteTask t = writeTaskQueue.removeFirst();
                    if (t.lastTerm > 0) {
                        if (syncWriteTaskQueueHead == null) {
                            syncWriteTaskQueueHead = t;
                        } else {
                            syncWriteTaskQueueHead.nextNeedSyncTask = t;
                        }
                    }
                }
                if (syncWriteTaskQueueHead != null) {
                    needFsyncCondition.signalLater();
                }
            }
        } finally {
            if (wt.getIoBuffer() != null) {
                groupConfig.getDirectPool().release(wt.getIoBuffer());
                wt.setIoBuffer(null);
            }
        }
    }

    private class FsyncFiberFrame extends FiberFrame<Void> {
        @Override
        public FrameCallResult execute(Void input) {
            if (logFileQueue.isClosed()) {
                return Fiber.frameReturn();
            }
            if (syncWriteTaskQueueHead == null) {
                return needFsyncCondition.await(this);
            } else {
                return fsync().await(this::afterFsync);
            }
        }

        private FiberFuture<Void> fsync() {
            WriteTask task = syncWriteTaskQueueHead;
            if (task == null) {
                return FiberFuture.completedFuture(fiberGroup, null);
            }
            while (task.nextNeedSyncTask != null) {
                if (task.getDtFile() == task.nextNeedSyncTask.getDtFile()) {
                    task = task.nextNeedSyncTask;
                }
            }
            WriteTask finalTask = task;
            FiberFuture<Void> f = fiberGroup.newFuture();
            groupConfig.getIoExecutor().submit(() -> {
                try {
                    finalTask.getDtFile().getChannel().force(false);
                    f.fireComplete(null);
                } catch (IOException e) {
                    // TODO ex handle
                    e.printStackTrace();
                }
            });
            return f;
        }

        private FrameCallResult afterFsync(Void unused) {
            WriteTask head = syncWriteTaskQueueHead;
            if (head != null) {
                appendCallback.finish(head.lastTerm, head.lastIndex);
                if (head.lastIndex >= cache.getLastIndex()) {
                    noPendingCondition.signalAll();
                }
                syncWriteTaskQueueHead = head.nextNeedSyncTask;
            }
            return Fiber.resume(null, this);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private long initItemSize(LogItem item, long calculatedItemIndex) {
        if (calculatedItemIndex >= item.getIndex()) {
            return calculatedItemIndex;
        }
        if (item.getHeaderBuffer() != null) {
            item.setActualHeaderSize(item.getHeaderBuffer().remaining());
        } else if (item.getHeader() != null) {
            Encoder encoder = codecFactory.createHeaderEncoder(item.getBizType());
            item.setActualHeaderSize(encoder.actualSize(item.getHeader()));
        }
        if (item.getBodyBuffer() != null) {
            item.setActualBodySize(item.getBodyBuffer().remaining());
        } else if (item.getBody() != null) {
            Encoder encoder = codecFactory.createBodyEncoder(item.getBizType());
            item.setActualBodySize(encoder.actualSize(item.getBody()));
        }
        return item.getIndex();
    }

    public void setNext(long nextPersistIndex, long nextPersistPos) {
        this.nextPersistIndex = nextPersistIndex;
        this.nextPersistPos = nextPersistPos;
    }

    public FiberFrame<Void> waitWriteFinishOrShouldStopOrClose() {
        return new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                if (isGroupShouldStopPlain() || logFileQueue.isClosed()) {
                    return Fiber.frameReturn();
                }
                if (nextPersistIndex <= cache.getLastIndex() || writeTaskQueue.size() > 0
                        || syncWriteTaskQueueHead != null) {
                    return noPendingCondition.await(1000, this);
                } else {
                    return Fiber.frameReturn();
                }
            }
        };
    }

}
