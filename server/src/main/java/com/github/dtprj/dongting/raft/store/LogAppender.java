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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.function.Supplier;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
class LogAppender {
    private static final DtLog log = DtLogs.getLogger(LogAppender.class);

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

    private long nextPersistIndex = -1;
    private long nextPersistPos = -1;

    private final IndexedQueue<WriteTask> writeTaskQueue = new IndexedQueue<>(32);

    private final Fiber appendFiber;
    private final AppendFiberFrame appendFiberFrame = new AppendFiberFrame();
    private final FiberCondition needAppendCondition;

    // 5 temp status fields, should reset in afterPosReady()
    private final ArrayList<LogItem> items = new ArrayList<>(32);
    private long calculatedItemIndex = -1;
    private LogItem lastItem;
    private long writeStartPosInFile;
    private int restBytes;

    private final Supplier<Boolean> writeStopIndicator;


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
        this.needAppendCondition = fiberGroup.newCondition();
        this.writeStopIndicator = logFileQueue::isClosed;
    }

    public void startFiber() {
        appendFiber.start();
    }

    private class AppendFiberFrame extends FiberFrame<Void> {

        @Override
        public FrameCallResult execute(Void input) {
            TailCache tailCache = LogAppender.this.cache;
            long nextPersistIndex = LogAppender.this.nextPersistIndex;
            if (tailCache.size() > 0 && tailCache.getLastIndex() > nextPersistIndex) {
                needAppendCondition.awaitOn(this);
            }
            if (nextPersistIndex < tailCache.getFirstIndex()) {
                BugLog.getLog().error("nextPersistIndex {} < tailCache.getFirstIndex() {}",
                        nextPersistIndex, tailCache.getFirstIndex());
                return Fiber.fatal(new RaftException("nextPersistIndex<tailCache.getFirstIndex()"));
            }
            return Fiber.call(logFileQueue.ensureWritePosReady(nextPersistPos), LogAppender.this::afterPosReady);
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    static class WriteTask {
        private ByteBuffer buffer;
        private FiberFuture<Void> future;
        private int lastTerm;
        private long lastIndex;

        public WriteTask(ByteBuffer buffer, LogItem lastItem, FiberFuture<Void> future) {
            this.buffer = buffer;
            this.future = future;
            if (lastItem != null) {
                this.lastTerm = lastItem.getTerm();
                this.lastIndex = lastItem.getIndex();
            }
        }
    }

    public void append() {
        needAppendCondition.signal();
    }

    private ByteBuffer borrowBuffer(int size) {
        size = Math.min(size, 128 * 1024);
        return groupConfig.getDirectPool().getPool().borrow(size);
    }

    private FrameCallResult afterPosReady(Void unused) throws Exception {
        // reset 5 status fields
        writeStartPosInFile = nextPersistPos & fileLenMask;
        restBytes = 0;
        ArrayList<LogItem> items = this.items;
        items.clear();
        calculatedItemIndex = -1;
        lastItem = null;

        LogFile file = logFileQueue.getLogFile(nextPersistPos);
        boolean writeEndHeader = false;
        for (long lastIndex = cache.getLastIndex(), i = this.nextPersistIndex,
             fileRestBytes = file.endPos - writeStartPosInFile; i <= lastIndex; i++) {
            RaftTask rt = cache.get(i);
            LogItem li = rt.getItem();
            initItemSize(li);
            int len = LogHeader.computeTotalLen(0, li.getActualHeaderSize(),
                    li.getActualBodySize());
            if (len <= fileRestBytes) {
                items.add(li);
                restBytes += len;
                fileRestBytes -= len;
            } else {
                if (fileRestBytes >= LogHeader.ITEM_HEADER_SIZE) {
                    writeEndHeader = true;
                    restBytes += LogHeader.ITEM_HEADER_SIZE;
                }
                if (!items.isEmpty() || writeEndHeader) {
                    break;
                } else {
                    nextPersistPos = logFileQueue.nextFilePos(nextPersistPos);
                    FiberFrame<Void> f = logFileQueue.ensureWritePosReady(nextPersistPos);
                    // continue loop
                    return Fiber.call(f, appendFiberFrame);
                }
            }
        }

        ByteBuffer buffer = borrowBuffer(restBytes);
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
            groupConfig.getDirectPool().getPool().release(buffer);
        }

        nextPersistIndex += items.size();
        nextPersistPos += writeStartPosInFile - (nextPersistPos & fileLenMask);
        items.clear();
        // continue loop
        return Fiber.resume(null, appendFiberFrame);
    }

    private ByteBuffer writeItems(ArrayList<LogItem> items, LogFile file, ByteBuffer buffer) {
        for (int count = items.size(), i = 0; i < count; i++) {
            LogItem li = items.get(i);
            if (file.firstIndex == 0) {
                file.firstIndex = li.getIndex();
                file.firstTerm = li.getTerm();
                file.firstTimestamp = li.getTimestamp();
            }
            idxOps.put(li.getIndex(), file.startPos + writeStartPosInFile);
            if (buffer.remaining() < LogHeader.ITEM_HEADER_SIZE) {
                buffer = write(file, buffer);
            }
            LogHeader.writeHeader(crc32c, buffer, li, 0, li.getActualHeaderSize(), li.getActualBodySize());
            buffer = writeBizHeader(li, buffer, file);
            buffer = writeBizBody(li, buffer, file);
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
        AsyncIoTask task = new AsyncIoTask(fiberGroup, file.channel, writeStopIndicator, retry, true);
        FiberFuture<Void> future;
        if (lastItem == null) {
            future = task.write(buffer, writeStartPosInFile);
        } else {
            future = task.writeAndFlush(buffer, writeStartPosInFile, false);
        }
        WriteTask wt = new WriteTask(buffer, lastItem, future);
        future.registerCallback((v, ex) -> processWriteResult(wt, ex));
        writeTaskQueue.addLast(wt);

        writeStartPosInFile += bytes;
        restBytes -= bytes;
        lastItem = null;

        return borrowBuffer(restBytes);
    }

    private void processWriteResult(WriteTask wt, Throwable ex) {
        groupConfig.getDirectPool().getPool().release(wt.buffer);
        if (fiberGroup.isShouldStop()) {
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
            int lastItem = 0;
            long lastIndex = 0;
            while (writeTaskQueue.size() > 0) {
                if (writeTaskQueue.get(0).future.isDone()) {
                    WriteTask head = writeTaskQueue.removeFirst();
                    if (head.lastTerm > 0) {
                        lastItem = head.lastTerm;
                        lastIndex = head.lastIndex;
                    }
                } else {
                    break;
                }
            }
            if (lastItem > 0) {
                appendCallback.finish(lastItem, lastIndex);
            }
        }
    }


    @SuppressWarnings({"rawtypes", "unchecked"})
    private void initItemSize(LogItem item) {
        if (calculatedItemIndex >= item.getIndex()) {
            return;
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
        calculatedItemIndex = item.getIndex();
    }

    public void setNextPersistIndex(long nextPersistIndex) {
        this.nextPersistIndex = nextPersistIndex;
    }

    public void setNextPersistPos(long nextPersistPos) {
        this.nextPersistPos = nextPersistPos;
    }

}
