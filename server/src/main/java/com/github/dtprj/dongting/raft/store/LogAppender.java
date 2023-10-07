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
import java.util.concurrent.CompletableFuture;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
class LogAppender {
    private static final DtLog log = DtLogs.getLogger(LogAppender.class);
    private static final int STATE_WRITE_ITEM_HEADER = 0;
    private static final int STATE_WRITE_BIZ_HEADER = 1;
    private static final int STATE_WRITE_BIZ_BODY = 2;

    private static final int APPEND_OK = 100;
    private static final int CHANGE_BUFFER = 200;
    private static final int CHANGE_FILE = 300;

    private static final int MIN_WRITE_SIZE = 1024;

    private final IdxOps idxOps;
    private final LogFileQueue logFileQueue;
    private final RaftCodecFactory codecFactory;
    private final ByteBuffer writeBuffer;
    private final CRC32C crc32c = new CRC32C();
    private final EncodeContext encodeContext;
    private final long fileLenMask;
    private final long bufferLenMask;
    private final RaftStatusImpl raftStatus;
    private final RaftLog.AppendCallback appendCallback;

    private long nextPersistIndex = -1;
    private long nextPersistPos = -1;

    private int state;
    private int pendingBytes;
    @SuppressWarnings("rawtypes")
    private Encoder currentHeaderEncoder;
    @SuppressWarnings("rawtypes")
    private Encoder currentBodyEncoder;

    private long bufferWriteAccumulatePos;

    private final IndexedQueue<WriteTask> writeTaskQueue = new IndexedQueue<>(32);

    LogAppender(IdxOps idxOps, LogFileQueue logFileQueue, RaftGroupConfig groupConfig,
                ByteBuffer writeBuffer, RaftLog.AppendCallback appendCallback) {
        this.idxOps = idxOps;
        this.logFileQueue = logFileQueue;
        this.codecFactory = groupConfig.getCodecFactory();
        this.encodeContext = new EncodeContext(groupConfig.getHeapPool());
        this.fileLenMask = logFileQueue.fileLength() - 1;
        this.writeBuffer = writeBuffer;
        this.bufferLenMask = writeBuffer.capacity() - 1;
        this.raftStatus = (RaftStatusImpl) groupConfig.getRaftStatus();
        this.appendCallback = appendCallback;
    }

    static class WriteTask {
        final LogFile logFile;
        final ByteBuffer buffer;
        final long writeStartPosInFile;
        final int writeStartPosInBuffer;
        final long startIndex;
        LogItem lastItem;
        boolean finished;
        int size;

        public WriteTask(LogFile logFile, ByteBuffer buffer, long startIndex,
                         long writeStartPosInFile, int writeStartPosInBuffer) {
            this.logFile = logFile;
            this.buffer = buffer;
            this.startIndex = startIndex;
            this.writeStartPosInFile = writeStartPosInFile;
            this.writeStartPosInBuffer = writeStartPosInBuffer;
        }
    }

    private WriteTask createWriteTask(LogFile logFile) {
        long writeStartPosInFile = nextPersistPos & fileLenMask;
        int writeStartPosInBuffer = (int) (bufferWriteAccumulatePos & bufferLenMask);
        int rest;
        int writeLimit = writeTaskQueue.size() > 0 ? writeTaskQueue.get(0).writeStartPosInBuffer : -1;

        if (writeStartPosInBuffer > writeLimit) {
            rest = writeBuffer.capacity() - writeStartPosInBuffer;
            if (rest < MIN_WRITE_SIZE) {
                writeStartPosInBuffer = 0;
                bufferWriteAccumulatePos += rest;
                if (writeLimit == -1) {
                    rest = writeBuffer.capacity();
                } else if (writeLimit == 0) {
                    return null;
                } else {
                    rest = writeLimit;
                }
            }
        } else if (writeStartPosInBuffer == writeLimit) {
            return null;
        } else {
            rest = writeLimit - writeStartPosInBuffer;
        }

        ByteBuffer buffer = writeBuffer.duplicate();
        buffer.position(writeStartPosInBuffer);
        buffer.limit(writeStartPosInBuffer + rest);
        return new WriteTask(logFile, buffer, nextPersistIndex, writeStartPosInFile, writeStartPosInBuffer);
    }

    public void append(TailCache tailCache) throws InterruptedException {
        if (tailCache.size() == 0) {
            BugLog.getLog().error("tailCache.size() == 0, nextPersistIndex={}", nextPersistIndex);
            return;
        }
        if (nextPersistIndex < tailCache.getFirstIndex()) {
            BugLog.getLog().error("nextPersistIndex {} < tailCache.getFirstIndex() {}",
                    nextPersistIndex, tailCache.getFirstIndex());
            return;
        }
        if (nextPersistIndex > tailCache.getLastIndex()) {
            if (log.isDebugEnabled()) {
                log.debug("nextPersistIndex {} > tailCache.getLastIndex() {}",
                        nextPersistIndex, tailCache.getLastIndex());
            }
            return;
        }
        logFileQueue.ensureWritePosReady(nextPersistPos, true);
        LogFile file = logFileQueue.getLogFile(nextPersistPos);

        WriteTask wt = createWriteTask(file);
        if (wt == null) {
            return;
        }

        while (nextPersistIndex <= tailCache.getLastIndex()) {
            RaftTask rt = tailCache.get(nextPersistIndex);
            LogItem li = rt.getItem();
            int r = appendItem(wt, li);
            if (r == CHANGE_BUFFER) {
                int bytes = write(wt);
                nextPersistPos += bytes;
                bufferWriteAccumulatePos += bytes;
                wt = createWriteTask(file);
                if (wt == null) {
                    return;
                }
            } else if (r == CHANGE_FILE) {
                nextPersistPos = logFileQueue.nextFilePos(nextPersistPos);
                file = logFileQueue.getLogFile(nextPersistPos);
                wt = createWriteTask(file);
                if (wt == null) {
                    return;
                }
            } else {
                long pos = nextPersistPos + wt.buffer.position() - wt.writeStartPosInBuffer;
                idxOps.put(li.getIndex(), pos, false);
                nextPersistIndex++;
            }
        }
        int bytes = write(wt);
        nextPersistPos += bytes;
        bufferWriteAccumulatePos += bytes;
    }

    private int write(WriteTask wt) {
        ByteBuffer buf = wt.buffer;
        if (buf.position() == wt.writeStartPosInBuffer) {
            return 0;
        }
        buf.limit(buf.position());
        buf.position(wt.writeStartPosInBuffer);
        int x = buf.remaining();
        wt.size = x;
        AsyncIoTask ioTask = new AsyncIoTask(wt.logFile.channel, raftStatus::isStop, null);
        boolean needFlush = wt.lastItem != null;
        CompletableFuture<Void> f = ioTask.write(needFlush, false, buf, wt.writeStartPosInFile);
        f.whenCompleteAsync((v, ex) -> processWriteResult(ex, ioTask, wt), raftStatus.getRaftExecutor());
        writeTaskQueue.addLast(wt);
        return x;
    }

    private void processWriteResult(Throwable ex, AsyncIoTask ioTask, WriteTask wt) {
        if (raftStatus.isStop()) {
            return;
        }
        if (ex != null) {
            log.error("write error, will retry", ex);
            // TODO use retry interval
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                DtUtil.restoreInterruptStatus();
                throw new RaftException(e);
            }
            CompletableFuture<Void> f = ioTask.retry();
            f.whenCompleteAsync((v, ex1) -> processWriteResult(ex1, ioTask, wt), raftStatus.getRaftExecutor());
        } else {
            wt.finished = true;
            LogItem lastFinishedItem = null;
            while (writeTaskQueue.size() > 0) {
                if (writeTaskQueue.get(0).finished) {
                    WriteTask head = writeTaskQueue.removeFirst();
                    if (head.lastItem != null) {
                        lastFinishedItem = head.lastItem;
                    }
                } else {
                    break;
                }
            }
            if (lastFinishedItem != null) {
                appendCallback.finish(lastFinishedItem.getTerm(), lastFinishedItem.getIndex());
            }
        }
    }

    private int appendItem(WriteTask wt, LogItem li) {
        ByteBuffer buf = wt.buffer;
        switch (state) {
            case STATE_WRITE_ITEM_HEADER:
                if (buf.remaining() < LogHeader.ITEM_HEADER_SIZE) {
                    return CHANGE_BUFFER;
                }
                currentHeaderEncoder = initEncoderAndSize(li, true);
                currentBodyEncoder = initEncoderAndSize(li, false);
                long fileRest = wt.logFile.endPos - wt.writeStartPosInFile;
                if (LogHeader.computeTotalLen(0, li.getActualHeaderSize(), li.getActualBodySize()) > fileRest) {
                    if (fileRest >= LogHeader.ITEM_HEADER_SIZE) {
                        // write an empty header to indicate the end of the file
                        LogHeader.writeEndHeader(crc32c, buf);
                    }
                    return CHANGE_FILE;
                }
                LogHeader.writeHeader(crc32c, buf, li, 0, li.getActualHeaderSize(), li.getActualBodySize());
                state = STATE_WRITE_BIZ_HEADER;
                // not break
            case STATE_WRITE_BIZ_HEADER:
                if (!encodeData(buf, li, true)) {
                    return CHANGE_BUFFER;
                }
                state = STATE_WRITE_BIZ_BODY;
                // not break
            case STATE_WRITE_BIZ_BODY:
                if (!encodeData(buf, li, false)) {
                    return CHANGE_BUFFER;
                }
                wt.lastItem = li;
                state = STATE_WRITE_ITEM_HEADER;
        }
        return APPEND_OK;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean encodeData(ByteBuffer buf, LogItem li, boolean header) {
        int len = header ? li.getActualHeaderSize() : li.getActualBodySize();
        if (len <= 0) {
            return true;
        }
        if (!buf.hasRemaining()) {
            return false;
        }
        if (pendingBytes == 0) {
            crc32c.reset();
        }
        if (pendingBytes < len) {
            int start = buf.position();
            boolean finish;
            if (header) {
                //noinspection unchecked
                finish = currentHeaderEncoder.encode(encodeContext, buf,
                        li.getHeaderBuffer() != null ? li.getHeaderBuffer() : li.getHeader());
            } else {
                //noinspection unchecked
                finish = currentBodyEncoder.encode(encodeContext, buf,
                        li.getBodyBuffer() != null ? li.getBodyBuffer() : li.getBody());
            }
            int encodeSize = buf.position() - start;
            RaftUtil.updateCrc(crc32c, buf, start, encodeSize);
            if (!finish) {
                pendingBytes += encodeSize;
                return false;
            } else {
                pendingBytes = 0;
                encodeContext.setStatus(null);
            }
        }
        if (buf.remaining() < 4) {
            return false;
        }
        buf.putInt((int) crc32c.getValue());
        return true;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Encoder initEncoderAndSize(LogItem item, boolean header) {
        if (header) {
            if (item.getHeaderBuffer() != null) {
                item.setActualHeaderSize(item.getHeaderBuffer().remaining());
                return ByteBufferEncoder.INSTANCE;
            } else if (item.getHeader() != null) {
                Encoder encoder = codecFactory.createHeaderEncoder(item.getBizType());
                item.setActualHeaderSize(encoder.actualSize(item.getHeader()));
                return encoder;
            }
        } else {
            if (item.getBodyBuffer() != null) {
                item.setActualBodySize(item.getBodyBuffer().remaining());
                return ByteBufferEncoder.INSTANCE;
            } else if (item.getBody() != null) {
                Encoder encoder = codecFactory.createBodyEncoder(item.getBizType());
                item.setActualBodySize(encoder.actualSize(item.getBody()));
                return encoder;
            }
        }
        return null;
    }

    public void setNextPersistIndex(long nextPersistIndex) {
        this.nextPersistIndex = nextPersistIndex;
    }

    public void setNextPersistPos(long nextPersistPos) {
        this.nextPersistPos = nextPersistPos;
    }

    public long getNextPersistPos() {
        return nextPersistPos;
    }
}
