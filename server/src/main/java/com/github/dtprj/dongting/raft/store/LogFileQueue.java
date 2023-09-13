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
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.Encoder;
import com.github.dtprj.dongting.common.BitUtil;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.FileUtil;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.ChecksumException;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.sm.RaftCodecFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
class LogFileQueue extends FileQueue implements FileOps {
    private static final DtLog log = DtLogs.getLogger(LogFileQueue.class);

    private final long logFileSize;
    private final long fileLenMask;
    private final int fileLenShiftBits;

    protected final RefBufferFactory heapPool;
    protected final ByteBufferPool directPool;

    private final IdxOps idxOps;

    private final ByteBuffer writeBuffer;
    private final CRC32C crc32c = new CRC32C();

    private final EncodeContext encodeContext;
    private final RaftCodecFactory codecFactory;
    private final Timestamp ts;

    private long writePos;

    public LogFileQueue(File dir, RaftGroupConfigEx groupConfig, IdxOps idxOps) {
        this(dir, groupConfig, idxOps, 1024 * 1024 * 1024, 128 * 1024);
    }

    public LogFileQueue(File dir, RaftGroupConfigEx groupConfig, IdxOps idxOps, long logFileSize, int writeBufferSize) {
        super(dir, groupConfig);
        this.idxOps = idxOps;
        this.ts = groupConfig.getTs();
        this.encodeContext = new EncodeContext(groupConfig.getHeapPool());
        this.codecFactory = groupConfig.getCodecFactory();
        this.heapPool = groupConfig.getHeapPool();
        this.directPool = groupConfig.getDirectPool();
        this.logFileSize = logFileSize;
        this.fileLenMask = logFileSize - 1;
        this.fileLenShiftBits = BitUtil.zeroCountOfBinary(logFileSize);
        this.writeBuffer = ByteBuffer.allocateDirect(writeBufferSize);
    }

    @Override
    protected long getFileSize() {
        return logFileSize;
    }

    @Override
    public int getFileLenShiftBits() {
        return fileLenShiftBits;
    }

    protected long getWritePos() {
        return writePos;
    }

    public int restore(long restoreIndex, long restoreIndexPos, Supplier<Boolean> stopIndicator)
            throws IOException, InterruptedException {
        log.info("restore from {}, {}", restoreIndex, restoreIndexPos);
        Restorer restorer = new Restorer(idxOps, this, restoreIndex, restoreIndexPos);
        if (queue.size() == 0) {
            return 0;
        }
        if (restoreIndexPos < queue.get(0).startPos) {
            throw new RaftException("restoreIndexPos is illegal. " + restoreIndexPos);
        }
        if (restoreIndexPos >= queue.get(queue.size() - 1).endPos) {
            throw new RaftException("restoreIndexPos is illegal. " + restoreIndexPos);
        }
        for (int i = 0; i < queue.size(); i++) {
            RaftUtil.checkStop(stopIndicator);
            LogFile lf = queue.get(i);
            Pair<Boolean, Long> result = restorer.restoreFile(this.writeBuffer, lf, stopIndicator);
            writePos = result.getRight();
            if (result.getLeft()) {
                break;
            }
        }
        log.info("restore finished. lastTerm={}, lastIndex={}, lastPos={}, lastFile={}",
                restorer.previousTerm, restorer.previousIndex, writePos, queue.get(queue.size() - 1).file.getPath());
        return restorer.previousTerm;
    }

    @SuppressWarnings("rawtypes")
    public void append(List<LogItem> logs) throws IOException, InterruptedException {
        ensureWritePosReady(writePos);
        ByteBuffer writeBuffer = this.writeBuffer;
        writeBuffer.clear();
        long pos = writePos;
        long bufferPosOfFile = writePos & fileLenMask;
        LogFile file = getLogFile(pos);
        for (int i = 0; i < logs.size(); i++) {
            LogItem log = logs.get(i);
            Encoder headerEncoder = initEncoderAndSize(log, true);
            Encoder bodyEncoder = initEncoderAndSize(log, false);

            int totalLen = LogHeader.computeTotalLen(0, log.getActualHeaderSize(), log.getActualBodySize());
            if ((pos & fileLenMask) == 0) {
                if (i != 0) {
                    // last item exactly fill the file
                    bufferPosOfFile = writeAndClearBuffer(writeBuffer, file, bufferPosOfFile);
                    ensureWritePosReady(pos);
                    file = getLogFile(pos);
                }
                file.firstTimestamp = log.getTimestamp();
                file.firstIndex = log.getIndex();
                file.firstTerm = log.getTerm();
            } else {
                long fileRest = file.endPos - pos;
                if (fileRest < totalLen) {
                    // file rest space is not enough
                    if (fileRest >= LogHeader.ITEM_HEADER_SIZE) {
                        // write an empty header to indicate the end of the file
                        if (writeBuffer.remaining() < LogHeader.ITEM_HEADER_SIZE) {
                            bufferPosOfFile = writeAndClearBuffer(writeBuffer, file, bufferPosOfFile);
                        }
                        LogHeader.writeEndHeader(crc32c, writeBuffer);
                    }
                    writeAndClearBuffer(writeBuffer, file, bufferPosOfFile);

                    // roll to next file
                    pos = nextFilePos(pos);
                    bufferPosOfFile = pos;
                    ensureWritePosReady(pos);
                    file = getLogFile(pos);

                    file.firstTimestamp = log.getTimestamp();
                    file.firstIndex = log.getIndex();
                    file.firstTerm = log.getTerm();
                }
            }

            if (writeBuffer.remaining() < LogHeader.ITEM_HEADER_SIZE) {
                bufferPosOfFile = writeAndClearBuffer(writeBuffer, file, bufferPosOfFile);
            }

            long itemStartPos = pos;
            LogHeader.writeHeader(crc32c, writeBuffer, log, 0, log.getActualHeaderSize(), log.getActualBodySize());
            pos += LogHeader.ITEM_HEADER_SIZE;

            if (headerEncoder != null && log.getActualHeaderSize() > 0) {
                Object data = log.getHeaderBuffer() != null ? log.getHeaderBuffer() : log.getHeader();
                bufferPosOfFile = writeData(writeBuffer, bufferPosOfFile, file, data, headerEncoder);
                pos += log.getActualHeaderSize() + 4;
            }
            if (bodyEncoder != null && log.getActualBodySize() > 0) {
                Object data = log.getBodyBuffer() != null ? log.getBodyBuffer() : log.getBody();
                bufferPosOfFile = writeData(writeBuffer, bufferPosOfFile, file, data, bodyEncoder);
                pos += log.getActualBodySize() + 4;
            }

            idxOps.put(log.getIndex(), itemStartPos, false);
        }
        writeAndClearBuffer(writeBuffer, file, bufferPosOfFile);
        file.channel.force(false);
        this.writePos = pos;
    }

    @Override
    public long nextFilePos(long absolutePos) {
        return ((absolutePos >>> fileLenShiftBits) + 1) << fileLenShiftBits;
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
            } else {
                return null;
            }
        } else {
            if (item.getBodyBuffer() != null) {
                item.setActualBodySize(item.getBodyBuffer().remaining());
                return ByteBufferEncoder.INSTANCE;
            } else if (item.getBody() != null) {
                Encoder encoder = codecFactory.createBodyEncoder(item.getBizType());
                item.setActualBodySize(encoder.actualSize(item.getBody()));
                return encoder;
            } else {
                return null;
            }
        }
    }

    @SuppressWarnings("rawtypes")
    private long writeData(ByteBuffer writeBuffer, long bufferPosOfFile, LogFile file, Object data, Encoder encoder) throws IOException {
        crc32c.reset();
        if (writeBuffer.remaining() == 0) {
            bufferPosOfFile = writeAndClearBuffer(writeBuffer, file, bufferPosOfFile);
        }
        try {
            while (true) {
                int lastPos = writeBuffer.position();
                @SuppressWarnings("unchecked") boolean encodeFinish = encoder.encode(encodeContext, writeBuffer, data);
                if (writeBuffer.position() > lastPos) {
                    RaftUtil.updateCrc(crc32c, writeBuffer, lastPos, writeBuffer.position() - lastPos);
                }
                if (encodeFinish) {
                    break;
                }
                bufferPosOfFile = writeAndClearBuffer(writeBuffer, file, bufferPosOfFile);
            }
        } finally {
            encodeContext.setStatus(null);
        }

        if (writeBuffer.remaining() < 4) {
            bufferPosOfFile = writeAndClearBuffer(writeBuffer, file, bufferPosOfFile);
        }
        writeBuffer.putInt((int) crc32c.getValue());
        return bufferPosOfFile;
    }

    private long writeAndClearBuffer(ByteBuffer buffer, LogFile file, long pos) throws IOException {
        if (buffer.position() == 0) {
            return pos;
        }
        long posOfFile = pos & fileLenMask;
        buffer.flip();
        int count = buffer.remaining();
        FileUtil.syncWriteFull(file.channel, buffer, posOfFile);
        buffer.clear();
        return pos + count;
    }

    public void syncTruncateTail(long startPosition, long endPosition) throws IOException {
        DtUtil.checkNotNegative(startPosition, "startPosition");
        DtUtil.checkNotNegative(endPosition, "endPosition");
        log.info("truncate tail from {} to {}, currentWritePos={}", startPosition, endPosition, writePos);
        writePos = startPosition;
        int startQueueIndex = (int) ((startPosition - queueStartPosition) >>> fileLenShiftBits);
        ByteBuffer buffer = directPool.borrow(64 * 1024);
        try {
            while (buffer.hasRemaining()) {
                // fill with zero
                buffer.putLong(0);
            }
            for (int i = startQueueIndex; i < queue.size(); i++) {
                LogFile lf = queue.get(i);
                if (lf.startPos >= startPosition) {
                    lf.firstTerm = 0;
                    lf.firstIndex = 0;
                    lf.firstTimestamp = 0;
                }
                fillWithZero(buffer, lf, startPosition, endPosition);
            }
        } finally {
            directPool.release(buffer);
        }
    }

    private void fillWithZero(ByteBuffer buffer, LogFile lf, long startPosition, long endPosition) throws IOException {
        if (lf.startPos >= endPosition) {
            return;
        }
        long start = Math.max(lf.startPos, startPosition);
        long end = Math.min(lf.endPos, endPosition);
        if (start >= end) {
            return;
        }
        start = start & fileLenMask;
        // now end is included
        end = (end - 1) & fileLenMask;

        // not remove end magic, but it's ok
        log.info("truncate tail, file zero from {} to {}, file={}", start, end, lf.file.getPath());
        for (long i = start; i <= end; ) {
            buffer.clear();
            int fileRest = (int) (end - i + 1);
            if (buffer.capacity() > fileRest) {
                buffer.limit(fileRest);
            }
            int count = buffer.remaining();
            FileUtil.syncWriteFull(lf.channel, buffer, i);
            i += count;
        }
    }

    private void checkPos(long pos) {
        if (pos < queueStartPosition) {
            throw new RaftException("pos too small: " + pos);
        }
        if (pos >= writePos) {
            throw new RaftException("pos too large: " + pos);
        }
    }

    public void markDelete(long boundIndex, long timestampMillis, long delayMills) {
        markDelete(delayMills, nextFile -> timestampMillis > nextFile.firstTimestamp
                && boundIndex >= nextFile.firstIndex);
    }

    private void markDelete(long delayMillis, Predicate<LogFile> predicate) {
        long deleteTimestamp = ts.getWallClockMillis() + delayMillis;
        int queueSize = queue.size();
        for (int i = 0; i < queueSize - 1; i++) {
            LogFile logFile = queue.get(i);
            LogFile nextFile = queue.get(i + 1);

            if (nextFile.firstTimestamp == 0) {
                return;
            }
            if (predicate.test(nextFile)) {
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

                AsyncIoTask task = new AsyncIoTask(logFile.channel, stopIndicator , cancel);
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

    @Override
    public long filePos(long absolutePos) {
        return absolutePos & fileLenMask;
    }

    @Override
    public long restInCurrentFile(long absolutePos) {
        checkPos(absolutePos);
        long totalRest = writePos - absolutePos;
        long fileRest = logFileSize - filePos(absolutePos);
        return Math.min(totalRest, fileRest);
    }

    @Override
    public LogFile getLogFile(long filePos) {
        return super.getLogFile(filePos);
    }

    @Override
    public long fileLength() {
        return logFileSize;
    }

}
