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
import java.util.concurrent.CompletionException;
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

    private final ByteBuffer writeBuffer = ByteBuffer.allocateDirect(128 * 1024);
    private final CRC32C crc32c = new CRC32C();

    private final EncodeContext encodeContext;
    private final RaftCodecFactory codecFactory;
    private final Timestamp ts;

    private long writePos;

    public LogFileQueue(File dir, RaftGroupConfigEx groupConfig, IdxOps idxOps) {
        this(dir, groupConfig, idxOps, 1024 * 1024 * 1024);
    }

    public LogFileQueue(File dir, RaftGroupConfigEx groupConfig, IdxOps idxOps, long logFileSize) {
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

    public int restore(long restoreIndex, long restoreIndexPos, Supplier<Boolean> cancelIndicator)
            throws IOException, InterruptedException {
        log.info("restore from {}, {}", restoreIndex, restoreIndexPos);
        Restorer restorer = new Restorer(idxOps, this, restoreIndex, restoreIndexPos);
        for (int i = 0; i < queue.size(); i++) {
            RaftUtil.checkStop(cancelIndicator);
            LogFile lf = queue.get(i);
            Pair<Boolean, Long> result = restorer.restoreFile(this.writeBuffer, lf, cancelIndicator);
            writePos = result.getRight();
            if (result.getLeft()) {
                break;
            }
        }
        if (queue.size() > 0) {
            if (restoreIndexPos >= queue.get(queue.size() - 1).endPos) {
                throw new RaftException("restoreIndexPos is illegal. " + restoreIndexPos);
            }
            log.info("restore finished. lastTerm={}, lastIndex={}, lastPos={}, lastFile={}",
                    restorer.previousTerm, restorer.previousIndex, writePos, queue.get(queue.size() - 1).file.getPath());
        }
        return restorer.previousTerm;
    }

    @SuppressWarnings("rawtypes")
    public void append(List<LogItem> logs) throws IOException, InterruptedException {
        ensureWritePosReady(writePos);
        ByteBuffer writeBuffer = this.writeBuffer;
        writeBuffer.clear();
        long pos = writePos;
        LogFile file = getLogFile(pos);
        for (int i = 0; i < logs.size(); i++) {
            LogItem log = logs.get(i);
            long posOfFile = (pos + writeBuffer.position()) & fileLenMask;
            Encoder headerEncoder = initEncoderAndSize(log, true);
            Encoder bodyEncoder = initEncoderAndSize(log, false);

            int totalLen = LogHeader.computeTotalLen(0, log.getActualHeaderSize(), log.getActualBodySize());
            if (posOfFile == 0) {
                if (i != 0) {
                    // last item exactly fill the file
                    pos = writeAndClearBuffer(writeBuffer, file, pos);
                    ensureWritePosReady(pos);
                    file = getLogFile(pos);
                }
                file.firstTimestamp = log.getTimestamp();
                file.firstIndex = log.getIndex();
                file.firstTerm = log.getTerm();
            } else {
                long fileRest = logFileSize - posOfFile;
                if (fileRest < totalLen) {
                    // file rest space is not enough
                    if (fileRest >= LogHeader.ITEM_HEADER_SIZE) {
                        // write an empty header to indicate the end of the file
                        if (writeBuffer.remaining() < LogHeader.ITEM_HEADER_SIZE) {
                            // don't update pos
                            writeAndClearBuffer(writeBuffer, file, pos);
                        }
                        LogHeader.writeEndHeader(crc32c, writeBuffer);
                    }
                    // don't update pos
                    writeAndClearBuffer(writeBuffer, file, pos);

                    // roll to next file
                    pos = nextFilePos(pos);
                    ensureWritePosReady(pos);
                    file = getLogFile(pos);

                    file.firstTimestamp = log.getTimestamp();
                    file.firstIndex = log.getIndex();
                    file.firstTerm = log.getTerm();
                }
            }

            if (writeBuffer.remaining() < LogHeader.ITEM_HEADER_SIZE) {
                pos = writeAndClearBuffer(writeBuffer, file, pos);
            }

            long itemStartPos = pos;
            LogHeader.writeHeader(crc32c, writeBuffer, log, 0, log.getActualHeaderSize(), log.getActualBodySize());
            pos += LogHeader.ITEM_HEADER_SIZE;

            if (headerEncoder != null && log.getActualHeaderSize() > 0) {
                Object data = log.getHeaderBuffer() != null ? log.getHeaderBuffer() : log.getHeader();
                pos = writeData(writeBuffer, pos, file, data, headerEncoder);
            }
            if (bodyEncoder != null && log.getActualBodySize() > 0) {
                Object data = log.getBodyBuffer() != null ? log.getBodyBuffer() : log.getBody();
                pos = writeData(writeBuffer, pos, file, data, bodyEncoder);
            }

            idxOps.put(log.getIndex(), itemStartPos, false);
        }
        pos = writeAndClearBuffer(writeBuffer, file, pos);
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
    private long writeData(ByteBuffer writeBuffer, long pos, LogFile file, Object data, Encoder encoder) throws IOException {
        crc32c.reset();
        if (writeBuffer.remaining() == 0) {
            pos = writeAndClearBuffer(writeBuffer, file, pos);
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
                pos = writeAndClearBuffer(writeBuffer, file, pos);
            }
        } finally {
            encodeContext.setStatus(null);
        }

        if (writeBuffer.remaining() < 4) {
            pos = writeAndClearBuffer(writeBuffer, file, pos);
        }
        writeBuffer.putInt((int) crc32c.getValue());
        return pos;
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
        while (buffer.hasRemaining()) {
            // fill with zero
            buffer.putLong(0);
        }
        try {
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
        end = end & fileLenMask;
        log.info("truncate tail, file zero from {} to {}, file={}", start, end, lf.file.getPath());
        for (long i = start; i < end; ) {
            buffer.clear();
            int fileRest = (int) (end - i);
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

    static void prepareNextRead(ByteBuffer buf) {
        if (buf.hasRemaining()) {
            ByteBuffer temp = buf.slice();
            buf.clear();
            buf.put(temp);
        } else {
            buf.clear();
        }
    }

    public void markDelete(long bound, long timestampMillis, long delayMills) {
        markDelete(delayMills, nextFile -> timestampMillis > nextFile.firstTimestamp
                && bound >= nextFile.firstIndex);
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

    public void submitDeleteTask(long startTimestamp) {
        submitDeleteTask(logFile -> {
            long deleteTimestamp = logFile.deleteTimestamp;
            return deleteTimestamp > 0 && deleteTimestamp < startTimestamp && logFile.use <= 0;
        });
    }

    public long getFirstIndex() {
        if (queue.size() > 0) {
            return queue.get(0).firstIndex;
        }
        return 0;
    }

    public CompletableFuture<Pair<Integer, Long>> tryFindMatchPos(
            int suggestTerm, long suggestIndex, long lastIndex, Supplier<Boolean> cancelIndicator) {
        LogFile logFile = findMatchLogFile(suggestTerm, suggestIndex, lastIndex);
        if (logFile == null) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Pair<Integer, Long>> future = new CompletableFuture<>();
        ioExecutor.execute(() -> tryFindMatchPos(cancelIndicator, logFile, suggestTerm, suggestTerm, future));
        return future;
    }

    // in io thread
    private void tryFindMatchPos(Supplier<Boolean> cancel, LogFile logFile, int suggestTerm,
                                 long suggestIndex, CompletableFuture<Pair<Integer, Long>> future) {
        try {
            if (cancel.get()) {
                future.cancel(false);
                return;
            }
            long leftIndex = logFile.firstIndex;
            int leftTerm = logFile.firstTerm;
            long rightIndex = suggestIndex;
            LogHeader header = new LogHeader();
            while (leftIndex < rightIndex) {
                long midIndex = (leftIndex + rightIndex + 1) >>> 1;
                loadHeaderInIoThread(logFile, midIndex, header);
                if (cancel.get()) {
                    future.cancel(false);
                    return;
                }
                if (midIndex == suggestIndex && header.term == suggestTerm) {
                    future.complete(new Pair<>(header.term, midIndex));
                    return;
                } else if (midIndex < suggestIndex && header.term <= suggestTerm) {
                    leftIndex = midIndex;
                    leftTerm = header.term;
                } else {
                    rightIndex = midIndex - 1;
                }
            }
            future.complete(new Pair<>(leftTerm, leftIndex));
        } catch (Throwable e) {
            future.completeExceptionally(e);
        }
    }

    private void loadHeaderInIoThread(LogFile logFile, long index, LogHeader header) throws Exception {
        CompletableFuture<Long> posFuture = CompletableFuture.supplyAsync(() -> {
            try {
                return idxOps.syncLoadLogPos(index);
            } catch (Throwable e) {
                throw new CompletionException(e);
            }
        }, raftExecutor);
        long pos = posFuture.get();
        ByteBuffer buf = ByteBuffer.allocate(LogHeader.ITEM_HEADER_SIZE);
        FileUtil.syncReadFull(logFile.channel, buf, pos & fileLenMask);
        buf.flip();
        header.read(buf);
        if (!header.crcMatch()) {
            throw new ChecksumException();
        }
        if (header.index != index) {
            throw new RaftException("index not match");
        }
    }

    private LogFile findMatchLogFile(int suggestTerm, long suggestIndex, long lastIndex) {
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
            if (logFile.firstIndex > lastIndex) {
                right = mid - 1;
                continue;
            }
            if (logFile.firstIndex == suggestIndex && logFile.firstTerm == suggestTerm) {
                return logFile;
            } else if (logFile.firstIndex < suggestIndex && logFile.firstTerm <= suggestTerm) {
                if (left == right) {
                    return logFile;
                } else {
                    left = mid;
                }
            } else {
                right = mid - 1;
            }
        }
        return null;
    }

    @Override
    public long filePos(long absolutePos) {
        return (int) (absolutePos & fileLenMask);
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
