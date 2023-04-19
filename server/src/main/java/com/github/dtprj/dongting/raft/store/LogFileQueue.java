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
import com.github.dtprj.dongting.buf.RefByteBuffer;
import com.github.dtprj.dongting.common.BitUtil;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.client.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftExecutor;
import com.github.dtprj.dongting.raft.server.LogItem;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
public class LogFileQueue extends FileQueue {
    private static final DtLog log = DtLogs.getLogger(LogFileQueue.class);

    private static final int FILE_MAGIC = 0x7C3FA7B6;
    private static final short MAJOR_VERSION = 1;
    private static final short MINOR_VERSION = 0;

    static final int LOG_FILE_SIZE = 1024 * 1024 * 1024;
    static final int FILE_LEN_MASK = LOG_FILE_SIZE - 1;
    private static final int FILE_LEN_SHIFT_BITS = BitUtil.zeroCountOfBinary(LOG_FILE_SIZE);

    private final IdxOps idxOps;

    // crc32c 4 bytes
    // total len 4 bytes
    // head len 2 bytes
    // context len 4 bytes
    // type 1 byte
    // term 4 bytes
    // prevLogTerm 4 bytes
    // index 8 bytes
    static final short ITEM_HEADER_SIZE = 4 + 4 + 2 + 4 + 1 + 4 + 4 + 8;

    private final ByteBuffer buffer = ByteBuffer.allocateDirect(128 * 1024);
    private final CRC32C crc32c = new CRC32C();
    private long writePos;

    public LogFileQueue(File dir, Executor ioExecutor, RaftExecutor raftExecutor, Supplier<Boolean> stopIndicator,
                        IdxOps idxOps, ByteBufferPool heapPool, ByteBufferPool directPool) {
        super(dir, ioExecutor, raftExecutor, stopIndicator, heapPool, directPool);
        this.idxOps = idxOps;
    }

    @Override
    protected long getFileSize() {
        return LOG_FILE_SIZE;
    }

    @Override
    public int getFileLenShiftBits() {
        return FILE_LEN_SHIFT_BITS;
    }

    @Override
    protected long getWritePos() {
        return writePos;
    }

    @Override
    protected void afterFileAllocated(File f, AsynchronousFileChannel channel) throws Exception {
        ByteBuffer header = ByteBuffer.allocateDirect(8);
        header.putInt(FILE_MAGIC);
        header.putShort(MAJOR_VERSION);
        header.putShort(MINOR_VERSION);
        header.flip();
        int x = header.remaining();
        while (x > 0) {
            Future<Integer> future = channel.write(header, 0);
            x -= future.get();
        }
        channel.force(false);
    }

    public int restore(long commitIndex, long commitIndexPos) throws IOException {
        log.info("restore from {}, {}", commitIndex, commitIndexPos);
        Restorer restorer = new Restorer(idxOps, commitIndex, commitIndexPos);
        for (int i = 0; i < queue.size(); i++) {
            LogFile lf = queue.get(i);
            if (commitIndexPos < lf.endPos) {
                long pos = restorer.restoreFile(this.buffer, lf);
                writePos = lf.startPos + pos;
            } else {
                writePos = lf.endPos;
            }
        }
        if (queue.size() > 0) {
            if (commitIndexPos >= queue.get(queue.size() - 1).endPos) {
                throw new RaftException("commitIndexPos is illegal. " + commitIndexPos);
            }
            log.info("restore finished. lastTerm={}, lastIndex={}, lastPos={}, lastFile={}",
                    restorer.previousTerm, restorer.previousIndex, writePos, queue.get(queue.size() - 1).pathname);
        }
        return restorer.previousTerm;
    }

    static void updateCrc(CRC32C crc32c, ByteBuffer buf, int startPos, int len) {
        int oldPos = buf.position();
        int oldLimit = buf.limit();
        buf.limit(startPos + len);
        buf.position(startPos);
        crc32c.update(buf);
        buf.limit(oldLimit);
        buf.position(oldPos);
    }

    public void append(List<LogItem> logs) throws IOException {
        ensureWritePosReady();
        ByteBuffer buffer = this.buffer;
        buffer.clear();
        long pos = writePos;
        LogFile file = getLogFile(pos);
        for (LogItem log : logs) {
            ByteBuffer dataBuffer = log.getBuffer().getBuffer();
            long posOfFile = (pos + buffer.position()) & FILE_LEN_MASK;
            // if posOfFile == 0, it means last item exactly fill the file
            if (posOfFile == 0 || LOG_FILE_SIZE - posOfFile < ITEM_HEADER_SIZE + dataBuffer.remaining()) {
                pos = writeAndClearBuffer(buffer, file, pos);
                if (posOfFile != 0) {
                    // roll to next file
                    pos = ((pos >>> FILE_LEN_SHIFT_BITS) + 1) << FILE_LEN_SHIFT_BITS;
                }
                ensureWritePosReady(pos);
                file = getLogFile(pos);
            }
            if (buffer.remaining() < ITEM_HEADER_SIZE) {
                pos = writeAndClearBuffer(buffer, file, pos);
            }

            long startPos = pos;
            int totalLen = dataBuffer.remaining() + ITEM_HEADER_SIZE;
            writeHeader(buffer, dataBuffer, log, totalLen);

            while (dataBuffer.hasRemaining()) {
                buffer.put(dataBuffer);
                if (!buffer.hasRemaining()) {
                    pos = writeAndClearBuffer(buffer, file, pos);
                }
            }
            idxOps.put(log.getIndex(), startPos, totalLen);
        }
        pos = writeAndClearBuffer(buffer, file, pos);
        file.channel.force(false);
        this.writePos = pos;
    }

    private void writeHeader(ByteBuffer buffer, ByteBuffer dataBuffer, LogItem log, int totalLen) {
        // crc32c 4 bytes
        // total len 4 bytes
        // head len 2 bytes
        // context len 4 bytes
        // type 1 byte
        // term 4 bytes
        // prevLogTerm 4 bytes
        // index 8 bytes
        int crcPos = buffer.position();
        buffer.putInt(0);
        buffer.putInt(totalLen);
        buffer.putShort(ITEM_HEADER_SIZE);
        // TODO support context
        buffer.putInt(0);
        buffer.put((byte) log.getType());
        buffer.putInt(log.getTerm());
        buffer.putInt(log.getPrevLogTerm());
        buffer.putLong(log.getIndex());

        CRC32C crc32c = this.crc32c;
        crc32c.reset();

        updateCrc(crc32c, buffer, crcPos + 4, totalLen - 4);

        // backup position, the data buffer is a read-only buffer, so we don't need to change its limit
        int pos = dataBuffer.position();
        crc32c.update(dataBuffer);
        dataBuffer.position(pos);
        buffer.putInt(crcPos, (int) crc32c.getValue());
    }

    private long writeAndClearBuffer(ByteBuffer buffer, LogFile file, long filePos) throws IOException {
        if (buffer.position() == 0) {
            return filePos;
        }
        buffer.flip();
        filePos = FileUtil.syncWriteFull(file.channel, buffer, filePos);
        buffer.clear();
        return filePos;
    }

    public void truncateTail(long dataPosition) {
        DtUtil.checkNotNegative(dataPosition, "dataPosition");
        writePos = dataPosition;
    }

    public CompletableFuture<List<LogItem>> loadLog(long pos, DefaultLogIterator it, int limit, int bytesLimit) {
        if (pos < queueStartPosition) {
            throw new RaftException("pos too small: " + pos);
        }
        if (pos >= writePos) {
            throw new RaftException("pos too large: " + pos);
        }
        int rest = (int) (writePos - pos);
        if (rest <= 0) {
            BugLog.getLog().error("rest is illegal. pos={}, writePos={}", pos, writePos);
            throw new RaftException("rest is illegal.");
        }
        ByteBuffer buf = it.buffer;
        List<LogItem> result = new ArrayList<>();
        it.resetBeforeLoad();
        if (buf.hasRemaining()) {
            if (buf.remaining() > rest) {
                buf.limit(buf.position() + rest);
            }
            int oldRemaining = buf.remaining();
            if (extractItems(it, result, limit, bytesLimit)) {
                return CompletableFuture.completedFuture(result);
            } else {
                pos += oldRemaining - buf.remaining();
                prepareNextRead(buf);
                CompletableFuture<List<LogItem>> future = new CompletableFuture<>();
                loadLogFromStore(pos, it, limit, bytesLimit, result, future);
                return future;
            }
        } else {
            buf.clear();
            CompletableFuture<List<LogItem>> future = new CompletableFuture<>();
            loadLogFromStore(pos, it, limit, bytesLimit, result, future);
            return future;
        }
    }

    private void prepareNextRead(ByteBuffer buf) {
        if (buf.hasRemaining()) {
            ByteBuffer temp = buffer.slice();
            buffer.clear();
            buffer.put(temp);
        } else {
            buf.clear();
        }
    }

    private void loadLogFromStore(long pos, DefaultLogIterator it, int limit, int bytesLimit,
                                  List<LogItem> result, CompletableFuture<List<LogItem>> future) {
        int rest = (int) (writePos - pos);
        if (rest <= 0) {
            log.warn("rest is illegal. pos={}, writePos={}", pos, writePos);
            future.completeExceptionally(new RaftException("rest is illegal."));
            return;
        }
        LogFile logFile = getLogFile(pos);
        int fileStartPos = (int) (pos & FILE_LEN_MASK);
        rest = Math.min(rest, LOG_FILE_SIZE - fileStartPos);
        ByteBuffer buf = it.buffer;
        if (rest < buf.remaining()) {
            buf.limit(buf.position() + rest);
        }
        long newReadPos = pos + buf.remaining();
        AsyncReadTask t = new AsyncReadTask(buf, fileStartPos, logFile.channel, it.stopIndicator);
        t.exec().whenCompleteAsync((v, ex) -> resumeAfterLoad(newReadPos, it, limit, bytesLimit,
                result, future, ex), raftExecutor);
    }

    private void resumeAfterLoad(long newReadPos, DefaultLogIterator it, int limit, int bytesLimit,
                                 List<LogItem> result, CompletableFuture<List<LogItem>> future, Throwable ex) {
        try {
            if (it.stopIndicator.get()) {
                future.cancel(false);
            } else if (ex != null) {
                future.completeExceptionally(ex);
            } else {
                ByteBuffer buf = it.buffer;
                buf.flip();
                if (extractItems(it, result, limit, bytesLimit)) {
                    future.complete(result);
                    it.nextIndex = it.nextIndex + result.size();
                } else {
                    prepareNextRead(buf);
                    loadLogFromStore(newReadPos, it, limit, bytesLimit, result, future);
                }
            }
        } catch (Throwable e) {
            future.completeExceptionally(e);
        }
    }

    private boolean extractItems(DefaultLogIterator it, List<LogItem> result, int limit, int bytesLimit) {
        ByteBuffer buf = it.buffer;

        while (buf.remaining() > ITEM_HEADER_SIZE) {
            if (it.item == null) {
                LogItem li = new LogItem();
                it.item = li;

                // crc32c 4 bytes
                // total len 4 bytes
                // head len 2 bytes
                // context len 4 bytes
                // type 1 byte
                // term 4 bytes
                // prevLogTerm 4 bytes
                // index 8 bytes
                int startPos = buf.position();
                it.crc = buf.getInt();
                int totalLen = buf.getInt();
                short headLen = buf.getShort();
                int contextLen = buf.getInt();
                li.setType(buf.get());
                li.setTerm(buf.getInt());
                li.setPrevLogTerm(buf.getInt());
                li.setIndex(buf.getLong());

                it.payLoad = totalLen - headLen;
                int bodyLen = totalLen - contextLen - headLen;
                if (totalLen <= 0 || contextLen < 0 || headLen < 0 || it.payLoad < 0
                        || it.payLoad < 0 || bodyLen < 0 || it.payLoad > LOG_FILE_SIZE) {
                    throw new RaftException("invalid log item length: " + totalLen + "," + contextLen + "," + headLen);
                }

                if (result.size() > 0 && it.bytes + it.payLoad > bytesLimit) {
                    // rollback position for next use
                    buf.position(startPos);
                    return true;
                }


                if (buf.remaining() >= it.payLoad) {
                    updateCrc(it.crc32c, buf, startPos + 4, totalLen - 4);
                    if (it.crc32c.getValue() != it.crc) {
                        throw new RaftException("crc32c not match");
                    }
                    RefByteBuffer rbb = RefByteBuffer.create(heapPool, bodyLen, 800);
                    li.setBuffer(rbb);
                    ByteBuffer destBuf = rbb.getBuffer();
                    buf.get(destBuf.array(), 0, bodyLen);
                    destBuf.limit(bodyLen);
                    result.add(li);
                    it.bytes += it.payLoad;
                    it.resetItem();
                    if (result.size() >= limit) {
                        return true;
                    }
                } else {
                    updateCrc(it.crc32c, buf, startPos + 4, buf.limit());
                    RefByteBuffer rbb = RefByteBuffer.create(heapPool, bodyLen, 800);
                    li.setBuffer(rbb);
                    rbb.getBuffer().put(buf);
                }
            } else {
                ByteBuffer destBuf = it.item.getBuffer().getBuffer();
                int read = destBuf.position();
                int restBytes = it.payLoad - read;
                if (buf.remaining() >= restBytes) {
                    updateCrc(it.crc32c, buf, buf.position(), restBytes);
                    if (it.crc32c.getValue() != it.crc) {
                        throw new RaftException("crc32c not match");
                    }
                    buf.get(destBuf.array(), read, it.payLoad - read);
                    destBuf.limit(it.payLoad);
                    destBuf.position(0);
                    result.add(it.item);
                    it.bytes += it.payLoad;
                    it.resetItem();
                    if (result.size() >= limit) {
                        return true;
                    }
                } else {
                    updateCrc(it.crc32c, buf, buf.position(), buf.limit());
                    destBuf.put(buf);
                }
            }
        }
        return false;
    }


}
