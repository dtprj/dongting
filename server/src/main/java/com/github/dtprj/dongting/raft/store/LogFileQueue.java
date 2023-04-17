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
import com.github.dtprj.dongting.common.BitUtil;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.client.RaftException;
import com.github.dtprj.dongting.raft.server.LogItem;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
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

    static final int FILE_HEADER_SIZE = 4096;

    private final IdxOps idxOps;

    // crc32c 4 bytes
    // total len 4 bytes
    // head len 2 bytes
    // type 1 byte
    // term 4 bytes
    // prevLogTerm 4 bytes
    // index 8 bytes
    static final short ITEM_HEADER_SIZE = 4 + 4 + 2 + 1 + 4 + 4 + 8;

    private final ByteBuffer buffer = ByteBuffer.allocateDirect(128 * 1024);
    private final CRC32C crc32c = new CRC32C();
    private long writePos;

    public LogFileQueue(File dir, Executor ioExecutor, IdxOps idxOps, ByteBufferPool heapPool, ByteBufferPool directPool) {
        super(dir, ioExecutor, heapPool, directPool);
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
        ByteBuffer fileHeaderBuffer = ByteBuffer.allocate(FILE_HEADER_SIZE);
        log.info("restore from {}, {}", commitIndex, commitIndexPos);
        Restorer restorer = new Restorer(idxOps, commitIndex, commitIndexPos);
        for (int i = 0; i < queue.size(); i++) {
            LogFile lf = queue.get(i);
            AsynchronousFileChannel channel = lf.channel;
            fileHeaderBuffer.clear();
            FileUtil.syncReadFull(channel, fileHeaderBuffer, 0);
            fileHeaderBuffer.flip();
            if (fileHeaderBuffer.getInt() != FILE_MAGIC) {
                if (commitIndexPos < lf.startPos) {
                    log.warn("file {} magic is illegal. ignore it.", lf.pathname, fileHeaderBuffer.getInt());
                    break;
                } else {
                    throw new RaftException("file magic is illegal " + lf.pathname);
                }
            }
            short majorVersion = fileHeaderBuffer.getShort();
            if (majorVersion > MAJOR_VERSION) {
                log.error("unsupported major version: {}, {}", majorVersion, lf.pathname);
                throw new RaftException("unsupported major version: " + majorVersion);
            }
            if (commitIndexPos < lf.endPos) {
                long pos = restorer.restoreFile(this.buffer, lf);
                if (pos <= FILE_HEADER_SIZE) {
                    break;
                } else {
                    writePos = lf.startPos + pos;
                }
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
        if (writePos == 0) {
            writePos = FILE_HEADER_SIZE;
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
                pos += FILE_HEADER_SIZE;
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
        // type 1 byte
        // term 4 bytes
        // prevLogTerm 4 bytes
        // index 8 bytes
        int crcPos = buffer.position();
        buffer.putInt(0);
        buffer.putInt(totalLen);
        buffer.putShort(ITEM_HEADER_SIZE);
        buffer.put((byte) log.getType());
        buffer.putInt(log.getTerm());
        buffer.putInt(log.getPrevLogTerm());
        buffer.putLong(log.getIndex());

        CRC32C crc32c = this.crc32c;
        crc32c.reset();

        // backup position and limit
        int pos = buffer.position();
        int limit = buffer.limit();

        buffer.position(crcPos + 4);
        buffer.limit(pos);
        crc32c.update(buffer);

        // restore position and limit
        buffer.limit(limit);
        buffer.position(pos);

        // backup position, the data buffer is a read-only buffer, so we don't need to change its limit
        pos = dataBuffer.position();
        crc32c.update(dataBuffer);
        dataBuffer.position(pos);
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

    public void loadLog(long[] pos, int[] size, CompletableFuture<LogItem[]> result) {
        int totalSize = 0;
        for (int i = 0; i < pos.length; i++) {
            totalSize += size[i];
        }

    }
}
