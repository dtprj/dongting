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
package com.github.dtprj.dongting.raft.file;

import com.github.dtprj.dongting.common.BitUtil;
import com.github.dtprj.dongting.common.ObjUtil;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.client.RaftException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

/**
 * @author huangli
 */
public class IdxFileQueue extends FileQueue implements IdxOps {
    private static final DtLog log = DtLogs.getLogger(IdxFileQueue.class);
    private static final int ITEM_LEN = 8;
    private static final int ITEMS_PER_FILE = 1024 * 1024;
    private static final int REMOVE_ITEMS = 512;
    private static final int MAX_CACHE_ITEMS = 16 * 1024;

    private static final int IDX_FILE_SIZE = ITEM_LEN * ITEMS_PER_FILE; //should divisible by FLUSH_ITEMS
    private static final int FILE_LEN_MASK = IDX_FILE_SIZE - 1;
    private static final int FILE_LEN_SHIFT_BITS = BitUtil.zeroCountOfBinary(IDX_FILE_SIZE);

    private static final int FLUSH_ITEMS = MAX_CACHE_ITEMS / 2;
    private static final int FLUSH_ITEMS_MAST = FLUSH_ITEMS - 1;
    private final LongLongSeqMap cache = new LongLongSeqMap();

    private long nextPersistIndex;
    private long nextIndex;
    private long firstIndex;

    private final ByteBuffer writeBuffer = ByteBuffer.allocateDirect(FLUSH_ITEMS * ITEM_LEN);
    private LogFile currentWriteFile;
    private long currentWriteFilePos;
    private CompletableFuture<Void> writeFuture;
    private long persistIndexAfterWrite;

    public IdxFileQueue(File dir, Executor ioExecutor) {
        super(dir, ioExecutor);
    }

    @Override
    protected long getFileSize() {
        return IDX_FILE_SIZE;
    }

    public void initWithCommitIndex(long commitIndex) {
        this.nextPersistIndex = commitIndex + 1;
        this.nextIndex = commitIndex + 1;
        this.firstIndex = posToIndex(queueStartPosition);
    }

    @Override
    protected long getWritePos() {
        return indexToPos(nextIndex);
    }

    @Override
    public int getFileLenShiftBits() {
        return FILE_LEN_SHIFT_BITS;
    }

    private static long indexToPos(long index) {
        // each item 8 bytes
        return index << 3;
    }

    private static long posToIndex(long pos) {
        // each item 8 bytes
        return pos >>> 3;
    }

    public long findLogPosByItemIndex(long itemIndex) throws IOException {
        checkIndex(itemIndex);
        if (itemIndex < firstIndex) {
            return -1;
        }
        long result = cache.get(itemIndex);
        if (result > 0) {
            return result;
        }
        long pos = indexToPos(itemIndex);
        long filePos = pos & FILE_LEN_MASK;
        LogFile lf = getLogFile(pos);
        // TODO reuse it
        ByteBuffer buffer = ByteBuffer.allocate(8);
        while (buffer.hasRemaining()) {
            lf.channel.read(buffer, filePos);
        }
        buffer.flip();
        return buffer.getLong();
    }

    public long truncateTail(long index) throws IOException {
        ObjUtil.checkPositive(index, "index");
        if (index < firstIndex) {
            throw new RaftException("truncateTail index is too small: " + index);
        }
        long value = findLogPosByItemIndex(index);
        cache.truncate(index);
        nextIndex = index;
        if (nextPersistIndex > index) {
            nextPersistIndex = index;
        }
        if (writeFuture != null && persistIndexAfterWrite > index) {
            writeFuture.cancel(false);
            persistIndexAfterWrite = 0;
        }
        return value;
    }

    private void checkIndex(long index) {
        if (index > nextIndex) {
            BugLog.getLog().error("index is too large : lastIndex={}, index={}", nextIndex, index);
            throw new RaftException("index is too large");
        }
    }

    @Override
    public void put(long itemIndex, long dataPosition) throws IOException {
        checkIndex(itemIndex);
        if (itemIndex < firstIndex) {
            BugLog.getLog().error("index is too small : firstIndex={}, index={}", firstIndex, itemIndex);
            throw new RaftException("index is too small");
        }
        LongLongSeqMap cache = this.cache;
        if (itemIndex < nextIndex) {
            cache.truncate(itemIndex);
        }
        if (cache.size() >= MAX_CACHE_ITEMS) {
            cache.remove(REMOVE_ITEMS);
        }
        cache.put(itemIndex, dataPosition);
        nextIndex = itemIndex + 1;
        if ((nextIndex & FLUSH_ITEMS_MAST) == 0) {
            writeAndFlush();
        }
    }

    private void writeAndFlush() {
        if (!ensureWritePosReady()) {
            return;
        }
        if (writeFuture != null) {
            try {
                writeFuture.get();
                if (persistIndexAfterWrite > 0) {
                    nextPersistIndex = persistIndexAfterWrite;
                }
            } catch (CancellationException e) {
                log.info("previous write canceled");
                // don't return
            } catch (InterruptedException e) {
                log.info("write index interrupted: {}", currentWriteFile.pathname);
                return;
            } catch (ExecutionException e) {
                log.error("write idx file failed: {}", currentWriteFile.pathname, e);
                if (e.getCause() instanceof RaftException) {
                    RaftException re = (RaftException) e.getCause();
                    if (re.getCause() instanceof IOException) {
                        retryWrite();
                    }
                }
                throw new RaftException(e);
            } finally {
                currentWriteFile = null;
                currentWriteFilePos = 0;
                persistIndexAfterWrite = 0;
                writeFuture = null;
            }
        }
        ByteBuffer writeBuffer = this.writeBuffer;
        LongLongSeqMap cache = this.cache;
        writeBuffer.clear();
        long index = nextPersistIndex;
        long startPos = indexToPos(index);
        long lastKey = cache.getLastKey();
        for (int i = 0; i < FLUSH_ITEMS && index <= lastKey; i++, index++) {
            if ((index & FLUSH_ITEMS_MAST) == 0 && i != 0) {
                // don't pass end of file or sections
                break;
            }
            long value = cache.get(index);
            writeBuffer.putLong(value);
        }
        writeBuffer.flip();

        currentWriteFile = getLogFile(startPos);
        currentWriteFilePos = startPos & FILE_LEN_MASK;
        persistIndexAfterWrite = index;
        writeBuffer.mark();
        writeFuture = submitWriteTask();
    }

    private void retryWrite() {
        while (true) {
            try {
                writeBuffer.reset();
                doWrite();
                break;
            } catch (IOException e) {
                log.error("retry write idx file failed, file={}, message: {}",
                        currentWriteFile.pathname, e.getMessage());
            }
        }
    }

    private void doWrite() throws IOException {
        FileChannel channel = currentWriteFile.channel;
        channel.position(currentWriteFilePos);
        ByteBuffer writeBuffer = this.writeBuffer;
        while (writeBuffer.hasRemaining()) {
            //noinspection ResultOfMethodCallIgnored
            channel.write(writeBuffer);
        }
        channel.force(false);
    }

    private CompletableFuture<Void> submitWriteTask() {
        return CompletableFuture.runAsync(() -> {
            try {
                doWrite();
            } catch (IOException e) {
                throw new RaftException(e);
            }
        }, ioExecutor);
    }

    public long getNextIndex() {
        return nextIndex;
    }
}
