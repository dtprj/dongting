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

import com.github.dtprj.dongting.common.BitUtil;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.FileUtil;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

/**
 * @author huangli
 */
class IdxFileQueue extends FileQueue implements IdxOps {
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
    private final LongLongSeqMap tailCache = new LongLongSeqMap();

    private long nextPersistIndex;
    private long nextIndex;
    private long firstIndex;

    private final ByteBuffer writeBuffer = ByteBuffer.allocateDirect(FLUSH_ITEMS * ITEM_LEN);

    private CompletableFuture<Void> writeFuture;
    private AsyncIoTask writeTask;
    private LogFile currentWriteFile;
    private long nextPersistIndexAfterWrite;

    public IdxFileQueue(File dir, ExecutorService ioExecutor, RaftGroupConfigEx groupConfig) {
        super(dir, ioExecutor, groupConfig);
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

    public long findLogPosInMemCache(long itemIndex) {
        checkIndex(itemIndex, true);
        if (itemIndex < firstIndex) {
            throw new RaftException("index too small:" + itemIndex);
        }
        long result = tailCache.get(itemIndex);
        if (result > 0) {
            return result;
        }
        return -1;
    }

    @Override
    public long syncLoadLogPos(long itemIndex) throws IOException {
        checkIndex(itemIndex, true);
        if (itemIndex < firstIndex) {
            throw new RaftException("index too small:" + itemIndex);
        }
        long pos = indexToPos(itemIndex);
        long filePos = pos & FILE_LEN_MASK;
        LogFile lf = getLogFile(pos);
        ByteBuffer buffer = ByteBuffer.allocate(8);
        FileUtil.syncReadFull(lf.channel, buffer, filePos);
        buffer.flip();
        return buffer.getLong();
    }

    public long truncateTail(long index) throws Exception {
        DtUtil.checkPositive(index, "index");
        if (index < firstIndex) {
            throw new RaftException("truncateTail index is too small: " + index);
        }
        long value = findLogPosInMemCache(index);
        if (value < 0) {
            value = syncLoadLogPos(index);
        }
        tailCache.truncate(index);
        nextIndex = index;
        if (nextPersistIndex > index) {
            nextPersistIndex = index;
        }
        if (writeTask != null && nextPersistIndexAfterWrite > index) {
            writeFuture.cancel(false);
            cleanWriteState();
        }
        return value;
    }

    private void checkIndex(long index, boolean read) {
        DtUtil.checkPositive(index, "index");
        boolean ok = read ? index < nextIndex : index <= nextIndex;
        if (!ok) {
            BugLog.getLog().error("index is too large : lastIndex={}, index={}", nextIndex, index);
            throw new RaftException("index is too large");
        }
    }

    @Override
    public void put(long itemIndex, long dataPosition) throws InterruptedException, IOException {
        checkIndex(itemIndex, false);
        if (itemIndex < firstIndex) {
            BugLog.getLog().error("index is too small : firstIndex={}, index={}", firstIndex, itemIndex);
            throw new RaftException("index is too small");
        }
        LongLongSeqMap cache = this.tailCache;
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

    private void writeAndFlush() throws InterruptedException, IOException {
        ensureWritePosReady(indexToPos(nextIndex));
        if (!ensureLastWriteFinish()) {
            return;
        }
        ByteBuffer writeBuffer = this.writeBuffer;
        LongLongSeqMap cache = this.tailCache;
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


        nextPersistIndexAfterWrite = index;
        LogFile logFile = getLogFile(startPos);
        writeTask = new AsyncIoTask(logFile.channel, stopIndicator);
        currentWriteFile = logFile;
        logFile.use++;
        writeFuture = writeTask.write(false, false, writeBuffer, startPos & FILE_LEN_MASK);
    }

    private void cleanWriteState() {
        writeFuture = null;
        writeTask = null;
        currentWriteFile = null;
        nextPersistIndexAfterWrite = 0;
    }

    private boolean ensureLastWriteFinish() throws InterruptedException, IOException {
        if (writeTask == null) {
            return true;
        }
        try {
            if (stopIndicator.get()) {
                return false;
            }
            writeFuture.get();
            if (nextPersistIndexAfterWrite > nextPersistIndex && nextPersistIndexAfterWrite <= nextIndex) {
                nextPersistIndex = nextPersistIndexAfterWrite;
            }
            return true;
        } catch (CancellationException e) {
            log.info("previous write canceled");
            return !stopIndicator.get();
        } catch (ExecutionException e){
            throw new IOException(e);
        } finally {
            cleanWriteState();
            currentWriteFile.use--;
        }
    }

    public void submitDeleteTask(long firstIndex) {
        submitDeleteTask(logFile -> posToIndex(logFile.endPos) < firstIndex);
    }

    public long getNextIndex() {
        return nextIndex;
    }
}
