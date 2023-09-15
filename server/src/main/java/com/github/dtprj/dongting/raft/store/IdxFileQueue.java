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
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @author huangli
 */
class IdxFileQueue extends FileQueue implements IdxOps {
    private static final DtLog log = DtLogs.getLogger(IdxFileQueue.class);
    private static final int ITEM_LEN = 8;
    static final String IDX_FILE_PERSIST_INDEX_KEY = "idxFilePersistIndex";

    public static final int DEFAULT_ITEMS_PER_FILE = 1024 * 1024;
    public static final int DEFAULT_MAX_CACHE_ITEMS = 16 * 1024;

    private final StatusManager statusManager;

    private final int maxCacheItems;

    private final int idxFileSize;
    private final int fileLenMask;
    private final int fileLenShiftBits;

    private final int flushItems;
    final LongLongSeqMap tailCache = new LongLongSeqMap(1024);
    private final Timestamp ts;
    private final RaftStatusImpl raftStatus;

    private final ByteBuffer writeBuffer;

    private long nextPersistIndex;
    private long nextIndex;
    private long firstIndex;

    private CompletableFuture<Void> writeFuture;
    private LogFile currentWriteFile;
    private long nextPersistIndexAfterWrite;
    private CompletableFuture<Void> statusFuture;

    private long lastFlushNanos;
    private static final long FLUSH_INTERVAL_NANOS = 15L * 1000 * 1000 * 1000;

    public IdxFileQueue(File dir, StatusManager statusManager, RaftGroupConfigEx groupConfig,
                        int itemsPerFile, int maxCacheItems) {
        super(dir, groupConfig);
        if (BitUtil.nextHighestPowerOfTwo(itemsPerFile) != itemsPerFile) {
            throw new IllegalArgumentException("itemsPerFile not power of 2: " + itemsPerFile);
        }
        this.statusManager = statusManager;
        this.ts = groupConfig.getTs();
        this.lastFlushNanos = ts.getNanoTime();
        this.raftStatus = (RaftStatusImpl) groupConfig.getRaftStatus();

        this.maxCacheItems = maxCacheItems;
        this.idxFileSize = ITEM_LEN * itemsPerFile;
        this.fileLenMask = idxFileSize - 1;
        this.fileLenShiftBits = BitUtil.zeroCountOfBinary(idxFileSize);
        this.flushItems = this.maxCacheItems / 2;
        this.writeBuffer = ByteBuffer.allocateDirect(flushItems * ITEM_LEN);
    }

    public Pair<Long, Long> initRestorePos() throws Exception {
        firstIndex = posToIndex(queueStartPosition);
        long restoreIndex = Long.parseLong(raftStatus.getExtraPersistProps()
                .getProperty(IDX_FILE_PERSIST_INDEX_KEY, "0"));

        log.info(IDX_FILE_PERSIST_INDEX_KEY + " in raft status file: {}", restoreIndex);
        long restoreIndexPos;
        if (restoreIndex == 0) {
            restoreIndex = 1;
            restoreIndexPos = 0;
            nextIndex = 1;
            nextPersistIndex = 1;
        } else {
            if (restoreIndex < firstIndex) {
                // truncate head may cause persistIndex < firstIndex, since we save to raft.status asynchronously.
                // however, in this case the firstIndex must have data, so we can restore from firstIndex.
                log.warn("restore index < firstIndex: {}, {}", restoreIndex, firstIndex);
                restoreIndex = firstIndex;
                nextPersistIndex = firstIndex + 1;
                nextIndex = firstIndex + 1;
            } else {
                nextPersistIndex = restoreIndex + 1;
                nextIndex = restoreIndex + 1;
            }
            restoreIndexPos = loadLogPos(restoreIndex).get();
        }
        if (queueEndPosition == 0) {
            tryAllocate();
        }
        log.info("restore index: {}, pos: {}", restoreIndex, restoreIndexPos);
        return new Pair<>(restoreIndex, restoreIndexPos);
    }

    @Override
    protected long getFileSize() {
        return idxFileSize;
    }

    @Override
    public int getFileLenShiftBits() {
        return fileLenShiftBits;
    }

    private long indexToPos(long index) {
        // each item 8 bytes
        return index << 3;
    }

    long posToIndex(long pos) {
        // each item 8 bytes
        return pos >>> 3;
    }

    @Override
    public CompletableFuture<Long> loadLogPos(long itemIndex) {
        DtUtil.checkPositive(itemIndex, "index");
        if (itemIndex >= nextIndex) {
            BugLog.getLog().error("index is too large : lastIndex={}, index={}", nextIndex, itemIndex);
            return CompletableFuture.failedFuture(new RaftException("index is too large"));
        }
        if (itemIndex < firstIndex) {
            BugLog.getLog().error("index is too small : firstIndex={}, index={}", firstIndex, itemIndex);
            return CompletableFuture.failedFuture(new RaftException("index too small"));
        }
        if (itemIndex >= tailCache.getFirstKey() && itemIndex <= tailCache.getLastKey()) {
            return CompletableFuture.completedFuture(tailCache.get(itemIndex));
        }
        long pos = indexToPos(itemIndex);
        long filePos = pos & fileLenMask;
        LogFile lf = getLogFile(pos);
        ByteBuffer buffer = ByteBuffer.allocate(8);
        AsyncIoTask t = new AsyncIoTask(lf.channel, stopIndicator, null);
        return t.read(buffer, filePos).thenApply(v -> {
            buffer.flip();
            return buffer.getLong();
        });
    }

    public void truncateTail(long index) {
        DtUtil.checkPositive(index, "index");
        if (index <= raftStatus.getCommitIndex()) {
            throw new RaftException("truncateTail index is too small: " + index);
        }
        if (index < tailCache.getFirstKey() || index > tailCache.getLastKey()) {
            throw new RaftException("truncateTail out of cache range: " + index);
        }
        tailCache.truncate(index);
        nextIndex = index;
    }

    @Override
    public void put(long itemIndex, long dataPosition, boolean recover) throws InterruptedException, IOException {
        if (itemIndex > nextIndex) {
            throw new RaftException("index not match : " + nextIndex + ", " + itemIndex);
        }
        if (!recover && itemIndex <= raftStatus.getCommitIndex()) {
            throw new RaftException("try update committed index: " + itemIndex);
        }
        if (itemIndex < nextIndex) {
            if (recover && tailCache.size() == 0) {
                // normal case
            } else {
                // last put failed
                log.info("put index!=nextIndex, truncate tailCache: {}, {}", itemIndex, nextIndex);
                tailCache.truncate(itemIndex);
            }
        }
        LongLongSeqMap tailCache = this.tailCache;
        removeHead(tailCache);
        tailCache.put(itemIndex, dataPosition);
        nextIndex = itemIndex + 1;
        if (writeFuture == null) {
            if (ts.getNanoTime() - lastFlushNanos > FLUSH_INTERVAL_NANOS) {
                writeAndFlush();
            } else {
                if (raftStatus.getCommitIndex() - nextPersistIndex + 1 >= flushItems) {
                    writeAndFlush();
                }
            }
        } else {
            if (tailCache.size() > maxCacheItems) {
                log.warn("cache size exceed {}: {}", maxCacheItems, tailCache.size());
                processWriteResult(true);
                removeHead(tailCache);
                writeAndFlush();
            }
        }
        processWriteResult(false);
    }

    private void removeHead(LongLongSeqMap tailCache) {
        while (tailCache.size() >= maxCacheItems && tailCache.getFirstKey() < nextPersistIndex) {
            tailCache.remove();
        }
    }

    private void writeAndFlush() throws InterruptedException, IOException {
        ensureWritePosReady(indexToPos(nextIndex));

        this.lastFlushNanos = ts.getNanoTime();

        ByteBuffer writeBuffer = this.writeBuffer;
        writeBuffer.clear();
        long index = nextPersistIndex;
        long startPos = indexToPos(index);
        long lastKey = Math.min(raftStatus.getCommitIndex(), tailCache.getLastKey());
        LongLongSeqMap tailCache = this.tailCache;
        for (int i = 0; i < flushItems && index <= lastKey; i++, index++) {
            if ((indexToPos(index) & fileLenMask) == 0 && i != 0) {
                // don't pass end of file
                break;
            }
            long value = tailCache.get(index);
            writeBuffer.putLong(value);
        }
        writeBuffer.flip();
        if (writeBuffer.remaining() == 0) {
            return;
        }

        nextPersistIndexAfterWrite = index;
        LogFile logFile = getLogFile(startPos);
        AsyncIoTask writeTask = new AsyncIoTask(logFile.channel, stopIndicator, null);
        currentWriteFile = logFile;
        logFile.use++;
        writeFuture = writeTask.write(false, false, writeBuffer, startPos & fileLenMask);
    }

    private void processWriteResult(boolean wait) throws InterruptedException, IOException {
        if (writeFuture == null) {
            return;
        }
        if (writeFuture.isDone() || wait) {
            try {
                RaftUtil.checkStop(stopIndicator);
                writeFuture.get();
                if (nextPersistIndexAfterWrite > nextPersistIndex && nextPersistIndexAfterWrite <= nextIndex) {
                    nextPersistIndex = nextPersistIndexAfterWrite;
                }
                long idxPersisIndex = nextPersistIndex - 1;
                raftStatus.getExtraPersistProps().setProperty(IDX_FILE_PERSIST_INDEX_KEY, String.valueOf(idxPersisIndex));
                statusFuture = statusManager.persistAsync();
            } catch (ExecutionException e) {
                throw new IOException(e);
            } finally {
                currentWriteFile.use--;
                writeFuture = null;
                currentWriteFile = null;
                nextPersistIndexAfterWrite = 0;
            }
        }
    }

    public void submitDeleteTask(long bound) {
        submitDeleteTask(logFile -> posToIndex(logFile.endPos) < bound);
    }

    @Override
    protected void afterDelete() {
        firstIndex = posToIndex(queueStartPosition);
    }

    public long getNextIndex() {
        return nextIndex;
    }

    public long getNextPersistIndex() {
        return nextPersistIndex;
    }

    public long getFirstIndex() {
        return firstIndex;
    }

    @Override
    public void close() {
        try {
            processWriteResult(true);
            writeAndFlush();
            processWriteResult(true);
            if (statusFuture != null) {
                statusFuture.get();
            }
        } catch (Exception e) {
            throw new RaftException(e);
        }
        super.close();
    }
}
