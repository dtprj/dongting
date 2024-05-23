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
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FrameCall;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;

import java.io.File;
import java.nio.ByteBuffer;

/**
 * @author huangli
 */
class IdxFileQueue extends FileQueue implements IdxOps {
    private static final DtLog log = DtLogs.getLogger(IdxFileQueue.class);
    private static final int ITEM_LEN = 8;
    static final String KEY_PERSIST_IDX_INDEX = "persistIdxIndex";
    static final String KEY_NEXT_IDX_AFTER_INSTALL_SNAPSHOT = "nextIdxAfterInstallSnapshot";
    static final String KEY_NEXT_POS_AFTER_INSTALL_SNAPSHOT = "nextPosAfterInstallSnapshot";

    public static final int DEFAULT_ITEMS_PER_FILE = 1024 * 1024;
    public static final int MAX_BATCH_ITEMS = 16 * 1024;

    private final StatusManager statusManager;

    private final int maxCacheItems;

    private final int flushThreshold;
    final LongLongSeqMap cache;
    private final Timestamp ts;
    private final RaftStatusImpl raftStatus;

    private long persistedIndexInStatusFile;
    private long nextPersistIndex;
    private long nextIndex;
    private long firstIndex;

    private long lastFlushNanos;
    private static final long FLUSH_INTERVAL_NANOS = 15L * 1000 * 1000 * 1000;

    private long lastUpdateStatusNanos;

    private final Fiber flushFiber;
    private final FiberCondition needFlushCondition;
    private final FiberCondition flushDoneCondition;

    private boolean closed;

    public IdxFileQueue(File dir, StatusManager statusManager, RaftGroupConfigEx groupConfig, int itemsPerFile) {
        super(dir, groupConfig, (long) ITEM_LEN * itemsPerFile, false);
        if (BitUtil.nextHighestPowerOfTwo(itemsPerFile) != itemsPerFile) {
            throw new IllegalArgumentException("itemsPerFile not power of 2: " + itemsPerFile);
        }
        this.statusManager = statusManager;
        this.ts = groupConfig.getTs();
        this.lastFlushNanos = ts.getNanoTime();
        this.lastUpdateStatusNanos = ts.getNanoTime();
        this.raftStatus = (RaftStatusImpl) groupConfig.getRaftStatus();

        this.maxCacheItems = groupConfig.getIdxCacheSize();
        this.flushThreshold = groupConfig.getIdxFlushThreshold();
        this.cache = new LongLongSeqMap(maxCacheItems);
        this.flushFiber = new Fiber("idxFlush-" + groupConfig.getGroupId(),
                groupConfig.getFiberGroup(), new FlushLoopFrame());
        this.needFlushCondition = groupConfig.getFiberGroup().newCondition("IdxNeedFlush-" + groupConfig.getGroupId());
        this.flushDoneCondition = groupConfig.getFiberGroup().newCondition("IdxFlushDone-" + groupConfig.getGroupId());
    }

    public FiberFrame<Pair<Long, Long>> initRestorePos() throws Exception {
        super.initQueue();
        this.firstIndex = posToIndex(queueStartPosition);
        long firstValidIndex = Long.parseLong(statusManager.getProperties()
                .getProperty(KEY_NEXT_IDX_AFTER_INSTALL_SNAPSHOT, "0"));
        this.persistedIndexInStatusFile = Long.parseLong(statusManager.getProperties()
                .getProperty(KEY_PERSIST_IDX_INDEX, "0"));
        long restoreIndex = persistedIndexInStatusFile;

        log.info("load raft status file. firstIndex={}, {}={}, {}={}", firstIndex, KEY_PERSIST_IDX_INDEX, restoreIndex,
                KEY_NEXT_IDX_AFTER_INSTALL_SNAPSHOT, firstValidIndex);
        restoreIndex = Math.max(restoreIndex, firstValidIndex);
        restoreIndex = Math.max(restoreIndex, firstIndex);

        if (restoreIndex == 0) {
            restoreIndex = 1;
            long restoreStartPos = 0;
            nextIndex = 1;
            nextPersistIndex = 1;

            if (queueEndPosition == 0) {
                tryAllocateAsync(0);
            }
            log.info("restore from index: {}, pos: {}", restoreIndex, restoreStartPos);
            flushFiber.start();
            return FiberFrame.completedFrame(new Pair<>(restoreIndex, restoreStartPos));
        } else {
            nextIndex = restoreIndex + 1;
            nextPersistIndex = restoreIndex + 1;
            final long finalRestoreIndex = restoreIndex;
            return new FiberFrame<>() {
                @Override
                public FrameCallResult execute(Void input) {
                    return loadLogPos(finalRestoreIndex, this::afterLoad);
                }

                private FrameCallResult afterLoad(Long restoreIndexPos) {
                    if (queueEndPosition == 0) {
                        tryAllocateAsync(0);
                    }
                    log.info("restore from index: {}, pos: {}", finalRestoreIndex, restoreIndexPos);
                    setResult(new Pair<>(finalRestoreIndex, restoreIndexPos));
                    flushFiber.start();
                    return Fiber.frameReturn();
                }

                @Override
                protected FrameCallResult handle(Throwable ex) throws Throwable {
                    if (finalRestoreIndex == firstValidIndex) {
                        // next index not write after install snapshot
                        // return null will cause install snapshot
                        log.warn("load log pos failed", ex);
                        setResult(null);
                        return Fiber.frameReturn();
                    }
                    throw ex;
                }
            };
        }
    }

    public long indexToPos(long index) {
        // each item 8 bytes
        return index << 3;
    }

    public long posToIndex(long pos) {
        // each item 8 bytes
        return pos >>> 3;
    }

    @Override
    public void put(long itemIndex, long dataPosition) {
        if (itemIndex > nextIndex) {
            throw new RaftException("index not match : " + nextIndex + ", " + itemIndex);
        }
        if (initialized && itemIndex <= raftStatus.getCommitIndex()) {
            throw new RaftException("try update committed index: " + itemIndex);
        }
        if (itemIndex < nextIndex) {
            if (initialized || itemIndex != nextIndex - 1) {
                throw new RaftException("put index!=nextIndex " + itemIndex + ", " + nextIndex);
            }
        }
        cache.put(itemIndex, dataPosition);
        nextIndex = itemIndex + 1;
        tryAllocateAsync(indexToPos(nextIndex));
        if (shouldFlush()) {
            needFlushCondition.signal();
        }
    }

    @Override
    public boolean needWaitFlush() {
        removeHead();
        if (cache.size() > maxCacheItems && shouldFlush()) {
            long first = cache.getFirstKey();
            long last = cache.getLastKey();
            log.warn("group {} cache size exceed {}({}), may cause block. cache from {} to {}, commitIndex={}(diff={}), " +
                            "lastWriteIndex={}(diff={}), lastForceIndex={}(diff={}), ",
                    raftStatus.getGroupId(), maxCacheItems, cache.size(), first, last,
                    raftStatus.getCommitIndex(), (last - raftStatus.getCommitIndex()),
                    raftStatus.getLastWriteLogIndex(), (last - raftStatus.getLastWriteLogIndex()),
                    raftStatus.getLastForceLogIndex(), (last - raftStatus.getLastForceLogIndex()));
            return true;
        }
        return false;
    }

    @Override
    public FiberFrame<Void> waitFlush() {
        // block until flush done
        return new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                removeHead();
                if (shouldFlush()) {
                    needFlushCondition.signal();
                    if (cache.size() > maxCacheItems) {
                        return flushDoneCondition.await(1000, this);
                    }
                }
                return Fiber.frameReturn();
            }
        };
    }

    private long getFlushDiff() {
        // in recovery, the commit index may be larger than last key
        long lastNeedFlushItem = Math.min(cache.getLastKey(), raftStatus.getCommitIndex());
        return lastNeedFlushItem - nextPersistIndex + 1;
    }

    private boolean shouldFlush() {
        boolean timeout = ts.getNanoTime() - lastFlushNanos > FLUSH_INTERVAL_NANOS;
        long diff = getFlushDiff();
        return (diff >= flushThreshold || (timeout && diff > 0)) && !raftStatus.isInstallSnapshot();
    }

    private void removeHead() {
        LongLongSeqMap cache = this.cache;
        long maxCacheItems = this.maxCacheItems;
        long nextPersistIndex = this.nextPersistIndex;
        // notice: we are not write no-commited logs to the idx file
        while (cache.size() >= maxCacheItems && cache.getFirstKey() < nextPersistIndex) {
            cache.remove();
        }
    }

    private class FlushLoopFrame extends FiberFrame<Void> {

        private boolean lastFlushTriggered;

        @Override
        public FrameCallResult execute(Void input) {
            if (closed) {
                if (lastFlushTriggered) {
                    log.info("idx flush fiber exit, groupId={}", groupConfig.getGroupId());
                    return Fiber.frameReturn();
                } else {
                    lastFlushTriggered = true;
                }
            } else {
                if (!shouldFlush()) {
                    return needFlushCondition.await(this);
                }
            }

            lastFlushNanos = ts.getNanoTime();
            return Fiber.call(ensureWritePosReady(indexToPos(nextPersistIndex)), this::afterPosReady);
        }

        private FrameCallResult afterPosReady(Void v) {
            if (raftStatus.isInstallSnapshot()) {
                return needFlushCondition.await(1000, this);
            }
            LogFile logFile = getLogFile(indexToPos(nextPersistIndex));
            if (logFile.shouldDelete()) {
                BugLog.getLog().error("idx file deleted, flush fail: {}", logFile.getFile().getPath());
                throw Fiber.fatal(new RaftException("idx file deleted, flush fail"));
            }
            return Fiber.call(new FlushFrame(logFile), this);
        }

        @Override
        protected FrameCallResult handle(Throwable ex) {
            throw Fiber.fatal(ex);
        }
    }

    private class FlushFrame extends FiberFrame<Void> {

        private final LogFile logFile;
        private ByteBuffer buf;

        public FlushFrame(LogFile logFile) {
            this.logFile = logFile;
        }

        @Override
        public FrameCallResult execute(Void v) {
            AsyncIoTask currentWriteTask = new AsyncIoTask(groupConfig, logFile, true, true);
            if (getFlushDiff() <= 0) {
                if (closed) {
                    return Fiber.call(currentWriteTask.lockForce(false),
                            noUseVoid -> afterFlush(nextPersistIndex, true));
                } else {
                    BugLog.getLog().error("no data to flush");
                    return Fiber.frameReturn();
                }
            }
            boolean endOfFile = prepareBuffer(logFile);

            long startIndex = nextPersistIndex;
            long index = startIndex;
            //noinspection UnnecessaryLocalVariable
            LongLongSeqMap c = cache;
            ByteBuffer buf = this.buf;
            while (buf.hasRemaining()) {
                long value = c.get(index++);
                buf.putLong(value);
            }
            buf.flip();

            long nextPersistIndexAfterWrite = index;

            long filePos = indexToPos(startIndex) & fileLenMask;
            FiberFrame<Void> f;
            boolean updateStatusFile;
            if (endOfFile || closed || ts.getNanoTime() - lastUpdateStatusNanos > FLUSH_INTERVAL_NANOS) {
                f = currentWriteTask.lockWriteAndForce(buf, filePos, false);
                updateStatusFile = true;
            } else {
                f = currentWriteTask.lockWrite(buf, filePos);
                updateStatusFile = false;
            }
            return Fiber.call(f, notUsedVoid -> afterFlush(nextPersistIndexAfterWrite, updateStatusFile));
        }

        private FrameCallResult afterFlush(long nextPersistIndexAfterWrite, boolean updateStatusFile) {
            removeHead();
            nextPersistIndex = nextPersistIndexAfterWrite;
            if (updateStatusFile) {
                // if we set syncForce to false, nextPersistIndex - 1 (committed) may less than lastForceLogIndex
                long persistedIndex = Math.min(nextPersistIndex - 1, raftStatus.getLastForceLogIndex());
                if (persistedIndex > persistedIndexInStatusFile) {
                    lastUpdateStatusNanos = ts.getNanoTime();
                    statusManager.getProperties().setProperty(KEY_PERSIST_IDX_INDEX, String.valueOf(persistedIndex));
                    statusManager.persistAsync(true);
                    if (closed) {
                        return statusManager.waitUpdateFinish(this::justReturn);
                    }
                }
            }
            return Fiber.frameReturn();
        }

        // return true if return buffer is reached end of file
        private boolean prepareBuffer(LogFile logFile) {
            long startIdx = nextPersistIndex;
            long startIdxPos = indexToPos(startIdx);
            long lastIdx = Math.min(raftStatus.getCommitIndex(), cache.getLastKey());
            if (lastIdx - startIdx > MAX_BATCH_ITEMS) {
                lastIdx = startIdx + MAX_BATCH_ITEMS;
            }
            long lastIdxPos = indexToPos(lastIdx);
            long fileStartPos1 = startIdxPos & ~fileLenMask;
            long fileStartPos2 = lastIdxPos & ~fileLenMask;
            int len;
            if (fileStartPos1 == fileStartPos2) {
                len = (int) (lastIdxPos - startIdxPos + ITEM_LEN);
            } else {
                // don't cross file
                len = (int) (logFile.endPos - startIdxPos);
            }
            buf = groupConfig.getDirectPool().borrow(len);
            buf.limit(len);
            return logFile.endPos - startIdxPos == len;
        }

        @Override
        protected FrameCallResult doFinally() {
            if (buf != null) {
                groupConfig.getDirectPool().release(buf);
            }
            flushDoneCondition.signalAll();
            return Fiber.frameReturn();
        }
    }

    public long loadLogPosInCache(long index) {
        return cache.get(index);
    }

    @Override
    public FrameCallResult loadLogPos(long itemIndex, FrameCall<Long> resumePoint) {
        DtUtil.checkPositive(itemIndex, "index");
        if (itemIndex >= nextIndex) {
            BugLog.getLog().error("index is too large : lastIndex={}, index={}", nextIndex, itemIndex);
            throw new RaftException("index is too large");
        }
        if (itemIndex < firstIndex) {
            BugLog.getLog().error("index is too small : firstIndex={}, index={}", firstIndex, itemIndex);
            throw new RaftException("index too small");
        }
        if (itemIndex >= cache.getFirstKey() && itemIndex <= cache.getLastKey()) {
            long result = cache.get(itemIndex);
            return Fiber.resume(result, resumePoint);
        }
        long pos = indexToPos(itemIndex);
        ByteBuffer buffer = ByteBuffer.allocate(8);
        LogFile lf = getLogFile(pos);
        if (lf.isDeleted()) {
            throw new RaftException("file deleted: " + lf.getFile().getPath());
        }
        FiberFrame<Long> loadFrame = new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void v) {
                long filePos = pos & fileLenMask;
                AsyncIoTask t = new AsyncIoTask(groupConfig, lf);
                return Fiber.call(t.lockRead(buffer, filePos), this::afterLoad);
            }

            private FrameCallResult afterLoad(Void unused) {
                buffer.flip();
                setResult(buffer.getLong());
                return Fiber.frameReturn();
            }
        };
        return Fiber.call(loadFrame, resumePoint);
    }

    /**
     * truncate tail index (inclusive)
     */
    public void truncateTail(long index) {
        DtUtil.checkPositive(index, "index");
        if (index <= raftStatus.getCommitIndex()) {
            throw new RaftException("truncateTail index is too small: " + index);
        }
        if (cache.size() == 0) {
            return;
        }
        if (index < cache.getFirstKey() || index > cache.getLastKey()) {
            throw new RaftException("truncateTail out of cache range: " + index);
        }
        cache.truncate(index);
        nextIndex = index;
    }

    @Override
    protected void afterDelete() {
        if (queue.size() > 0) {
            firstIndex = posToIndex(queueStartPosition);
        } else {
            firstIndex = 0;
            nextIndex = 0;
            nextPersistIndex = 0;
        }
    }

    public long getNextIndex() {
        return nextIndex;
    }

    public long getNextPersistIndex() {
        return nextPersistIndex;
    }

    public FiberFuture<Void> close() {
        closed = true;
        needFlushCondition.signal();
        FiberFuture<Void> f;
        if (flushFiber.isStarted()) {
            f = flushFiber.join();
        } else {
            f = FiberFuture.completedFuture(groupConfig.getFiberGroup(), null);
        }
        return f.convertWithHandle("closeIdxFileQueue", (v, ex) -> {
            if (ex != null) {
                log.error("close idx file queue failed", ex);
            }
            closeChannel();
            return null;
        });
    }

    public FiberFrame<Void> beginInstall() {
        while (cache.size() > 0) {
            cache.remove();
        }
        return super.beginInstall();
    }

    public FiberFrame<Void> finishInstall(long nextLogIndex) {
        long newFileStartPos = startPosOfFile(indexToPos(nextLogIndex));
        queueStartPosition = newFileStartPos;
        queueEndPosition = newFileStartPos;
        firstIndex = nextLogIndex;
        nextIndex = nextLogIndex;
        nextPersistIndex = nextLogIndex;
        return ensureWritePosReady(nextLogIndex);
    }
}
