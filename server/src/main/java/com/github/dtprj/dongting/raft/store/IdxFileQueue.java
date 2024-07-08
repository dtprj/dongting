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

import com.github.dtprj.dongting.buf.SimpleByteBufferPool;
import com.github.dtprj.dongting.common.BitUtil;
import com.github.dtprj.dongting.common.DtThread;
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
import com.github.dtprj.dongting.net.PerfConsts;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
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
    private final int blockCacheItems;

    private final int flushThreshold;
    final LongLongSeqMap cache;
    private final Timestamp ts;
    private final RaftStatusImpl raftStatus;

    private long persistedIndexInStatusFile;
    private long nextPersistIndex;
    private long persistedIndex;
    private long nextIndex;
    private long firstIndex;

    private long lastFlushNanos;
    private static final long FLUSH_INTERVAL_NANOS = 15L * 1000 * 1000 * 1000;

    private final Fiber flushFiber;
    private final FiberCondition needFlushCondition;
    private final FiberCondition flushDoneCondition;

    private final IdxChainWriter chainWriter;

    private boolean closed;

    public IdxFileQueue(File dir, StatusManager statusManager, RaftGroupConfigEx groupConfig, int itemsPerFile) {
        super(dir, groupConfig, (long) ITEM_LEN * itemsPerFile, false);
        if (BitUtil.nextHighestPowerOfTwo(itemsPerFile) != itemsPerFile) {
            throw new IllegalArgumentException("itemsPerFile not power of 2: " + itemsPerFile);
        }
        this.statusManager = statusManager;
        this.ts = groupConfig.getTs();
        this.lastFlushNanos = ts.getNanoTime();
        this.raftStatus = (RaftStatusImpl) groupConfig.getRaftStatus();

        this.maxCacheItems = groupConfig.getIdxCacheSize();
        this.flushThreshold = groupConfig.getIdxFlushThreshold();
        this.blockCacheItems = maxCacheItems << 2;
        this.cache = new LongLongSeqMap(maxCacheItems);

        this.flushFiber = new Fiber("idxFlush-" + groupConfig.getGroupId(),
                groupConfig.getFiberGroup(), new FlushLoopFrame());
        this.needFlushCondition = groupConfig.getFiberGroup().newCondition("IdxNeedFlush-" + groupConfig.getGroupId());
        this.flushDoneCondition = groupConfig.getFiberGroup().newCondition("IdxFlushDone-" + groupConfig.getGroupId());

        this.chainWriter = new IdxChainWriter(groupConfig, 0, PerfConsts.RAFT_D_IDX_WRITE,
                PerfConsts.RAFT_D_IDX_FORCE);
    }

    public FiberFrame<Pair<Long, Long>> initRestorePos() throws Exception {
        super.initQueue();
        this.firstIndex = posToIndex(queueStartPosition);
        long firstValidIndex = RaftUtil.parseLong(statusManager.getProperties(),
                KEY_NEXT_IDX_AFTER_INSTALL_SNAPSHOT, 0);
        this.persistedIndexInStatusFile = RaftUtil.parseLong(statusManager.getProperties(),
                KEY_PERSIST_IDX_INDEX, 0);
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
            persistedIndex = 0;

            if (queueEndPosition == 0) {
                tryAllocateAsync(0);
            }
            log.info("restore from index: {}, pos: {}", restoreIndex, restoreStartPos);
            flushFiber.start();
            chainWriter.startForceFiber();
            return FiberFrame.completedFrame(new Pair<>(restoreIndex, restoreStartPos));
        } else {
            nextIndex = restoreIndex + 1;
            nextPersistIndex = restoreIndex + 1;
            persistedIndex = restoreIndex;
            final long finalRestoreIndex = restoreIndex;
            return new FiberFrame<>() {
                @Override
                public FrameCallResult execute(Void input) {
                    FiberFrame<Long> f = loadLogPos(finalRestoreIndex);
                    return Fiber.call(f, this::afterLoad);
                }

                private FrameCallResult afterLoad(Long restoreIndexPos) {
                    if (queueEndPosition == 0) {
                        tryAllocateAsync(0);
                    }
                    log.info("restore from index: {}, pos: {}", finalRestoreIndex, restoreIndexPos);
                    setResult(new Pair<>(finalRestoreIndex, restoreIndexPos));
                    flushFiber.start();
                    chainWriter.startForceFiber();
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

    private class IdxChainWriter extends ChainWriter {

        public IdxChainWriter(RaftGroupConfigEx config, int writePerfType1, int writePerfType2, int forcePerfType) {
            super(config, writePerfType1, writePerfType2, forcePerfType);
        }

        @Override
        protected void writeFinish(WriteTask writeTask) {
            // nothing to do
        }

        @Override
        protected void forceFinish(WriteTask writeTask) {
            // if we set syncForce to false, lastRaftIndex(committed) may less than lastForceLogIndex
            long idx = Math.min(writeTask.getLastRaftIndex(), raftStatus.getLastForceLogIndex());
            if (idx > persistedIndexInStatusFile) {
                statusManager.getProperties().put(KEY_PERSIST_IDX_INDEX, String.valueOf(idx));
                statusManager.persistAsync(true);
            }
            persistedIndex = writeTask.getLastRaftIndex();
            flushDoneCondition.signalAll();
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
        if (getDiff() > flushThreshold) {
            needFlushCondition.signal();
        }
    }

    private void flush(LogFile logFile, boolean suggestForce) {
        long startIdx = nextPersistIndex;
        long lastIdx = Math.min(raftStatus.getCommitIndex(), cache.getLastKey());
        if (lastIdx < startIdx) {
            submitForceOnlyTask();
            return;
        }
        if (lastIdx - startIdx > MAX_BATCH_ITEMS) {
            lastIdx = startIdx + MAX_BATCH_ITEMS;
        }
        long startIdxPos = indexToPos(startIdx);
        long lastIdxPos = indexToPos(lastIdx);
        long fileStartPos1 = startIdxPos & ~fileLenMask;
        long fileStartPos2 = lastIdxPos & ~fileLenMask;
        int len;
        if (fileStartPos1 == fileStartPos2) {
            // in same file
            len = (int) (lastIdxPos - startIdxPos + ITEM_LEN);
        } else {
            // don't cross file
            len = (int) (logFile.endPos - startIdxPos);
        }
        DtThread t = (DtThread) Thread.currentThread();
        ByteBuffer buf = t.getDirectPool().borrow(len);
        buf.limit(len);
        fillAndSubmit(buf, startIdx, logFile, suggestForce);
    }

    private void submitForceOnlyTask() {
        LogFile logFile = getLogFile(indexToPos(nextPersistIndex));
        long filePos = indexToPos(nextPersistIndex) & fileLenMask;
        ByteBuffer buf = SimpleByteBufferPool.EMPTY_BUFFER;
        int[] retryInterval = initialized ? groupConfig.getIoRetryInterval() : null;
        ChainWriter.WriteTask wt = new ChainWriter.WriteTask(groupConfig.getFiberGroup(), logFile, retryInterval, true,
                () -> closed, buf, filePos, true, 0, nextPersistIndex - 1);
        chainWriter.submitWrite(wt);
    }

    private void fillAndSubmit(ByteBuffer buf, long startIndex, LogFile logFile, boolean suggestForce) {
        long index = startIndex;
        //noinspection UnnecessaryLocalVariable
        LongLongSeqMap c = cache;
        while (buf.hasRemaining()) {
            long value = c.get(index++);
            buf.putLong(value);
        }
        buf.flip();
        nextPersistIndex = index;

        long filePos = indexToPos(startIndex) & fileLenMask;
        boolean fileEnd = filePos + buf.remaining() == fileSize;
        boolean force = fileEnd || suggestForce;
        int items = (int) (index - startIndex);
        int[] retryInterval = initialized ? groupConfig.getIoRetryInterval() : null;
        ChainWriter.WriteTask wt = new ChainWriter.WriteTask(groupConfig.getFiberGroup(), logFile, retryInterval, true,
                () -> closed, buf, filePos, force, items, index - 1);
        chainWriter.submitWrite(wt);
        removeHead();
    }

    @Override
    public boolean needWaitFlush() {
        removeHead();
        LongLongSeqMap cache = this.cache;
        if (cache.size() > blockCacheItems && persistedIndex < Math.min(raftStatus.getCommitIndex(), cache.getLastKey())) {
            long first = cache.getFirstKey();
            long last = cache.getLastKey();
            log.warn("group {} cache size exceed {}({}), may cause block. cache from {} to {}, commitIndex={}(diff={}), " +
                            "lastWriteIndex={}(diff={}), lastForceIndex={}(diff={}), ",
                    raftStatus.getGroupId(), blockCacheItems, cache.size(), first, last,
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
                if (cache.size() > blockCacheItems && persistedIndex < Math.min(raftStatus.getCommitIndex(), cache.getLastKey())) {
                    return flushDoneCondition.await(1000, this);
                }
                return Fiber.frameReturn();
            }
        };
    }

    private long getDiff() {
        // in recovery, the commit index may be larger than last key, lastKey may be -1
        long lastNeedFlushItem = Math.min(cache.getLastKey(), raftStatus.getCommitIndex());
        return Math.max(lastNeedFlushItem - nextPersistIndex + 1, 0);
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

        @Override
        public FrameCallResult execute(Void input) {
            long diff = getDiff();
            if (diff > flushThreshold) {
                return prepareFlush(0);
            } else if (closed) {
                if (diff > 0) {
                    return prepareFlush(1);
                } else {
                    return prepareFlush(2);
                }
            } else {
                long restMillis = (lastFlushNanos + FLUSH_INTERVAL_NANOS - ts.getNanoTime()) / 1000 / 1000;
                if (restMillis > 0) {
                    return needFlushCondition.await(restMillis, this);
                } else {
                    return prepareFlush(1);
                }
            }
        }

        private FrameCallResult prepareFlush(int type) {
            lastFlushNanos = ts.getNanoTime();
            FrameCall<Void> resumePoint;
            if (type == 0) {
                resumePoint = v -> afterPosReady(false);
            } else if (type == 1) {
                resumePoint = v -> afterPosReady(true);
            } else {
                resumePoint = v -> {
                    log.info("idx flush fiber exit, groupId={}", groupConfig.getGroupId());
                    submitForceOnlyTask();
                    return Fiber.frameReturn();
                };
            }
            return Fiber.call(ensureWritePosReady(indexToPos(nextPersistIndex)), resumePoint);
        }

        private FrameCallResult afterPosReady(boolean suggestForce) {
            if (raftStatus.isInstallSnapshot()) {
                return needFlushCondition.await(1000, this);
            }
            LogFile logFile = getLogFile(indexToPos(nextPersistIndex));
            if (logFile.shouldDelete()) {
                BugLog.getLog().error("idx file deleted, flush fail: {}", logFile.getFile().getPath());
                throw Fiber.fatal(new RaftException("idx file deleted, flush fail"));
            }
            flush(logFile, suggestForce);
            return Fiber.resume(null, this);
        }

        @Override
        protected FrameCallResult handle(Throwable ex) {
            throw Fiber.fatal(ex);
        }
    }

    @Override
    public FiberFrame<Long> loadLogPos(long itemIndex) {
        DtUtil.checkPositive(itemIndex, "index");

        return new FiberFrame<>() {
            final ByteBuffer buffer = ByteBuffer.allocate(8);
            @Override
            public FrameCallResult execute(Void v) {
                if (itemIndex < firstIndex) {
                    BugLog.getLog().error("load index is too small: index={}, firstIndex={}", itemIndex, firstIndex);
                    throw new RaftException("index too small");
                }
                if (itemIndex > persistedIndex) {
                    if (itemIndex >= cache.getFirstKey() && itemIndex <= cache.getLastKey()) {
                        setResult(cache.get(itemIndex));
                        return Fiber.frameReturn();
                    }
                    BugLog.getLog().error("load index too large: index={}, persistedIndex={}", itemIndex, persistedIndex);
                    throw new RaftException("index is too large");
                }
                long pos = indexToPos(itemIndex);
                LogFile lf = getLogFile(pos);
                if (lf.isDeleted()) {
                    throw new RaftException("file deleted: " + lf.getFile().getPath());
                }
                long filePos = pos & fileLenMask;
                AsyncIoTask t = new AsyncIoTask(groupConfig.getFiberGroup(), lf);
                return t.read(buffer, filePos).await(this::afterLoad);
            }

            private FrameCallResult afterLoad(Void unused) {
                buffer.flip();
                setResult(buffer.getLong());
                return Fiber.frameReturn();
            }
        };
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
            persistedIndex = 0;
        }
    }

    public long getNextIndex() {
        return nextIndex;
    }

    public long getNextPersistIndex() {
        return nextPersistIndex;
    }

    public long getPersistedIndex() {
        return persistedIndex;
    }

    public FiberFuture<Void> close() {
        closed = true;
        needFlushCondition.signal();
        FiberFuture<Void> closeFuture = groupConfig.getFiberGroup().newFuture("idxClose");
        FiberFuture<Void> f1;
        if (flushFiber.isStarted()) {
            f1 = flushFiber.join();
        } else {
            f1 = FiberFuture.completedFuture(groupConfig.getFiberGroup(), null);
        }
        f1.registerCallback((v, ex) -> {
            if (ex != null) {
                closeFuture.completeExceptionally(ex);
            } else {
                chainWriter.shutdownForceFiber().registerCallback((v2, ex2) -> {
                    if (ex2 != null) {
                        closeFuture.completeExceptionally(ex2);
                    } else {
                        closeChannel();
                        closeFuture.complete(null);
                    }
                });
            }
        });
        return closeFuture;
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
        persistedIndex = nextLogIndex - 1;
        return ensureWritePosReady(nextLogIndex);
    }
}
