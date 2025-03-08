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
import com.github.dtprj.dongting.common.PerfConsts;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.fiber.DispatcherThread;
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
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;

import java.io.File;
import java.nio.ByteBuffer;

/**
 * @author huangli
 */
final class IdxFileQueue extends FileQueue implements IdxOps {
    private static final DtLog log = DtLogs.getLogger(IdxFileQueue.class);
    private static final int ITEM_LEN = 8;
    static final String KEY_PERSIST_IDX_INDEX = "persistIdxIndex";
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
    private static final long FLUSH_INTERVAL_NANOS = 2L * 1000 * 1000 * 1000;

    private final Fiber flushFiber;
    private final FiberCondition needFlushCondition;
    final FiberCondition flushDoneCondition;

    final ChainWriter chainWriter;

    public IdxFileQueue(File dir, StatusManager statusManager, RaftGroupConfigEx groupConfig, int itemsPerFile) {
        super(dir, groupConfig, (long) ITEM_LEN * itemsPerFile, false);
        if (BitUtil.nextHighestPowerOfTwo(itemsPerFile) != itemsPerFile) {
            throw new IllegalArgumentException("itemsPerFile not power of 2: " + itemsPerFile);
        }
        this.statusManager = statusManager;
        this.ts = groupConfig.ts;
        this.lastFlushNanos = ts.getNanoTime();
        this.raftStatus = (RaftStatusImpl) groupConfig.raftStatus;

        this.maxCacheItems = groupConfig.idxCacheSize;
        this.flushThreshold = groupConfig.idxFlushThreshold;
        this.blockCacheItems = maxCacheItems << 2;
        this.cache = new LongLongSeqMap(maxCacheItems);

        this.flushFiber = new Fiber("idxFlush-" + groupConfig.groupId,
                groupConfig.fiberGroup, new FlushLoopFrame());
        this.needFlushCondition = groupConfig.fiberGroup.newCondition("IdxNeedFlush-" + groupConfig.groupId);
        this.flushDoneCondition = groupConfig.fiberGroup.newCondition("IdxFlushDone-" + groupConfig.groupId);

        this.chainWriter = new ChainWriter("IdxForce", groupConfig, null, this::forceFinish);
        chainWriter.setWritePerfType1(0);
        chainWriter.setWritePerfType2(PerfConsts.RAFT_D_IDX_WRITE);
        chainWriter.setForcePerfType(PerfConsts.RAFT_D_IDX_FORCE);
    }

    public FiberFrame<Pair<Long, Long>> initRestorePos() throws Exception {
        this.firstIndex = posToIndex(queueStartPosition);
        this.persistedIndexInStatusFile = RaftUtil.parseLong(statusManager.getProperties(),
                KEY_PERSIST_IDX_INDEX, 0);
        long restoreIndex = persistedIndexInStatusFile;

        log.info("load raft status file. firstIndex={}, {}={}, {}={}", firstIndex, KEY_PERSIST_IDX_INDEX, restoreIndex,
                StatusManager.FIRST_VALID_IDX, raftStatus.firstValidIndex);
        firstIndex = Math.max(firstIndex, raftStatus.firstValidIndex);
        restoreIndex = Math.max(restoreIndex, firstIndex);

        if (restoreIndex == 1) {
            long restoreStartPos = 0;
            nextIndex = 1;
            nextPersistIndex = 1;
            persistedIndex = 0;

            if (queueEndPosition == 0) {
                tryAllocateAsync(0);
            }
            log.info("restore from index: {}, pos: {}", restoreIndex, restoreStartPos);
            return FiberFrame.completedFrame(new Pair<>(restoreIndex, restoreStartPos));
        } else {
            nextIndex = restoreIndex + 1;
            nextPersistIndex = restoreIndex + 1;
            persistedIndex = restoreIndex;
            final long finalRestoreIndex = restoreIndex;
            return new FiberFrame<>() {
                @Override
                public FrameCallResult execute(Void input) {
                    if (finalRestoreIndex == raftStatus.firstValidIndex) {
                        // return null will cause install snapshot
                        setResult(null);
                        return Fiber.frameReturn();
                    }
                    FiberFrame<Long> f = loadLogPos(finalRestoreIndex);
                    return Fiber.call(f, this::afterLoad);
                }

                private FrameCallResult afterLoad(Long restoreIndexPos) {
                    log.info("restore from index: {}, pos: {}", finalRestoreIndex, restoreIndexPos);
                    setResult(new Pair<>(finalRestoreIndex, restoreIndexPos));
                    return Fiber.frameReturn();
                }
            };
        }
    }

    public void startFibers() {
        flushFiber.start();
        chainWriter.start();
        startQueueAllocFiber();
    }

    // run in Future callback
    private void forceFinish(ChainWriter.WriteTask writeTask) {
        flushDoneCondition.signalAll();
        // if we set syncForce to false, lastRaftIndex(committed) may less than lastForceLogIndex
        long idx = Math.min(writeTask.getLastRaftIndex(), raftStatus.lastForceLogIndex);
        if (idx > persistedIndexInStatusFile) {
            statusManager.getProperties().put(KEY_PERSIST_IDX_INDEX, String.valueOf(idx));
            statusManager.persistAsync(true);
        }
        persistedIndex = writeTask.getLastRaftIndex();
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
        if (initialized && itemIndex <= raftStatus.commitIndex) {
            throw new RaftException("try update committed index: " + itemIndex);
        }
        if (itemIndex < nextIndex) {
            if (initialized || itemIndex != nextIndex - 1) {
                throw new RaftException("put index!=nextIndex " + itemIndex + ", " + nextIndex);
            }
        }
        cache.put(itemIndex, dataPosition);
        nextIndex = itemIndex + 1;
        if (getDiff() >= flushThreshold) {
            needFlushCondition.signal();
        }
    }

    private void flush(LogFile logFile, boolean suggestForce) {
        long startIdx = nextPersistIndex;
        long lastIdx = Math.min(raftStatus.commitIndex, cache.getLastKey());
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
        DispatcherThread t = groupConfig.fiberGroup.dispatcher.thread;
        ByteBuffer buf = t.directPool.borrow(len);
        buf.limit(len);
        fillAndSubmit(buf, startIdx, logFile, suggestForce);
    }

    private void submitForceOnlyTask() {
        LogFile logFile = getLogFile(indexToPos(nextPersistIndex));
        long filePos = indexToPos(nextPersistIndex) & fileLenMask;
        chainWriter.submitWrite(logFile, initialized, null, filePos, true, 0, nextPersistIndex - 1);
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
        chainWriter.submitWrite(logFile, initialized, buf, filePos, force, items, index - 1);
        removeHead();
    }

    @Override
    public boolean needWaitFlush() {
        removeHead();
        return cache.size() > blockCacheItems && getDiff() >= flushThreshold;
    }

    @Override
    public FiberFrame<Void> waitFlush() {
        // block until flush done
        return new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                if (needWaitFlush()) {
                    long first = cache.getFirstKey();
                    long last = cache.getLastKey();
                    log.warn("group {} cache size {} exceed {}, may cause block. cache from {} to {}, idxPersistedIndex={}," +
                                    " commitIndex={}, lastWriteIndex={}, lastForceIndex={}",
                            raftStatus.groupId, cache.size(), blockCacheItems, first, last, persistedIndex,
                            raftStatus.commitIndex, raftStatus.lastWriteLogIndex, raftStatus.lastForceLogIndex);
                    needFlushCondition.signalAll();
                    return flushDoneCondition.await(1000, this);
                }
                return Fiber.frameReturn();
            }
        };
    }

    private long getDiff() {
        // in recovery, the commit index may be larger than last key, lastKey may be -1
        long lastNeedFlushItem = Math.min(cache.getLastKey(), raftStatus.commitIndex);
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
            if (raftStatus.installSnapshot) {
                return Fiber.frameReturn();
            }
            long diff = getDiff();
            // 0: flush without force, 1 : flush with force, 2: force only
            int flushType;
            if (diff > 0 && nextPersistIndex <= firstIndex) {
                // after install snapshot
                flushType = 1;
            } else if (diff > flushThreshold) {
                flushType = 0;
            } else if (markClose) {
                if (diff > 0) {
                    flushType = 1;
                } else {
                    flushType = 2;
                }
            } else {
                long restMillis = (lastFlushNanos + FLUSH_INTERVAL_NANOS - ts.getNanoTime()) / 1000 / 1000;
                if (restMillis > 0) {
                    return needFlushCondition.await(restMillis, this);
                } else {
                    flushType = 1;
                }
            }

            // begin prepare flush
            lastFlushNanos = ts.getNanoTime();
            FrameCall<Void> resumePoint;
            if (flushType == 0) {
                resumePoint = v -> afterPosReady(false);
            } else if (flushType == 1) {
                resumePoint = v -> afterPosReady(true);
            } else {
                resumePoint = v -> {
                    log.info("idx flush fiber exit, groupId={}", groupConfig.groupId);
                    submitForceOnlyTask();
                    return Fiber.frameReturn();
                };
            }
            return Fiber.call(ensureWritePosReady(indexToPos(nextPersistIndex)), resumePoint);
        }

        private FrameCallResult afterPosReady(boolean suggestForce) {
            if (raftStatus.installSnapshot) {
                return Fiber.frameReturn();
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
            if (raftStatus.installSnapshot) {
                log.error("install snapshot and idx flush fiber exit, groupId={}", groupConfig.groupId, ex);
                return Fiber.frameReturn();
            } else {
                throw Fiber.fatal(ex);
            }
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
                AsyncIoTask t = new AsyncIoTask(groupConfig.fiberGroup, lf);
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
        if (index <= raftStatus.commitIndex) {
            throw new RaftException("truncateTail index is too small: " + index);
        }
        if (cache.size() == 0) {
            return;
        }
        if (index < cache.getFirstKey() || index > cache.getLastKey()) {
            throw new RaftException("truncateTail out of cache range: " + index);
        }
        log.info("truncate tail to {}(inclusive), old nextIndex={}", index, nextIndex);
        cache.truncate(index);
        nextIndex = index;
    }

    @Override
    protected void afterDelete() {
        if (queue.size() > 0) {
            firstIndex = posToIndex(queueStartPosition);
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
        markClose = true;
        needFlushCondition.signal();
        FiberFuture<Void> f;
        if (flushFiber.isStarted() && !flushFiber.isFinished()) {
            f = flushFiber.join();
        } else {
            f = FiberFuture.completedFuture(groupConfig.fiberGroup, null);
        }
        f = f.compose("idxChainStop", v -> chainWriter.stop());
        return f.compose("idxAllocStop", v -> stopFileQueue());
    }

    public FiberFrame<Void> finishInstall(long nextLogIndex) throws Exception {
        long newFileStartPos = startPosOfFile(indexToPos(nextLogIndex));
        queueStartPosition = newFileStartPos;
        queueEndPosition = newFileStartPos;
        firstIndex = nextLogIndex;
        nextIndex = nextLogIndex;
        nextPersistIndex = nextLogIndex;
        persistedIndex = nextLogIndex - 1;
        initQueue();
        startFibers();
        return ensureWritePosReady(nextLogIndex);
    }
}
