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
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;

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
    static final String KEY_INSTALL_SNAPSHOT = "installSnapshot";

    public static final int DEFAULT_ITEMS_PER_FILE = 1024 * 1024;
    public static final int DEFAULT_MAX_CACHE_ITEMS = 16 * 1024;

    private final StatusManager statusManager;

    private final int maxCacheItems;

    private final int flushItems;
    final LongLongSeqMap cache = new LongLongSeqMap(1024);
    private final Timestamp ts;
    private final RaftStatusImpl raftStatus;

    private long nextPersistIndex;
    private long nextIndex;
    private long firstIndex;

    private long lastFlushNanos;
    private static final long FLUSH_INTERVAL_NANOS = 15L * 1000 * 1000 * 1000;

    final Fiber flushFiber;
    private final FiberCondition needFlushCondition;
    private final FiberCondition flushDoneCondition;

    private boolean closed;

    public IdxFileQueue(File dir, StatusManager statusManager, RaftGroupConfig groupConfig,
                        int itemsPerFile, int maxCacheItems) {
        super(dir, groupConfig, (long) ITEM_LEN * itemsPerFile);
        if (BitUtil.nextHighestPowerOfTwo(itemsPerFile) != itemsPerFile) {
            throw new IllegalArgumentException("itemsPerFile not power of 2: " + itemsPerFile);
        }
        this.statusManager = statusManager;
        this.ts = groupConfig.getTs();
        this.lastFlushNanos = ts.getNanoTime();
        this.raftStatus = (RaftStatusImpl) groupConfig.getRaftStatus();

        this.maxCacheItems = maxCacheItems;
        this.flushItems = this.maxCacheItems / 2;
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
        long restoreIndex = Long.parseLong(statusManager.getProperties()
                .getProperty(KEY_PERSIST_IDX_INDEX, "0"));
        boolean installSnapshot = Boolean.parseBoolean(statusManager.getProperties()
                .getProperty(KEY_INSTALL_SNAPSHOT, "false"));
        if (installSnapshot) {
            log.warn("install snapshot not finished");
            return FiberFrame.completedFrame(null);
        }

        log.info("load raft status file. firstIndex={}, {}={}, {}={}, ", firstIndex, KEY_PERSIST_IDX_INDEX, restoreIndex,
                KEY_NEXT_IDX_AFTER_INSTALL_SNAPSHOT, firstValidIndex);
        restoreIndex = Math.max(restoreIndex, firstValidIndex);
        restoreIndex = Math.max(restoreIndex, firstIndex);

        if (restoreIndex == 0) {
            restoreIndex = 1;
            long restoreIndexPos = 0;
            nextIndex = 1;
            nextPersistIndex = 1;

            if (queueEndPosition == 0) {
                tryAllocateAsync(0);
            }
            log.info("restore index: {}, pos: {}", restoreIndex, restoreIndexPos);
            flushFiber.start();
            return FiberFrame.completedFrame(new Pair<>(restoreIndex, restoreIndexPos));
        } else {
            nextIndex = restoreIndex + 1;
            nextPersistIndex = restoreIndex + 1;
            final long finalRestoreIndex = restoreIndex;
            return new FiberFrame<>() {
                @Override
                public FrameCallResult execute(Void input) throws Throwable {
                    return loadLogPos(finalRestoreIndex, this::afterLoad);
                }

                private FrameCallResult afterLoad(Long restoreIndexPos) {
                    if (queueEndPosition == 0) {
                        tryAllocateAsync(0);
                    }
                    log.info("restore index: {}, pos: {}", finalRestoreIndex, restoreIndexPos);
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
             throw new RaftException("put index!=nextIndex " + itemIndex + ", " + nextIndex);
        }
        removeHead();
        cache.put(itemIndex, dataPosition);
        nextIndex = itemIndex + 1;
        tryAllocateAsync(indexToPos(nextIndex));
        if (shouldFlush()) {
            needFlushCondition.signal();
        }
    }

    @Override
    public boolean needWaitFlush() {
        return cache.size() >= maxCacheItems;
    }

    @Override
    public FiberFrame<Void> waitFlush() {
        needFlushCondition.signal();
        // block until flush done
        return new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                log.warn("cache size exceed {}: {}", maxCacheItems, cache.size());
                return flushDoneCondition.await(this::afterFlush);
            }

            private FrameCallResult afterFlush(Void unused) {
                if (shouldFlush()) {
                    needFlushCondition.signal();
                }
                return Fiber.frameReturn();
            }
        };
    }

    private long getFlushDiff() {
        return raftStatus.getCommitIndex() - nextPersistIndex + 1;
    }

    private boolean shouldFlush() {
        boolean timeout = ts.getNanoTime() - lastFlushNanos > FLUSH_INTERVAL_NANOS;
        long diff = getFlushDiff();
        return (diff >= flushItems || (timeout && diff > 0)) && !raftStatus.isInstallSnapshot();
    }

    private void removeHead() {
        while (cache.size() >= maxCacheItems && cache.getFirstKey() < nextPersistIndex) {
            cache.remove();
        }
    }

    private class FlushLoopFrame extends FiberFrame<Void> {

        private boolean lastFlushTriggered;

        @Override
        public FrameCallResult execute(Void input) {
            if (closed) {
                if (!lastFlushTriggered || getFlushDiff() <= 0) {
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
            LogFile logFile = getLogFile(indexToPos(nextPersistIndex));
            if (logFile.deleted) {
                log.warn("file deleted, ignore idx flush: {}", logFile.file.getPath());
                // loop
                return execute(null);
            }
            return Fiber.call(new FlushFrame(logFile), this);
        }

        @Override
        protected FrameCallResult handle(Throwable ex) {
            return Fiber.fatal(ex);
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
            logFile.incUseCount();
            prepareBuffer(logFile);

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

            AsyncIoTask currentWriteTask = new AsyncIoTask(getFiberGroup(), logFile.channel,
                    groupConfig.getIoRetryInterval(), true);
            long filePos = indexToPos(startIndex) & fileLenMask;
            FiberFuture<Void> f = currentWriteTask.writeAndFlush(buf, filePos, false);
            return f.await(notUsedVoid -> afterWrite(nextPersistIndexAfterWrite));
        }

        private FrameCallResult afterWrite(long nextPersistIndexAfterWrite) {
            nextPersistIndex = nextPersistIndexAfterWrite;
            long idxPersisIndex = nextPersistIndex - 1;
            removeHead();

            statusManager.getProperties().setProperty(KEY_PERSIST_IDX_INDEX, String.valueOf(idxPersisIndex));
            if (closed) {
                return Fiber.call(statusManager.persistSync(), this::afterStatusPersist);
            } else {
                statusManager.persistAsync();
                return afterStatusPersist(null);
            }
        }

        private FrameCallResult afterStatusPersist(Void unused) {
            flushDoneCondition.signal();
            return Fiber.frameReturn();
        }

        private void prepareBuffer(LogFile logFile) {
            long startIdx = nextPersistIndex;
            long startIdxPos = indexToPos(startIdx);
            long lastIdx = Math.min(raftStatus.getCommitIndex(), cache.getLastKey());
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
        }

        @Override
        protected FrameCallResult doFinally() {
            logFile.descUseCount();
            if (buf != null) {
                groupConfig.getDirectPool().release(buf);
            }
            return super.doFinally();
        }
    }

    @Override
    public FrameCallResult loadLogPos(long itemIndex, FrameCall<Long> resumePoint) throws Throwable {
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
        if (lf.deleted) {
            throw new RaftException("file mark deleted: " + lf.file.getPath());
        }
        FiberFrame<Long> loadFrame = new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void v) {
                lf.incUseCount();
                long filePos = pos & fileLenMask;
                AsyncIoTask t = new AsyncIoTask(getFiberGroup(), lf.channel);
                return t.read(buffer, filePos).await(this::afterLoad);
            }

            private FrameCallResult afterLoad(Void unused) {
                buffer.flip();
                setResult(buffer.getLong());
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult doFinally() {
                lf.descUseCount();
                return Fiber.frameReturn();
            }
        };
        return Fiber.call(loadFrame, resumePoint);
    }

    public void truncateTail(long index) {
        DtUtil.checkPositive(index, "index");
        if (index <= raftStatus.getCommitIndex()) {
            throw new RaftException("truncateTail index is too small: " + index);
        }
        if (index < cache.getFirstKey() || index > cache.getLastKey()) {
            throw new RaftException("truncateTail out of cache range: " + index);
        }
        cache.truncate(index);
        nextIndex = index;
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

    @Override
    public void close() {
        closed = true;
        needFlushCondition.signal();
        super.close();
    }

}
