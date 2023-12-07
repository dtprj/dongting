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

    private FiberFuture<Void> statusFuture;

    private long lastFlushNanos;
    private static final long FLUSH_INTERVAL_NANOS = 15L * 1000 * 1000 * 1000;

    private boolean closing;

    private final Fiber flushFiber;
    private final FiberCondition needFlushCondition;
    private final FiberCondition flushDoneCondition;

    private boolean inInit = true;

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
        this.needFlushCondition = groupConfig.getFiberGroup().newCondition();
        this.flushDoneCondition = groupConfig.getFiberGroup().newCondition();
    }

    public FiberFrame<Pair<Long, Long>> initRestorePos() {
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
                tryAllocate();
            }
            log.info("restore index: {}, pos: {}", restoreIndex, restoreIndexPos);
            return FiberFrame.completedFrame(new Pair<>(restoreIndex, restoreIndexPos));
        } else {
            nextIndex = restoreIndex + 1;
            nextPersistIndex = restoreIndex + 1;
            final long finalRestoreIndex = restoreIndex;
            return new FiberFrame<>() {
                @Override
                public FrameCallResult execute(Void input) throws Exception {
                    return loadLogPos(finalRestoreIndex, this::afterLoad);
                }

                private FrameCallResult afterLoad(Long restoreIndexPos) {
                    if (queueEndPosition == 0) {
                        tryAllocate();
                    }
                    log.info("restore index: {}, pos: {}", finalRestoreIndex, restoreIndexPos);
                    setResult(new Pair<>(finalRestoreIndex, restoreIndexPos));
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

    private long indexToPos(long index) {
        // each item 8 bytes
        return index << 3;
    }

    private long posToIndex(long pos) {
        // each item 8 bytes
        return pos >>> 3;
    }

    @Override
    public FiberFrame<Void> put(long itemIndex, long dataPosition) {
        if (itemIndex > nextIndex) {
            throw new RaftException("index not match : " + nextIndex + ", " + itemIndex);
        }
        if (!inInit && itemIndex <= raftStatus.getCommitIndex()) {
            throw new RaftException("try update committed index: " + itemIndex);
        }
        if (itemIndex < nextIndex) {
            //noinspection StatementWithEmptyBody
            if (inInit && cache.size() == 0) {
                // normal case
            } else {
                throw new RaftException("put index!=nextIndex " + itemIndex + ", " + nextIndex);
            }
        }
        removeHead();
        cache.put(itemIndex, dataPosition);
        nextIndex = itemIndex + 1;

        tryAllocateAsync(indexToPos(nextIndex));

        if (shouldFlush()) {
            needFlushCondition.signal();
        }
        if (cache.size() >= maxCacheItems) {
            // block until flush done
            return new FiberFrame<>() {
                @Override
                public FrameCallResult execute(Void input) {
                    log.warn("cache size exceed {}: {}", maxCacheItems, cache.size());
                    return flushDoneCondition.awaitOn(this::afterFlush);
                }

                private FrameCallResult afterFlush(Void unused) {
                    removeHead();
                    if (shouldFlush()) {
                        needFlushCondition.signal();
                    }
                    return Fiber.frameReturn();
                }
            };
        } else {
            return FiberFrame.voidCompletedFrame();
        }
    }

    private boolean shouldFlush() {
        boolean timeout = ts.getNanoTime() - lastFlushNanos > FLUSH_INTERVAL_NANOS;
        long diff = raftStatus.getCommitIndex() - nextPersistIndex + 1;
        return (diff >= flushItems || (timeout && diff > 0)) && !raftStatus.isInstallSnapshot();
    }

    private void removeHead() {
        while (cache.size() >= maxCacheItems && cache.getFirstKey() < nextPersistIndex) {
            cache.remove();
        }
    }

    private class FlushLoopFrame extends FiberFrame<Void> {
        @Override
        public FrameCallResult execute(Void input) {
            if (isGroupShouldStopPlain()) {
                return Fiber.frameReturn();
            }
            if (!shouldFlush()) {
                return needFlushCondition.awaitOn(this);
            }

            lastFlushNanos = ts.getNanoTime();

            long startIndex = nextPersistIndex;
            return Fiber.call(ensureWritePosReady(indexToPos(startIndex), true), v -> afterPosReady(startIndex));
        }

        private FrameCallResult afterPosReady(long startIndex) {
            if (isGroupShouldStopPlain()) {
                return Fiber.frameReturn();
            }
            LogFile logFile = getLogFile(startIndex);
            if (logFile.deleted) {
                log.warn("file deleted, ignore idx flush: {}", logFile.file.getPath());
                // loop
                return execute(null);
            }
            return Fiber.call(new FlushFrame(logFile), this);
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
            prepareBuffer();

            LongLongSeqMap c = cache;
            long startIndex = nextPersistIndex;
            long index = startIndex;
            while (buf.hasRemaining()) {
                long value = c.get(index++);
                buf.putLong(value);
            }
            buf.flip();

            long nextPersistIndexAfterWrite = index;

            AsyncIoTask currentWriteTask = new AsyncIoTask(getFiberGroup(), logFile.channel,
                    this::isGroupShouldStopPlain, groupConfig.getIoRetryInterval(), true);
            FiberFuture<Void> f = currentWriteTask.writeAndFlush(buf, indexToPos(startIndex), false);
            return f.awaitOn(notUsedVoid -> afterWrite(nextPersistIndexAfterWrite));
        }

        private FrameCallResult afterWrite(long nextPersistIndexAfterWrite) {
            nextPersistIndex = nextPersistIndexAfterWrite;
            long idxPersisIndex = nextPersistIndex - 1;
            statusManager.getProperties().setProperty(KEY_PERSIST_IDX_INDEX, String.valueOf(idxPersisIndex));
            statusManager.persistAsync();
            flushDoneCondition.signal();
            return Fiber.frameReturn();
        }

        private void prepareBuffer() {
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
                len = (int) (fileSize - startIdxPos);
            }
            buf = groupConfig.getDirectPool().getPool().borrow(len);
            buf.limit(len);
        }

        @Override
        protected FrameCallResult doFinally() {
            logFile.descUseCount();
            groupConfig.getDirectPool().getPool().release(buf);
            return super.doFinally();
        }
    }

    @Override
    public FrameCallResult loadLogPos(long itemIndex, FrameCall<Long> resumePoint) throws Exception {
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
                return t.read(buffer, filePos).awaitOn(this::afterLoad);
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

    public long getNextIndex() {
        return nextIndex;
    }

}
