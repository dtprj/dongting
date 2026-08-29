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
package com.github.dtprj.dongting.dtmq.server;

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.common.DtBugException;
import com.github.dtprj.dongting.common.IndexedQueue;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.fiber.FutureFrame;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.store.FileQueue;
import com.github.dtprj.dongting.raft.store.LogFile;
import com.github.dtprj.dongting.raft.store.RetryFrame;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

/**
 *
 * @author huangli
 */
final class QueueIdxInfo extends FileQueue {

    private static final int DISK_BLOCK_BYTES = MqIdxBlock.BLOCK_ITEMS * MqIdxManager.ITEM_LEN;

    final MqIdxManager manager;
    final long queueId;

    // cannot be rebuilt from raft logs, must be saved into snapshots
    long nextSeq;

    long firstSeqInCache;

    long forceFinishSeq;
    long writeFinishSeq;
    boolean flushing;
    long flushTargetSeq;
    boolean flushForce;

    boolean needLoadHead;
    FiberFuture<Void> loadFuture;

    final IndexedQueue<MqIdxBlock> blocks = new IndexedQueue<>(2);

    static final class FlushBatch {
        final long endSeq;
        final RefBuffer bufRef;
        final LogFile logFile;
        final long filePos;
        final boolean force;

        FlushBatch(long endSeq, RefBuffer bufRef, LogFile logFile, long filePos, boolean force) {
            this.endSeq = endSeq;
            this.bufRef = bufRef;
            this.logFile = logFile;
            this.filePos = filePos;
            this.force = force;
        }
    }

    QueueIdxInfo(MqIdxManager manager, long queueId, long nextSeq) {
        super(new File(manager.dir, String.valueOf(queueId)), manager.groupConfig,
                (long) MqIdxManager.ITEM_LEN * manager.groupConfig.mqIdxItemsPerFile, false);
        this.manager = manager;
        this.queueId = queueId;
        this.nextSeq = nextSeq;
        this.firstSeqInCache = nextSeq & ~((long) MqIdxBlock.BLOCK_MASK);
        this.forceFinishSeq = nextSeq - 1;
        this.writeFinishSeq = nextSeq - 1;
        this.flushTargetSeq = nextSeq - 1;
        this.needLoadHead = (nextSeq & MqIdxBlock.BLOCK_MASK) != 0;
    }

    long seqToPos(long seq) {
        return seq << 5;
    }

    long posToSeq(long pos) {
        return pos >>> 5;
    }

    MqIdxBlock getBlock(long seq) {
        if (blocks.getFirst() == null || seq < firstSeqInCache || seq >= nextSeq) {
            return null;
        }
        return blocks.get(blockIndexOf(seq));
    }

    private int blockIndexOf(long seq) {
        return (int) ((seq >>> MqIdxBlock.BLOCK_SHIFT) - (firstSeqInCache >>> MqIdxBlock.BLOCK_SHIFT));
    }

    void removeFirst(MqIdxBlock expected) {
        if (blocks.getFirst() != expected) {
            throw new IllegalStateException("fifo invariant broken: block " + expected.startSeq
                    + " is not the head block of queue " + queueId);
        }
        blocks.pollFirst();
        firstSeqInCache = expected.startSeq + expected.count;
    }

    void append(long seq, long pos, long timestamp, int itemSize) {
        if (seq != nextSeq) {
            throw new IllegalArgumentException("seq not continuous: queue=" + queueId
                    + ", seq=" + seq + ", nextSeq=" + nextSeq);
        }
        if (needLoadHead) {
            throw new DtBugException("append before head block loaded: queue=" + queueId);
        }
        MqIdxBlock b = blocks.getLast();
        if (b == null || b.isFull()) {
            // null: no block yet, or the queue was fully evicted (both imply nextSeq is aligned)
            manager.onNewBlock();
            b = new MqIdxBlock(this, nextSeq, 0);
            blocks.addLast(b);
        }
        b.append(pos, timestamp, itemSize);
        nextSeq++;
        if (b.isFull()) {
            manager.onSeal(b);
            manager.flusher.maybeStartRound(this);
        }
    }

    FiberFuture<Void> ensureHeadLoaded() {
        if (!needLoadHead && loadFuture == null) {
            return null;
        }
        if (loadFuture == null) {
            loadFuture = loadHeadBlock();
            loadFuture.registerCallback((v, ex) -> loadFuture = null);
        }
        return loadFuture;
    }

    FiberFuture<Void> loadHeadBlock() {
        RetryFrame<Void> rf = new RetryFrame<>(new LoadHeadFrame(),
                manager.groupConfig.ioRetryInterval, false, () -> manager.markClose);
        return FutureFrame.startWaitFiber("mqIdxHeadLoad-" + queueId,
                manager.groupConfig.fiberGroup, rf);
    }

    private class LoadHeadFrame extends FiberFrame<Void> {
        private RefBuffer bufRef;

        @Override
        public FrameCallResult execute(Void input) {
            long startSeq = nextSeq & ~((long) MqIdxBlock.BLOCK_MASK);
            long blockPos = seqToPos(startSeq);
            File file = createFileByStartPos(startPosOfFile(blockPos));
            int offsetInFile = (int) (blockPos & fileLenMask);
            if (bufRef == null) {
                bufRef = manager.groupConfig.fiberGroup.dispatcher.thread.buffers.borrowLocal(DISK_BLOCK_BYTES);
            }
            FiberFuture<Boolean> f = manager.groupConfig.fiberGroup.newFuture("mqIdxBlockLoad");
            try {
                manager.groupConfig.blockIoExecutor.execute(() -> loadBlock(f, file, offsetInFile, bufRef));
            } catch (Throwable t) {
                f.completeExceptionally(t);
            }
            return f.await(this::afterLoad);
        }

        private FrameCallResult afterLoad(Boolean loaded) {
            installHeadBlock(Boolean.TRUE.equals(loaded) ? bufRef.getBuffer() : null);
            return Fiber.frameReturn();
        }

        @Override
        protected FrameCallResult doFinally() {
            if (bufRef != null) {
                bufRef.release();
                bufRef = null;
            }
            return Fiber.frameReturn();
        }
    }

    void installHeadBlock(ByteBuffer src) {
        long startSeq = nextSeq & ~((long) MqIdxBlock.BLOCK_MASK);
        int count = (int) (nextSeq & MqIdxBlock.BLOCK_MASK);
        MqIdxBlock b = new MqIdxBlock(this, startSeq, count);
        if (src != null) {
            MqIdxManager.decode(src, count, b);
        }
        manager.onNewBlock();
        blocks.addLast(b);
        firstSeqInCache = startSeq;
        needLoadHead = false;
    }

    private void loadBlock(FiberFuture<Boolean> f, File file, int offsetInFile, RefBuffer bufRef) {
        try {
            if (!file.isFile()) {
                f.fireComplete(false);
                return;
            }
            ByteBuffer buf = bufRef.getBuffer();
            try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                raf.seek(offsetInFile);
                raf.readFully(buf.array(), buf.arrayOffset(), DISK_BLOCK_BYTES);
            }
            f.fireComplete(true);
        } catch (Throwable t) {
            f.fireCompleteExceptionally(new RaftException(
                    "load mq idx block fail: " + file.getPath() + ", offset=" + offsetInFile, t));
        }
    }

    boolean isDirty() {
        return forceFinishSeq < nextSeq - 1;
    }

    FiberFuture<Void> closeFiles() {
        return stopFileQueue();
    }

    // [block-aligned seq of writeFinishSeq+1, min(flushTargetSeq, fileLastSeq, batch cap)],
    // never crosses files: the flushed prefix is rewritten idempotently; dispatcher fiber only
    FlushBatch prepareBatch() {
        long startSeq = (writeFinishSeq + 1) & ~((long) MqIdxBlock.BLOCK_MASK);
        long startPos = seqToPos(startSeq);
        long lastSeq = fileLastSeq(startPos);
        long batchEnd = Math.min(flushTargetSeq, Math.min(lastSeq,
                startSeq + groupConfig.mqIdxFlushBatchItems - 1));

        int from = blockIndexOf(startSeq);
        int to = blockIndexOf(batchEnd);
        if (from < 0 || to >= blocks.size()) {
            BugLog.logAndThrow("flush source block evicted: queue=" + queueId
                    + ", seq=" + startSeq + ", cacheFrom=" + firstSeqInCache);
        }
        MqIdxBlock[] blockRefs = new MqIdxBlock[to - from + 1];
        for (int i = from; i <= to; i++) {
            blockRefs[i - from] = blocks.get(i);
        }
        int len = (int) ((batchEnd - startSeq + 1) * MqIdxManager.ITEM_LEN);
        // a file-completing batch always forces, so the unforced tail never spans files
        boolean force = batchEnd == lastSeq || (flushForce && batchEnd == flushTargetSeq);
        LogFile logFile = getLogFile(startPos);
        if (logFile == null) {
            BugLog.logAndThrow("idx file not allocated: queue=" + queueId + ", pos=" + startPos);
        }

        RefBuffer bufRef = groupConfig.fiberGroup.dispatcher.thread.buffers.borrowDirectLocal(len);
        try {
            ByteBuffer buf = bufRef.getBuffer();
            buf.limit(len);
            fillBlocks(blockRefs, batchEnd, buf);
            buf.flip();
        } catch (Throwable t) {
            bufRef.release();
            throw t;
        }
        return new FlushBatch(batchEnd, bufRef, logFile, startPos & fileLenMask, force);
    }

    boolean needAllocateFile() {
        return seqToPos(writeFinishSeq + 1) >= queueEndPosition;
    }

    // start pos of the file the next write (writeFinishSeq + 1) belongs to
    long nextWriteFileStartPos() {
        return startPosOfFile(seqToPos(writeFinishSeq + 1));
    }

    // the file writeFinishSeq belongs to; non-null whenever forceFinishSeq < writeFinishSeq
    LogFile currentWriteFile() {
        return getLogFile(seqToPos(writeFinishSeq));
    }

    long fileLastSeq(long pos) {
        return posToSeq(pos | fileLenMask);
    }

    void attachFile(LogFile lf, long fileStart) {
        lruAddLast(lf);
        queue.addLast(lf);
        if (queue.size() == 1) {
            queueStartPosition = fileStart;
        }
        queueEndPosition = fileStart + fileSize;
    }

    // dispatcher thread; dest position ends at its limit
    static void fillBlocks(MqIdxBlock[] blocks, long lastSeq, ByteBuffer dest) {
        CRC32C crc = new CRC32C();
        for (MqIdxBlock b : blocks) {
            ByteBuffer src = b.buffer;
            int n = (int) Math.min(lastSeq - b.startSeq + 1, MqIdxBlock.BLOCK_ITEMS);
            for (int slot = 0; slot < n; slot++) {
                int off = slot * MqIdxBlock.SLOT_SIZE;
                int recStart = dest.position();
                dest.putLong(src.getLong(off));
                dest.putLong(src.getLong(off + 8));
                dest.putLong(0L);
                dest.putInt(src.getInt(off + 16));
                crc.reset();
                RaftUtil.updateCrc(crc, dest, recStart, MqIdxManager.ITEM_LEN - 4);
                dest.putInt((int) crc.getValue());
            }
        }
    }
}
