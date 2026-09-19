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
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.store.AsyncIoTask;
import com.github.dtprj.dongting.raft.store.FileQueue;
import com.github.dtprj.dongting.raft.store.RetryFrame;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

/**
 *
 * @author huangli
 */
final class MqIdxQueue extends FileQueue<MqIdxFile> {

    private static final DtLog log = DtLogs.getLogger(MqIdxQueue.class);

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
    // true while the active FlushAllRoundFrame anchors this queue (flushTargetSeq frozen)
    boolean flushAllTarget;

    boolean needLoadHead;
    FiberFuture<Void> loadFuture;

    boolean lastCleanupFailed;

    // pos of the last appended item (seq nextSeq-1); -1 unknown after a restart
    long lastItemPos = -1;
    // true while a threshold request for this queue sits in the flusher's request queue
    boolean roundRequested;

    final IndexedQueue<MqIdxBlock> blocks = new IndexedQueue<>(2);

    static final class FlushBatch {
        final long endSeq;
        final RefBuffer bufRef;
        final MqIdxFile logFile;
        final long filePos;
        final boolean force;
        // pos of the last item of the file, if the batch seals it; -1 otherwise
        final long lastItemPos;

        FlushBatch(long endSeq, RefBuffer bufRef, MqIdxFile logFile, long filePos, boolean force, long lastItemPos) {
            this.endSeq = endSeq;
            this.bufRef = bufRef;
            this.logFile = logFile;
            this.filePos = filePos;
            this.force = force;
            this.lastItemPos = lastItemPos;
        }
    }

    MqIdxQueue(MqIdxManager manager, long queueId, long nextSeq) {
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

    void init() {
        initQueue();
        this.initialized = true;
    }

    @Override
    protected MqIdxFile createFile(File file, long startPos, long lastAccessTime) {
        return new MqIdxFile(startPos, startPos + getFileSize(), file,
                groupConfig.fiberGroup, ioExecutor, this::lruTouch, lastAccessTime);
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
            b = new MqIdxBlock(this, nextSeq, 0);
            blocks.addLast(b);
        }
        b.append(pos, timestamp, itemSize);
        nextSeq++;
        lastItemPos = pos;
        if (b.isFull()) {
            manager.onSeal(b);
            manager.flusher.requestRound(this);
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
                manager.groupConfig.ioRetryInterval, () -> manager.markClose);
        return FutureFrame.startWaitFiber("mqIdxHeadLoad-" + queueId,
                manager.groupConfig.fiberGroup, rf);
    }

    private class LoadHeadFrame extends FiberFrame<Void> {
        private final long blockStartPos;
        private RefBuffer bufRef;
        private MqIdxFile logFile;
        private boolean readerPending;

        LoadHeadFrame() {
            this.blockStartPos = seqToPos(nextSeq) & ~(DISK_BLOCK_BYTES - 1L);
        }

        @Override
        public FrameCallResult execute(Void input) {
            logFile = getLogFile(blockStartPos);
            if (logFile == null || logFile.isDeleted()) {
                // not on disk: never created after install, or deleted by cleanup;
                // the appends will rewrite it from the flushed position
                return afterLoad(false);
            }
            if (bufRef == null) {
                bufRef = groupConfig.fiberGroup.dispatcher.thread.buffers.borrowLocal(DISK_BLOCK_BYTES);
            }
            ByteBuffer buf = bufRef.getBuffer();
            buf.limit(DISK_BLOCK_BYTES);
            logFile.incReaders();
            readerPending = true;
            return new AsyncIoTask(groupConfig.fiberGroup, logFile)
                    .read(buf, blockStartPos & fileLenMask)
                    .await(v -> afterLoad(true));
        }

        private FrameCallResult afterLoad(boolean loaded) {
            installHeadBlock(loaded ? bufRef.getBuffer() : null);
            return Fiber.frameReturn();
        }

        @Override
        protected FrameCallResult doFinally() {
            if (readerPending) {
                logFile.decReaders();
                readerPending = false;
            }
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
        blocks.addLast(b);
        firstSeqInCache = startSeq;
        needLoadHead = false;
    }

    boolean isDirty() {
        return forceFinishSeq < nextSeq - 1;
    }

    // true if the write point is beyond the head file, i.e. all its items are flushed
    // and its content is frozen
    private boolean isHeadFileSealed() {
        return queueStartPosition < startPosOfFile(seqToPos(writeFinishSeq + 1));
    }

    // true if the cleanup frame must run: head unknown, or the head is deletable
    boolean needRunCleanup(long firstValidPos) {
        if (queue.size() == 0) {
            return false;
        }
        if (isHeadFileSealed()) {
            long firstFileLastItemPos = queue.get(0).lastItemPos;
            return firstFileLastItemPos == -1 || firstFileLastItemPos < firstValidPos;
        }
        if (queue.size() > 1) {
            // the head is the write file with files above (restart rewind): not deletable
            // in place; it becomes a sealed head once the write point moves past it
            return false;
        }
        long firstSeq = posToSeq(queueStartPosition);
        if (nextSeq <= firstSeq) {
            // the restored write point sits at the file start: replay re-appends and the
            // file is rewritten in place; strictly below is impossible, deleting lower
            // files requires the snapshot to cover them
            if (nextSeq < firstSeq) {
                BugLog.log("write point below head file: queue=" + queueId + ", nextSeq="
                        + nextSeq + ", firstSeq=" + firstSeq);
            }
            return false;
        }
        return lastItemPos == -1 || lastItemPos < firstValidPos;
    }

    FiberFuture<Void> close() {
        markClose = true;
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
        boolean sealsFile = batchEnd == lastSeq;
        boolean force = sealsFile || (flushForce && batchEnd == flushTargetSeq);
        MqIdxFile logFile = getLogFile(startPos);
        if (logFile == null) {
            BugLog.logAndThrow("idx file not allocated: queue=" + queueId + ", pos=" + startPos);
        }

        RefBuffer bufRef = groupConfig.fiberGroup.dispatcher.thread.buffers.borrowDirectLocal(len);
        long lastPos;
        try {
            ByteBuffer buf = bufRef.getBuffer();
            buf.limit(len);
            fillBlocks(blockRefs, batchEnd, buf);
            buf.flip();
            lastPos = sealsFile ? buf.getLong(len - MqIdxManager.ITEM_LEN) : -1;
        } catch (Throwable t) {
            bufRef.release();
            throw t;
        }
        return new FlushBatch(batchEnd, bufRef, logFile, startPos & fileLenMask, force, lastPos);
    }

    boolean needAllocateFile() {
        return seqToPos(writeFinishSeq + 1) >= queueEndPosition;
    }

    // start pos of the file the next write (writeFinishSeq + 1) belongs to
    long nextWriteFileStartPos() {
        return startPosOfFile(seqToPos(writeFinishSeq + 1));
    }

    // the file writeFinishSeq belongs to; non-null whenever forceFinishSeq < writeFinishSeq
    MqIdxFile currentWriteFile() {
        return getLogFile(seqToPos(writeFinishSeq));
    }

    long fileLastSeq(long pos) {
        return posToSeq(pos | fileLenMask);
    }

    @Override
    public MqIdxFile getLogFile(long filePos) {
        return super.getLogFile(filePos);
    }

    void attachFile(MqIdxFile lf, long fileStart) {
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

    FiberFrame<Void> createCleanupFrame() {
        lastCleanupFailed = false;
        return new CleanupFrame();
    }

    private class CleanupFrame extends FiberFrame<Void> {

        // snapshot: the round may await across log deletions, so all decisions use one watermark
        private final long firstValidPos;

        private MqIdxFile readLogFile;
        private boolean readerPending;
        // seq of the item the lazy read targets; guards against a stale result
        private long readSeq;

        CleanupFrame() {
            this.firstValidPos = raftStatus.firstValidPos;
        }

        @Override
        public FrameCallResult execute(Void input) {
            if (manager.markClose) {
                return Fiber.frameReturn();
            }
            if (queue.size() == 0) {
                return Fiber.frameReturn();
            }
            MqIdxFile head = queue.get(0);
            if (isHeadFileSealed()) {
                // content frozen: judged by the pos of the last item of the file
                if (head.lastItemPos == -1) {
                    return readItemPos(head, fileSize - MqIdxManager.ITEM_LEN, true);
                }
                if (head.lastItemPos >= firstValidPos) {
                    return Fiber.frameReturn();
                }
                return Fiber.call(deleteFirstFile(), v -> Fiber.resume(null, this));
            }
            if (queue.size() > 1) {
                // the write file with files above (restart rewind): deferred until sealed
                return Fiber.frameReturn();
            }
            if (flushing) {
                return Fiber.frameReturn();
            }
            if (nextSeq <= posToSeq(queueStartPosition)) {
                return Fiber.frameReturn();
            }
            if (lastItemPos == -1) {
                readSeq = nextSeq - 1;
                long offset = (nextSeq - 1 - posToSeq(queueStartPosition)) * MqIdxManager.ITEM_LEN;
                return readItemPos(head, offset, false);
            }
            if (lastItemPos >= firstValidPos) {
                return Fiber.frameReturn();
            }
            return Fiber.call(deleteFirstFile(), v -> Fiber.resume(null, this));
        }

        // returns the pos field of the item at offsetInFile, or null to give up this round
        private FrameCallResult readItemPos(MqIdxFile lf, long offsetInFile, boolean head) {
            ByteBuffer buf = ByteBuffer.allocate(MqIdxManager.ITEM_LEN);
            lf.incReaders();
            readLogFile = lf;
            readerPending = true;
            return new AsyncIoTask(groupConfig.fiberGroup, lf).read(buf, offsetInFile)
                    .await(v -> afterRead(lf, buf, offsetInFile, head));
        }

        private FrameCallResult afterRead(MqIdxFile lf, ByteBuffer buf, long offsetInFile, boolean head) {
            endRead();
            if (manager.markClose) {
                return Fiber.frameReturn();
            }
            CRC32C crc = new CRC32C();
            RaftUtil.updateCrc(crc, buf, 0, MqIdxManager.ITEM_LEN - 4);
            if (buf.getInt(MqIdxManager.ITEM_LEN - 4) == (int) crc.getValue()) {
                long pos = buf.getLong(0);
                if (head) {
                    lf.lastItemPos = pos;
                } else if (nextSeq - 1 == readSeq) {
                    lastItemPos = pos;
                } else {
                    // appends landed during the io: the read value is stale, re-judge with
                    // the lastItemPos maintained by those appends
                    return Fiber.resume(null, this);
                }
                return Fiber.resume(null, this);
            } else {
                log.warn("mq idx item crc check fail, skip cleanup: {}+{}",
                        lf.getFile().getPath(), offsetInFile);
                lastCleanupFailed = true;
                return Fiber.frameReturn();
            }
        }

        private void endRead() {
            if (readerPending) {
                readerPending = false;
                readLogFile.decReaders();
                readLogFile = null;
            }
        }

        @Override
        protected FrameCallResult doFinally() {
            endRead();
            return Fiber.frameReturn();
        }

        @Override
        protected FrameCallResult handle(Throwable ex) {
            // keep the flusher loop alive; the files are left to a later round
            lastCleanupFailed = true;
            if (manager.markClose) {
                // retry canceled by close, expected
                log.warn("mq idx cleanup canceled by close: queue={}", queueId);
            } else {
                log.error("mq idx cleanup fail: queue={}", queueId, ex);
            }
            return Fiber.frameReturn();
        }
    }
}
