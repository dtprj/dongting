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
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FrameCall;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.fiber.HandlerFrame;
import com.github.dtprj.dongting.fiber.SimpleFrame;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.ChecksumException;
import com.github.dtprj.dongting.raft.store.AsyncIoTask;
import com.github.dtprj.dongting.raft.store.LogHeader;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

/** Static memory scans serve queues with cached idx blocks; this frame handles missing blocks.
 * Returns false when a stale state collected mid-scan requires a full rescan. */
final class MqIdxReader extends FiberFrame<Boolean> {
    private final MqPullFrame pull;
    private final MqIdxQueue queue;
    private final MqPullFrame.QueueResult result;
    // snapshot of queue.nextSeq: the scan window stays fixed while appends continue
    private final long endSeq;
    private CRC32C crc;

    // seek search bounds: the first seq with pos >= minPos
    private long low;
    private long high;
    private long minPos;

    // the current item, filled by loadCachedItem() or decodeItem()
    private long seq;
    private long pos;
    private long raftIndex;
    private int size;

    // one idx block read from disk covers seqs [diskStart, diskEnd)
    private RefBuffer diskBuffer;
    private long diskStart = -1;
    private long diskEnd;

    private MqIdxFile readingFile;
    private FiberFuture<Void> pending;

    MqIdxReader(MqPullFrame pull, MqIdxQueue queue, MqPullFrame.QueueResult result) {
        this.pull = pull;
        this.queue = queue;
        this.result = result;
        this.endSeq = queue.nextSeq;
    }

    /** Phase 1: scans cached idx blocks; never suspends. */
    static void scanMemory(MqPullFrame pull, MqIdxQueue queue, MqPullFrame.QueueResult result) {
        MqIdxManager mgr = pull.mq.manager;
        RaftStatusImpl rs = pull.mq.raftStatus;
        long endSeq = queue.nextSeq;
        long seq = result.requestedSeq;
        MqIdxBlock block = queue.getBlock(seq);
        long pos = block == null ? -1 : mgr.getIdxItemInBlock(block, seq);
        if (seq == 0 || (block != null && pos < rs.firstValidPos)) {
            seq = findStartInMemory(pull, queue, endSeq);
            if (seq < 0) {
                result.stopReason = MqPullFrame.QueueResult.BLOCK_MISS;
                return;
            }
            result.fromSeq = seq;
            result.truncated = result.requestedSeq != 0;
            if (seq == endSeq) {
                return;
            }
            block = queue.getBlock(seq);
            //noinspection DataFlowIssue
            pos = mgr.getIdxItemInBlock(block, seq);
            if (result.truncated) {
                result.resetPos = pos;
                return;
            }
        } else if (block == null) {
            result.stopReason = MqPullFrame.QueueResult.BLOCK_MISS;
            return;
        }
        scanLoop(pull, queue, result, block, seq, endSeq, false);
    }

    // pos is monotonic in seq, so watermark checks only apply to a scan's first item
    private static void scanLoop(MqPullFrame pull, MqIdxQueue queue, MqPullFrame.QueueResult result,
                                 MqIdxBlock block, long seq, long endSeq, boolean collectDisk) {
        MqIdxManager mgr = pull.mq.manager;
        RaftStatusImpl rs = pull.mq.raftStatus;
        while (true) {
            long pos = mgr.getIdxItemInBlock(block, seq);
            long raftIndex = mgr.lastGetRaftIndex;
            RaftTask task = raftIndex == 0 ? null : rs.tailCache.get(raftIndex);
            if (!consumeItem(pull, result, endSeq, seq, pos, mgr.lastGetSize, task, collectDisk)) {
                return;
            }
            seq++;
            if ((seq & MqIdxBlock.BLOCK_MASK) == 0) {
                block = queue.getBlock(seq);
                if (block == null) {
                    result.stopReason = MqPullFrame.QueueResult.BLOCK_MISS;
                    return;
                }
            }
        }
    }

    /** Phase 2a: continue a TAIL_MISS queue after all phase 1 scans, adding disk items inline. */
    static void continueScan(MqPullFrame pull, MqIdxQueue queue, MqPullFrame.QueueResult result) {
        long seq = result.fromSeq + result.queueCount;
        MqIdxBlock block = queue.getBlock(seq);
        if (block == null) {
            // defensive: phase 1 read this seq without suspension; hand the queue to the idx reader
            result.stopReason = MqPullFrame.QueueResult.BLOCK_MISS;
            return;
        }
        scanLoop(pull, queue, result, block, seq, queue.nextSeq, true);
    }

    /** Same predicate as the seek below: the first seq with pos >= firstReadablePos. */
    private static long findStartInMemory(MqPullFrame pull, MqIdxQueue queue, long endSeq) {
        MqIdxManager mgr = pull.mq.manager;
        long minPos = pull.mq.raftLog.firstReadablePos();
        long low = queue.firstAvailableSeq();
        long high = endSeq;
        long seq = low;
        while (low < high) {
            MqIdxBlock block = queue.getBlock(seq);
            if (block == null) {
                return -1;
            }
            long pos = mgr.getIdxItemInBlock(block, seq);
            // a slot with no item is -1, also below minPos
            if (pos < minPos) {
                low = seq + 1;
            } else {
                high = seq;
            }
            seq = low + ((high - low) >>> 1);
        }
        return low;
    }

    private static boolean consumeItem(MqPullFrame pull, MqPullFrame.QueueResult result, long endSeq,
                                       long seq, long pos, int size, RaftTask task, boolean collectDisk) {
        if (size < LogHeader.ITEM_HEADER_SIZE) {
            throw new ChecksumException("invalid mq item size: queue=" + result.queueId + ", seq=" + seq);
        }
        if (!pull.hasCapacity()) {
            return false;
        }
        if (task == null && !collectDisk) {
            result.stopReason = MqPullFrame.QueueResult.TAIL_MISS;
            return false;
        }
        pull.add(pos, size, task);
        result.queueCount++;
        return seq + 1 != endSeq && pull.hasCapacity();
    }

    @Override
    public FrameCallResult execute(Void input) {
        seq = result.fromSeq + result.queueCount;
        if (seq == 0 || seq < queue.firstAvailableSeq()) {
            return seekStart();
        }
        return loadItem(this::afterItem);
    }

    private FrameCallResult seekStart() {
        if (result.queueCount > 0) {
            // unreachable: a miss only occurs at a scan's first item, so queueCount is 0 here
            BugLog.log("mq idx seek with collected records: queue={}, fromSeq={}, queueCount={}",
                    queue.queueId, result.fromSeq, result.queueCount);
            throw new RaftException("mq idx seek with collected records");
        }
        minPos = pull.mq.raftLog.firstReadablePos();
        low = Math.min(queue.firstAvailableSeq(), endSeq);
        high = endSeq;
        return search(null);
    }

    private FrameCallResult search(Void unused) {
        if (low == high) {
            // queueCount is always 0 here: BLOCK_MISS is set before any record is collected
            seq = low;
            result.fromSeq = seq;
            result.truncated = result.requestedSeq != 0;
            if (seq == endSeq) {
                return justReturn(true);
            }
            return loadItem(this::afterItem);
        }
        seq = low + ((high - low) >>> 1);
        return loadItem(this::searchStep);
    }

    private FrameCallResult searchStep(Void unused) {
        if (pos < minPos) {
            low = seq + 1;
        } else {
            high = seq;
        }
        return Fiber.resume(null, this::search);
    }

    // seek landings (truncated) are additionally checked against firstReadablePos, as in resetPositionsValid
    private FrameCallResult afterItem(Void unused) {
        if (pos < pull.mq.raftStatus.firstValidPos
                || (result.truncated && pos < pull.mq.raftLog.firstReadablePos())) {
            if (result.queueCount > 0) {
                return justReturn(false);
            }
            return seekStart();
        }
        if (result.truncated) {
            result.resetPos = pos;
            return justReturn(true);
        }
        return collect();
    }

    private FrameCallResult collect() {
        RaftStatusImpl rs = pull.mq.raftStatus;
        while (true) {
            RaftTask task = raftIndex == 0 ? null : rs.tailCache.get(raftIndex);
            if (!consumeItem(pull, result, endSeq, seq, pos, size, task, true)) {
                return justReturn(true);
            }
            seq++;
            if (!loadCachedItem()) {
                return loadItem(this::afterItem);
            }
        }
    }

    private boolean loadCachedItem() {
        MqIdxManager mgr = pull.mq.manager;
        MqIdxBlock b = queue.getBlock(seq);
        long cachedPos = b == null ? -1 : mgr.getIdxItemInBlock(b, seq);
        if (cachedPos >= 0) {
            pos = cachedPos;
            size = mgr.lastGetSize;
            raftIndex = mgr.lastGetRaftIndex;
            return true;
        }
        if (diskStart <= seq && seq < diskEnd) {
            decodeItem();
            return true;
        }
        return false;
    }

    private FrameCallResult loadItem(FrameCall<Void> next) {
        pull.checkCancelled();
        if (loadCachedItem()) {
            return Fiber.resume(null, next);
        }
        MqIdxFile file = queue.getLogFile(seq * MqIdxBlock.ITEM_LEN);
        if (file == null || file.isDeleted()) {
            pos = -1;
            size = 0;
            return Fiber.resume(null, next);
        }
        diskStart = seq & ~((long) MqIdxBlock.BLOCK_MASK);
        diskEnd = Math.min(diskStart + MqIdxBlock.BLOCK_ITEMS, endSeq);
        if (diskBuffer == null) {
            diskBuffer = pull.mq.groupConfig.fiberGroup.dispatcher.thread.buffers.borrowDirectLocal(
                    MqIdxBlock.DISK_BLOCK_BYTES);
        }
        ByteBuffer buf = diskBuffer.getBuffer();
        buf.clear().limit((int) (diskEnd - diskStart) * MqIdxBlock.ITEM_LEN);
        file.incReaders();
        readingFile = file;
        long offset = diskStart * MqIdxBlock.ITEM_LEN & (queue.getFileSize() - 1);
        pending = new AsyncIoTask(pull.mq.groupConfig.fiberGroup, file).read(buf, offset);
        return pending.await(v -> {
            endRead();
            pull.checkCancelled();
            decodeItem();
            return Fiber.resume(null, next);
        });
    }

    private void decodeItem() {
        raftIndex = 0;
        ByteBuffer buf = diskBuffer.getBuffer();
        int offset = (int) (seq - diskStart) * MqIdxBlock.ITEM_LEN;
        pos = buf.getLong(offset);
        long timestamp = buf.getLong(offset + 8);
        size = buf.getInt(offset + 24);
        int actualCrc = buf.getInt(offset + 28);
        // no item: a pre-allocated file that was never written
        if (pos == 0 && timestamp == 0 && size == 0 && actualCrc == 0) {
            pos = -1;
            return;
        }
        if (crc == null) {
            crc = new CRC32C();
        } else {
            crc.reset();
        }
        RaftUtil.updateCrc(crc, buf, offset, MqIdxBlock.ITEM_LEN - 4);
        if (actualCrc != (int) crc.getValue()) {
            throw new ChecksumException("mq idx crc check failed: queue=" + queue.queueId + ", seq=" + seq);
        }
        // a flushed hole has all-zero fields and a valid crc
        if (size == 0) {
            pos = -1;
        }
    }

    private void endRead() {
        if (readingFile != null) {
            readingFile.decReaders();
            readingFile = null;
        }
    }

    @Override
    protected FrameCallResult doFinally() {
        return awaitPending(null);
    }

    private FrameCallResult awaitPending(Pair<Void, Throwable> unused) {
        if (pending != null && !pending.isDone()) {
            return Fiber.call(new HandlerFrame<>(new SimpleFrame<>("waitMqIdxRead",
                    f -> pending.await(f::justReturn))), this::awaitPending);
        }
        endRead();
        if (diskBuffer != null) {
            diskBuffer.release();
            diskBuffer = null;
        }
        return Fiber.frameReturn();
    }
}
