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
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.ChecksumException;
import com.github.dtprj.dongting.raft.store.AsyncIoTask;
import com.github.dtprj.dongting.raft.store.LogHeader;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

final class MqIdxReader extends FiberFrame<Void> {
    private final MqPullFrame pull;
    private final MqIdxQueue queue;
    private final MqPullFrame.QueueResult result;
    private final long endSeq;
    private CRC32C crc;

    private long low;
    private long high;
    private long minPos;
    private long seq;
    private long pos;
    private long raftIndex;
    private int size;
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

    @Override
    public FrameCallResult execute(Void input) {
        seq = result.requestedSeq;
        if (seq >= endSeq) {
            return Fiber.frameReturn();
        }
        if (seq == 0 || seq < queue.firstAvailableSeq()) {
            return seekStart();
        }
        return loadItem(this::checkStart);
    }

    private FrameCallResult checkStart(Void unused) {
        if (size == 0 || pos < pull.raftStatus.firstValidPos) {
            return seekStart();
        }
        return collect(null);
    }

    private FrameCallResult seekStart() {
        minPos = pull.raftLog.firstReadablePos();
        low = Math.min(queue.firstAvailableSeq(), endSeq);
        high = endSeq;
        return search(null);
    }

    private FrameCallResult search(Void unused) {
        if (low == high) {
            seq = low;
            result.fromSeq = seq;
            result.nextSeq = seq;
            result.truncated = result.requestedSeq != 0;
            if (seq == endSeq) {
                return Fiber.frameReturn();
            }
            return loadItem(this::afterSearch);
        }
        seq = low + ((high - low) >>> 1);
        return loadItem(this::searchStep);
    }

    private FrameCallResult searchStep(Void unused) {
        if (size == 0 || pos < minPos) {
            low = seq + 1;
        } else {
            high = seq;
        }
        return Fiber.resume(null, this::search);
    }

    private FrameCallResult afterSearch(Void unused) {
        if (size == 0 || pos < pull.raftLog.firstReadablePos()) {
            return seekStart();
        }
        if (result.truncated) {
            result.resetPos = pos;
            return Fiber.frameReturn();
        }
        return collect(null);
    }

    private FrameCallResult collect(Void unused) {
        while (true) {
            if (size == 0 || pos < pull.raftStatus.firstValidPos) {
                pull.retry = true;
                return Fiber.frameReturn();
            }
            if (size < LogHeader.ITEM_HEADER_SIZE) {
                throw new ChecksumException("invalid mq item size: queue=" + queue.queueId + ", seq=" + seq);
            }
            if (!pull.add(pos, size, raftIndex)) {
                if (size > pull.bytesLimit) {
                    result.requiredBytes = size;
                }
                return Fiber.frameReturn();
            }
            result.count++;
            result.nextSeq = ++seq;
            if (seq == endSeq || pull.count == pull.limit) {
                return Fiber.frameReturn();
            }
            if (!loadCachedItem()) {
                return loadItem(this::collect);
            }
        }
    }

    private boolean loadCachedItem() {
        pos = pull.manager.getIdxItemInCache(queue.queueId, seq);
        if (pos >= 0) {
            size = pull.manager.lastGetSize;
            raftIndex = pull.manager.lastGetRaftIndex;
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
        MqIdxFile file = queue.getLogFile(seq * MqIdxManager.ITEM_LEN);
        if (file == null || file.isDeleted()) {
            pos = -1;
            size = 0;
            return Fiber.resume(null, next);
        }
        diskStart = seq & ~((long) MqIdxBlock.BLOCK_MASK);
        diskEnd = Math.min(diskStart + MqIdxBlock.BLOCK_ITEMS, endSeq);
        if (diskBuffer == null) {
            diskBuffer = pull.groupConfig.fiberGroup.dispatcher.thread.buffers.borrowDirectLocal(
                    MqIdxBlock.BLOCK_ITEMS * MqIdxManager.ITEM_LEN);
        }
        ByteBuffer buf = diskBuffer.getBuffer();
        buf.clear().limit((int) (diskEnd - diskStart) * MqIdxManager.ITEM_LEN);
        file.incReaders();
        readingFile = file;
        long offset = diskStart * MqIdxManager.ITEM_LEN & (queue.getFileSize() - 1);
        pending = new AsyncIoTask(pull.groupConfig.fiberGroup, file).read(buf, offset);
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
        int offset = (int) (seq - diskStart) * MqIdxManager.ITEM_LEN;
        // Snapshot recovery can leave whole, unwritten blocks before the append point.
        if (buf.getLong(offset) == 0 && buf.getLong(offset + 8) == 0
                && buf.getLong(offset + 16) == 0 && buf.getLong(offset + 24) == 0) {
            pos = -1;
            size = 0;
            return;
        }
        if (crc == null) {
            crc = new CRC32C();
        } else {
            crc.reset();
        }
        RaftUtil.updateCrc(crc, buf, offset, MqIdxManager.ITEM_LEN - 4);
        if (buf.getInt(offset + 28) != (int) crc.getValue()) {
            throw new ChecksumException("mq idx crc check failed: queue=" + queue.queueId + ", seq=" + seq);
        }
        pos = buf.getLong(offset);
        size = buf.getInt(offset + 24);
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
            return Fiber.call(new HandlerFrame<>(new SimpleFrame<Void>("waitMqIdxRead",
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
