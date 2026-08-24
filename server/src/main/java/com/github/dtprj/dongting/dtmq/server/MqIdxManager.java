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

import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.IndexedQueue;
import com.github.dtprj.dongting.common.LongObjMap;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.store.FileQueue;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

/**
 * Mq idx storage of a raft group, keyed by queueId: the queue map, the global block FIFO
 * and eviction. Flushing is driven by {@link MqIdxFlusher}. Not thread-safe.
 *
 * @author huangli
 */
class MqIdxManager {

    static final int ITEM_LEN = 32;

    final RaftGroupConfigEx groupConfig;
    final File dir;
    final MqIdxFlusher flusher;

    final LongObjMap<QueueIdxInfo> queues = new LongObjMap<>();

    private final int maxCachedBlocks;

    private final IndexedQueue<MqIdxBlock> fifo;
    private int totalBlockCount;

    long lastGetTimestamp;
    int lastGetSize;

    MqIdxManager(RaftGroupConfigEx groupConfig, File dir) {
        this.groupConfig = groupConfig;
        this.dir = dir;
        DtUtil.checkPositive(groupConfig.mqIdxItemsPerFile, "mqIdxItemsPerFile");
        if (groupConfig.mqIdxItemsPerFile % MqIdxBlock.BLOCK_ITEMS != 0) {
            // flushes write whole blocks, so file boundaries must align to block boundaries
            throw new IllegalArgumentException("mqIdxItemsPerFile must be a multiple of "
                    + MqIdxBlock.BLOCK_ITEMS + ": " + groupConfig.mqIdxItemsPerFile);
        }
        if (groupConfig.mqIdxFlushBatchItems < MqIdxBlock.BLOCK_ITEMS) {
            // a batch starts at the block-aligned writeFinishSeq+1, so a smaller cap
            // could not advance past a mid-block writeFinishSeq
            throw new IllegalArgumentException("mqIdxFlushBatchItems must be at least "
                    + MqIdxBlock.BLOCK_ITEMS + ": " + groupConfig.mqIdxFlushBatchItems);
        }
        DtUtil.checkPositive(groupConfig.mqIdxFlushIntervalMillis, "mqIdxFlushIntervalMillis");
        DtUtil.checkPositive(groupConfig.mqIdxFlushAllConcurrency, "mqIdxFlushAllConcurrency");
        DtUtil.checkPositive(groupConfig.mqIdxFlushThreshold, "mqIdxFlushThreshold");
        DtUtil.checkPositive(groupConfig.mqIdxCacheBlocks, "mqIdxCacheBlocks");
        this.maxCachedBlocks = groupConfig.mqIdxCacheBlocks;
        this.fifo = new IndexedQueue<>(maxCachedBlocks);
        this.flusher = new MqIdxFlusher(this);
    }

    void start() {
        flusher.start();
    }

    QueueIdxInfo get(long queueId) {
        return queues.get(queueId);
    }

    /**
     * Registers a queue from a snapshot. On restart, src holds the block read from the
     * idx file; only the first {@code nextSeq & 127} items are decoded, guaranteed on
     * disk since save-snapshot flushes before persisting nextSeq. On install, src is
     * null and the head slots of the first block stay invalid.
     * <p>
     * Only called on a fresh manager: install closes the old manager and rebuilds it
     * before re-registering, so a queue entry is never replaced in place.
     */
    QueueIdxInfo register(long queueId, long nextSeq, ByteBuffer src) {
        QueueIdxInfo q = new QueueIdxInfo(this, queueId, nextSeq);
        if (src != null) {
            if (src.limit() < MqIdxBlock.BLOCK_ITEMS * ITEM_LEN) {
                throw new IllegalArgumentException("bad buffer size " + src.limit());
            }
            decode(src, (int) (nextSeq & MqIdxBlock.BLOCK_MASK), q.blocks.getFirst());
        }
        queues.put(queueId, q);
        return q;
    }

    void append(long queueId, long seq, long pos, long timestamp, int itemSize) {
        QueueIdxInfo q = queues.get(queueId);
        if (q == null) {
            q = new QueueIdxInfo(this, queueId, 0);
            queues.put(queueId, q);
        }
        q.append(seq, pos, timestamp, itemSize);
    }

    void onSeal(MqIdxBlock b) {
        fifo.addLast(b);
    }

    void onNewBlock() {
        totalBlockCount++;
        evict();
    }

    /**
     * Returns the position, or -1 on miss (caller reads the idx file). On hit, timestamp and
     * size are exposed via lastGetTimestamp/lastGetSize.
     */
    long getIdxItemInCache(long queueId, long seq) {
        QueueIdxInfo q = queues.get(queueId);
        MqIdxBlock b = q == null ? null : q.getBlock(seq);
        if (b == null) {
            return -1;
        }
        ByteBuffer buffer = b.buffer;
        int offset = ((int) (seq & MqIdxBlock.BLOCK_MASK)) * MqIdxBlock.SLOT_SIZE;
        lastGetTimestamp = buffer.getLong(offset + 8);
        lastGetSize = buffer.getInt(offset + 16);
        return buffer.getLong(offset);
    }

    private void decode(ByteBuffer src, int itemCount, MqIdxBlock b) {
        ByteBuffer buffer = b.buffer;
        CRC32C crc = new CRC32C();
        for (int i = 0; i < itemCount; i++) {
            int recStart = i * ITEM_LEN;
            long pos = src.getLong(recStart);
            long timestamp = src.getLong(recStart + 8);
            // reserved at recStart + 16, not cached
            int itemSize = src.getInt(recStart + 24);
            RaftUtil.updateCrc(crc, src, recStart, ITEM_LEN - 4);
            int actualCrc = src.getInt(recStart + 28);
            if (actualCrc != (int) crc.getValue()) {
                throw new RaftException("mq idx crc check failed: queue=" + b.owner.queueId
                        + ", seq=" + (b.startSeq + i));
            }
            crc.reset();
            int offset = i * MqIdxBlock.SLOT_SIZE;
            buffer.putLong(offset, pos);
            buffer.putLong(offset + 8, timestamp);
            buffer.putInt(offset + 16, itemSize);
        }
    }

    /**
     * Removes and returns the fifo head block, or null. The caller must ensure the block is
     * fully flushed before dropping it.
     */
    MqIdxBlock remove() {
        MqIdxBlock b = fifo.pollFirst();
        if (b == null) {
            return null;
        }
        b.owner.removeFirst(b);
        totalBlockCount--;
        return b;
    }

    /**
     * Discards flushed blocks while over the threshold; stops at the first block not fully
     * flushed. Called when a block is created, and by the flush path when writeFinishSeq advances.
     */
    void evict() {
        while (totalBlockCount > maxCachedBlocks) {
            MqIdxBlock b = fifo.getFirst();
            if (b == null || b.owner.writeFinishSeq< b.lastSeq()) {
                return;
            }
            remove();
        }
    }

    FiberFuture<Void> close() {
        return flusher.close();
    }

    FiberFuture<Void> destroyAllBeforeInstallSnapshot() {
        return flusher.close().composeFrame("destroyMqIdx",
                v -> new FileQueue.DeleteFrame(dir, groupConfig.blockIoExecutor, true, true));
    }
}
