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

import com.github.dtprj.dongting.common.IndexedQueue;

/**
 * Blocks are held in seq order: the head block covers firstSeqInCache, the last block is the
 * append target.
 *
 * @author huangli
 */
final class QueueIdxInfo {

    final MqIdxManager manager;
    final long queueId;

    // cannot be rebuilt from raft logs, must be saved into snapshots
    long nextSeq;

    long firstSeqInCache;

    final IndexedQueue<MqIdxBlock> blocks = new IndexedQueue<>(2);

    QueueIdxInfo(MqIdxManager manager, long queueId) {
        this(manager, queueId, 0);
    }

    // restore: the first block covers the window of nextSeq; slots before nextSeq hold no
    // valid data (install), or are filled by register() from the idx file (restart)
    QueueIdxInfo(MqIdxManager manager, long queueId, long nextSeq) {
        this.manager = manager;
        this.queueId = queueId;
        this.nextSeq = nextSeq;
        long startSeq = nextSeq & ~((long) MqIdxBlock.BLOCK_MASK);
        this.firstSeqInCache = startSeq;
        blocks.addLast(new MqIdxBlock(this, startSeq, (int) (nextSeq - startSeq)));
        manager.onNewBlock();
    }

    MqIdxBlock getBlock(long seq) {
        if (seq < firstSeqInCache || seq >= nextSeq) {
            return null;
        }
        int index = (int) ((seq >>> MqIdxBlock.BLOCK_SHIFT)
                - (firstSeqInCache >>> MqIdxBlock.BLOCK_SHIFT));
        return blocks.get(index);
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
        MqIdxBlock b = blocks.getLast();
        if (b == null || b.isFull()) {
            // null: the queue was fully evicted (only happens when nextSeq is block-aligned)
            manager.onNewBlock();
            b = new MqIdxBlock(this, nextSeq, 0);
            blocks.addLast(b);
        }
        b.append(pos, timestamp, itemSize);
        nextSeq++;
        if (b.isFull()) {
            manager.onSeal(b);
        }
    }
}
