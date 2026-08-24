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

import java.nio.ByteBuffer;

/**
 * Slot layout (20 bytes): pos(8) + timestamp(8) + size(4). On-disk item is 32 bytes, padded
 * with 8 reserved bytes and a 4-byte CRC32C, so loading one block reads 4096 bytes.
 *
 * @author huangli
 */
final class MqIdxBlock {

    static final int BLOCK_ITEMS = 128; // 128 * 32 = 4096
    static final int BLOCK_SHIFT = Integer.numberOfTrailingZeros(BLOCK_ITEMS);
    static final int BLOCK_MASK = BLOCK_ITEMS - 1;
    static final int SLOT_SIZE = 20;
    static final int BLOCK_BYTES = BLOCK_ITEMS * SLOT_SIZE; // 2560

    final QueueIdxInfo owner;
    final long startSeq;
    int count;

    final ByteBuffer buffer = ByteBuffer.wrap(new byte[BLOCK_BYTES]);

    MqIdxBlock(QueueIdxInfo owner, long startSeq, int count) {
        this.owner = owner;
        this.startSeq = startSeq;
        this.count = count;
    }

    void append(long pos, long timestamp, int itemSize) {
        int index = count * SLOT_SIZE;
        ByteBuffer buffer = this.buffer;
        buffer.putLong(index, pos);
        buffer.putLong(index + 8, timestamp);
        buffer.putInt(index + 16, itemSize);
        count++;
    }

    boolean isFull() {
        return count == BLOCK_ITEMS;
    }

    long lastSeq() {
        return startSeq + BLOCK_ITEMS - 1;
    }
}
