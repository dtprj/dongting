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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author huangli
 */
class MqIdxBlockTest {

    private MqIdxBlock newBlock(long startSeq, int count) {
        MqIdxBlock b = new MqIdxBlock(null, startSeq, 0);
        for (int i = 0; i < count; i++) {
            long seq = startSeq + i;
            b.append(seq * 10, seq * 100, (int) seq + 1);
        }
        return b;
    }

    private void assertDiskRecord(ByteBuffer buf, long seq) {
        int off = (int) seq * MqIdxManager.ITEM_LEN;
        assertEquals(seq * 10, buf.getLong(off));
        assertEquals(seq * 100, buf.getLong(off + 8));
        assertEquals(0L, buf.getLong(off + 16));
        assertEquals((int) seq + 1, buf.getInt(off + 24));
        CRC32C crc = new CRC32C();
        crc.update(buf.array(), off, MqIdxManager.ITEM_LEN - 4);
        assertEquals((int) crc.getValue(), buf.getInt(off + 28));
    }

    @Test
    void testAppend() {
        MqIdxBlock b = new MqIdxBlock(null, 128, 0);
        assertFalse(b.isFull());
        assertEquals(128 + MqIdxBlock.BLOCK_ITEMS - 1, b.lastSeq());
        for (int i = 0; i < MqIdxBlock.BLOCK_ITEMS; i++) {
            b.append(i * 10L, i * 100L, i + 1);
        }
        assertTrue(b.isFull());
        assertEquals(MqIdxBlock.BLOCK_ITEMS, b.count);
        ByteBuffer buf = b.buffer;
        for (int i = 0; i < MqIdxBlock.BLOCK_ITEMS; i++) {
            int off = i * MqIdxBlock.SLOT_SIZE;
            assertEquals(i * 10L, buf.getLong(off));
            assertEquals(i * 100L, buf.getLong(off + 8));
            assertEquals(i + 1, buf.getInt(off + 16));
        }
    }

    @Test
    void testFillBlocks() {
        MqIdxBlock b0 = newBlock(0, 128);
        MqIdxBlock b1 = newBlock(128, 100);
        ByteBuffer dest = ByteBuffer.allocate(228 * MqIdxManager.ITEM_LEN);
        QueueIdxInfo.fillBlocks(new MqIdxBlock[]{b0, b1}, 227, dest);
        assertEquals(dest.capacity(), dest.position());
        for (int i = 0; i < 228; i++) {
            assertDiskRecord(dest, i);
        }
    }

    @Test
    void testFillBlocksLastSeqBound() {
        MqIdxBlock b0 = newBlock(0, 128);
        MqIdxBlock b1 = newBlock(128, 128);
        ByteBuffer dest = ByteBuffer.allocate(151 * MqIdxManager.ITEM_LEN);
        QueueIdxInfo.fillBlocks(new MqIdxBlock[]{b0, b1}, 150, dest);
        assertEquals(dest.capacity(), dest.position());
        for (int i = 0; i <= 150; i++) {
            assertDiskRecord(dest, i);
        }
    }
}
