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

import com.github.dtprj.dongting.raft.RaftException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
class MqIdxManagerTest {

    private MqIdxManager m;

    @BeforeEach
    void setUp() {
        m = new MqIdxManager(16);
    }

    // pos equals seq, timestamp = seq * 2, size = (int) seq + 1
    private void append(MqIdxManager m, long queueId, long seq) {
        m.append(queueId, seq, seq, seq * 2, (int) seq + 1);
    }

    private void assertIdxHit(long queueId, long seq) {
        assertEquals(seq, m.getIdxItemInCache(queueId, seq));
        assertEquals(seq * 2, m.lastGetTimestamp);
        assertEquals((int) seq + 1, m.lastGetSize);
    }

    private void cleanDirty(long queueId) {
        QueueIdxInfo q = m.get(queueId);
        for (int i = 0; i < q.blocks.size(); i++) {
            q.blocks.get(i).dirty = false;
        }
    }

    @Test
    void testAppendAndIdx() {
        append(m, 1, 0);
        append(m, 1, 1);
        append(m, 1, 2);
        QueueIdxInfo q = m.get(1);
        assertEquals(3, q.nextSeq);
        assertEquals(0, q.firstSeqInCache);
        assertEquals(1, q.blocks.size());
        assertEquals(3, q.blocks.getFirst().count);
        assertFalse(q.blocks.getFirst().isFull());
        assertTrue(q.blocks.getFirst().dirty);
        assertIdxHit(1, 0);
        assertIdxHit(1, 1);
        assertIdxHit(1, 2);
    }

    @Test
    void testIdxMiss() {
        assertEquals(-1, m.getIdxItemInCache(1, 0));
        append(m, 1, 0);
        append(m, 1, 1);
        assertEquals(-1, m.getIdxItemInCache(2, 0));
        assertEquals(-1, m.getIdxItemInCache(1, -1));
        assertEquals(-1, m.getIdxItemInCache(1, 2));
    }

    @Test
    void testBlockBoundary() {
        for (int i = 0; i < 600; i++) {
            append(m, 1, i);
        }
        QueueIdxInfo q = m.get(1);
        assertEquals(600, q.nextSeq);
        assertEquals(0, q.firstSeqInCache);
        assertEquals(5, q.blocks.size());

        MqIdxBlock b0 = q.blocks.get(0);
        MqIdxBlock b4 = q.blocks.get(4);
        assertEquals(0, b0.startSeq);
        assertEquals(512, b4.startSeq);
        assertEquals(128, b0.count);
        assertEquals(88, b4.count);
        assertTrue(b0.isFull());
        assertFalse(b4.isFull());

        assertIdxHit(1, 0);
        assertIdxHit(1, 127);
        assertIdxHit(1, 128);
        assertIdxHit(1, 511);
        assertIdxHit(1, 512);
        assertIdxHit(1, 599);
        assertEquals(-1, m.getIdxItemInCache(1, 600));
    }

    @Test
    void testRestorePrefill() {
        // the whole 4K block read from file; only records before nextSeq are decoded
        m.register(1, 1000, buildBlockBuffer(896, 128));
        QueueIdxInfo q = m.get(1);
        assertEquals(896, q.firstSeqInCache);
        MqIdxBlock b = q.blocks.getFirst();
        assertEquals(896, b.startSeq);
        assertEquals(104, b.count);
        assertFalse(b.isFull());
        // restored data is already on disk
        assertFalse(b.dirty);

        assertIdxHit(1, 896);
        assertIdxHit(1, 999);
        assertEquals(-1, m.getIdxItemInCache(1, 895));
        assertEquals(-1, m.getIdxItemInCache(1, 1000));

        for (int i = 1000; i < 1024; i++) {
            append(m, 1, i);
        }
        assertTrue(b.isFull());
        append(m, 1, 1024);
        assertEquals(2, q.blocks.size());
        assertEquals(1024, q.blocks.getLast().startSeq);
        assertIdxHit(1, 1023);
        assertIdxHit(1, 1024);
    }

    @Test
    void testInstallNoPrefill() {
        m.register(1, 300, null);
        QueueIdxInfo q = m.get(1);
        assertEquals(256, q.firstSeqInCache);
        MqIdxBlock b = q.blocks.getFirst();
        assertEquals(256, b.startSeq);
        assertEquals(44, b.count);
        // slots before nextSeq hold zero data; the upper layer guards the boundary
        assertEquals(0, m.getIdxItemInCache(1, 299));

        append(m, 1, 300);
        assertIdxHit(1, 300);
        assertEquals(45, b.count);
    }

    @Test
    void testRegisterInvalidSrc() {
        // bad buffer size
        assertThrows(IllegalArgumentException.class,
                () -> m.register(2, 300, ByteBuffer.allocate(100)));
        // crc failure
        ByteBuffer buf = buildBlockBuffer(256, 44);
        buf.put(40, (byte) (buf.get(40) + 1));
        assertThrows(RaftException.class, () -> m.register(2, 300, buf));
        // aligned nextSeq: no data needed, src ignored
        m.register(3, 256, buildBlockBuffer(256, 1));
        assertEquals(256, m.get(3).firstSeqInCache);
    }

    @Test
    void testAppendSeqContinuity() {
        append(m, 1, 0);
        append(m, 1, 1);
        assertThrows(IllegalArgumentException.class, () -> append(m, 1, 3));
        assertThrows(IllegalArgumentException.class, () -> append(m, 1, 1));
        append(m, 1, 2);
        assertEquals(3, m.get(1).nextSeq);
        assertIdxHit(1, 2);
    }

    @Test
    void testEvictionKeepTailBlock() {
        m = new MqIdxManager(4);
        for (int q = 1; q <= 3; q++) {
            for (int i = 0; i < 1000; i++) {
                append(m, q, i);
            }
            // 8 blocks per queue, 24 in total; all dirty so nothing is evicted yet
            assertEquals(8, m.get(q).blocks.size());
        }
        m.evict();
        assertEquals(24, m.get(1).blocks.size() + m.get(2).blocks.size() + m.get(3).blocks.size());

        for (int q = 1; q <= 3; q++) {
            cleanDirty(q);
        }
        m.evict();
        // queue 1 and 2 drained to 1 block, queue 3 keeps 2
        assertEquals(1, m.get(1).blocks.size());
        assertEquals(896, m.get(1).firstSeqInCache);
        assertEquals(1, m.get(2).blocks.size());
        assertEquals(896, m.get(2).firstSeqInCache);
        assertEquals(2, m.get(3).blocks.size());
        assertEquals(768, m.get(3).firstSeqInCache);
        assertEquals(-1, m.getIdxItemInCache(1, 100));
        assertIdxHit(1, 896);
        assertIdxHit(1, 999);
        assertIdxHit(3, 768);
        assertEquals(-1, m.getIdxItemInCache(3, 767));
    }

    @Test
    void testEvictionGatedByDirty() {
        m = new MqIdxManager(2);
        for (int i = 0; i < 1000; i++) {
            append(m, 1, i);
        }
        assertEquals(8, m.get(1).blocks.size());
        m.evict();
        // all blocks are dirty, nothing is evictable
        assertEquals(8, m.get(1).blocks.size());

        // clean the first two blocks only
        m.get(1).blocks.get(0).dirty = false;
        m.get(1).blocks.get(1).dirty = false;
        m.evict();
        assertEquals(6, m.get(1).blocks.size());
        assertEquals(256, m.get(1).firstSeqInCache);
        assertEquals(-1, m.getIdxItemInCache(1, 255));
        assertIdxHit(1, 256);
    }

    @Test
    void testReadNoTouch() {
        m = new MqIdxManager(4);
        for (int i = 0; i < 500; i++) {
            append(m, 1, i);
        }
        for (int i = 0; i < 500; i++) {
            append(m, 2, i);
        }
        cleanDirty(1);
        cleanDirty(2);

        // reads do not affect eviction order: queue 1's block was completed first and is the victim
        assertIdxHit(1, 10);
        m.append(3, 0, 3L, 6L, 4);
        assertEquals(1, m.get(1).blocks.size());
        assertEquals(384, m.get(1).firstSeqInCache);
        assertEquals(2, m.get(2).blocks.size());
        assertEquals(256, m.get(2).firstSeqInCache);
        assertEquals(1, m.get(3).blocks.size());
    }

    @Test
    void testRemove() {
        for (int i = 0; i < 600; i++) {
            append(m, 1, i);
        }
        for (int i = 0; i < 600; i++) {
            append(m, 2, i);
        }
        // 4 full blocks + 1 append target block per queue
        assertEquals(5, m.get(1).blocks.size());
        assertEquals(5, m.get(2).blocks.size());

        MqIdxBlock b = m.remove();
        assertSame(m.get(1), b.owner);
        assertEquals(0, b.startSeq);
        assertEquals(4, m.get(1).blocks.size());
        assertEquals(128, m.get(1).firstSeqInCache);
        assertEquals(-1, m.getIdxItemInCache(1, 100));
        assertIdxHit(1, 300);

        assertEquals(128, m.remove().startSeq);
        assertEquals(3, m.get(1).blocks.size());
        assertEquals(256, m.get(1).firstSeqInCache);

        assertEquals(256, m.remove().startSeq);
        assertEquals(384, m.remove().startSeq);
        assertEquals(0, m.remove().startSeq);
        assertEquals(128, m.remove().startSeq);
        assertEquals(256, m.remove().startSeq);
        assertEquals(384, m.remove().startSeq);
        assertNull(m.remove());
        assertEquals(1, m.get(1).blocks.size());
        assertEquals(1, m.get(2).blocks.size());
    }

    @Test
    void testRegister() {
        QueueIdxInfo q = m.register(7, 100, null);
        assertSame(q, m.get(7));
        assertEquals(100, q.nextSeq);
        // the first block covers the window of nextSeq, head slots are invalid
        assertEquals(0, q.firstSeqInCache);
        assertEquals(1, q.blocks.size());
        assertEquals(0, q.blocks.getFirst().startSeq);
        assertEquals(100, q.blocks.getFirst().count);

        // append auto-creates a queue with nextSeq 0
        append(m, 2, 0);
        QueueIdxInfo q2 = m.get(2);
        assertEquals(1, q2.nextSeq);
        assertIdxHit(2, 0);
    }

    @Test
    void testAppendAfterFullEviction() {
        // 256 records: both blocks are sealed and nextSeq is block-aligned, no tail block
        for (int i = 0; i < 256; i++) {
            append(m, 1, i);
        }
        assertEquals(2, m.get(1).blocks.size());
        m.remove();
        m.remove();
        assertEquals(0, m.get(1).blocks.size());
        assertEquals(256, m.get(1).firstSeqInCache);

        // appending after the queue was fully evicted creates a new block
        append(m, 1, 256);
        assertEquals(1, m.get(1).blocks.size());
        assertEquals(256, m.get(1).blocks.getLast().startSeq);
        assertEquals(-1, m.getIdxItemInCache(1, 255));
        assertIdxHit(1, 256);
    }

    @Test
    void testAppendResetsDirty() {
        for (int i = 0; i < 100; i++) {
            append(m, 1, i);
        }
        // simulate the flush path cleaning the tail block
        m.get(1).blocks.getLast().dirty = false;
        append(m, 1, 100);
        assertTrue(m.get(1).blocks.getLast().dirty);
    }

    /**
     * Build a block buffer for the window starting at seqBase:
     * pos = seq, timestamp = seq * 2, size = seq + 1.
     */
    private ByteBuffer buildBlockBuffer(long seqBase, int count) {
        ByteBuffer buf = ByteBuffer.allocate(count * 32);
        CRC32C crc = new CRC32C();
        for (int i = 0; i < count; i++) {
            long seq = seqBase + i;
            int recStart = buf.position();
            buf.putLong(seq);
            buf.putLong(seq * 2L);
            buf.putLong(0L);
            buf.putInt((int) seq + 1);
            crc.reset();
            crc.update(buf.array(), recStart, 28);
            buf.putInt((int) crc.getValue());
        }
        buf.flip();
        return buf;
    }
}
