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

import com.github.dtprj.dongting.fiber.BaseFiberTest;
import com.github.dtprj.dongting.raft.server.ChecksumException;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.test.MockExecutors;
import com.github.dtprj.dongting.test.TestDir;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author huangli
 */
class MqIdxManagerTest extends BaseFiberTest {

    private File dir;
    private RaftGroupConfigEx config;
    private MqIdxManager manager;

    @BeforeEach
    void setup() throws Exception {
        dir = TestDir.createTestDir(MqIdxManagerTest.class.getSimpleName());
        config = newConfig();
        manager = createManager(config);
    }

    private RaftGroupConfigEx newConfig() {
        RaftGroupConfigEx c = new RaftGroupConfigEx(1, "1", "1");
        c.blockIoExecutor = MockExecutors.ioExecutor();
        RaftStatusImpl raftStatus = new RaftStatusImpl(1, dispatcher.ts);
        c.raftStatus = raftStatus;
        c.ts = raftStatus.ts;
        c.fiberGroup = fiberGroup;
        c.mqIdxItemsPerFile = 256;
        c.mqIdxCacheBlocks = 64;
        // never trigger flush in memory-only tests
        c.mqIdxFlushThreshold = Integer.MAX_VALUE;
        c.mqIdxFlushBatchItems = 128;
        return c;
    }

    private MqIdxManager createManager(RaftGroupConfigEx c) throws Exception {
        AtomicReference<MqIdxManager> ref = new AtomicReference<>();
        doInFiber(() -> ref.set(new MqIdxManager(c, dir)));
        return ref.get();
    }

    // pos = seq * 10, timestamp = seq * 100, size = seq + 1
    private void append(long queueId, long seq) {
        manager.append(queueId, seq * 10, seq * 100, (int) seq + 1);
    }

    private void assertHit(long queueId, long seq) {
        assertEquals(seq * 10, manager.getIdxItemInCache(queueId, seq));
        assertEquals(seq * 100, manager.lastGetTimestamp);
        assertEquals((int) seq + 1, manager.lastGetSize);
    }

    // a whole 4K block on disk, window starting at seqBase, same data mapping as append
    private ByteBuffer buildBlockBuffer(long seqBase) {
        ByteBuffer buf = ByteBuffer.allocate(MqIdxBlock.BLOCK_ITEMS * MqIdxManager.ITEM_LEN);
        CRC32C crc = new CRC32C();
        for (int i = 0; i < MqIdxBlock.BLOCK_ITEMS; i++) {
            long seq = seqBase + i;
            int recStart = buf.position();
            buf.putLong(seq * 10);
            buf.putLong(seq * 100);
            buf.putLong(0L);
            buf.putInt((int) seq + 1);
            crc.reset();
            crc.update(buf.array(), recStart, MqIdxManager.ITEM_LEN - 4);
            buf.putInt((int) crc.getValue());
        }
        buf.flip();
        return buf;
    }

    @Test
    void testConfigValidation() {
        assertInvalid(c -> c.mqIdxItemsPerFile = 0);
        assertInvalid(c -> c.mqIdxItemsPerFile = 100);
        assertInvalid(c -> c.mqIdxFlushBatchItems = 64);
        assertInvalid(c -> c.mqIdxFlushIntervalMillis = 0);
        assertInvalid(c -> c.mqIdxFlushAllConcurrency = 0);
        assertInvalid(c -> c.mqIdxFlushThreshold = 0);
        assertInvalid(c -> c.mqIdxCacheBlocks = 0);
    }

    private void assertInvalid(Consumer<RaftGroupConfigEx> mutator) {
        RaftGroupConfigEx c = newConfig();
        mutator.accept(c);
        // invalid configs are rejected before the flusher (and its fibers) is created
        assertThrows(IllegalArgumentException.class, () -> new MqIdxManager(c, dir));
    }

    @Test
    void testAppendAndGetIdx() {
        for (int i = 0; i < 600; i++) {
            append(1, i);
        }
        QueueIdxInfo q = manager.get(1);
        assertEquals(600, q.nextSeq);
        assertEquals(0, q.firstSeqInCache);
        assertEquals(5, q.blocks.size());
        assertEquals(0, q.blocks.get(0).startSeq);
        assertEquals(512, q.blocks.get(4).startSeq);
        assertEquals(88, q.blocks.get(4).count);

        assertHit(1, 0);
        assertHit(1, 127);
        assertHit(1, 128);
        assertHit(1, 599);
        assertEquals(-1, manager.getIdxItemInCache(2, 0));
        assertEquals(-1, manager.getIdxItemInCache(1, -1));
        assertEquals(-1, manager.getIdxItemInCache(1, 600));
        assertTrue(q.isDirty());

        append(2, 0);
        append(2, 1);
        assertEquals(2, manager.get(2).nextSeq);
        assertHit(2, 1);
        assertHit(1, 0);
    }

    @Test
    void testAppendSeqContinuity() {
        append(1, 0);
        QueueIdxInfo q = manager.get(1);
        assertThrows(IllegalArgumentException.class, () -> q.append(2, 20, 200, 3));
        assertThrows(IllegalArgumentException.class, () -> q.append(0, 0, 0, 1));
        append(1, 1);
        assertEquals(2, manager.get(1).nextSeq);
        assertHit(1, 1);
    }

    @Test
    void testRegister() {
        manager.register(1, 1000);
        QueueIdxInfo q = manager.get(1);
        assertEquals(1000, q.nextSeq);
        assertTrue(q.needLoadHead);
        assertEquals(0, q.blocks.size());
        assertEquals(896, q.firstSeqInCache);
        assertEquals(-1, manager.getIdxItemInCache(1, 999));
        assertFalse(q.isDirty());
        q.installHeadBlock(buildBlockBuffer(896));
        assertFalse(q.needLoadHead);
        assertEquals(896, q.firstSeqInCache);
        assertEquals(1, q.blocks.size());
        MqIdxBlock b = q.blocks.getFirst();
        assertEquals(896, b.startSeq);
        assertEquals(104, b.count);
        assertHit(1, 896);
        assertHit(1, 999);
        assertEquals(-1, manager.getIdxItemInCache(1, 895));
        assertEquals(-1, manager.getIdxItemInCache(1, 1000));
        // continue appending into the restored block
        for (int i = 1000; i < 1025; i++) {
            append(1, i);
        }
        assertEquals(2, q.blocks.size());
        assertEquals(1024, q.blocks.getLast().startSeq);
        assertHit(1, 1023);
        assertHit(1, 1024);

        manager.register(2, 300);
        QueueIdxInfo q2 = manager.get(2);
        q2.installHeadBlock(null);
        assertEquals(256, q2.firstSeqInCache);
        assertEquals(44, q2.blocks.getFirst().count);
        assertEquals(0, manager.getIdxItemInCache(2, 299));
        append(2, 300);
        assertHit(2, 300);

        manager.register(3, 256);
        QueueIdxInfo q3 = manager.get(3);
        assertFalse(q3.needLoadHead);
        assertEquals(256, q3.firstSeqInCache);
        assertEquals(-1, manager.getIdxItemInCache(3, 255));

        manager.register(4, 300);
        QueueIdxInfo q4 = manager.get(4);
        ByteBuffer corrupted = buildBlockBuffer(256);
        corrupted.put(40, (byte) (corrupted.get(40) + 1));
        assertThrows(ChecksumException.class, () -> q4.installHeadBlock(corrupted));
        assertTrue(q4.needLoadHead);
    }

    @Test
    void testEviction() throws Exception {
        config.mqIdxCacheBlocks = 2;
        manager = createManager(config);
        for (int i = 0; i < 600; i++) {
            append(1, i);
        }
        QueueIdxInfo q = manager.get(1);
        // over capacity, but eviction is gated by writeFinishSeq
        assertEquals(5, q.blocks.size());

        q.writeFinishSeq = 127;
        manager.evict();
        assertEquals(4, q.blocks.size());
        assertEquals(128, q.firstSeqInCache);
        assertEquals(-1, manager.getIdxItemInCache(1, 127));
        assertHit(1, 128);

        q.writeFinishSeq = 511;
        manager.evict();
        assertEquals(2, q.blocks.size());
        assertEquals(384, q.firstSeqInCache);
        assertEquals(-1, manager.getIdxItemInCache(1, 383));
        assertHit(1, 384);
        assertHit(1, 599);
    }

    @Test
    void testEvictionCrossQueue() throws Exception {
        config.mqIdxCacheBlocks = 1;
        manager = createManager(config);
        for (int i = 0; i < 128; i++) {
            append(1, i);
        }
        for (int i = 0; i < 256; i++) {
            append(2, i);
        }
        QueueIdxInfo q2 = manager.get(2);
        // global fifo: queue 1's unflushed head blocks queue 2's flushed blocks
        q2.writeFinishSeq = 255;
        manager.evict();
        assertEquals(2, q2.blocks.size());

        manager.get(1).writeFinishSeq = 127;
        manager.evict();
        assertEquals(0, manager.get(1).blocks.size());
        assertEquals(1, q2.blocks.size());
        assertEquals(128, q2.firstSeqInCache);
    }

    @Test
    void testRemove() {
        for (int i = 0; i < 256; i++) {
            append(1, i);
        }
        for (int i = 0; i < 384; i++) {
            append(2, i);
        }
        // the fifo holds sealed blocks in seal order
        long[][] expected = {{1, 0}, {1, 128}, {2, 0}, {2, 128}, {2, 256}};
        for (long[] e : expected) {
            MqIdxBlock b = manager.remove();
            assertNotNull(b);
            assertEquals(e[0], b.owner.queueId);
            assertEquals(e[1], b.startSeq);
        }
        assertNull(manager.remove());
        assertEquals(0, manager.get(1).blocks.size());
        assertEquals(256, manager.get(1).firstSeqInCache);
        assertEquals(0, manager.get(2).blocks.size());
        assertEquals(384, manager.get(2).firstSeqInCache);

        // append after the queue was fully evicted creates a new block
        append(1, 256);
        assertEquals(1, manager.get(1).blocks.size());
        assertEquals(256, manager.get(1).blocks.getLast().startSeq);
        assertEquals(-1, manager.getIdxItemInCache(1, 255));
        assertHit(1, 256);
    }
}
