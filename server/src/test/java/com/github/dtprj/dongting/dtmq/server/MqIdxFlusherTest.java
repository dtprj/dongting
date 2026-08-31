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
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.test.MockExecutors;
import com.github.dtprj.dongting.test.TestDir;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
class MqIdxFlusherTest extends BaseFiberTest {

    private static final long FILE_SIZE = 256L * MqIdxManager.ITEM_LEN;

    private File dir;
    private RaftGroupConfigEx config;
    private MqIdxManager manager;

    @BeforeEach
    void setup() {
        dir = TestDir.createTestDir(MqIdxFlusherTest.class.getSimpleName());
        RaftGroupConfigEx c = new RaftGroupConfigEx(1, "1", "1");
        c.blockIoExecutor = MockExecutors.ioExecutor();
        RaftStatusImpl raftStatus = new RaftStatusImpl(1, dispatcher.ts);
        c.raftStatus = raftStatus;
        c.ts = raftStatus.ts;
        c.fiberGroup = fiberGroup;
        c.mqIdxItemsPerFile = 256; // 2 blocks per file
        c.mqIdxCacheBlocks = 64;
        c.mqIdxFlushThreshold = 64;
        c.mqIdxFlushBatchItems = 128;
        c.mqIdxFlushIntervalMillis = 60_000;
        config = c;
    }

    private MqIdxManager createManager() throws Exception {
        AtomicReference<MqIdxManager> ref = new AtomicReference<>();
        doInFiber(() -> ref.set(new MqIdxManager(config, dir)));
        return ref.get();
    }

    // dispatcher thread only; pos = seq * 10, timestamp = seq * 100, size = seq + 1
    private void appendItems(long queueId, int fromInclusive, int toExclusive) {
        for (long seq = fromInclusive; seq < toExclusive; seq++) {
            manager.append(queueId, seq * 10, seq * 100, (int) seq + 1);
        }
    }

    private FiberFrame<Void> waitUntil(BooleanSupplier cond) {
        return new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                if (cond.getAsBoolean()) {
                    return Fiber.frameReturn();
                }
                return Fiber.sleep(1, this);
            }
        };
    }

    private byte[] readFile(long queueId, long startPos) throws Exception {
        File f = new File(new File(dir, String.valueOf(queueId)), String.format("%020d", startPos));
        assertTrue(f.exists());
        return Files.readAllBytes(f.toPath());
    }

    private void assertRecords(byte[] data, long fileStartSeq, long fromSeq, long toSeq) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        CRC32C crc = new CRC32C();
        for (long seq = fromSeq; seq <= toSeq; seq++) {
            int off = (int) (seq - fileStartSeq) * MqIdxManager.ITEM_LEN;
            assertEquals(seq * 10, buf.getLong(off), "pos of seq " + seq);
            assertEquals(seq * 100, buf.getLong(off + 8), "timestamp of seq " + seq);
            assertEquals(0L, buf.getLong(off + 16), "reserved of seq " + seq);
            assertEquals((int) seq + 1, buf.getInt(off + 24), "size of seq " + seq);
            crc.reset();
            crc.update(data, off, MqIdxManager.ITEM_LEN - 4);
            assertEquals((int) crc.getValue(), buf.getInt(off + 28), "crc of seq " + seq);
        }
    }

    @Test
    void testTriggerFlush() throws Exception {
        manager = createManager();
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                appendItems(1, 0, 384);
                QueueIdxInfo q = manager.get(1);
                // the first round starts at the first block seal with target 127; later seals
                // land while it is in flight and are left to the next seal
                return Fiber.call(waitUntil(() -> q.writeFinishSeq >= 127 && !q.flushing), this::phase2);
            }

            private FrameCallResult phase2(Void v) {
                QueueIdxInfo q = manager.get(1);
                assertEquals(127, q.writeFinishSeq);
                assertEquals(-1, q.forceFinishSeq);
                assertTrue(q.isDirty());
                appendItems(1, 384, 512);
                return Fiber.call(waitUntil(() -> q.writeFinishSeq >= 511 && !q.flushing), this::afterFlush);
            }

            private FrameCallResult afterFlush(Void v) {
                QueueIdxInfo q = manager.get(1);
                assertEquals(511, q.writeFinishSeq);
                // both files are completed by the round, and a file-completing batch always forces
                assertEquals(511, q.forceFinishSeq);
                assertFalse(q.isDirty());
                return manager.close().await(this::justReturn);
            }
        });
        byte[] f0 = readFile(1, 0);
        assertEquals(FILE_SIZE, f0.length);
        assertRecords(f0, 0, 0, 255);
        byte[] f1 = readFile(1, FILE_SIZE);
        assertEquals(FILE_SIZE, f1.length);
        assertRecords(f1, 256, 256, 511);
    }

    @Test
    void testFlushAll() throws Exception {
        manager = createManager();
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                manager.start();
                appendItems(1, 0, 100);
                appendItems(2, 0, 300);
                return manager.flusher.flushAll().await(this::afterFlushAll);
            }

            private FrameCallResult afterFlushAll(Void v) {
                QueueIdxInfo q1 = manager.get(1);
                QueueIdxInfo q2 = manager.get(2);
                assertEquals(99, q1.writeFinishSeq);
                assertEquals(99, q1.forceFinishSeq);
                assertEquals(299, q2.writeFinishSeq);
                assertEquals(299, q2.forceFinishSeq);
                assertFalse(q1.isDirty());
                assertFalse(q2.isDirty());
                // nothing dirty, the second flush-all finishes without any io
                return manager.flusher.flushAll().await(this::afterSecond);
            }

            private FrameCallResult afterSecond(Void v) {
                return manager.close().await(this::justReturn);
            }
        });
        byte[] q1f0 = readFile(1, 0);
        assertRecords(q1f0, 0, 0, 99);
        byte[] q2f0 = readFile(2, 0);
        assertRecords(q2f0, 0, 0, 255);
        byte[] q2f1 = readFile(2, FILE_SIZE);
        assertRecords(q2f1, 256, 256, 299);
    }

    @Test
    void testFlushAllPureForce() throws Exception {
        manager = createManager();
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                manager.start();
                // stop exactly at a seal: the trigger round writes seq 0..127, no force mid-file
                appendItems(1, 0, 128);
                QueueIdxInfo q = manager.get(1);
                return Fiber.call(waitUntil(() -> q.writeFinishSeq >= 127 && !q.flushing), this::afterTrigger);
            }

            private FrameCallResult afterTrigger(Void v) {
                QueueIdxInfo q = manager.get(1);
                assertEquals(127, q.writeFinishSeq);
                assertEquals(-1, q.forceFinishSeq);
                // nothing new to write: flush-all targets writeFinishSeq and issues a pure force
                return manager.flusher.flushAll().await(this::afterFlushAll);
            }

            private FrameCallResult afterFlushAll(Void v) {
                QueueIdxInfo q = manager.get(1);
                assertEquals(127, q.writeFinishSeq);
                assertEquals(127, q.forceFinishSeq);
                assertFalse(q.isDirty());
                return manager.close().await(this::justReturn);
            }
        });
    }

    @Test
    void testPeriodicFlush() throws Exception {
        config.mqIdxFlushIntervalMillis = 1;
        manager = createManager();
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                manager.start();
                appendItems(1, 0, 50);
                QueueIdxInfo q = manager.get(1);
                return Fiber.call(waitUntil(() -> q.forceFinishSeq >= 49), this::afterFlush);
            }

            private FrameCallResult afterFlush(Void v) {
                QueueIdxInfo q = manager.get(1);
                assertEquals(49, q.writeFinishSeq);
                return manager.close().await(this::justReturn);
            }
        });
    }

    @Test
    void testEvictionAfterFlush() throws Exception {
        config.mqIdxCacheBlocks = 1;
        manager = createManager();
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                manager.start();
                appendItems(1, 0, 600);
                return manager.flusher.flushAll().await(this::afterFlush);
            }

            private FrameCallResult afterFlush(Void v) {
                QueueIdxInfo q = manager.get(1);
                assertEquals(599, q.writeFinishSeq);
                // the fifo is evicted down to the cache limit; the tail block is not counted
                assertEquals(2, q.blocks.size());
                assertEquals(384, q.firstSeqInCache);
                assertEquals(-1, manager.getIdxItemInCache(1, 383));
                assertEquals(384 * 10, manager.getIdxItemInCache(1, 384));
                assertEquals(512 * 10, manager.getIdxItemInCache(1, 512));
                return manager.close().await(this::justReturn);
            }
        });
    }

    @Test
    void testRestore() throws Exception {
        manager = createManager();
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                manager.start();
                appendItems(1, 0, 400);
                return manager.flusher.flushAll().await(this::afterFlush);
            }

            private FrameCallResult afterFlush(Void v) {
                return manager.close().await(this::justReturn);
            }
        });

        // the block of the nextSeq(400) window: seq 384..511, second half of file 1
        byte[] block = Arrays.copyOfRange(readFile(1, FILE_SIZE), 4096, 8192);
        manager = createManager();
        manager.register(1, 400);
        QueueIdxInfo q = manager.get(1);
        q.installHeadBlock(ByteBuffer.wrap(block));
        assertEquals(384, q.firstSeqInCache);
        assertEquals(384 * 10, manager.getIdxItemInCache(1, 384));
        assertEquals(399 * 10, manager.getIdxItemInCache(1, 399));
        assertEquals(-1, manager.getIdxItemInCache(1, 383));
        assertEquals(-1, manager.getIdxItemInCache(1, 400));

        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                manager.start();
                appendItems(1, 400, 401);
                return manager.flusher.flushAll().await(this::afterFlush);
            }

            private FrameCallResult afterFlush(Void v) {
                QueueIdxInfo q = manager.get(1);
                assertEquals(400, q.writeFinishSeq);
                assertEquals(400, q.forceFinishSeq);
                return manager.close().await(this::justReturn);
            }
        });
        byte[] f1 = readFile(1, FILE_SIZE);
        assertRecords(f1, 256, 384, 400);
    }

    @Test
    void testCloseGuards() throws Exception {
        manager = createManager();
        doInFiber(new FiberFrame<>() {
            private FiberFuture<Void> closeFuture;

            @Override
            public FrameCallResult execute(Void input) {
                manager.start();
                appendItems(1, 0, 10);
                closeFuture = manager.close();
                return closeFuture.await(this::afterClose);
            }

            private FrameCallResult afterClose(Void v) {
                assertSame(closeFuture, manager.close());
                FiberFuture<Void> f = manager.flusher.flushAll();
                assertTrue(f.isDone());
                assertInstanceOf(RaftException.class, f.getEx());
                return Fiber.frameReturn();
            }
        });
    }

    @Test
    void testBelowThresholdNoFlush() throws Exception {
        config.mqIdxFlushThreshold = 200;
        manager = createManager();
        doInFiber(() -> {
            appendItems(1, 0, 128);
            QueueIdxInfo q = manager.get(1);
            assertFalse(q.flushing);
            assertEquals(-1, q.writeFinishSeq);
        });
    }

    @Test
    void testFlushAllConcurrencyCap() throws Exception {
        config.mqIdxFlushAllConcurrency = 1;
        manager = createManager();
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                manager.start();
                appendItems(1, 0, 100);
                appendItems(2, 0, 100);
                return manager.flusher.flushAll().await(this::afterFlushAll);
            }

            private FrameCallResult afterFlushAll(Void v) {
                // the second queue is deferred by the concurrency cap, both complete eventually
                assertEquals(99, manager.get(1).forceFinishSeq);
                assertEquals(99, manager.get(2).forceFinishSeq);
                return manager.close().await(this::justReturn);
            }
        });
    }

    @Test
    void testCloseWithActiveRound() throws Exception {
        manager = createManager();
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                appendItems(1, 0, 384);
                QueueIdxInfo q = manager.get(1);
                // the round started synchronously at the first seal and its io cannot complete
                // on the dispatcher thread before this fiber suspends
                assertTrue(q.flushing);
                return manager.close().await(this::afterClose);
            }

            private FrameCallResult afterClose(Void v) {
                // close waits for the in-flight round to end, no hang
                assertFalse(manager.get(1).flushing);
                return Fiber.frameReturn();
            }
        });
    }

    @Test
    void testFlushAllFailAfterRetryExhausted() throws Exception {
        config.ioRetryInterval = new int[]{1, 1};
        // a dedicated group, because retry exhaustion shuts down the group
        FiberGroup g = new FiberGroup("mqFlushFail", dispatcher);
        dispatcher.startGroup(g).get();
        config.fiberGroup = g;
        manager = new MqIdxManager(config, dir);
        CompletableFuture<Void> testDone = new CompletableFuture<>();
        g.fireFiber("test", new FiberFrame<>() {
            FiberFuture<Void> flushFut;

            @Override
            public FrameCallResult execute(Void input) {
                manager.start();
                appendItems(1, 0, 300);
                // occupy the second idx file path with a directory, so allocation keeps failing
                File bad = new File(new File(dir, "1"), String.format("%020d", FILE_SIZE));
                assertTrue(bad.mkdirs());
                flushFut = manager.flusher.flushAll();
                return Fiber.call(waitUntil(flushFut::isDone), this::afterFail);
            }

            private FrameCallResult afterFail(Void v) {
                assertInstanceOf(RaftException.class, flushFut.getEx());
                FiberFuture<Void> f2 = manager.flusher.flushAll();
                assertTrue(f2.isDone());
                assertInstanceOf(RaftException.class, f2.getEx());
                testDone.complete(null);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                testDone.completeExceptionally(ex);
                return Fiber.frameReturn();
            }
        });
        try {
            testDone.get(5, TimeUnit.SECONDS);
            g.shutdownFuture.get(5, TimeUnit.SECONDS);
        } finally {
            config.fiberGroup = fiberGroup;
        }
    }

    @Test
    void testFlowControl() throws Exception {
        config.mqIdxCacheBlocks = 1;
        manager = createManager();
        doInFiber(new FiberFrame<>() {
            private final ArrayList<FiberFuture<Void>> futures = new ArrayList<>();

            @Override
            public FrameCallResult execute(Void input) {
                manager.start();
                // appends run far ahead of the async io, so the shared block future engages
                for (long seq = 0; seq < 400; seq++) {
                    futures.add(manager.appendAsync(1, seq * 10, seq * 100, (int) seq + 1));
                }
                QueueIdxInfo q = manager.get(1);
                return Fiber.call(waitUntil(() -> q.writeFinishSeq >= 399), this::afterFlush);
            }

            private FrameCallResult afterFlush(Void v) {
                for (FiberFuture<Void> f : futures) {
                    assertTrue(f.isDone());
                    assertNull(f.getEx());
                }
                assertEquals(400, manager.get(1).nextSeq);
                return manager.close().await(this::justReturn);
            }
        });
        byte[] f0 = readFile(1, 0);
        assertRecords(f0, 0, 0, 255);
        byte[] f1 = readFile(1, FILE_SIZE);
        assertRecords(f1, 256, 256, 399);
    }

    @Test
    void testDestroy() throws Exception {
        manager = createManager();
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                manager.start();
                appendItems(1, 0, 200);
                return manager.flusher.flushAll().await(this::afterFlush);
            }

            private FrameCallResult afterFlush(Void v) {
                return manager.destroyAllBeforeInstallSnapshot().await(this::justReturn);
            }
        });
        assertFalse(new File(dir, "1").exists());
        assertTrue(dir.exists());
    }
}
