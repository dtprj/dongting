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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
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
    private RaftStatusImpl raftStatus;
    private MqIdxManager manager;

    @BeforeEach
    void setup() {
        dir = TestDir.createTestDir(MqIdxFlusherTest.class.getSimpleName());
        RaftGroupConfigEx c = new RaftGroupConfigEx(1, "1", "1");
        c.blockIoExecutor = MockExecutors.ioExecutor();
        RaftStatusImpl rs = new RaftStatusImpl(1, dispatcher.ts);
        c.raftStatus = rs;
        c.ts = rs.ts;
        c.fiberGroup = fiberGroup;
        c.mqIdxItemsPerFile = 256; // 2 blocks per file
        c.mqIdxCacheBlocks = 64;
        c.mqIdxFlushThreshold = 64;
        c.mqIdxFlushBatchItems = 128;
        c.mqIdxFlushIntervalMillis = 60_000;
        config = c;
        raftStatus = rs;
    }

    private MqIdxManager createManager() throws Exception {
        AtomicReference<MqIdxManager> ref = new AtomicReference<>();
        doInFiber(() -> ref.set(new MqIdxManager(config, dir)));
        return ref.get();
    }

    // dispatcher thread only; pos = seq * 10, timestamp = seq * 100, size = seq + 1; the
    // returned future is ignored: no head load is pending at these call sites
    private void appendItems(long queueId, int fromInclusive, int toExclusive) {
        for (long seq = fromInclusive; seq < toExclusive; seq++) {
            manager.appendAsync(queueId, seq * 10, seq * 100, (int) seq + 1);
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
                manager.start();
                // the first seal requests a round; the loop starts it with target 127
                appendItems(1, 0, 128);
                MqIdxQueue q = manager.get(1);
                return Fiber.call(waitUntil(() -> q.writeFinishSeq >= 127 && !q.flushing), this::phase2);
            }

            private FrameCallResult phase2(Void v) {
                MqIdxQueue q = manager.get(1);
                assertEquals(127, q.writeFinishSeq);
                assertEquals(-1, q.forceFinishSeq);
                assertTrue(q.isDirty());
                // all three seals land before the loop wakes, so the loop batches them into
                // one round with the final target 511
                appendItems(1, 128, 512);
                return Fiber.call(waitUntil(() -> q.writeFinishSeq >= 511 && !q.flushing), this::afterFlush);
            }

            private FrameCallResult afterFlush(Void v) {
                MqIdxQueue q = manager.get(1);
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
                MqIdxQueue q1 = manager.get(1);
                MqIdxQueue q2 = manager.get(2);
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
                // stop exactly at a seal: the threshold round writes seq 0..127, no force mid-file
                appendItems(1, 0, 128);
                MqIdxQueue q = manager.get(1);
                return Fiber.call(waitUntil(() -> q.writeFinishSeq >= 127 && !q.flushing), this::afterTrigger);
            }

            private FrameCallResult afterTrigger(Void v) {
                MqIdxQueue q = manager.get(1);
                assertEquals(127, q.writeFinishSeq);
                assertEquals(-1, q.forceFinishSeq);
                // nothing new to write: flush-all targets writeFinishSeq and issues a pure force
                return manager.flusher.flushAll().await(this::afterFlushAll);
            }

            private FrameCallResult afterFlushAll(Void v) {
                MqIdxQueue q = manager.get(1);
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
                MqIdxQueue q = manager.get(1);
                return Fiber.call(waitUntil(() -> q.forceFinishSeq >= 49), this::afterFlush);
            }

            private FrameCallResult afterFlush(Void v) {
                MqIdxQueue q = manager.get(1);
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
                MqIdxQueue q = manager.get(1);
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

        // restart-like: register rewinds to nextSeq 400; initQueue attached both files
        manager = createManager();
        manager.register(1, 400);
        assertFalse(manager.get(1).needAllocateFile());
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                manager.start();
                FiberFuture<Void> load = manager.get(1).ensureHeadLoaded();
                if (load != null) {
                    return load.await(this::afterHeadLoad);
                }
                return afterHeadLoad(null);
            }

            private FrameCallResult afterHeadLoad(Void v) {
                // the head block of the nextSeq(400) window: seq 384..399, second half of file 1
                assertEquals(384, manager.get(1).firstSeqInCache);
                assertEquals(384 * 10, manager.getIdxItemInCache(1, 384));
                assertEquals(399 * 10, manager.getIdxItemInCache(1, 399));
                assertEquals(-1, manager.getIdxItemInCache(1, 383));
                assertEquals(-1, manager.getIdxItemInCache(1, 400));
                appendItems(1, 400, 401);
                return manager.flusher.flushAll().await(this::afterFlush);
            }

            private FrameCallResult afterFlush(Void v) {
                MqIdxQueue q = manager.get(1);
                assertEquals(400, q.writeFinishSeq);
                assertEquals(400, q.forceFinishSeq);
                return manager.close().await(this::justReturn);
            }
        });
        // the rewrite into the attached file is idempotent: 384..399 survive, 400 appended
        byte[] f1 = readFile(1, FILE_SIZE);
        assertRecords(f1, 256, 256, 400);
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
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                manager.start();
                // 128 pending items never cross the threshold, so no round is ever requested
                appendItems(1, 0, 128);
                MqIdxQueue q = manager.get(1);
                assertFalse(q.roundRequested);
                assertFalse(q.flushing);
                assertEquals(-1, q.writeFinishSeq);
                return manager.close().await(this::justReturn);
            }
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
    void testFlushAllExitAtAnchoredTarget() throws Exception {
        config.mqIdxFlushAllConcurrency = 1;
        manager = createManager();
        doInFiber(new FiberFrame<>() {
            private FiberFuture<Void> flushFut;

            @Override
            public FrameCallResult execute(Void input) {
                manager.start();
                appendItems(1, 0, 0, 100);
                appendItems(2, 100_000, 0, 100);
                flushFut = manager.flusher.flushAll();
                // q2 is still pending under the cap when q1 is done, so the appends below
                // land after the anchor was taken
                return Fiber.call(waitUntil(() -> manager.get(1).forceFinishSeq >= 99), this::phase2);
            }

            private FrameCallResult phase2(Void v) {
                appendItems(2, 100_000, 100, 200);
                return flushFut.await(this::afterFlushAll);
            }

            private FrameCallResult afterFlushAll(Void v) {
                assertEquals(99, manager.get(1).forceFinishSeq);
                assertTrue(manager.get(2).forceFinishSeq >= 99);
                // flushed by request-driven rounds alone, without any tick
                return Fiber.call(waitUntil(() -> manager.get(2).writeFinishSeq >= 199), this::afterTail);
            }

            private FrameCallResult afterTail(Void v) {
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
                manager.start();
                appendItems(1, 0, 384);
                // the wait condition is monotonic, so it cannot miss: by the first completed
                // batch the round is typically still in flight, and close runs under it
                MqIdxQueue q = manager.get(1);
                return Fiber.call(waitUntil(() -> q.writeFinishSeq >= 127), this::afterRoundStarted);
            }

            private FrameCallResult afterRoundStarted(Void v) {
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
                MqIdxQueue q = manager.get(1);
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

    @Test
    void testInitQueueGapFailFast() throws Exception {
        File qDir = new File(dir, "1");
        assertTrue(qDir.mkdirs());
        // two full-size files with a hole between them
        for (long startPos : new long[]{0, 2 * FILE_SIZE}) {
            try (RandomAccessFile raf = new RandomAccessFile(
                    new File(qDir, String.format("%020d", startPos)), "rw")) {
                raf.setLength(FILE_SIZE);
            }
        }
        manager = createManager();
        assertThrows(RaftException.class, () -> manager.register(1, 400));
    }

    private File idxFile(long queueId, long startPos) {
        return new File(new File(dir, String.valueOf(queueId)), String.format("%020d", startPos));
    }

    // pos = seq * 10 + base, so queues can map to disjoint log position ranges
    private void appendItems(long queueId, long posBase, int fromInclusive, int toExclusive) {
        for (long seq = fromInclusive; seq < toExclusive; seq++) {
            manager.appendAsync(queueId, seq * 10 + posBase, seq * 100, (int) seq + 1);
        }
    }

    @Test
    void testCleanupDeleteHeadFiles() throws Exception {
        manager = createManager();
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                manager.start();
                appendItems(1, 0, 0, 768);
                appendItems(2, 100_000, 0, 300);
                appendItems(3, 0, 0, 300);
                return manager.flusher.flushAll().await(this::afterFlush);
            }

            private FrameCallResult afterFlush(Void v) {
                // queue 1: f0 ends at 2550, f1 at 5110, f2 at 7670; queue 3: f0 at 2550, f1 at 2990
                raftStatus.firstValidPos = 5120;
                return Fiber.call(manager.flusher.createCleanupFrame(), this::afterCleanup);
            }

            private FrameCallResult afterCleanup(Void v) {
                return manager.close().await(this::justReturn);
            }
        });
        // queue 1: sealed heads f0/f1 deleted, f2 kept (last item 7670)
        assertFalse(idxFile(1, 0).exists());
        assertFalse(idxFile(1, FILE_SIZE).exists());
        assertTrue(idxFile(1, 2 * FILE_SIZE).exists());
        // queue 2: last item pos far above firstValidPos, kept
        assertTrue(idxFile(2, 0).exists());
        assertTrue(idxFile(2, FILE_SIZE).exists());
        // queue 3: f0 deleted as a sealed head, then the single write file (last item 2990,
        // the queue stopped writing mid-file) is deleted too
        assertFalse(idxFile(3, 0).exists());
        assertFalse(idxFile(3, FILE_SIZE).exists());
        // cached blocks of deleted files still hit
        assertEquals(100, manager.getIdxItemInCache(1, 10));
        assertEquals(1000, manager.getIdxItemInCache(1, 100));
        MqIdxQueue q1 = manager.get(1);
        assertEquals(768, q1.nextSeq);
        assertEquals(0, q1.firstSeqInCache);
    }

    @Test
    void testCleanupBoundary() throws Exception {
        manager = createManager();
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                manager.start();
                appendItems(1, 0, 0, 512);
                return manager.flusher.flushAll().await(this::afterFlush);
            }

            private FrameCallResult afterFlush(Void v) {
                // last item pos of f0 is exactly 2550: not below firstValidPos, kept
                raftStatus.firstValidPos = 2550;
                return Fiber.call(manager.flusher.createCleanupFrame(), this::afterCleanup);
            }

            private FrameCallResult afterCleanup(Void v) {
                return manager.close().await(this::justReturn);
            }
        });
        assertTrue(idxFile(1, 0).exists());
        assertTrue(idxFile(1, FILE_SIZE).exists());
    }

    @Test
    void testCleanupAfterRestart() throws Exception {
        manager = createManager();
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                manager.start();
                appendItems(1, 0, 0, 300);
                return manager.flusher.flushAll().await(this::afterFlush);
            }

            private FrameCallResult afterFlush(Void v) {
                return manager.close().await(this::justReturn);
            }
        });

        // restart-like: register rewinds to nextSeq 300; initQueue attached both files, so
        // f0 is a sealed head and f1 is the single write file
        manager = createManager();
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                manager.start();
                manager.register(1, 300);
                raftStatus.firstValidPos = 2560;
                return Fiber.call(manager.flusher.createCleanupFrame(), this::afterCleanup);
            }

            private FrameCallResult afterCleanup(Void v) {
                // f0 (last item 2550) deleted; f1 kept: lastItemPos is unknown after the
                // restart, the lazy read finds item 299 at pos 2990
                return appendPhase();
            }

            private FrameCallResult appendPhase() {
                FiberFuture<Void> load = manager.get(1).ensureHeadLoaded();
                if (load != null) {
                    return load.await(this::afterHeadLoad);
                }
                return afterHeadLoad(null);
            }

            private FrameCallResult afterHeadLoad(Void v) {
                appendItems(1, 0, 300, 768);
                return manager.flusher.flushAll().await(this::afterFlush);
            }

            private FrameCallResult afterFlush(Void v) {
                raftStatus.firstValidPos = 5120;
                return Fiber.call(manager.flusher.createCleanupFrame(), this::afterSecondCleanup);
            }

            private FrameCallResult afterSecondCleanup(Void v) {
                return manager.close().await(this::justReturn);
            }
        });
        assertFalse(idxFile(1, 0).exists());
        // f1 (last item 5110) deleted as a sealed head after the replay, f2 kept
        assertFalse(idxFile(1, FILE_SIZE).exists());
        assertTrue(idxFile(1, 2 * FILE_SIZE).exists());
        byte[] f2 = readFile(1, 2 * FILE_SIZE);
        assertRecords(f2, 512, 512, 767);
    }

    @Test
    void testPeriodicCleanup() throws Exception {
        config.mqIdxFlushIntervalMillis = 1;
        manager = createManager();
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                manager.start();
                appendItems(1, 0, 0, 512);
                return manager.flusher.flushAll().await(this::afterFlush);
            }

            private FrameCallResult afterFlush(Void v) {
                raftStatus.firstValidPos = 5120;
                // f1 is deleted strictly after f0 in the same round, so waiting on the last
                // file makes the assertions deterministic
                return Fiber.call(waitUntil(() -> !idxFile(1, FILE_SIZE).exists()), this::afterDelete);
            }

            private FrameCallResult afterDelete(Void v) {
                assertFalse(idxFile(1, 0).exists());
                return manager.close().await(this::justReturn);
            }
        });
    }

    @Test
    void testCleanupLastFileAfterStop() throws Exception {
        manager = createManager();
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                manager.start();
                appendItems(1, 0, 0, 512);
                return manager.flusher.flushAll().await(this::afterFlush);
            }

            private FrameCallResult afterFlush(Void v) {
                // both files deleted: the write point is beyond f1, both are sealed heads
                raftStatus.firstValidPos = 5120;
                return Fiber.call(manager.flusher.createCleanupFrame(), this::afterCleanup);
            }

            private FrameCallResult afterCleanup(Void v) {
                return manager.close().await(this::justReturn);
            }
        });
        assertFalse(idxFile(1, 0).exists());
        assertFalse(idxFile(1, FILE_SIZE).exists());

        // revive like a restart: the file is lazily re-created at the same name
        manager = createManager();
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                manager.start();
                manager.register(1, 512);
                appendItems(1, 0, 512, 640);
                return manager.flusher.flushAll().await(this::afterFlush);
            }

            private FrameCallResult afterFlush(Void v) {
                assertEquals(639, manager.get(1).writeFinishSeq);
                return Fiber.frameReturn();
            }
        });
        byte[] f2 = readFile(1, 2 * FILE_SIZE);
        assertRecords(f2, 512, 512, 639);

        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                // the single write file (last item 6390) is below the raised watermark
                raftStatus.firstValidPos = 6400;
                return Fiber.call(manager.flusher.createCleanupFrame(), this::afterCleanup);
            }

            private FrameCallResult afterCleanup(Void v) {
                return manager.close().await(this::justReturn);
            }
        });
        assertFalse(idxFile(1, 2 * FILE_SIZE).exists());
    }

    @Test
    void testCleanupRetryAfterFailure() throws Exception {
        manager = createManager();
        doInFiber(new FiberFrame<>() {
            private byte[] orig;

            @Override
            public FrameCallResult execute(Void input) {
                // the flush interval stays 60s during the setup, so no cleanup tick can
                // cache a good headFileLastItemPos before the file is corrupted; threshold flushes
                // do not depend on it: the round requests wake the loop directly
                manager.start();
                return appendPhase(0);
            }

            private FrameCallResult appendPhase(int from) {
                if (from >= 512) {
                    return afterFlush(null);
                }
                appendItems(1, 0, from, from + 128);
                MqIdxQueue q = manager.get(1);
                return Fiber.call(waitUntil(() -> q.writeFinishSeq >= from + 127 && !q.flushing),
                        v -> appendPhase(from + 128));
            }

            private FrameCallResult afterFlush(Void v) {
                try {
                    // corrupt the last item of f0 in place: the crc check makes the round
                    // give up on this queue
                    orig = new byte[MqIdxManager.ITEM_LEN];
                    try (RandomAccessFile raf = new RandomAccessFile(idxFile(1, 0), "rw")) {
                        raf.seek(FILE_SIZE - MqIdxManager.ITEM_LEN);
                        raf.readFully(orig);
                        byte[] bad = orig.clone();
                        for (int i = 0; i < bad.length; i++) {
                            bad[i] ^= 0x5a;
                        }
                        raf.seek(FILE_SIZE - MqIdxManager.ITEM_LEN);
                        raf.write(bad);
                    }
                } catch (IOException e) {
                    throw new RaftException(e);
                }
                raftStatus.firstValidPos = 5120;
                return Fiber.call(manager.flusher.createCleanupFrame(), this::afterFailedRound);
            }

            private FrameCallResult afterFailedRound(Void v) {
                // the failed round deleted nothing and scheduled a retry
                assertTrue(manager.get(1).lastCleanupFailed);
                assertTrue(idxFile(1, 0).exists());
                try {
                    try (RandomAccessFile raf = new RandomAccessFile(idxFile(1, 0), "rw")) {
                        raf.seek(FILE_SIZE - MqIdxManager.ITEM_LEN);
                        raf.write(orig);
                    }
                } catch (IOException e) {
                    throw new RaftException(e);
                }
                // firstValidPos does not move again: shrink the interval and wake the loop,
                // the retry alone must delete the files
                config.mqIdxFlushIntervalMillis = 1;
                return manager.flusher.flushAll().await(this::afterRetryKick);
            }

            private FrameCallResult afterRetryKick(Void v) {
                return Fiber.call(waitUntil(() -> !idxFile(1, FILE_SIZE).exists()), this::afterRetry);
            }

            private FrameCallResult afterRetry(Void v) {
                assertFalse(manager.get(1).lastCleanupFailed);
                assertFalse(idxFile(1, 0).exists());
                return manager.close().await(this::justReturn);
            }
        });
    }
}
