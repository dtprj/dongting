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
package com.github.dtprj.dongting.buf;

import com.github.dtprj.dongting.common.DtBugException;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.test.Tick;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
public class BuddyBufferPoolTest {

    // pool unit tests are not about idle-list maintenance: disable async prefill (targetSize=0)
    // and async eviction (one-day timeout) so assertions are deterministic.
    private static GlobalIdleChunkList unlimited(boolean direct, int chunkSize, int minBlock) {
        return new GlobalIdleChunkList(Long.MAX_VALUE, direct, chunkSize, minBlock,
                TimeUnit.DAYS.toMillis(1), 0);
    }

    private BuddyBufferPool newPool(boolean direct, int chunkSize, int minBlock, int minChunk, int maxChunk) {
        return new BuddyBufferPool(new BuddyBufferPoolConfig(
                direct, true, new Timestamp(),
                chunkSize, minBlock, minChunk, maxChunk, 60000), unlimited(direct, chunkSize, minBlock));
    }

    @Test
    public void testBorrowSize() {
        BuddyBufferPool pool = newPool(false, 256, 16, 1, 2);
        // requestSize <= threshold(minBlock/2=8) goes unpooled with exact size
        assertEquals(1, pool.borrow(false, 1).getBuffer().capacity());
        assertEquals(16, pool.borrow(false, 16).getBuffer().capacity());
        assertEquals(32, pool.borrow(false, 17).getBuffer().capacity());
        assertEquals(256, pool.borrow(false, 256).getBuffer().capacity());
        RefBuffer big = pool.borrow(false, 257);
        assertEquals(257, big.getBuffer().capacity());
        big.release();
    }

    @Test
    public void testFullChunkReuse() {
        BuddyBufferPool pool = newPool(false, 256, 16, 1, 1);
        RefBuffer full = pool.borrow(false, 256);
        assertEquals(256, full.getBuffer().capacity());
        full.release();
        // after release the whole chunk is available again; it must be reused, not reallocated
        RefBuffer full2 = pool.borrow(false, 256);
        assertEquals(256, full2.getBuffer().capacity());
        assertTrue(pool.formatStat().contains("chunks 1("));
        assertTrue(pool.formatStat().contains("unpooled 0"));
        full2.release();
    }

    @Test
    public void testBuddyMergeAfterRelease() {
        BuddyBufferPool pool = newPool(false, 256, 16, 1, 1);
        // split chunk into two 128-byte halves
        RefBuffer a = pool.borrow(false, 128);
        RefBuffer b = pool.borrow(false, 128);
        assertEquals(128, a.getBuffer().capacity());
        assertEquals(128, b.getBuffer().capacity());
        a.release();
        b.release();
        // both halves freed -> coalesce -> a 256-byte block available again in the same chunk
        RefBuffer full = pool.borrow(false, 256);
        assertEquals(256, full.getBuffer().capacity());
        assertTrue(pool.formatStat().contains("chunks 1("));
        assertTrue(pool.formatStat().contains("unpooled 0"));
        full.release();
    }

    @Test
    public void testExhaustChunk() {
        // no shared budget: the pool cannot grow beyond maxChunkCount
        GlobalIdleChunkList noBudget = new GlobalIdleChunkList(0, false, 256, 16,
                TimeUnit.DAYS.toMillis(1), 0);
        BuddyBufferPool pool = new BuddyBufferPool(new BuddyBufferPoolConfig(
                false, true, new Timestamp(), 256, 16, 1, 1, 60000), noBudget);
        RefBuffer[] rbs = new RefBuffer[16];
        for (int i = 0; i < 16; i++) {
            rbs[i] = pool.borrow(false, 16);
            assertEquals(16, rbs[i].getBuffer().capacity());
        }
        // chunk full, maxChunk reached and shared budget exhausted: falls back to unpooled
        RefBuffer extra = pool.borrow(false, 64);
        assertEquals(64, extra.getBuffer().capacity());
        assertTrue(pool.formatStat().contains("unpooled 1"));
        for (RefBuffer rb : rbs) {
            rb.release();
        }
        extra.release();
    }

    @Test
    public void testChunkGrowth() {
        BuddyBufferPool pool = newPool(false, 256, 16, 0, 2);
        RefBuffer c1 = pool.borrow(false, 256);
        RefBuffer c2 = pool.borrow(false, 256);
        RefBuffer c3 = pool.borrow(false, 256);
        assertEquals(256, c1.getBuffer().capacity());
        assertEquals(256, c2.getBuffer().capacity());
        assertEquals(256, c3.getBuffer().capacity());
        // the third chunk is grown through the shared budget, not served as unpooled
        assertTrue(pool.formatStat().contains("newChunk 3"));
        assertTrue(pool.formatStat().contains("unpooled 0"));
        c1.release();
        c2.release();
        c3.release();
    }

    @Test
    public void testDoubleRelease() {
        BuddyBufferPool pool = newPool(false, 256, 16, 1, 1);
        ByteBuffer buf = pool.borrow(false, 16).getBuffer();
        pool.releaseBuffer(buf);
        assertThrows(DtBugException.class, () -> pool.releaseBuffer(buf));
        // detection writes to BugLog; reset immediately after the trigger
        BugLog.reset();
    }

    @Test
    public void testWriteAfterRelease() {
        BuddyBufferPool pool = newPool(false, 256, 16, 1, 1);
        ByteBuffer buf = pool.borrow(false, 16).getBuffer();
        pool.releaseBuffer(buf);
        // write through the stale reference after release corrupts the pool's MAGIC marker
        buf.put(0, (byte) 0);
        DtBugException ex = assertThrows(DtBugException.class, () -> pool.borrow(false, 16));
        // detection writes to BugLog; reset immediately after the trigger
        BugLog.reset();
        // the message must locate the corrupted block for troubleshooting
        assertTrue(ex.getMessage().contains("offset=0"));
        assertTrue(ex.getMessage().contains("chunk=0x"));
    }

    @Test
    public void testPoolUsableAfterDetection() {
        BuddyBufferPool pool = newPool(false, 256, 16, 1, 2);
        ByteBuffer buf = pool.borrow(false, 16).getBuffer();
        pool.releaseBuffer(buf);
        buf.put(0, (byte) 0);
        assertThrows(DtBugException.class, () -> pool.borrow(false, 16));
        // detection writes to BugLog; reset immediately after the trigger
        BugLog.reset();
        // the corrupted chunk is quarantined: the next borrow allocates a fresh chunk
        RefBuffer rb = pool.borrow(false, 16);
        assertEquals(16, rb.getBuffer().capacity());
        rb.release();
    }

    @Test
    public void testCorruptedChunkNotSharedAcrossPools() {
        // budget 0 so the shared idle list never allocates chunks asynchronously
        GlobalIdleChunkList shared = new GlobalIdleChunkList(0, false, 256, 16,
                TimeUnit.DAYS.toMillis(1), 0);
        Timestamp ts1 = new Timestamp();
        BuddyBufferPool poolA = new BuddyBufferPool(new BuddyBufferPoolConfig(
                false, true, ts1, 256, 16, 0, 2, 60000), shared);
        Timestamp ts2 = new Timestamp();
        BuddyBufferPool poolB = new BuddyBufferPool(new BuddyBufferPoolConfig(
                false, true, ts2, 256, 16, 0, 2, 60000), shared);

        ByteBuffer buf = poolA.borrow(false, 16).getBuffer();
        poolA.releaseBuffer(buf);
        buf.put(0, (byte) 0);
        assertThrows(DtBugException.class, () -> poolA.borrow(false, 16));
        // detection writes to BugLog; reset immediately after the trigger
        BugLog.reset();
        ts1.nanoTime += TimeUnit.MILLISECONDS.toNanos(70000);
        poolA.shrink();
        assertTrue(poolA.formatStat().contains("chunks 0("));

        // the corrupted chunk is orphaned instead of entering the shared idle list,
        // so poolB gets a fresh chunk and never observes the corruption
        RefBuffer b = poolB.borrow(false, 256);
        assertEquals(256, b.getBuffer().capacity());
        assertTrue(poolB.formatStat().contains("chunkListHit 0"));
        assertTrue(poolB.formatStat().contains("newChunk 1"));
        b.release();
    }

    @Test
    public void testReleaseIntoCorruptedChunk() {
        Timestamp ts = new Timestamp();
        BuddyBufferPool pool = new BuddyBufferPool(new BuddyBufferPoolConfig(
                false, true, ts, 256, 16, 0, 1, 100), unlimited(false, 256, 16));
        RefBuffer a = pool.borrow(false, 16);
        RefBuffer b = pool.borrow(false, 16);
        ByteBuffer staleA = a.getBuffer();
        a.release();
        // corrupt the freed block A while block B is still outstanding
        staleA.put(0, (byte) 0);
        // detection quarantines the chunk; block B remains live inside it
        assertThrows(DtBugException.class, () -> pool.borrow(false, 16));
        // detection writes to BugLog; reset immediately after the trigger
        BugLog.reset();
        // releasing the live block must still work: free() coalesces it with the corrupted
        // free block and drives the chunk back to fully free without any validation
        b.release();
        ts.nanoTime += TimeUnit.MILLISECONDS.toNanos(200);
        pool.shrink();
        assertTrue(pool.formatStat().contains("chunks 0("));
    }

    @Test
    public void testChunkStillShrinkableAfterDetection() {
        Timestamp ts = new Timestamp();
        BuddyBufferPool pool = new BuddyBufferPool(new BuddyBufferPoolConfig(
                false, true, ts, 256, 16, 0, 1, 100), unlimited(false, 256, 16));
        ByteBuffer buf = pool.borrow(false, 16).getBuffer();
        pool.releaseBuffer(buf);
        buf.put(0, (byte) 0);
        assertThrows(DtBugException.class, () -> pool.borrow(false, 16));
        // detection writes to BugLog; reset immediately after the trigger
        BugLog.reset();
        // detection must not consume the block: the chunk stays fully free and shrinkable
        ts.nanoTime += TimeUnit.MILLISECONDS.toNanos(200);
        pool.shrink();
        assertTrue(pool.formatStat().contains("chunks 0("));
    }

    @Test
    public void testCorruptedChunkReleasesBudget() {
        // one-day timeout keeps the returned chunk in the idle list
        GlobalIdleChunkList budget = new GlobalIdleChunkList(256, false, 256, 16,
                TimeUnit.DAYS.toMillis(1), 0);
        Timestamp ts = new Timestamp();
        BuddyBufferPool pool = new BuddyBufferPool(new BuddyBufferPoolConfig(
                false, true, ts, 256, 16, 0, 1, 60000), budget);
        RefBuffer a = pool.borrow(false, 16);
        ByteBuffer stale = a.getBuffer();
        a.release();
        stale.put(0, (byte) 0);
        // the hinted, fully-free chunk is detected and quarantined
        assertThrows(DtBugException.class, () -> pool.borrow(false, 256));
        BugLog.reset();
        // the second chunk consumes the only budget unit: chunks > maxChunkCount
        RefBuffer b = pool.borrow(false, 256);
        ts.nanoTime += TimeUnit.MILLISECONDS.toNanos(70000);
        pool.shrink();
        assertTrue(pool.formatStat().contains("chunks 1("));
        b.release();
        ts.nanoTime += TimeUnit.MILLISECONDS.toNanos(70000);
        pool.shrink();
        // the released chunk re-enters the idle list only if the corrupted chunk returned its budget
        RefBuffer c = pool.borrow(false, 256);
        assertEquals(256, c.getBuffer().capacity());
        assertTrue(pool.formatStat().contains("chunkListHit 1"));
        assertTrue(pool.formatStat().contains("newChunk 2"));
        c.release();
    }

    @Test
    public void testForeignBufferRejected() {
        BuddyBufferPool directPool = newPool(true, 256, 16, 0, 1);
        // foreign buffers (not from this pool) are always rejected, regardless of size or type
        assertThrows(DtBugException.class,
                () -> directPool.releaseBuffer(ByteBuffer.allocateDirect(8)));
        assertThrows(DtBugException.class,
                () -> directPool.releaseBuffer(ByteBuffer.allocateDirect(32)));

        BuddyBufferPool heapPool = newPool(false, 256, 16, 0, 1);
        assertThrows(DtBugException.class, () -> heapPool.releaseBuffer(ByteBuffer.allocate(8)));
        assertThrows(DtBugException.class, () -> heapPool.releaseBuffer(ByteBuffer.allocate(32)));
        // detection writes to BugLog; reset immediately after the trigger
        BugLog.reset();
    }

    @Test
    public void testShrinkExpiredChunk() {
        Timestamp ts = new Timestamp();
        BuddyBufferPool pool = new BuddyBufferPool(new BuddyBufferPoolConfig(
                false, true, ts, 256, 16, 1, 3, 100), unlimited(false, 256, 16));
        RefBuffer b1 = pool.borrow(false, 256);
        RefBuffer b2 = pool.borrow(false, 256);
        RefBuffer b3 = pool.borrow(false, 256);
        b1.release();
        b2.release();
        b3.release();
        // advance the pool clock past the timeout so shrink treats chunks as expired
        ts.nanoTime += TimeUnit.MILLISECONDS.toNanos(200);
        pool.shrink();
        // expired fully-free chunks beyond minChunkCount are released
        assertTrue(pool.formatStat().contains("chunks 1("));
    }

    @Test
    public void testShrinkKeepsMinChunks() {
        Timestamp ts = new Timestamp();
        BuddyBufferPool pool = new BuddyBufferPool(new BuddyBufferPoolConfig(
                false, true, ts, 256, 16, 2, 4, 100), unlimited(false, 256, 16));
        RefBuffer b1 = pool.borrow(false, 256);
        RefBuffer b2 = pool.borrow(false, 256);
        RefBuffer b3 = pool.borrow(false, 256);
        RefBuffer b4 = pool.borrow(false, 256);
        b1.release();
        b2.release();
        b3.release();
        b4.release();
        // advance the pool clock past the timeout; minChunkCount chunks are still kept
        ts.nanoTime += TimeUnit.MILLISECONDS.toNanos(200);
        pool.shrink();
        // minChunkCount=2 stops the loop, so 2 chunks are kept
        assertTrue(pool.formatStat().contains("chunks 2("));
    }

    @Test
    public void testHintClearedAfterShrink() {
        // minChunk=0 so shrink reclaims every chunk
        Timestamp ts = new Timestamp();
        BuddyBufferPool pool = new BuddyBufferPool(new BuddyBufferPoolConfig(
                true, true, ts, 256, 16, 0, 2, 100), unlimited(true, 256, 16));
        RefBuffer b1 = pool.borrow(false, 256);
        RefBuffer b2 = pool.borrow(false, 256);
        b1.release();
        b2.release();
        // advance the pool clock past the timeout so shrink reclaims all chunks
        ts.nanoTime += TimeUnit.MILLISECONDS.toNanos(200);
        pool.shrink();
        assertTrue(pool.formatStat().contains("chunks 0("));
        // after all chunks (including the hinted one) are reclaimed, the next borrow must
        // acquire a chunk from the idle list instead of the stale hint
        RefBuffer after = pool.borrow(false, 256);
        assertEquals(256, after.getBuffer().capacity());
        assertTrue(pool.formatStat().contains("chunkListHit 1"));
        // a stale hint would serve this borrow without adding a chunk back to the pool
        assertTrue(pool.formatStat().contains("chunks 1("));
        after.release();
    }

    @Test
    public void testDirectSlicesDoNotAlias() {
        BuddyBufferPool pool = newPool(true, 256, 16, 1, 1);
        RefBuffer a = pool.borrow(false, 16);
        RefBuffer b = pool.borrow(false, 16);
        ByteBuffer ba = a.getBuffer();
        ByteBuffer bb = b.getBuffer();
        ba.put(0, (byte) 0xA1);
        bb.put(0, (byte) 0xB2);
        assertEquals((byte) 0xA1, ba.get(0));
        assertEquals((byte) 0xB2, bb.get(0));
        a.release();
        b.release();
    }

    @Test
    public void testNonPositiveRequest() {
        BuddyBufferPool pool = newPool(false, 256, 16, 1, 2);
        assertThrows(IllegalArgumentException.class, () -> pool.borrow(false, 0));
        assertThrows(IllegalArgumentException.class, () -> pool.borrow(false, -1));
    }

    @Test
    public void testConcurrent() throws Exception {
        BuddyBufferPool pool = newPool(false, 1024, 16, 1, 4);
        int threads = 4;
        ExecutorService es = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicReference<Throwable> err = new AtomicReference<>();
        try {
            Runnable r = () -> {
                try {
                    Random rand = new Random(1);
                    for (int i = 0; i < 1000; i++) {
                        RefBuffer rb = pool.borrow(false, rand.nextInt(1024) + 1);
                        rb.release();
                    }
                } catch (Throwable t) {
                    err.compareAndSet(null, t);
                } finally {
                    latch.countDown();
                }
            };
            for (int i = 0; i < threads; i++) {
                es.submit(r);
            }
            assertTrue(latch.await(Tick.tick(5000), TimeUnit.MILLISECONDS));
            assertNull(err.get());
        } finally {
            es.shutdown();
        }
    }

    @Test
    public void testShareBudgetLimit() {
        // maxChunkCount=2: the first 2 chunks are free; beyond that each chunk must borrow
        // from the shared budget. budget=256 allows exactly one extra chunk.
        // targetSize=0 and a one-day timeout keep async prefill/eviction from competing for
        // the budget or removing the chunk from the idle list.
        GlobalIdleChunkList budget = new GlobalIdleChunkList(256, false, 256, 16,
                TimeUnit.DAYS.toMillis(1), 0);
        Timestamp ts = new Timestamp();
        BuddyBufferPool pool = new BuddyBufferPool(new BuddyBufferPoolConfig(
                false, true, ts, 256, 16, 0, 2, 60000), budget);
        // two chunks within maxChunkCount (no budget consumed)
        RefBuffer c1 = pool.borrow(false, 256);
        RefBuffer c2 = pool.borrow(false, 256);
        // third chunk: beyond maxChunkCount, budget allows one
        RefBuffer c3 = pool.borrow(false, 256);
        assertEquals(256, c3.getBuffer().capacity());
        // fourth: budget exhausted -> falls back to unpooled
        RefBuffer c4 = pool.borrow(false, 256);
        assertEquals(256, c4.getBuffer().capacity());
        assertTrue(pool.formatStat().contains("unpooled 1"));
        c4.release();
        // release the budgeted chunk and let shrink return it to the idle list
        c3.release();
        ts.nanoTime += TimeUnit.MILLISECONDS.toNanos(70000);
        pool.shrink();
        // c3's chunk should now be in the idle list
        RefBuffer c5 = pool.borrow(false, 256);
        assertEquals(256, c5.getBuffer().capacity());
        assertTrue(pool.formatStat().contains("chunkListHit 1"));
        assertTrue(pool.formatStat().contains("unpooled 1"));
        // c5's chunk kept its budget when it pushed the pool past maxChunkCount
        RefBuffer c6 = pool.borrow(false, 256);
        assertEquals(256, c6.getBuffer().capacity());
        assertTrue(pool.formatStat().contains("unpooled 2"));
        c6.release();
        c1.release();
        c2.release();
        c5.release();
    }

    @Test
    public void testDestroyReclaimsMinChunks() {
        Timestamp ts = new Timestamp();
        BuddyBufferPool pool = new BuddyBufferPool(new BuddyBufferPoolConfig(
                false, true, ts, 256, 16, 2, 4, 60000), unlimited(false, 256, 16));
        RefBuffer b1 = pool.borrow(false, 256);
        RefBuffer b2 = pool.borrow(false, 256);
        RefBuffer b3 = pool.borrow(false, 256);
        RefBuffer b4 = pool.borrow(false, 256);
        assertTrue(pool.formatStat().contains("chunks 4("));
        b1.release();
        b2.release();
        b3.release();
        b4.release();
        // destroy ignores minChunkCount and reclaims every fully-free chunk
        pool.destroy();
        assertTrue(pool.formatStat().contains("chunks 0("));
    }

    @Test
    public void testReleaseAfterDestroyReclaimsChunk() {
        Timestamp ts = new Timestamp();
        BuddyBufferPool pool = new BuddyBufferPool(new BuddyBufferPoolConfig(
                false, true, ts, 256, 16, 0, 2, 60000), unlimited(false, 256, 16));
        RefBuffer b1 = pool.borrow(false, 256);
        RefBuffer b2 = pool.borrow(false, 256);
        // both chunks have outstanding buffers, destroy reclaims nothing
        pool.destroy();
        assertTrue(pool.formatStat().contains("chunks 2("));
        // releasing a fully-free chunk in destroyed state triggers immediate shrink
        b1.release();
        assertTrue(pool.formatStat().contains("chunks 1("));
        b2.release();
        assertTrue(pool.formatStat().contains("chunks 0("));
    }

    @Test
    public void testIdleChunkSharedAcrossPools() {
        // two pools sharing one GlobalIdleChunkList: a chunk shrunk from poolA should be
        // reused by poolB via borrowIdleChunk, avoiding a fresh allocation.
        // targetSize=0 and a one-day timeout keep async maintenance out of the test.
        GlobalIdleChunkList shared = new GlobalIdleChunkList(Long.MAX_VALUE, false, 256, 16,
                TimeUnit.DAYS.toMillis(1), 0);
        Timestamp ts1 = new Timestamp();
        BuddyBufferPool poolA = new BuddyBufferPool(new BuddyBufferPoolConfig(
                false, true, ts1, 256, 16, 0, 2, 60000), shared);
        Timestamp ts2 = new Timestamp();
        BuddyBufferPool poolB = new BuddyBufferPool(new BuddyBufferPoolConfig(
                false, true, ts2, 256, 16, 0, 2, 60000), shared);

        // poolA allocates a chunk, then returns it to the shared idle list via shrink
        RefBuffer a = poolA.borrow(false, 256);
        a.release();
        ts1.nanoTime += TimeUnit.MILLISECONDS.toNanos(70000);
        poolA.shrink();
        assertTrue(poolA.formatStat().contains("chunks 0("));

        // poolB borrow hits the shared idle list (chunkListHit) instead of allocating fresh
        RefBuffer b = poolB.borrow(false, 256);
        assertEquals(256, b.getBuffer().capacity());
        assertTrue(poolB.formatStat().contains("chunkListHit 1"));
        b.release();
    }
}
