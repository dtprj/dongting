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

    private BuddyBufferPool newPool(boolean direct, int chunkSize, int minBlock, int minChunk, int maxChunk) {
        return new BuddyBufferPool(new BuddyBufferPoolConfig(direct, chunkSize, minBlock, minChunk, maxChunk, 60000));
    }

    @Test
    public void testBorrowSize() {
        BuddyBufferPool pool = newPool(false, 256, 16, 1, 2);
        // requestSize <= threshold(minBlock/2=8) goes unpooled with exact size
        assertEquals(1, pool.borrow(false, 1, 0).getBuffer().capacity());
        assertEquals(16, pool.borrow(false, 16, 0).getBuffer().capacity());
        assertEquals(32, pool.borrow(false, 17, 0).getBuffer().capacity());
        assertEquals(256, pool.borrow(false, 256, 0).getBuffer().capacity());
        RefBuffer big = pool.borrow(false, 257, 0);
        assertEquals(257, big.getBuffer().capacity());
        big.release();
    }

    @Test
    public void testSplitAndCoalesce() {
        BuddyBufferPool pool = newPool(false, 256, 16, 1, 1);
        RefBuffer full = pool.borrow(false, 256, 0);
        assertEquals(256, full.getBuffer().capacity());
        full.release();
        // after release the whole chunk coalesces back; a full-chunk borrow must succeed again
        RefBuffer full2 = pool.borrow(false, 256, 0);
        assertEquals(256, full2.getBuffer().capacity());
        full2.release();
    }

    @Test
    public void testBuddyMergeAfterRelease() {
        BuddyBufferPool pool = newPool(false, 256, 16, 1, 1);
        // split chunk into two 128-byte halves
        RefBuffer a = pool.borrow(false, 128, 0);
        RefBuffer b = pool.borrow(false, 128, 0);
        assertEquals(128, a.getBuffer().capacity());
        assertEquals(128, b.getBuffer().capacity());
        a.release();
        b.release();
        // both halves freed -> coalesce -> a 256-byte block available again
        RefBuffer full = pool.borrow(false, 256, 0);
        assertEquals(256, full.getBuffer().capacity());
        full.release();
    }

    @Test
    public void testExhaustChunk() {
        BuddyBufferPool pool = newPool(false, 256, 16, 1, 1);
        RefBuffer[] rbs = new RefBuffer[16];
        for (int i = 0; i < 16; i++) {
            rbs[i] = pool.borrow(false, 16, 0);
            assertEquals(16, rbs[i].getBuffer().capacity());
        }
        // chunk full, maxChunk reached: a following pooled-size request falls back to unpooled
        RefBuffer extra = pool.borrow(false, 64, 0);
        assertEquals(64, extra.getBuffer().capacity());
        for (RefBuffer rb : rbs) {
            rb.release();
        }
        extra.release();
    }

    @Test
    public void testChunkGrowth() {
        BuddyBufferPool pool = newPool(false, 256, 16, 0, 2);
        RefBuffer c1 = pool.borrow(false, 256, 0);
        RefBuffer c2 = pool.borrow(false, 256, 0);
        RefBuffer c3 = pool.borrow(false, 256, 0);
        assertEquals(256, c1.getBuffer().capacity());
        assertEquals(256, c2.getBuffer().capacity());
        assertEquals(256, c3.getBuffer().capacity());
        c1.release();
        c2.release();
        c3.release();
    }

    @Test
    public void testDoubleRelease() {
        BuddyBufferPool pool = newPool(false, 256, 16, 1, 1);
        ByteBuffer buf = pool.borrow(false, 16, 0).getBuffer();
        pool.releaseBuffer(buf);
        assertThrows(DtBugException.class, () -> pool.releaseBuffer(buf));
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
    }

    @Test
    public void testShrinkExpiredChunk() {
        BuddyBufferPool pool = new BuddyBufferPool(new BuddyBufferPoolConfig(
                false, 256, 16, 1, 3, 100));
        RefBuffer b1 = pool.borrow(false, 256, 0);
        RefBuffer b2 = pool.borrow(false, 256, 0);
        RefBuffer b3 = pool.borrow(false, 256, 0);
        b1.release();
        b2.release();
        b3.release();
        // advance the pool clock past the timeout so shrink treats chunks as expired
        pool.ts.nanoTime += TimeUnit.MILLISECONDS.toNanos(200);
        pool.shrink();
        // expired fully-free chunks beyond minChunkCount are released
        assertTrue(pool.formatStat().contains("chunks 1("));
    }

    @Test
    public void testShrinkKeepsMinChunks() {
        BuddyBufferPool pool = new BuddyBufferPool(new BuddyBufferPoolConfig(
                false, 256, 16, 2, 4, 100));
        RefBuffer b1 = pool.borrow(false, 256, 0);
        RefBuffer b2 = pool.borrow(false, 256, 0);
        b1.release();
        b2.release();
        // advance the pool clock past the timeout; minChunkCount chunks are still kept
        pool.ts.nanoTime += TimeUnit.MILLISECONDS.toNanos(200);
        pool.shrink();
        // minChunkCount=2 preallocated chunks are never reclaimed
        assertTrue(pool.formatStat().contains("chunks 2("));
    }

    @Test
    public void testHintClearedAfterShrink() {
        // minChunk=0 so shrink reclaims every chunk; direct=true so an uncleared hint would point
        // at a freed direct buffer and corrupt the next borrow.
        BuddyBufferPool pool = new BuddyBufferPool(new BuddyBufferPoolConfig(
                true, 256, 16, 0, 2, 100));
        RefBuffer b1 = pool.borrow(false, 256, 0);
        RefBuffer b2 = pool.borrow(false, 256, 0);
        b1.release();
        b2.release();
        // advance the pool clock past the timeout so shrink reclaims all chunks
        pool.ts.nanoTime += TimeUnit.MILLISECONDS.toNanos(200);
        pool.shrink();
        assertTrue(pool.formatStat().contains("chunks 0("));
        // after all chunks (including the hinted one) are reclaimed, borrow must allocate fresh
        RefBuffer after = pool.borrow(false, 256, 0);
        assertEquals(256, after.getBuffer().capacity());
        after.release();
    }

    @Test
    public void testDirectSliceContent() {
        BuddyBufferPool pool = newPool(true, 256, 16, 1, 1);
        RefBuffer a = pool.borrow(false, 16, 0);
        RefBuffer b = pool.borrow(false, 16, 0);
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
        assertThrows(IllegalArgumentException.class, () -> pool.borrow(false, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> pool.borrow(false, -1, 0));
    }

    @Test
    public void testConcurrent() throws Exception {
        BuddyBufferPool pool = newPool(false, 1024, 16, 1, 4);
        int threads = 4;
        @SuppressWarnings("resource") ExecutorService es = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicReference<Throwable> err = new AtomicReference<>();
        try {
            Runnable r = () -> {
                try {
                    Random rand = new Random();
                    for (int i = 0; i < 1000; i++) {
                        RefBuffer rb = pool.borrow(false, rand.nextInt(1024) + 1, 0);
                        rb.release();
                    }
                } catch (Throwable t) {
                    err.set(t);
                } finally {
                    latch.countDown();
                }
            };
            for (int i = 0; i < threads; i++) {
                es.submit(r);
            }
            assertTrue(latch.await(5000, TimeUnit.MILLISECONDS));
            assertNull(err.get());
        } finally {
            es.shutdown();
        }
    }
}
