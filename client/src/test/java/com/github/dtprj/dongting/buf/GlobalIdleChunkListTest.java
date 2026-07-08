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

import com.github.dtprj.dongting.common.DtUtil;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author huangli
 */
public class GlobalIdleChunkListTest {

    private static GlobalIdleChunkList newList(boolean direct) {
        return new GlobalIdleChunkList(Long.MAX_VALUE, direct, 256, 16, 60000, 2);
    }

    // returnIdleChunk in production is always preceded by a budget borrow (in BuddyBufferPool.shrink),
    // so tests must mirror that pairing to keep the budget consistent.
    private static void returnWithBudget(GlobalIdleChunkList list, BuddyChunk chunk) {
        list.borrow(chunk.chunkSize);
        list.returnIdleChunk(chunk);
    }

    /**
     * Block until all asynchronously scheduled {@code run()} invocations have completed.
     * Because {@code LOW_PRIORITY_SCHEDULER} is a single-thread executor, submitting a no-op
     * task and waiting for it guarantees that every previously queued task has finished.
     */
    private static void flushScheduler() {
        try {
            DtUtil.LOW_PRIORITY_SCHEDULER.submit(() -> {}).get(5, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testRunAllocatesToTargetSize() {
        // budget is exactly targetSize * chunkSize so async run() cannot allocate beyond target
        GlobalIdleChunkList list = new GlobalIdleChunkList(512, false, 256, 16, 60000, 2);
        list.run();
        BuddyChunk c1 = list.borrowIdleChunk();
        BuddyChunk c2 = list.borrowIdleChunk();
        assertNotNull(c1);
        assertNotNull(c2);
        assertEquals(256, c1.rootBuffer.capacity());
        // borrowing the last chunk schedules async run(); budget is exhausted so nothing is allocated
        flushScheduler();
        assertNull(list.borrowIdleChunk());
        flushScheduler();
    }

    @Test
    public void testRunEvictsTimedOutChunks() {
        // direct mode: weakRefCache is disabled, so timed-out chunks are destroyed by run()
        GlobalIdleChunkList list = newList(true);
        long now = System.nanoTime();
        BuddyChunk stale = new BuddyChunk(true, 256, 16);
        stale.lastFullFreeNanos = now - TimeUnit.MILLISECONDS.toNanos(120000);
        returnWithBudget(list, stale);
        BuddyChunk fresh1 = new BuddyChunk(true, 256, 16);
        fresh1.lastFullFreeNanos = now;
        returnWithBudget(list, fresh1);
        BuddyChunk fresh2 = new BuddyChunk(true, 256, 16);
        fresh2.lastFullFreeNanos = now;
        returnWithBudget(list, fresh2);
        // returning the 3rd chunk triggered async run(); flush to let it evict the stale chunk
        flushScheduler();
        // stale was destroyed; only fresh1 and fresh2 remain
        assertSame(fresh2, list.borrowIdleChunk());
        assertSame(fresh1, list.borrowIdleChunk());
        flushScheduler();
    }

    @Test
    public void testRunKeepsFreshChunksWhenOverTarget() {
        // regression: the cleanup loop must break (not spin forever) when the head chunk
        // has not timed out, even though idleChunks.size() > targetSize
        GlobalIdleChunkList list = newList(false);
        long now = System.nanoTime();
        BuddyChunk c1 = new BuddyChunk(false, 256, 16);
        c1.lastFullFreeNanos = now;
        returnWithBudget(list, c1);
        BuddyChunk c2 = new BuddyChunk(false, 256, 16);
        c2.lastFullFreeNanos = now;
        returnWithBudget(list, c2);
        BuddyChunk c3 = new BuddyChunk(false, 256, 16);
        c3.lastFullFreeNanos = now;
        returnWithBudget(list, c3);
        // async run() was scheduled but all chunks are fresh, so nothing is evicted
        flushScheduler();
        // explicit run() must also return promptly instead of looping forever
        list.run();
        assertSame(c3, list.borrowIdleChunk());
        assertSame(c2, list.borrowIdleChunk());
        assertSame(c1, list.borrowIdleChunk());
        flushScheduler();
    }

    @Test
    public void testHeapTimeoutGoesToWeakRefCache() {
        // heap mode: timed-out chunks beyond targetSize are moved to the weak-ref cache
        // (instead of being destroyed) so they can be reclaimed if still referenced.
        // budget is capped at 3*chunkSize so async run() cannot allocate fresh chunks.
        GlobalIdleChunkList list = new GlobalIdleChunkList(768, false, 256, 16, 60000, 2);
        long now = System.nanoTime();
        BuddyChunk stale = new BuddyChunk(false, 256, 16);
        stale.lastFullFreeNanos = now - TimeUnit.MILLISECONDS.toNanos(120000);
        returnWithBudget(list, stale);
        BuddyChunk fresh1 = new BuddyChunk(false, 256, 16);
        fresh1.lastFullFreeNanos = now;
        returnWithBudget(list, fresh1);
        BuddyChunk fresh2 = new BuddyChunk(false, 256, 16);
        fresh2.lastFullFreeNanos = now;
        returnWithBudget(list, fresh2);
        // async run() moved stale to weak-ref cache
        flushScheduler();
        // borrow fresh chunks
        assertSame(fresh2, list.borrowIdleChunk());
        assertSame(fresh1, list.borrowIdleChunk());
        // draining the list triggers async run() which reclaims stale from weak-ref cache
        flushScheduler();
        // stale is reclaimable (not destroyed in heap mode)
        assertSame(stale, list.borrowIdleChunk());
        flushScheduler();
    }

    @Test
    public void testWeakRefCacheBudgetReleasedAndReacquired() {
        // total budget allows 3 chunks (768 = 3 * 256)
        GlobalIdleChunkList list = new GlobalIdleChunkList(768, false, 256, 16, 60000, 2);
        long now = System.nanoTime();
        BuddyChunk stale = new BuddyChunk(false, 256, 16);
        stale.lastFullFreeNanos = now - TimeUnit.MILLISECONDS.toNanos(120000);
        returnWithBudget(list, stale);
        BuddyChunk fresh1 = new BuddyChunk(false, 256, 16);
        fresh1.lastFullFreeNanos = now;
        returnWithBudget(list, fresh1);
        BuddyChunk fresh2 = new BuddyChunk(false, 256, 16);
        fresh2.lastFullFreeNanos = now;
        returnWithBudget(list, fresh2);
        // async run() moved stale to weak-ref cache, releasing its budget (used = 512)
        flushScheduler();

        // the released budget allows one more borrow
        assertTrue(list.borrow(256));
        list.release(256);

        // drain idleChunks so the next borrow comes from the weak-ref cache
        list.borrowIdleChunk(); // fresh2
        list.borrowIdleChunk(); // fresh1, triggers async run()
        flushScheduler();
        // async run() reclaimed stale from weak-ref cache, re-acquiring budget (used = 768)

        BuddyChunk cached = list.borrowIdleChunk();
        assertSame(stale, cached);
        flushScheduler();
    }

    @Test
    public void testRunReclaimsFromWeakRefCache() {
        // after timed-out chunks are moved to the weak-ref cache, a subsequent run() should
        // reclaim them into idleChunks instead of allocating fresh chunks.
        // budget allows only 1 chunk (256), so the run can only reclaim, not allocate
        GlobalIdleChunkList list = new GlobalIdleChunkList(256, false, 256, 16, 60000, 2);
        long now = System.nanoTime();
        BuddyChunk stale = new BuddyChunk(false, 256, 16);
        stale.lastFullFreeNanos = now - TimeUnit.MILLISECONDS.toNanos(120000);
        returnWithBudget(list, stale);
        BuddyChunk fresh1 = new BuddyChunk(false, 256, 16);
        fresh1.lastFullFreeNanos = now;
        returnWithBudget(list, fresh1);
        BuddyChunk fresh2 = new BuddyChunk(false, 256, 16);
        fresh2.lastFullFreeNanos = now;
        returnWithBudget(list, fresh2);
        // first async run(): stale (beyond targetSize) is moved to weak-ref cache
        flushScheduler();

        // drain idleChunks so the reclaim path is exercised on the next run
        list.borrowIdleChunk();
        list.borrowIdleChunk();
        // second async run(): idleChunks is empty, reclaims stale from weak-ref cache
        flushScheduler();

        BuddyChunk reclaimed = list.borrowIdleChunk();
        assertSame(stale, reclaimed);
        flushScheduler();
    }

    @Test
    public void testBorrowIdleChunkReturnsNullWhenBudgetExhausted() {
        // when budget is insufficient, borrowIdleChunk must not take a chunk from weakRefCache
        GlobalIdleChunkList list = new GlobalIdleChunkList(768, false, 256, 16, 60000, 2);
        long now = System.nanoTime();
        BuddyChunk stale = new BuddyChunk(false, 256, 16);
        stale.lastFullFreeNanos = now - TimeUnit.MILLISECONDS.toNanos(120000);
        returnWithBudget(list, stale);
        BuddyChunk fresh1 = new BuddyChunk(false, 256, 16);
        fresh1.lastFullFreeNanos = now;
        returnWithBudget(list, fresh1);
        BuddyChunk fresh2 = new BuddyChunk(false, 256, 16);
        fresh2.lastFullFreeNanos = now;
        returnWithBudget(list, fresh2);
        // async run() moved stale to weak-ref cache, releasing budget (used = 512)
        flushScheduler();

        // exhaust remaining budget so async run() cannot reclaim stale from weak-ref cache
        list.borrow(256);

        // drain idleChunks; async run() cannot reclaim (budget full), stale stays in cache
        list.borrowIdleChunk(); // fresh2
        list.borrowIdleChunk(); // fresh1
        flushScheduler();

        // budget exhausted, weakRefCache still has stale, but borrowIdleChunk returns null
        assertNull(list.borrowIdleChunk());
        flushScheduler();

        // after releasing budget, the cached chunk becomes available again
        list.release(256);
        BuddyChunk cached = list.borrowIdleChunk();
        assertSame(stale, cached);
        flushScheduler();
    }
}
