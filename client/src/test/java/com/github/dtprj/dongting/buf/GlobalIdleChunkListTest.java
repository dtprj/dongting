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

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

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
        return new GlobalIdleChunkList(Long.MAX_VALUE, direct, 256, 16, 60000);
    }

    // returnIdleChunk in production is always preceded by a budget borrow (in BuddyBufferPool.shrink),
    // so tests must mirror that pairing to keep the budget consistent.
    private static void returnWithBudget(GlobalIdleChunkList list, BuddyChunk chunk) {
        list.borrow(chunk.chunkSize);
        list.returnIdleChunk(chunk);
    }

    @Test
    public void testRunAllocatesToTargetSize() {
        GlobalIdleChunkList list = newList(false);
        // empty list: run() should pre-allocate exactly TARGET_SIZE chunks
        list.run();
        BuddyChunk c1 = list.borrowIdleChunk();
        BuddyChunk c2 = list.borrowIdleChunk();
        assertNotNull(c1);
        assertNotNull(c2);
        assertEquals(256, c1.rootBuffer.capacity());
        // only TARGET_SIZE chunks were pre-allocated
        assertNull(list.borrowIdleChunk());
    }

    @Test
    public void testRunEvictsTimedOutChunks() {
        // direct mode: weakRefCache is disabled, so idleChunks can exceed TARGET_SIZE and
        // timed-out chunks are pruned by run()
        GlobalIdleChunkList list = newList(true);
        long now = System.nanoTime();
        // fill with 3 chunks (more than TARGET_SIZE=2); the first one (head) is stale
        BuddyChunk stale = new BuddyChunk(true, 256, 16);
        stale.lastFullFreeNanos = now - TimeUnit.MILLISECONDS.toNanos(120000);
        returnWithBudget(list, stale);
        BuddyChunk fresh1 = new BuddyChunk(true, 256, 16);
        fresh1.lastFullFreeNanos = now;
        returnWithBudget(list, fresh1);
        BuddyChunk fresh2 = new BuddyChunk(true, 256, 16);
        fresh2.lastFullFreeNanos = now;
        returnWithBudget(list, fresh2);

        list.run();
        // stale evicted, the two fresh chunks remain
        assertNotNull(list.borrowIdleChunk());
        assertNotNull(list.borrowIdleChunk());
        assertNull(list.borrowIdleChunk());
    }

    @Test
    public void testRunKeepsFreshChunksWhenOverTarget() {
        // regression: the cleanup loop must break (not spin forever) when the head chunk
        // has not timed out, even though idleChunks.size() > TARGET_SIZE
        GlobalIdleChunkList list = newList(false);
        long now = System.nanoTime();
        for (int i = 0; i < 3; i++) {
            BuddyChunk c = new BuddyChunk(false, 256, 16);
            c.lastFullFreeNanos = now;
            returnWithBudget(list, c);
        }
        // all chunks are fresh; run() must return promptly instead of looping forever
        list.run();
        assertNotNull(list.borrowIdleChunk());
        assertNotNull(list.borrowIdleChunk());
        assertNotNull(list.borrowIdleChunk());
    }

    @Test
    public void testHeapTimeoutGoesToWeakRefCache() {
        // heap mode: timed-out chunks beyond TARGET_SIZE are moved to the weak-ref cache
        // (instead of being destroyed) so they can be reclaimed if still referenced
        GlobalIdleChunkList list = newList(false);
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

        list.run();
        // two fresh chunks remain in the strong list
        assertNotNull(list.borrowIdleChunk());
        assertNotNull(list.borrowIdleChunk());
        // stale is reclaimable from the weak-ref cache (strong ref held by test)
        BuddyChunk cached = list.borrowIdleChunk();
        assertSame(stale, cached);
    }

    @Test
    public void testWeakRefCacheBudgetReleasedAndReacquired() {
        // total budget allows 3 chunks (768 = 3 * 256)
        GlobalIdleChunkList list = new GlobalIdleChunkList(768, false, 256, 16, 60000);
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
        // all 3 chunks in idleChunks, budget used = 768

        list.run();
        // stale moved to weak-ref cache, its budget released; budget used = 512
        // the released budget allows one more borrow
        assertTrue(list.borrow(256));
        list.release(256);

        // drain idleChunks so the next borrow comes from the weak-ref cache
        list.borrowIdleChunk(); // fresh2
        list.borrowIdleChunk(); // fresh1
        // borrowing from weak-ref cache re-acquires budget
        BuddyChunk cached = list.borrowIdleChunk();
        assertSame(stale, cached);
    }

    @Test
    public void testRunReclaimsFromWeakRefCache() {
        // after timed-out chunks are moved to the weak-ref cache, a subsequent run() should
        // reclaim them into idleChunks instead of allocating fresh chunks
        // budget allows only 1 chunk (256), so the second run can only reclaim, not allocate
        GlobalIdleChunkList list = new GlobalIdleChunkList(256, false, 256, 16, 60000);
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

        // first run: stale (beyond TARGET_SIZE) is moved to weak-ref cache
        list.run();
        // drain idleChunks so the reclaim path is exercised on the next run
        list.borrowIdleChunk();
        list.borrowIdleChunk();

        // second run: idleChunks is empty, reclaims stale from weak-ref cache (budget allows 1)
        list.run();
        BuddyChunk reclaimed = list.borrowIdleChunk();
        assertSame(stale, reclaimed);
    }

    @Test
    public void testBorrowIdleChunkReturnsNullWhenBudgetExhausted() {
        // when budget is insufficient, borrowIdleChunk must not take a chunk from weakRefCache
        GlobalIdleChunkList list = new GlobalIdleChunkList(768, false, 256, 16, 60000);
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

        // run() moves stale (beyond TARGET_SIZE) to weak-ref cache, releasing its budget
        list.run();
        // drain idleChunks
        list.borrowIdleChunk();
        list.borrowIdleChunk();

        // exhaust the budget so borrow0 inside borrowIdleChunk fails
        list.borrow(256);
        assertNull(list.borrowIdleChunk());

        // after releasing budget, the cached chunk becomes available again
        list.release(256);
        BuddyChunk cached = list.borrowIdleChunk();
        assertSame(stale, cached);
    }
}
