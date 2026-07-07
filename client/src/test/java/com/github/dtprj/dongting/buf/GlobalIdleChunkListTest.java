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
        GlobalIdleChunkList list = newList(false);
        long now = System.nanoTime();
        // fill with 3 chunks (more than TARGET_SIZE=2); the first one (head) is stale
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
}
