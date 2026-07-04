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

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
public class FixSizeBufferPoolTest {

    private FixSizeBufferPool createFixPool(int bufferSize, int minCount, int maxCount, long shareSize) {
        return new FixSizeBufferPool(false, new ShareBudget(shareSize), minCount, maxCount, bufferSize, 4096);
    }

    private FixSizeBufferPool createFixPool(int bufferSize, int minCount, int maxCount) {
        return createFixPool(bufferSize, minCount, maxCount, 0);
    }

    private ByteBuffer borrowOrAllocate(FixSizeBufferPool pool, int size) {
        ByteBuffer buf = pool.borrow();
        return buf != null ? buf : ByteBuffer.allocate(size);
    }

    @Test
    public void testShareSize() {
        FixSizeBufferPool pool = createFixPool(200, 1, 2, 500);
        ByteBuffer buf1 = ByteBuffer.allocate(200);
        ByteBuffer buf2 = ByteBuffer.allocate(200);
        ByteBuffer buf3 = ByteBuffer.allocate(200);
        ByteBuffer buf4 = ByteBuffer.allocate(200);
        ByteBuffer buf5 = ByteBuffer.allocate(200);

        pool.release(buf1);
        pool.release(buf2);
        pool.release(buf3);
        pool.release(buf4);
        pool.release(buf5);

        assertSame(buf4, pool.borrow());
        pool.shrink();
        assertSame(buf3, pool.borrow());
    }

    @Test
    public void testShrinkQuarterWhenIdle() {
        FixSizeBufferPool pool = createFixPool(128, 0, 8);
        for (int i = 0; i < 8; i++) {
            pool.release(ByteBuffer.allocate(128));
        }
        // no borrow this period: shrink a quarter of the untouched portion each shrink
        pool.shrink();
        assertEquals(6, pool.bufferStack.size());
        pool.shrink();
        assertEquals(5, pool.bufferStack.size());
        pool.shrink();
        assertEquals(4, pool.bufferStack.size());
        pool.shrink();
        assertEquals(3, pool.bufferStack.size());
        pool.shrink();
        assertEquals(2, pool.bufferStack.size());
        pool.shrink();
        assertEquals(1, pool.bufferStack.size());
        pool.shrink();
        assertEquals(0, pool.bufferStack.size());
    }

    @Test
    public void testShrinkToMinCount() {
        FixSizeBufferPool pool = createFixPool(128, 2, 8);
        for (int i = 0; i < 8; i++) {
            pool.release(ByteBuffer.allocate(128));
        }
        for (int i = 0; i < 10; i++) {
            pool.shrink();
        }
        assertEquals(2, pool.bufferStack.size());
    }

    @Test
    public void testKeepUsedPortion() {
        FixSizeBufferPool pool = createFixPool(128, 0, 8);
        for (int i = 0; i < 8; i++) {
            pool.release(ByteBuffer.allocate(128));
        }
        // borrow 5: stack drops to 3, so periodMinStackSize==3 (only bottom 3 untouched)
        for (int i = 0; i < 5; i++) {
            assertNotNull(pool.borrow());
        }
        assertEquals(3, pool.bufferStack.size());
        pool.shrink();
        // untouched=3, shrink floor(3/4)=0->1, target=3-1=2
        assertEquals(2, pool.bufferStack.size());
    }

    @Test
    public void testNoShrinkWhenFullyDrained() {
        FixSizeBufferPool pool = createFixPool(128, 0, 4);
        for (int i = 0; i < 4; i++) {
            pool.release(ByteBuffer.allocate(128));
        }
        // drain the whole stack: minSize==0, nothing shrinks
        for (int i = 0; i < 4; i++) {
            assertNotNull(pool.borrow());
        }
        assertEquals(0, pool.bufferStack.size());
        pool.shrink();
        assertEquals(0, pool.bufferStack.size());
    }

    @Test
    public void testShrinkOnEmptyPoolThenFill() {
        // empty pool's first shrink must not lock periodMinStackSize to 0
        FixSizeBufferPool pool = createFixPool(128, 0, 8);
        pool.shrink();
        for (int i = 0; i < 8; i++) {
            pool.release(ByteBuffer.allocate(128));
        }
        // no borrow this period: should still shrink a quarter
        pool.shrink();
        assertEquals(6, pool.bufferStack.size());
    }

    @Test
    public void testShrinkAfterReleaseOnlyGrowth() {
        // after a shrink, releasing more buffers without any borrow must shrink again
        FixSizeBufferPool pool = createFixPool(128, 0, 16);
        for (int i = 0; i < 8; i++) {
            pool.release(ByteBuffer.allocate(128));
        }
        pool.shrink();
        assertEquals(6, pool.bufferStack.size());
        // release-only growth in the next period (no borrow)
        for (int i = 0; i < 10; i++) {
            pool.release(ByteBuffer.allocate(128));
        }
        assertEquals(16, pool.bufferStack.size());
        pool.shrink();
        // whole stack untouched this period: shrink a quarter of 16
        assertEquals(12, pool.bufferStack.size());
    }

    @Test
    public void testWeakRefNotEnabledForDirect() {
        FixSizeBufferPool pool = new FixSizeBufferPool(true, new ShareBudget(0), 1, 2, 128, 128);
        ByteBuffer buf1 = ByteBuffer.allocateDirect(128);
        ByteBuffer buf2 = ByteBuffer.allocateDirect(128);
        ByteBuffer buf3 = ByteBuffer.allocateDirect(128);
        pool.release(buf1);
        pool.release(buf2);
        pool.release(buf3);
        assertSame(buf2, pool.borrow());
        assertSame(buf1, pool.borrow());
        assertNotSame(buf3, pool.borrow());
    }

    @Test
    public void testWeakRefNotEnabledForSmallBuffer() {
        FixSizeBufferPool pool = new FixSizeBufferPool(false, new ShareBudget(0), 1, 2, 128, 256);
        ByteBuffer buf1 = ByteBuffer.allocate(128);
        ByteBuffer buf2 = ByteBuffer.allocate(128);
        ByteBuffer buf3 = ByteBuffer.allocate(128);
        pool.release(buf1);
        pool.release(buf2);
        pool.release(buf3);
        assertSame(buf2, pool.borrow());
        assertSame(buf1, pool.borrow());
        assertNotSame(buf3, pool.borrow());
    }

    @Test
    public void testWeakRefReleaseToWeakStack() {
        for (int attempt = 0; attempt < 3; attempt++) {
            FixSizeBufferPool pool = new FixSizeBufferPool(false, new ShareBudget(0), 1, 2, 128, 128);
            ByteBuffer buf1 = ByteBuffer.allocate(128);
            ByteBuffer buf2 = ByteBuffer.allocate(128);
            ByteBuffer buf3 = ByteBuffer.allocate(128);
            pool.release(buf1);
            pool.release(buf2);
            pool.release(buf3);
            ByteBuffer b1 = pool.borrow();
            ByteBuffer b2 = pool.borrow();
            ByteBuffer b3 = pool.borrow();
            if (b1 == buf3 && b2 == buf2 && b3 == buf1) {
                return;
            }
        }
        fail("weak ref test failed after 3 attempts");
    }

    @Test
    public void testWeakRefShrinkToWeakStack() {
        for (int attempt = 0; attempt < 3; attempt++) {
            FixSizeBufferPool pool = new FixSizeBufferPool(false, new ShareBudget(0), 1, 3, 128, 128);
            ByteBuffer buf1 = ByteBuffer.allocate(128);
            ByteBuffer buf2 = ByteBuffer.allocate(128);
            ByteBuffer buf3 = ByteBuffer.allocate(128);
            pool.release(buf1);
            pool.release(buf2);
            pool.release(buf3);

            // shrink moves floor(untouched/4)=0->1 bottom buffer to weak ref cache
            pool.shrink();

            ByteBuffer buf4 = pool.borrow();
            ByteBuffer buf5 = pool.borrow();
            ByteBuffer buf6 = pool.borrow();

            if (buf4 == buf1 && buf5 == buf3 && buf6 == buf2) {
                return;
            }
        }
        fail("weak ref test failed after 3 attempts");
    }

    @Test
    public void testWeakRefGCAndClean() {
        FixSizeBufferPool testPool = new FixSizeBufferPool(false, new ShareBudget(1), 0, 1, 128, 128);

        ByteBuffer buf1 = ByteBuffer.allocate(128);
        ByteBuffer buf2 = ByteBuffer.allocate(128);
        ByteBuffer buf3 = ByteBuffer.allocate(128);
        testPool.release(buf1);
        testPool.release(buf2);
        testPool.release(buf3);

        //noinspection UnusedAssignment
        buf2 = null;

        System.gc();
        System.runFinalization();

        testPool.clean();

        ByteBuffer borrowed1 = testPool.borrow();
        ByteBuffer borrowed2 = testPool.borrow();

        assertNotNull(borrowed1);
        assertNotNull(borrowed2);
        assertEquals(128, borrowed1.capacity());
        assertEquals(128, borrowed2.capacity());
    }
}
