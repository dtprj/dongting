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

import com.github.dtprj.dongting.common.Timestamp;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
public class FixSizeBufferPoolTest {
    
    private static final Timestamp TS = new Timestamp();

    private SimpleByteBufferPoolConfig createTestConfig() {
        // just use currentUsedShareSize field, so set other params to invalid value
        return new SimpleByteBufferPoolConfig(TS, false, 0, false,
                null, null, null);
    }

    private FixSizeBufferPool createFixPool(int bufferSize, int minCount, int maxCount, long shareSize) {
        SimpleByteBufferPoolConfig config = createTestConfig();
        return new FixSizeBufferPool(config, false, shareSize, minCount, maxCount, bufferSize, 4096);
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

        pool.release(buf1, TS.nanoTime);
        pool.release(buf2, TS.nanoTime);
        pool.release(buf3, TS.nanoTime);
        pool.release(buf4, TS.nanoTime);
        pool.release(buf5, TS.nanoTime);

        assertSame(buf4, pool.borrow());
        TS.nanoTime += 1001;
        pool.clean(TS.nanoTime);
        assertSame(buf3, pool.borrow());
    }

    @Test
    public void testClean1() {
        // Test: no cleanup when time elapsed < timeout (500ms < 1000ms)
        long timeoutMillis = 1000;
        long timeoutNanos = timeoutMillis * 1000 * 1000;
        
        FixSizeBufferPool pool = createFixPool(128, 1, 2);
        ByteBuffer buf1 = ByteBuffer.allocate(128);
        ByteBuffer buf2 = ByteBuffer.allocate(128);
        pool.release(buf1, TS.nanoTime);
        pool.release(buf2, TS.nanoTime);

        TS.nanoTime += 500 * 1000 * 1000;
        long expireNanos = TS.nanoTime - timeoutNanos;
        pool.clean(expireNanos);
        
        ByteBuffer buf3 = pool.borrow();
        ByteBuffer buf4 = pool.borrow();
        assertSame(buf2, buf3);
        assertSame(buf1, buf4);
        pool.release(buf1, TS.nanoTime);
        pool.release(buf2, TS.nanoTime);
    }

    @Test
    public void testClean2() {
        // Test: cleanup to minCount when time elapsed > timeout (minCount=1, maxCount=3)
        long timeoutMillis = 1000;
        long timeoutNanos = timeoutMillis * 1000 * 1000;
        
        FixSizeBufferPool pool = createFixPool(128, 1, 3);
        ByteBuffer buf1 = ByteBuffer.allocate(128);
        ByteBuffer buf2 = ByteBuffer.allocate(128);
        ByteBuffer buf3 = ByteBuffer.allocate(128);
        pool.release(buf1, TS.nanoTime);
        pool.release(buf2, TS.nanoTime);
        pool.release(buf3, TS.nanoTime);

        for (int i = 0; i < 5; i++) {
            TS.nanoTime += 1001 * 1000 * 1000;
            long expireNanos = TS.nanoTime - timeoutNanos;
            pool.clean(expireNanos);
            
            ByteBuffer buf4 = borrowOrAllocate(pool, 128);
            ByteBuffer buf5 = borrowOrAllocate(pool, 128);
            ByteBuffer buf6 = borrowOrAllocate(pool, 128);
            assertSame(buf3, buf4);
            assertTrue(buf5 != buf1 && buf5 != buf2 && buf5 != buf3);
            assertTrue(buf6 != buf1 && buf6 != buf2 && buf6 != buf3);
            
            buf1 = buf4;
            buf2 = buf5;
            buf3 = buf6;
            pool.release(buf1, TS.nanoTime);
            pool.release(buf2, TS.nanoTime);
            pool.release(buf3, TS.nanoTime);
        }
    }

    @Test
    public void testClean3() {
        // Test: cleanup to minCount when time elapsed > timeout (minCount=1, maxCount=2)
        long timeoutMillis = 1000;
        long timeoutNanos = timeoutMillis * 1000 * 1000;
        
        FixSizeBufferPool pool = createFixPool(128, 1, 2);
        ByteBuffer buf1 = ByteBuffer.allocate(128);
        ByteBuffer buf2 = ByteBuffer.allocate(128);
        pool.release(buf1, TS.nanoTime);
        pool.release(buf2, TS.nanoTime);

        for (int i = 0; i < 5; i++) {
            TS.nanoTime += 1001 * 1000 * 1000;
            long expireNanos = TS.nanoTime - timeoutNanos;
            pool.clean(expireNanos);
            
            ByteBuffer buf3 = borrowOrAllocate(pool, 128);
            ByteBuffer buf4 = borrowOrAllocate(pool, 128);
            assertSame(buf2, buf3);
            assertTrue(buf4 != buf1 && buf4 != buf2);
            
            buf1 = buf3;
            buf2 = buf4;
            pool.release(buf1, TS.nanoTime);
            pool.release(buf2, TS.nanoTime);
        }
    }

    @Test
    public void testClean4() {
        // Test: cleanup all buffers when minCount=0 and time elapsed > timeout
        long timeoutMillis = 1000;
        long timeoutNanos = timeoutMillis * 1000 * 1000;
        
        FixSizeBufferPool pool = createFixPool(128, 0, 2);
        ByteBuffer buf1 = ByteBuffer.allocate(128);
        ByteBuffer buf2 = ByteBuffer.allocate(128);
        pool.release(buf1, TS.nanoTime);
        pool.release(buf2, TS.nanoTime);

        for (int i = 0; i < 5; i++) {
            TS.nanoTime += 1001 * 1000 * 1000;
            long expireNanos = TS.nanoTime - timeoutNanos;
            pool.clean(expireNanos);
            
            ByteBuffer buf3 = borrowOrAllocate(pool, 128);
            ByteBuffer buf4 = borrowOrAllocate(pool, 128);
            assertTrue(buf3 != buf1 && buf3 != buf2);
            assertTrue(buf4 != buf1 && buf4 != buf2);
            
            buf1 = buf3;
            buf2 = buf4;
            pool.release(buf1, TS.nanoTime);
            pool.release(buf2, TS.nanoTime);
        }
    }

    @Test
    public void testWeakRefNotEnabledForDirect() {
        SimpleByteBufferPoolConfig config = createTestConfig();
        FixSizeBufferPool pool = new FixSizeBufferPool(config, true, 0, 1, 2, 128, 128);
        ByteBuffer buf1 = ByteBuffer.allocateDirect(128);
        ByteBuffer buf2 = ByteBuffer.allocateDirect(128);
        ByteBuffer buf3 = ByteBuffer.allocateDirect(128);
        pool.release(buf1, TS.nanoTime);
        pool.release(buf2, TS.nanoTime);
        pool.release(buf3, TS.nanoTime);
        assertSame(buf2, pool.borrow());
        assertSame(buf1, pool.borrow());
        assertNotSame(buf3, pool.borrow());
    }

    @Test
    public void testWeakRefNotEnabledForSmallBuffer() {
        SimpleByteBufferPoolConfig config = createTestConfig();
        FixSizeBufferPool pool = new FixSizeBufferPool(config, false, 0, 1, 2, 128, 256);
        ByteBuffer buf1 = ByteBuffer.allocate(128);
        ByteBuffer buf2 = ByteBuffer.allocate(128);
        ByteBuffer buf3 = ByteBuffer.allocate(128);
        pool.release(buf1, TS.nanoTime);
        pool.release(buf2, TS.nanoTime);
        pool.release(buf3, TS.nanoTime);
        assertSame(buf2, pool.borrow());
        assertSame(buf1, pool.borrow());
        assertNotSame(buf3, pool.borrow());
    }

    @Test
    public void testWeakRefReleaseToWeakStack() {
        for (int attempt = 0; attempt < 3; attempt++) {
            SimpleByteBufferPoolConfig config = createTestConfig();
            FixSizeBufferPool pool = new FixSizeBufferPool(config, false, 0, 1, 2, 128, 128);
            ByteBuffer buf1 = ByteBuffer.allocate(128);
            ByteBuffer buf2 = ByteBuffer.allocate(128);
            ByteBuffer buf3 = ByteBuffer.allocate(128);
            pool.release(buf1, TS.nanoTime);
            pool.release(buf2, TS.nanoTime);
            pool.release(buf3, TS.nanoTime);
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
    public void testWeakRefCleanToWeakStack() {
        for (int attempt = 0; attempt < 3; attempt++) {
            SimpleByteBufferPoolConfig config = createTestConfig();
            FixSizeBufferPool pool = new FixSizeBufferPool(config, false, 0, 1, 3, 128, 128);
            ByteBuffer buf1 = ByteBuffer.allocate(128);
            ByteBuffer buf2 = ByteBuffer.allocate(128);
            ByteBuffer buf3 = ByteBuffer.allocate(128);
            pool.release(buf1, TS.nanoTime);
            pool.release(buf2, TS.nanoTime);
            pool.release(buf3, TS.nanoTime);

            TS.nanoTime += 1001;
            pool.clean(TS.nanoTime);

            ByteBuffer buf4 = pool.borrow();
            ByteBuffer buf5 = pool.borrow();
            ByteBuffer buf6 = pool.borrow();

            if (buf4 == buf1 && buf5 == buf2 && buf6 == buf3) {
                return;
            }
        }
        fail("weak ref test failed after 3 attempts");
    }

    @Test
    public void testWeakRefGCAndClean() {
        SimpleByteBufferPoolConfig config = createTestConfig();
        FixSizeBufferPool testPool = new FixSizeBufferPool(config, false, 1, 0, 1, 128, 128);

        ByteBuffer buf1 = ByteBuffer.allocate(128);
        ByteBuffer buf2 = ByteBuffer.allocate(128);
        ByteBuffer buf3 = ByteBuffer.allocate(128);
        testPool.release(buf1, TS.nanoTime);
        testPool.release(buf2, TS.nanoTime);
        testPool.release(buf3, TS.nanoTime);

        //noinspection UnusedAssignment
        buf2 = null;

        System.gc();
        System.runFinalization();

        TS.nanoTime += 1001;
        testPool.clean(TS.nanoTime);

        ByteBuffer borrowed1 = testPool.borrow();
        ByteBuffer borrowed2 = testPool.borrow();

        assertNotNull(borrowed1);
        assertNotNull(borrowed2);
        assertEquals(128, borrowed1.capacity());
        assertEquals(128, borrowed2.capacity());
    }
}
