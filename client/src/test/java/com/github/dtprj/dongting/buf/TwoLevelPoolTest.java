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

import com.github.dtprj.dongting.common.DtException;
import com.github.dtprj.dongting.common.Timestamp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author huangli
 */
public class TwoLevelPoolTest {

    SimpleByteBufferPool p1;
    SimpleByteBufferPool p2;


    @BeforeEach
    public void init() {
        SimpleByteBufferPoolConfig c = new SimpleByteBufferPoolConfig(new Timestamp(), false, 0,
                false, new int[]{16, 32}, new int[]{1, 2}, new int[]{2, 2}, 1000, 0);
        p1 = new SimpleByteBufferPool(c);

        c = new SimpleByteBufferPoolConfig(null, false, 0, true,
                new int[]{128, 256}, new int[]{1, 2}, new int[]{2, 2}, 1000, 0);
        p2 = new SimpleByteBufferPool(c);
    }

    @Test
    public void test() {
        TwoLevelPool p = new TwoLevelPool(false, p1, p2);
        RefBuffer b1 = p.borrow(false, 31, 0);
        RefBuffer b2 = p.borrow(false, 32, 0);
        RefBuffer b3 = p.borrow(false, 33, 0);
        assertEquals(32, b1.getBuffer().capacity());
        assertEquals(32, b2.getBuffer().capacity());
        assertEquals(128, b3.getBuffer().capacity());
        b1.release();
        b2.release();
        b3.release();
    }

    @Test
    public void testBorrowInOtherThread() {
        TwoLevelPool p = new TwoLevelPool(false, p1, p2);
        //noinspection InstantiatingAThreadWithDefaultRunMethod
        TwoLevelPool p2 = p.toReleaseInOtherThreadInstance(new Thread(), (rb, c) -> true);
        assertThrows(DtException.class, () -> p2.borrow(false, 1, 0));
    }

    @Test
    public void testReleaseInOtherThread1() {
        TwoLevelPool p = new TwoLevelPool(false, p1, p2);
        AtomicInteger releaseCount = new AtomicInteger(0);
        TwoLevelPool p2 = p.toReleaseInOtherThreadInstance(Thread.currentThread(), (rb, c) -> {
            releaseCount.incrementAndGet();
            c.accept(rb);
            return true;
        });
        RefBuffer b1 = p2.borrow(false, 16, 0);
        // release in same thread, not invoke callback
        b1.release();
        assertEquals(0, releaseCount.get());
    }

    @Test
    public void testReleaseInOtherThread2() throws Exception {
        TwoLevelPool p = new TwoLevelPool(false, p1, p2);
        AtomicInteger releaseCount = new AtomicInteger(0);
        TwoLevelPool p2 = p.toReleaseInOtherThreadInstance(Thread.currentThread(), (rb, c) -> {
            releaseCount.incrementAndGet();
            c.accept(rb);
            return true;
        });
        RefBuffer b1 = p2.borrow(false, 16, 0);
        Thread t = new Thread(b1::release);
        t.start();
        t.join();
        assertEquals(1, releaseCount.get());
    }

    @Test
    public void testReleaseInOtherThread3() throws Exception {
        TwoLevelPool p = new TwoLevelPool(false, p1, p2);
        AtomicInteger releaseCount = new AtomicInteger(0);
        TwoLevelPool p2 = p.toReleaseInOtherThreadInstance(Thread.currentThread(), (rb, c) -> {
            releaseCount.incrementAndGet();
            c.accept(rb);
            return true;
        });
        RefBuffer b1 = p2.borrow(false, 128, 0);
        // large buffer not invoke callback when release
        Thread t = new Thread(b1::release);
        t.start();
        t.join();
        assertEquals(0, releaseCount.get());
    }
}
