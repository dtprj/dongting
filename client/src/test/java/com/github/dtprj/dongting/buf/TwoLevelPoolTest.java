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

import java.nio.ByteBuffer;
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
        ByteBuffer b1 = p.borrow(31);
        ByteBuffer b2 = p.borrow(32);
        ByteBuffer b3 = p.borrow(33);
        assertEquals(32, b1.capacity());
        assertEquals(32, b2.capacity());
        assertEquals(128, b3.capacity());
        p.release(b1);
        p.release(b2);
        p.release(b3);
    }

    @Test
    public void testBorrowInOtherThread() {
        TwoLevelPool p = new TwoLevelPool(false, p1, p2);
        //noinspection InstantiatingAThreadWithDefaultRunMethod
        TwoLevelPool p2 = p.toReleaseInOtherThreadInstance(new Thread(), buf -> {
        });
        assertThrows(DtException.class, () -> p2.borrow(1));
    }

    @Test
    public void testReleaseInOtherThread1() {
        TwoLevelPool p = new TwoLevelPool(false, p1, p2);
        AtomicInteger releaseCount = new AtomicInteger(0);
        TwoLevelPool p2 = p.toReleaseInOtherThreadInstance(Thread.currentThread(), buf -> releaseCount.incrementAndGet());
        ByteBuffer b1 = p2.borrow(1);
        // release in same thread, not invoke callback
        p2.release(b1);
        assertEquals(0, releaseCount.get());
    }

    @Test
    public void testReleaseInOtherThread2() throws Exception {
        TwoLevelPool p = new TwoLevelPool(false, p1, p2);
        AtomicInteger releaseCount = new AtomicInteger(0);
        TwoLevelPool p2 = p.toReleaseInOtherThreadInstance(Thread.currentThread(), buf -> releaseCount.incrementAndGet());
        ByteBuffer b1 = p2.borrow(1);
        Thread t = new Thread(() -> p2.release(b1));
        t.start();
        t.join();
        assertEquals(1, releaseCount.get());
    }

    @Test
    public void testReleaseInOtherThread3() throws Exception {
        TwoLevelPool p = new TwoLevelPool(false, p1, p2);
        AtomicInteger releaseCount = new AtomicInteger(0);
        TwoLevelPool p2 = p.toReleaseInOtherThreadInstance(Thread.currentThread(), buf -> releaseCount.incrementAndGet());
        ByteBuffer b1 = p2.borrow(128);
        // large buffer not invoke callback when release
        Thread t = new Thread(() -> p2.release(b1));
        t.start();
        t.join();
        assertEquals(0, releaseCount.get());
    }
}
