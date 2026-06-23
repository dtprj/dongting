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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link Buffers} cross-thread release routing, superseding the old TwoLevelPoolTest.
 *
 * @author huangli
 */
public class BuffersTest {

    private static SimpleByteBufferPool largePool() {
        SimpleByteBufferPoolConfig c = new SimpleByteBufferPoolConfig(null, false, 0, true,
                new int[]{128, 256}, new int[]{1, 2}, new int[]{2, 2}, 1000, 0);
        return new SimpleByteBufferPool(c);
    }

    private static Buffers newBuffers(Thread owner,
                                      BiFunction<RefBuffer, Consumer<RefBuffer>, Boolean> cb) {
        SimpleByteBufferPool large = largePool();
        SimpleByteBufferPool heapSmall = new SimpleByteBufferPool(
                new SimpleByteBufferPoolConfig(new Timestamp(), false, 0, false,
                        new int[]{16, 32}, new int[]{1, 2}, new int[]{2, 2}, 1000, 0), large);
        SimpleByteBufferPool directSmall = new SimpleByteBufferPool(
                new SimpleByteBufferPoolConfig(new Timestamp(), true, 0, false,
                        new int[]{16, 32}, new int[]{1, 2}, new int[]{2, 2}, 1000, 0));
        Buffers buffers = new Buffers(heapSmall, directSmall);
        buffers.init(owner, cb);
        return buffers;
    }

    @Test
    public void testRouteSmallAndLarge() {
        Buffers buffers = newBuffers(Thread.currentThread(), (rb, c) -> {
            c.accept(rb);
            return true;
        });
        RefBuffer b1 = buffers.borrow(31, false, true, 0);
        RefBuffer b2 = buffers.borrow(32, false, true, 0);
        RefBuffer b3 = buffers.borrow(33, false, true, 0);
        assertEquals(32, b1.getBuffer().capacity());
        assertEquals(32, b2.getBuffer().capacity());
        assertEquals(128, b3.getBuffer().capacity());
        b1.release();
        b2.release();
        b3.release();
    }

    @Test
    public void testReleaseInSameThreadNotInvokeCallback() {
        AtomicInteger releaseCount = new AtomicInteger(0);
        Buffers buffers = newBuffers(Thread.currentThread(), (rb, c) -> {
            releaseCount.incrementAndGet();
            c.accept(rb);
            return true;
        });
        RefBuffer b1 = buffers.borrow(16, false, true, 0);
        // release in same thread must not invoke callback
        b1.release();
        assertEquals(0, releaseCount.get());
    }

    @Test
    public void testReleaseInOtherThreadInvokesCallback() throws Exception {
        AtomicInteger releaseCount = new AtomicInteger(0);
        Buffers buffers = newBuffers(Thread.currentThread(), (rb, c) -> {
            releaseCount.incrementAndGet();
            c.accept(rb);
            return true;
        });
        RefBuffer b1 = buffers.borrow(16, false, true, 0);
        Thread t = new Thread(b1::release);
        t.start();
        t.join();
        assertEquals(1, releaseCount.get());
    }

    @Test
    public void testLargeBufferReleaseInOtherThreadSkipsCallback() throws Exception {
        AtomicInteger releaseCount = new AtomicInteger(0);
        Buffers buffers = newBuffers(Thread.currentThread(), (rb, c) -> {
            releaseCount.incrementAndGet();
            c.accept(rb);
            return true;
        });
        RefBuffer b1 = buffers.borrow(128, false, true, 0);
        // large buffer is borrowed from the shared large pool, whose releasor releases directly
        // (the large pool is thread-safe), so the cross-thread callback is never involved
        Thread t = new Thread(b1::release);
        t.start();
        t.join();
        assertEquals(0, releaseCount.get());
    }
}
