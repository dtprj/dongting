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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link Buffers} cross-thread release routing and small/large pool dispatch.
 *
 * @author huangli
 */
public class BuffersTest {

    private static BuddyBufferPool largePool(boolean direct) {
        // minBlock=64 so threshold=32; small pool max bucket is 32, so requests > 32 go to large
        return new BuddyBufferPool(new BuddyBufferPoolConfig(direct, 1024, 64, 1, 2, 60000, true));
    }

    private static SimpleByteBufferPool smallPool(boolean direct) {
        return new SimpleByteBufferPool(new SimpleByteBufferPoolConfig(direct, 0,
                new int[]{16, 32}, new int[]{1, 2}, new int[]{2, 2}, 0));
    }

    private static Buffers newBuffers(Thread owner,
                                      BiFunction<RefBuffer, Consumer<RefBuffer>, Boolean> cb) {
        SimpleByteBufferPool heapSmall = smallPool(false);
        SimpleByteBufferPool directSmall = smallPool(true);
        Buffers buffers = new Buffers(heapSmall, directSmall, largePool(false), largePool(true));
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
        assertEquals(64, b3.getBuffer().capacity());
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
        RefBuffer b1 = buffers.borrow(64, false, true, 0);
        // large buffer is borrowed from the shared large pool, whose releasor releases directly
        // (the large pool is thread-safe), so the cross-thread callback is never involved
        Thread t = new Thread(b1::release);
        t.start();
        t.join();
        assertEquals(0, releaseCount.get());
    }

    @Test
    public void testDirectLargeBufferReleaseInOtherThreadSkipsCallback() throws Exception {
        AtomicInteger releaseCount = new AtomicInteger(0);
        Buffers buffers = newBuffers(Thread.currentThread(), (rb, c) -> {
            releaseCount.incrementAndGet();
            c.accept(rb);
            return true;
        });
        RefBuffer b1 = buffers.borrowDirect(64, false, true, 0);
        assertTrue(b1.getBuffer().isDirect());
        assertEquals(64, b1.getBuffer().capacity());
        // large direct buffer: thread-safe pool, callback not involved
        Thread t = new Thread(b1::release);
        t.start();
        t.join();
        assertEquals(0, releaseCount.get());
    }

    @Test
    public void testLocalReleaseSkipsCallback() {
        AtomicInteger releaseCount = new AtomicInteger(0);
        // callback returns false to simulate owner thread queue shut down
        Buffers buffers = newBuffers(Thread.currentThread(), (rb, c) -> {
            releaseCount.incrementAndGet();
            return false;
        });
        RefBuffer b1 = buffers.borrow(16, false, false, 0);
        // local release: no callback, no cross-thread
        assertEquals(16, b1.getBuffer().capacity());
        b1.release();
        assertEquals(0, releaseCount.get());
    }

    @Test
    public void testReleaseAfterShutdownHeapFromOtherThread() throws Exception {
        AtomicInteger releaseCount = new AtomicInteger(0);
        // callback returns false to simulate owner thread queue shut down
        Buffers buffers = newBuffers(Thread.currentThread(), (rb, c) -> {
            releaseCount.incrementAndGet();
            return false;
        });
        RefBuffer b1 = buffers.borrow(16, false, true, 0);
        // release from non-owner thread with shut-down queue: buffer should be discarded locally
        Thread t = new Thread(b1::release);
        t.start();
        t.join();
        assertEquals(1, releaseCount.get());
    }

    @Test
    public void testDirectBorrowAndCrossThreadRelease() throws Exception {
        AtomicInteger releaseCount = new AtomicInteger(0);
        Buffers buffers = newBuffers(Thread.currentThread(), (rb, c) -> {
            releaseCount.incrementAndGet();
            c.accept(rb);
            return true;
        });
        RefBuffer b1 = buffers.borrowDirect(16, false, true, 0);
        assertEquals(16, b1.getBuffer().capacity());
        assertTrue(b1.getBuffer().isDirect());
        Thread t = new Thread(b1::release);
        t.start();
        t.join();
        assertEquals(1, releaseCount.get());
    }
}
