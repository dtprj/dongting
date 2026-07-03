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

import com.github.dtprj.dongting.common.VersionFactory;

import java.nio.ByteBuffer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * Entry point for borrowing heap/direct buffers. Each instance is owned by a single thread
 * (dispatcher / NIO worker) and holds a small thread-local pool plus a shared global large pool.
 * Routing between the two is done here: requests within the small pool's bucket range are served
 * by the small pool; oversized requests go to the large pool ({@link BuddyBufferPool}).
 *
 * <p>Construction is two-phase: build via {@code new Buffers(heapPool, directPool, ...)}, then call
 * {@link #init(Thread, BiFunction)} before any borrow that may release cross-thread.
 *
 * @author huangli
 */
public class Buffers {

    final SimpleByteBufferPool heapPool;
    final SimpleByteBufferPool directPool;
    final BuddyBufferPool heapLargePool;
    final BuddyBufferPool directLargePool;

    private Thread owner;
    private BiFunction<RefBuffer, Consumer<RefBuffer>, Boolean> crossThreadCallback;

    private final Consumer<RefBuffer> heapLocalReleasor;
    private Consumer<RefBuffer> heapThreadSafeReleasor;
    private final Consumer<RefBuffer> directLocalReleasor;
    private Consumer<RefBuffer> directThreadSafeReleasor;

    private static final long CLEAN_INTERVAL_MILLIS = 60_000;
    private long lastHeapCleanTime = System.currentTimeMillis();
    private long lastDirectCleanTime = lastHeapCleanTime - 500;

    public Buffers(SimpleByteBufferPool heapPool, SimpleByteBufferPool directPool) {
        this(heapPool, directPool, null, null);
    }

    public Buffers(SimpleByteBufferPool heapPool, SimpleByteBufferPool directPool,
                   BuddyBufferPool heapLargePool, BuddyBufferPool directLargePool) {
        this.heapPool = heapPool;
        this.directPool = directPool;
        this.heapLargePool = heapLargePool;
        this.directLargePool = directLargePool;
        // heapPool/directPool is null only in unit test
        this.heapLocalReleasor = heapPool == null ? null : heapPool::release;
        this.directLocalReleasor = directPool == null ? null : directPool::release;
    }

    /**
     * Second-phase initialization: wires the owner thread and cross-thread release callback.
     */
    public void init(Thread owner, BiFunction<RefBuffer, Consumer<RefBuffer>, Boolean> crossThreadCallback) {
        this.owner = owner;
        this.crossThreadCallback = crossThreadCallback;
        this.heapThreadSafeReleasor = rb -> threadSafeRelease(rb, heapLocalReleasor, false);
        this.directThreadSafeReleasor = rb -> threadSafeRelease(rb, directLocalReleasor, true);
    }

    private void threadSafeRelease(RefBuffer rb, Consumer<RefBuffer> localReleasor, boolean direct) {
        if (Thread.currentThread() == owner) {
            localReleasor.accept(rb);
        } else if (!crossThreadCallback.apply(rb, localReleasor)) {
            // owner thread queue is shut down; release locally
            releaseAfterShutdown(rb, direct);
        }
    }

    private void releaseAfterShutdown(RefBuffer rb, boolean direct) {
        ByteBuffer buf = rb.buffer;
        if (direct) {
            VersionFactory.getInstance().releaseDirectBuffer(buf);
        }
        rb.buffer = null;
    }

    /**
     * Only cleans small pools; large pools are global and cleaned by a background scheduler.
     */
    public void clean() {
        long now = System.currentTimeMillis();
        if (now - lastHeapCleanTime > CLEAN_INTERVAL_MILLIS) {
            heapPool.clean();
            lastHeapCleanTime = now;
        }
        if (now - lastDirectCleanTime > CLEAN_INTERVAL_MILLIS) {
            directPool.clean();
            lastDirectCleanTime = now;
        }
    }

    // --- heap borrow ---

    public RefBuffer borrow(int requestSize) {
        return borrowHeap0(false, requestSize, 0, heapThreadSafeReleasor);
    }

    public RefBuffer borrow(int requestSize, boolean plain) {
        return borrowHeap0(plain, requestSize, 0, heapThreadSafeReleasor);
    }

    public RefBuffer borrow(int requestSize, boolean plain, boolean threadSafeRelease, int threshold) {
        Consumer<RefBuffer> releasor = threadSafeRelease ? heapThreadSafeReleasor : heapLocalReleasor;
        return borrowHeap0(plain, requestSize, threshold, releasor);
    }

    /** Shortcut for {@code borrow(requestSize, true, false, 0)}. */
    public RefBuffer borrowLocal(int requestSize) {
        return borrowHeap0(true, requestSize, 0, heapLocalReleasor);
    }

    private RefBuffer borrowHeap0(boolean plain, int requestSize, int threshold, Consumer<RefBuffer> releasor) {
        if (requestSize > heapPool.bufSizeMax && heapLargePool != null) {
            return heapLargePool.borrow(plain, requestSize, threshold);
        }
        return heapPool.borrow0(plain, requestSize, threshold, releasor);
    }

    // --- direct borrow ---

    public RefBuffer borrowDirect(int requestSize) {
        return borrowDirect0(false, requestSize, 0, directThreadSafeReleasor);
    }

    public RefBuffer borrowDirect(int requestSize, boolean plain) {
        return borrowDirect0(plain, requestSize, 0, directThreadSafeReleasor);
    }

    public RefBuffer borrowDirect(int requestSize, boolean plain, boolean threadSafeRelease, int threshold) {
        Consumer<RefBuffer> releasor = threadSafeRelease ? directThreadSafeReleasor : directLocalReleasor;
        return borrowDirect0(plain, requestSize, threshold, releasor);
    }

    /** Shortcut for {@code borrowDirect(requestSize, true, false, 0)}. */
    public RefBuffer borrowDirectLocal(int requestSize) {
        return borrowDirect0(true, requestSize, 0, directLocalReleasor);
    }

    private RefBuffer borrowDirect0(boolean plain, int requestSize, int threshold, Consumer<RefBuffer> releasor) {
        if (requestSize > directPool.bufSizeMax && directLargePool != null) {
            return directLargePool.borrow(plain, requestSize, threshold);
        }
        return directPool.borrow0(plain, requestSize, threshold, releasor);
    }
}
