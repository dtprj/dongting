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
 * (dispatcher / NIO worker) and holds a pool chain (small thread-local pool with a shared global
 * large pool as its {@code next}).
 *
 * <p>Construction is two-phase: build via {@code new Buffers(heapPool, directPool)}, then call
 * {@link #init(Thread, BiFunction)} before any borrow that may release cross-thread.
 *
 * @author huangli
 */
public class Buffers {

    final SimpleByteBufferPool heapPool;
    final SimpleByteBufferPool directPool;

    private Thread owner;
    private BiFunction<RefBuffer, Consumer<RefBuffer>, Boolean> crossThreadCallback;

    private final Consumer<RefBuffer> heapLocalReleasor;
    private Consumer<RefBuffer> heapThreadSafeReleasor;
    private final Consumer<RefBuffer> directLocalReleasor;
    private Consumer<RefBuffer> directThreadSafeReleasor;

    public Buffers(SimpleByteBufferPool heapPool, SimpleByteBufferPool directPool) {
        this.heapPool = heapPool;
        this.directPool = directPool;
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
        if (buf != null) {
            if (direct) {
                VersionFactory.getInstance().releaseDirectBuffer(buf);
            }
            rb.buffer = null;
        }
    }

    public void clean() {
        heapPool.clean();
        directPool.clean();
    }

    public RefBuffer borrow(int requestSize) {
        return heapPool.borrow0(false, requestSize, 0, heapThreadSafeReleasor);
    }

    public RefBuffer borrow(int requestSize, boolean plain) {
        return heapPool.borrow0(plain, requestSize, 0, heapThreadSafeReleasor);
    }

    public RefBuffer borrow(int requestSize, boolean plain, boolean threadSafeRelease, int threshold) {
        Consumer<RefBuffer> releasor = threadSafeRelease ? heapThreadSafeReleasor : heapLocalReleasor;
        return heapPool.borrow0(plain, requestSize, threshold, releasor);
    }

    /**
     * Borrow a heap buffer for single-thread (local) usage: both borrow and release happen in the
     * same thread. Equivalent to {@code borrow(requestSize, true, false, 0)}.
     */
    public RefBuffer borrowLocal(int requestSize) {
        return heapPool.borrow0(true, requestSize, 0, heapLocalReleasor);
    }

    public RefBuffer borrowDirect(int requestSize) {
        return directPool.borrow0(false, requestSize, 0, directThreadSafeReleasor);
    }

    public RefBuffer borrowDirect(int requestSize, boolean plain) {
        return directPool.borrow0(plain, requestSize, 0, directThreadSafeReleasor);
    }

    public RefBuffer borrowDirect(int requestSize, boolean plain, boolean threadSafeRelease, int threshold) {
        Consumer<RefBuffer> releasor = threadSafeRelease ? directThreadSafeReleasor : directLocalReleasor;
        return directPool.borrow0(plain, requestSize, threshold, releasor);
    }

    /**
     * Borrow a direct buffer for single-thread (local) usage: both borrow and release happen in the
     * same thread. Equivalent to {@code borrowDirect(requestSize, true, false, 0)}.
     */
    public RefBuffer borrowDirectLocal(int requestSize) {
        return directPool.borrow0(true, requestSize, 0, directLocalReleasor);
    }
}
