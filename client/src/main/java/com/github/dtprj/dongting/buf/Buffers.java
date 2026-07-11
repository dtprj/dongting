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

import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.VersionFactory;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * Entry point for borrowing heap/direct buffers. Each instance is owned by a single thread
 * (dispatcher / NIO worker) and holds thread-local small and large pools. Multiple per-thread
 * large pools share a {@link ShareBudget} to cap the total chunk memory across threads.
 * Routing between the two is done here: requests within the small pool's bucket range are served
 * by the small pool; oversized requests go to the large pool ({@link BuddyBufferPool}).
 *
 * <p>Construction is two-phase: build via {@code new Buffers(heapPool, directPool, heapLargePool, directLargePool)},
 * then call {@link #init(Thread, BiFunction)} before any borrow that may release cross-thread.
 *
 * @author huangli
 */
public class Buffers {

    private static final DtLog log = DtLogs.getLogger(Buffers.class);

    public final int heapSmallPoolMax;
    public final int directSmallPoolMax;

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

    private final Consumer<RefBuffer> largeHeapLocalReleasor;
    private Consumer<RefBuffer> largeHeapThreadSafeReleasor;
    private final Consumer<RefBuffer> largeDirectLocalReleasor;
    private Consumer<RefBuffer> largeDirectThreadSafeReleasor;

    private static final long SHRINK_INTERVAL_MILLIS = 60_000;
    private long lastHeapShrinkTime = System.currentTimeMillis();
    private long lastDirectShrinkTime = lastHeapShrinkTime - 500;
    private long lastHeapLargeShrinkTime = lastHeapShrinkTime - 1000;
    private long lastDirectLargeShrinkTime = lastHeapLargeShrinkTime - 1500;

    private boolean destroyed;
    private LinkedList<RefBuffer> waitingDestroyQueue;

    /**
     * Test-only constructor: subclasses override borrow methods, so all backing pools stay null.
     */
    public Buffers() {
        this.heapPool = null;
        this.directPool = null;
        this.heapLargePool = null;
        this.directLargePool = null;
        this.heapLocalReleasor = null;
        this.directLocalReleasor = null;
        this.largeHeapLocalReleasor = null;
        this.largeDirectLocalReleasor = null;
        this.heapSmallPoolMax = 0;
        this.directSmallPoolMax = 0;
    }

    public Buffers(SimpleByteBufferPool heapPool, SimpleByteBufferPool directPool,
                   BuddyBufferPool heapLargePool, BuddyBufferPool directLargePool) {
        this.heapPool = heapPool;
        this.directPool = directPool;
        this.heapLargePool = heapLargePool;
        this.directLargePool = directLargePool;
        this.heapLocalReleasor = heapPool::release;
        this.directLocalReleasor = directPool::release;
        this.largeHeapLocalReleasor = heapLargePool::release;
        this.largeDirectLocalReleasor = directLargePool::release;
        this.heapSmallPoolMax = heapPool.bufSizeMax;
        this.directSmallPoolMax = directPool.bufSizeMax;
    }

    /**
     * Second-phase initialization: wires the owner thread and cross-thread release callback.
     */
    public void init(Thread owner, BiFunction<RefBuffer, Consumer<RefBuffer>, Boolean> crossThreadCallback) {
        this.owner = owner;
        this.crossThreadCallback = crossThreadCallback;
        this.heapThreadSafeReleasor = rb -> threadSafeRelease(rb, heapLocalReleasor, false);
        this.directThreadSafeReleasor = rb -> threadSafeRelease(rb, directLocalReleasor, true);
        this.largeHeapThreadSafeReleasor = rb -> threadSafeRelease(rb, largeHeapLocalReleasor, false);
        this.largeDirectThreadSafeReleasor = rb -> threadSafeRelease(rb, largeDirectLocalReleasor, true);
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
        if ((direct && buf.capacity() > directSmallPoolMax) || (!direct && buf.capacity() > heapSmallPoolMax)) {
            BuddyBufferPool pool = direct ? directLargePool : heapLargePool;
            synchronized (this) {
                if (destroyed) {
                    pool.release(rb);
                } else {
                    if (waitingDestroyQueue == null) {
                        waitingDestroyQueue = new LinkedList<>();
                    }
                    waitingDestroyQueue.add(rb);
                }
            }
        } else {
            if (direct) {
                VersionFactory.getInstance().releaseDirectBuffer(buf);
            }
            rb.buffer = null;
        }
    }

    /**
     * Sweeps weak ref caches (call frequency is up to the caller) and shrinks idle
     * buffers every minute.
     */
    public void clean() {
        long now = System.currentTimeMillis();
        heapPool.clean();
        directPool.clean();
        if (now - lastHeapShrinkTime > SHRINK_INTERVAL_MILLIS) {
            heapPool.shrink();
            lastHeapShrinkTime = now;
        }
        if (now - lastDirectShrinkTime > SHRINK_INTERVAL_MILLIS) {
            directPool.shrink();
            lastDirectShrinkTime = now;
        }
        if (now - lastHeapLargeShrinkTime > SHRINK_INTERVAL_MILLIS) {
            heapLargePool.shrink();
            lastHeapLargeShrinkTime = now;
        }
        if (now - lastDirectLargeShrinkTime > SHRINK_INTERVAL_MILLIS) {
            directLargePool.shrink();
            lastDirectLargeShrinkTime = now;
        }
    }

    // --- heap borrow ---

    public RefBuffer borrow(int requestSize) {
        return borrowHeap0(false, requestSize, 0, true);
    }

    public RefBuffer borrow(int requestSize, boolean plain) {
        return borrowHeap0(plain, requestSize, 0, true);
    }

    public RefBuffer borrow(int requestSize, boolean plain, boolean threadSafeRelease, int threshold) {
        return borrowHeap0(plain, requestSize, threshold, threadSafeRelease);
    }

    /**
     * Shortcut for {@code borrow(requestSize, true, false, 0)}.
     */
    public RefBuffer borrowLocal(int requestSize) {
        return borrowHeap0(true, requestSize, 0, false);
    }

    private RefBuffer borrowHeap0(boolean plain, int requestSize, int threshold, boolean crossThreadRelease) {
        if (requestSize <= threshold) {
            return heapPool.newUnpooledRefBuffer(plain, requestSize);
        }
        if (requestSize > heapSmallPoolMax) {
            return heapLargePool.borrow(plain, requestSize, crossThreadRelease ? largeHeapThreadSafeReleasor : largeHeapLocalReleasor);
        } else {
            return heapPool.borrow(plain, requestSize, crossThreadRelease ? heapThreadSafeReleasor : heapLocalReleasor);
        }
    }

    // --- direct borrow ---

    public RefBuffer borrowDirect(int requestSize) {
        return borrowDirect0(false, requestSize, 0, true);
    }

    public RefBuffer borrowDirect(int requestSize, boolean plain) {
        return borrowDirect0(plain, requestSize, 0, true);
    }

    public RefBuffer borrowDirect(int requestSize, boolean plain, boolean threadSafeRelease, int threshold) {
        return borrowDirect0(plain, requestSize, threshold, threadSafeRelease);
    }

    /**
     * Shortcut for {@code borrowDirect(requestSize, true, false, 0)}.
     */
    public RefBuffer borrowDirectLocal(int requestSize) {
        return borrowDirect0(true, requestSize, 0, false);
    }

    private RefBuffer borrowDirect0(boolean plain, int requestSize, int threshold, boolean crossThreadRelease) {
        if (requestSize <= threshold) {
            return directPool.newUnpooledRefBuffer(plain, requestSize);
        }
        if (requestSize > directSmallPoolMax) {
            return directLargePool.borrow(plain, requestSize, crossThreadRelease ? largeDirectThreadSafeReleasor : largeDirectLocalReleasor);
        } else {
            return directPool.borrow(plain, requestSize, crossThreadRelease ? directThreadSafeReleasor : directLocalReleasor);
        }
    }

    public void destroy() {
        if (DtUtil.DEBUG >= 2) {
            log.info("direct pool stat: {}\nheap pool stat: {}",
                    directPool.formatStat(), heapPool.formatStat());
        }
        heapPool.cleanAll();
        directPool.cleanAll();
        synchronized (this) {
            if (waitingDestroyQueue != null) {
                for (RefBuffer rb : waitingDestroyQueue) {
                    BuddyBufferPool pool = rb.buffer.isDirect() ? directLargePool : heapLargePool;
                    pool.release(rb);
                }
                waitingDestroyQueue = null;
            }
            heapLargePool.destroy();
            directLargePool.destroy();
            this.destroyed = true;
        }
    }
}
