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

import com.github.dtprj.dongting.common.DtBugException;
import com.github.dtprj.dongting.common.IndexedQueue;

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;

/**
 * @author huangli
 */
class FixSizeBufferPool {
    private static final int MAGIC_INDEX = 0;
    private static final int RETURN_TIME_INDEX = 4;
    private final int bufferSize;
    private final int maxCount;
    private final int minCount;
    private final SimpleByteBufferPoolConfig config;
    private final boolean direct;
    private final long shareSize;
    private static final int MAGIC = 0xEA1D9C07;

    private final IndexedQueue<ByteBuffer> bufferStack;
    private final IndexedQueue<WeakReference<ByteBuffer>> weakRefStack;
    private final boolean weakRefEnabled;

    long statBorrowCount;
    long statBorrowHitCount;
    long statReleaseCount;
    long statReleaseHitCount;

    public FixSizeBufferPool(SimpleByteBufferPoolConfig config, boolean direct, long shareSize,
                              int minCount, int maxCount, int bufferSize, int weakRefThreshold) {
        this.config = config;
        this.direct = direct;
        this.shareSize = shareSize;
        if (bufferSize < 16) {
            throw new IllegalArgumentException("buffer size too small: " + bufferSize);
        }
        this.minCount = minCount;
        this.maxCount = maxCount;
        this.bufferSize = bufferSize;
        this.bufferStack = new IndexedQueue<>(maxCount);
        // Enable weak reference feature for heap buffers with size >= threshold
        // Direct buffers are excluded because they create "iceberg" objects
        this.weakRefEnabled = !direct && bufferSize >= weakRefThreshold;
        this.weakRefStack = weakRefEnabled ? new IndexedQueue<>(16) : null;
    }

    public ByteBuffer borrow() {
        statBorrowCount++;
        ByteBuffer buf = borrow0();
        if (buf != null) {
            int bufMagic = buf.getInt(MAGIC_INDEX);
            if (bufMagic != MAGIC) {
                throw new DtBugException("A bug may exist where the buffer is written to after release.");
            }
            buf.putInt(MAGIC_INDEX, 0);
            statBorrowHitCount++;
            buf.clear();
        }
        return buf;
    }

    private ByteBuffer borrow0() {
        if (weakRefEnabled) {
            while (weakRefStack.size() > 0) {
                WeakReference<ByteBuffer> ref = weakRefStack.removeLast();
                ByteBuffer buf = ref.get();
                if (buf != null) {
                    return buf;
                }
            }
        }

        ByteBuffer buf = bufferStack.removeLast();
        if (buf != null) {
            updateCurrentUsedShareSizeAfterRemove();
            return buf;
        }
        return null;
    }

    private void updateCurrentUsedShareSizeAfterRemove() {
        int size = bufferStack.size();
        if (size >= maxCount) {
            config.currentUsedShareSize -= bufferSize;
        }
    }

    public boolean release(ByteBuffer buf, long nanos) {
        statReleaseCount++;
        IndexedQueue<ByteBuffer> bufferStack = this.bufferStack;
        // so most operation on this buffer may fail (if the user use it after release)
        buf.limit(buf.capacity());
        buf.position(buf.capacity());
        if (buf.getInt(MAGIC_INDEX) == MAGIC) {
            // shit
            for (int i = 0, stackSize = bufferStack.size(); i < stackSize; i++) {
                if (bufferStack.get(i) == buf) {
                    throw new DtBugException("A bug may exist where the buffer is released twice.");
                }
            }
        }

        if (bufferStack.size() >= maxCount) {
            long newUsedShareSize = config.currentUsedShareSize + bufferSize;
            if (newUsedShareSize > shareSize) {
                if (weakRefEnabled) {
                    // only write magic, return time is not needed
                    buf.putInt(MAGIC_INDEX, MAGIC);
                    weakRefStack.addLast(new WeakReference<>(buf));
                }
                return false;
            } else {
                config.currentUsedShareSize = newUsedShareSize;
            }
        }
        statReleaseHitCount++;

        // return it to pool
        // use the buffer to store return time and magic
        buf.putInt(MAGIC_INDEX, MAGIC);
        buf.putLong(RETURN_TIME_INDEX, nanos);

        bufferStack.addLast(buf);
        return true;
    }

    public void clean(long expireNanos) {
        IndexedQueue<ByteBuffer> stack = this.bufferStack;
        int size = stack.size();
        for (int i = 0; i < size - minCount; i++) {
            ByteBuffer buf = stack.get(0);
            if (buf.getLong(RETURN_TIME_INDEX) - expireNanos > 0) {
                break;
            } else {
                stack.removeFirst();
                updateCurrentUsedShareSizeAfterRemove();
                if (direct) {
                    SimpleByteBufferPool.VF.releaseDirectBuffer(buf);
                } else if (weakRefEnabled) {
                    // the buffer is not used recently, add to bottom
                    weakRefStack.addFirst(new WeakReference<>(buf));
                }
            }
        }
        if (weakRefEnabled) {
            cleanWeakRefHeadAndTail();
        }
    }

    private void cleanWeakRefHeadAndTail() {
        WeakReference<ByteBuffer> ref = weakRefStack.getFirst();
        if (ref == null) {
            return;
        }
        while (ref.get() == null) {
            weakRefStack.removeFirst();
            ref = weakRefStack.getFirst();
            if (ref == null) {
                return;
            }
        }
        ref = weakRefStack.getLast();
        while (ref != null && ref.get() == null) {
            weakRefStack.removeLast();
            ref = weakRefStack.getLast();
        }
    }

    public void cleanAll() {
        IndexedQueue<ByteBuffer> stack = this.bufferStack;
        int size = stack.size();
        for (int i = 0; i < size; i++) {
            ByteBuffer buf = stack.removeFirst();
            updateCurrentUsedShareSizeAfterRemove();
            if (direct) {
                SimpleByteBufferPool.VF.releaseDirectBuffer(buf);
            }
        }
    }
}
