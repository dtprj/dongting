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

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
class FixSizeBufferPool {
    private static final int MAGIC_INDEX = 0;
    private final int bufferSize;
    private final int maxCount;
    private final int minCount;
    private final boolean direct;
    private final ShareBudget shareBudget;
    private static final int MAGIC = 0xEA1D9C07;

    final IndexedQueue<ByteBuffer> bufferStack;
    private final WeakRefCache<ByteBuffer> weakRefCache;

    // min stack size observed within current shrink period; updated on borrow, reset to
    // MAX_VALUE on clean(). Equals the count of bottom elements never borrowed (LIFO).
    private int periodMinStackSize = Integer.MAX_VALUE;

    long statBorrowCount;
    long statBorrowHitCount;
    long statReleaseCount;
    long statReleaseHitCount;

    public FixSizeBufferPool(boolean direct, ShareBudget shareBudget,
                              int minCount, int maxCount, int bufferSize, int weakRefThreshold) {
        this.direct = direct;
        this.shareBudget = shareBudget;
        if (bufferSize < 16) {
            throw new IllegalArgumentException("buffer size too small: " + bufferSize);
        }
        this.minCount = minCount;
        this.maxCount = maxCount;
        this.bufferSize = bufferSize;
        this.bufferStack = new IndexedQueue<>(maxCount);
        // Enable weak reference feature for heap buffers with size >= threshold
        // Direct buffers are excluded because they create "iceberg" objects
        boolean weakRefEnabled = !direct && bufferSize >= weakRefThreshold;
        this.weakRefCache = weakRefEnabled ? new WeakRefCache<>(16) : null;
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
        if (weakRefCache != null) {
            ByteBuffer buf = weakRefCache.borrow();
            if (buf != null) {
                return buf;
            }
        }

        ByteBuffer buf = bufferStack.pollLast();
        if (buf != null) {
            int s = bufferStack.size();
            if (s < periodMinStackSize) {
                periodMinStackSize = s;
            }
            updateCurrentUsedShareSizeAfterRemove();
            return buf;
        }
        return null;
    }

    private void updateCurrentUsedShareSizeAfterRemove() {
        int size = bufferStack.size();
        if (size >= maxCount) {
            shareBudget.release(bufferSize);
        }
    }

    public boolean release(ByteBuffer buf) {
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
            if (!shareBudget.borrow(bufferSize)) {
                if (weakRefCache != null) {
                    buf.putInt(MAGIC_INDEX, MAGIC);
                    weakRefCache.releaseToCache(buf);
                }
                return false;
            }
        }
        statReleaseHitCount++;

        // return it to pool
        buf.putInt(MAGIC_INDEX, MAGIC);
        bufferStack.addLast(buf);
        return true;
    }

    public void clean() {
        WeakRefCache<ByteBuffer> weakRefCache = this.weakRefCache;
        if (weakRefCache != null) {
            weakRefCache.cleanHeadAndTail();
        }
        IndexedQueue<ByteBuffer> stack = this.bufferStack;
        int size = stack.size();
        // minSize is the lowest stack size in this period; equals the count of bottom
        // elements never borrowed (LIFO). If no borrow happened this period, the sentinel
        // MAX_VALUE is clamped by size, meaning the whole stack is untouched.
        int minSize = periodMinStackSize > size ? size : periodMinStackSize;
        periodMinStackSize = Integer.MAX_VALUE;

        if (minSize <= 0) {
            return;
        }
        // shrink half of the untouched portion (floor). at least one so the last idle buffer
        // is reachable when minCount==0. lower bound is minCount; no upper bound so borrowed
        // ShareBudget returns naturally as usage drops.
        int toClean = minSize / 2;
        if (toClean == 0) {
            toClean = 1;
        }
        int target = size - toClean;
        if (target < minCount) {
            target = minCount;
        }
        int cleanCount = size - target;
        for (int i = 0; i < cleanCount; i++) {
            ByteBuffer buf = stack.pollFirst();
            updateCurrentUsedShareSizeAfterRemove();
            if (direct) {
                SimpleByteBufferPool.VF.releaseDirectBuffer(buf);
            } else if (weakRefCache != null) {
                weakRefCache.moveIdleElementsToCache(buf);
            }
        }
    }

    public void cleanAll() {
        IndexedQueue<ByteBuffer> stack = this.bufferStack;
        int size = stack.size();
        for (int i = 0; i < size; i++) {
            ByteBuffer buf = stack.pollFirst();
            updateCurrentUsedShareSizeAfterRemove();
            if (direct) {
                SimpleByteBufferPool.VF.releaseDirectBuffer(buf);
            }
        }
    }
}
