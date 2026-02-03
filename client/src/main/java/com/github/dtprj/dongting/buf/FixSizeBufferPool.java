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
import com.github.dtprj.dongting.common.IndexedQueue;

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
    private final SimpleByteBufferPool p;
    private final boolean direct;
    private final long shareSize;
    private static final int MAGIC = 0xEA1D9C07;

    private final IndexedQueue<ByteBuffer> bufferStack;

    long statBorrowCount;
    long statBorrowHitCount;
    long statReleaseCount;
    long statReleaseHitCount;

    public FixSizeBufferPool(SimpleByteBufferPool p, boolean direct, long shareSize, int minCount, int maxCount, int bufferSize) {
        this.p = p;
        this.direct = direct;
        this.shareSize = shareSize;
        if (bufferSize < 16) {
            throw new IllegalArgumentException("buffer size too small: " + bufferSize);
        }
        this.minCount = minCount;
        this.maxCount = maxCount;
        this.bufferSize = bufferSize;
        this.bufferStack = new IndexedQueue<>(maxCount);
    }

    public ByteBuffer borrow() {
        statBorrowCount++;
        ByteBuffer buf = bufferStack.removeLast();
        if (buf != null) {
            int bufMagic = buf.getInt(MAGIC_INDEX);
            if (bufMagic != MAGIC) {
                throw new DtException("A bug may exist where the buffer is written to after release.");
            }
            buf.putInt(MAGIC_INDEX, 0);
            statBorrowHitCount++;
            updateCurrentUsedShareSizeAfterRemove();
            buf.clear();
        }
        return buf;
    }

    private void updateCurrentUsedShareSizeAfterRemove() {
        int size = bufferStack.size();
        if (size >= maxCount) {
            p.currentUsedShareSize -= bufferSize;
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
                    throw new DtException("A bug may exist where the buffer is released twice.");
                }
            }
        }

        if (bufferStack.size() >= maxCount) {
            long newUsedShareSize = p.currentUsedShareSize + bufferSize;
            if (newUsedShareSize > shareSize) {
                // too many buffer in pool
                return false;
            } else {
                p.currentUsedShareSize = newUsedShareSize;
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
                }
            }
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
