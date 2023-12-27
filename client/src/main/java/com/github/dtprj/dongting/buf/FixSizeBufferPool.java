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
import com.github.dtprj.dongting.common.VersionFactory;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
class FixSizeBufferPool {
    private static final int MAGIC_INDEX = 0;
    private static final int RETURN_TIME_INDEX = 8;
    private final int bufferSize;
    private final int maxCount;
    private final int minCount;
    private final SimpleByteBufferPool p;
    private final boolean direct;
    private final long shareSize;
    private final long magic;

    private final IndexedQueue<ByteBuffer> bufferStack;
    private static final VersionFactory VF = VersionFactory.getInstance();

    long statBorrowCount;
    long statBorrowHitCount;
    long statReleaseCount;
    long statReleaseHitCount;

    public FixSizeBufferPool(SimpleByteBufferPool p, boolean direct, long shareSize, int minCount, int maxCount, int bufferSize, long magic) {
        this.p = p;
        this.direct = direct;
        this.shareSize = shareSize;
        this.magic = magic;
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
            long bufMagic = buf.getLong(MAGIC_INDEX);
            if (bufMagic != magic) {
                throw new DtException("A bug may exist where the buffer is written to after release.");
            }
            statBorrowHitCount++;
        }
        return buf;
    }

    public void release(ByteBuffer buf, long nanos) {
        statReleaseCount++;
        IndexedQueue<ByteBuffer> bufferStack = this.bufferStack;
        // ByteBuffer.getLong may check limit, so we clear buffer first
        buf.clear();
        if (buf.getLong(MAGIC_INDEX) == magic) {
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
                if (direct) {
                    VF.releaseDirectBuffer(buf);
                }
                return;
            } else {
                p.currentUsedShareSize = newUsedShareSize;
            }
        }
        statReleaseHitCount++;

        // return it to pool
        // use the buffer to store return time and magic
        buf.putLong(MAGIC_INDEX, magic);
        buf.putLong(RETURN_TIME_INDEX, nanos);

        bufferStack.addLast(buf);
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
            }
        }
    }
}
