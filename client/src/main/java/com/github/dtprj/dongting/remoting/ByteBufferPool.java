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
package com.github.dtprj.dongting.remoting;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Simple ByteBuffer pool, not thread safe.
 *
 * @author huangli
 */
class ByteBufferPool {
    private final int[] bufSizes;
    private final ByteBuffer[][] bufferStacks;
    private final long[][] returnTimes;
    private final int[] bottomIndices;
    private final int[] topIndices;
    private final int[] stackSizes;
    private final long timeoutNanos;
    private final boolean direct;

    private static final int[] DEFAULT_BUF_SIZE = new int[]{1024, 2048, 4096, 8192, 16 * 1024,
            32 * 1024, 64 * 1024, 128 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024, 2 * 1024 * 1024,
            4 * 1024 * 1024};

    private static final int[] DEFAULT_MAX_COUNT = new int[]{128, 128, 128, 128, 128,
            128, 128, 64, 64, 32, 16, 8,
            4};

    private static final int[] DEFAULT_MIN_COUNT = new int[]{16, 16, 16, 16, 16,
            16, 8, 4, 2, 1, 0, 0,
            0};

    public ByteBufferPool(boolean direct) {
        this(direct, DEFAULT_BUF_SIZE, DEFAULT_MIN_COUNT, DEFAULT_MAX_COUNT, 10 * 1000);
    }

    public ByteBufferPool(boolean direct, int[] bufSizes, int[] minCount, int[] maxCount, long timeoutMillis) {
        Objects.requireNonNull(bufSizes);
        Objects.requireNonNull(minCount);
        Objects.requireNonNull(maxCount);
        if (bufSizes.length != minCount.length || bufSizes.length != maxCount.length) {
            throw new IllegalArgumentException();
        }
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("timeout: " + timeoutMillis);
        }
        for (int i : bufSizes) {
            if (i <= 0) {
                throw new IllegalArgumentException();
            }
        }
        for (int i : minCount) {
            if (i < 0) {
                throw new IllegalArgumentException();
            }
        }
        for (int i : maxCount) {
            if (i <= 0) {
                throw new IllegalArgumentException();
            }
        }
        this.direct = direct;
        this.bufSizes = bufSizes;
        this.timeoutNanos = timeoutMillis * 1000 * 1000;
        bufferStacks = new ByteBuffer[bufSizes.length][];
        returnTimes = new long[bufSizes.length][];
        for (int i = 0; i < bufSizes.length; i++) {
            bufferStacks[i] = new ByteBuffer[maxCount.length];
            returnTimes[i] = new long[maxCount.length];
        }
        bottomIndices = new int[bufSizes.length];
        topIndices = new int[bufSizes.length];
        stackSizes = new int[bufSizes.length];
    }

    private ByteBuffer allocate(int size) {
        return this.direct ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
    }

    public ByteBuffer borrow(int requestSize) {
        int[] bufSizes = this.bufSizes;
        int stackCount = bufSizes.length;
        int stackIndex = 0;
        for (; stackIndex < stackCount; stackIndex++) {
            if (bufSizes[stackIndex] >= requestSize) {
                break;
            }
        }

        if (stackIndex >= stackCount) {
            // request buffer too large, allocate without pool
            return allocate(requestSize);
        }
        int[] stackSizes = this.stackSizes;
        if (stackSizes[stackIndex] == 0) {
            // no buffer available
            return allocate(bufSizes[stackIndex]);
        }
        int[] topIndices = this.topIndices;
        ByteBuffer[] bufferStack = this.bufferStacks[stackIndex];
        int stackCapacity = bufferStack.length;
        int topIndex = topIndices[stackIndex];
        topIndex = topIndex == 0 ? stackCapacity - 1 : topIndex - 1;
        topIndices[stackIndex] = topIndex;
        stackSizes[stackIndex]--;

        // get from pool
        ByteBuffer buf = bufferStack[topIndex];
        bufferStack[topIndex] = null;
        return buf;
    }

    public void release(ByteBuffer buf, long nanos) {
        int[] bufSizes = this.bufSizes;
        int stackCount = bufSizes.length;
        int stackIndex = 0;
        for (; stackIndex < stackCount; stackIndex++) {
            if (bufSizes[stackIndex] == buf.capacity()) {
                break;
            }
        }
        if (stackIndex >= stackCount) {
            // buffer too large, release it without pool
            return;
        }
        int[] stackSizes = this.stackSizes;
        ByteBuffer[] bufferStack = this.bufferStacks[stackIndex];
        int stackCapacity = bufferStack.length;
        if (stackSizes[stackIndex] >= stackCapacity) {
            // too many buffer in pool
            return;
        }
        int[] topIndices = this.topIndices;
        int topIndex = topIndices[stackIndex];

        // return it to pool
        buf.clear();
        bufferStack[topIndex] = buf;
        this.returnTimes[stackIndex][topIndex] = nanos;

        topIndex = topIndex + 1 >= stackCapacity ? 0 : topIndex + 1;
        topIndices[stackIndex] = topIndex;
        stackSizes[stackIndex]++;
    }

    public void clean(long nanos) {
        ByteBuffer[][] bufferStacks = this.bufferStacks;
        long[][] returnTimes = this.returnTimes;
        int[] bottomIndices = this.bottomIndices;
        int[] stackSizes = this.stackSizes;
        int stackCount = bufferStacks.length;
        long expireNanos = nanos - this.timeoutNanos;
        for (int stackIndex = 0; stackIndex < stackCount; stackIndex++) {
            ByteBuffer[] stack = bufferStacks[stackIndex];
            long[] stackReturnTime = returnTimes[stackIndex];
            int size = stackSizes[stackIndex];
            int bottom = bottomIndices[stackIndex];
            int capacity = stack.length;
            while (size > 0) {
                if (stackReturnTime[bottom] - expireNanos > 0) {
                    break;
                } else {
                    // expired
                    stack[bottom] = null;
                    size--;
                    bottom++;
                    if (bottom >= capacity) {
                        bottom = 0;
                    }
                }
            }
            bottomIndices[stackIndex] = bottom;
            stackSizes[stackIndex] = size;
        }
    }
}
