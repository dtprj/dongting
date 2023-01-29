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

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Objects;

/**
 * Simple ByteBuffer pool, not thread safe.
 *
 * @author huangli
 */
// TODO byte buffer pool need optimise
public class SimpleByteBufferPool extends ByteBufferPool {
    public static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0).asReadOnlyBuffer();
    public static final int DEFAULT_THRESHOLD = 128;

    private final int threshold;
    private final int[] bufSizes;
    private final ByteBuffer[][] bufferStacks;
    private final long[][] returnTimes;
    private final int[] bottomIndices;
    private final int[] topIndices;
    private final int[] stackSizes;
    private final long timeoutNanos;
    private final boolean direct;
    private final int[] minCount;

    private final long[] statBorrowCount;
    private final long[] statBorrowHitCount;
    private long statBorrowTooSmallCount;
    private long statBorrowTooLargeCount;
    private final long[] statReleaseCount;
    private final long[] statReleaseHitCount;

    private long currentNanos = System.nanoTime();

    public static final int[] DEFAULT_BUF_SIZE = new int[]{1024, 2048, 4096, 8192, 16 * 1024,
            32 * 1024, 64 * 1024, 128 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024, 2 * 1024 * 1024,
            4 * 1024 * 1024};

    public static final int[] DEFAULT_MAX_COUNT = new int[]{1024, 512, 256, 128, 128,
            128, 128, 64, 64, 32, 16, 8,
            4};

    public static final int[] DEFAULT_MIN_COUNT = new int[]{16, 16, 16, 16, 16,
            16, 8, 4, 2, 1, 0, 0,
            0};

    public static final long DEFAULT_TIME_OUT_MILLIS = 10 * 1000;

    public SimpleByteBufferPool(boolean direct, int threshold) {
        this(direct, threshold, DEFAULT_BUF_SIZE, DEFAULT_MIN_COUNT, DEFAULT_MAX_COUNT, 10 * 1000);
    }

    public SimpleByteBufferPool(boolean direct) {
        this(direct, DEFAULT_THRESHOLD, DEFAULT_BUF_SIZE, DEFAULT_MIN_COUNT, DEFAULT_MAX_COUNT, 10 * 1000);
    }

    public SimpleByteBufferPool(boolean direct, int threshold, int[] bufSizes, int[] minCount, int[] maxCount, long timeoutMillis) {
        Objects.requireNonNull(bufSizes);
        Objects.requireNonNull(minCount);
        Objects.requireNonNull(maxCount);
        int bufferTypeCount = bufSizes.length;
        if (bufferTypeCount != minCount.length || bufferTypeCount != maxCount.length) {
            throw new IllegalArgumentException();
        }
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("timeout<=0. timeout=" + timeoutMillis);
        }
        for (int i : bufSizes) {
            if (i <= 0) {
                throw new IllegalArgumentException("bufSize<0");
            }
        }
        for (int i : minCount) {
            if (i < 0) {
                throw new IllegalArgumentException("minCount<0");
            }
        }
        for (int i = 0; i < maxCount.length; i++) {
            if (maxCount[i] <= 0) {
                throw new IllegalArgumentException("maxCount<0");
            }
            if (maxCount[i] < minCount[i]) {
                throw new IllegalArgumentException("maxCount<minCount");
            }
        }
        this.direct = direct;
        this.threshold = threshold;
        this.bufSizes = bufSizes;
        this.timeoutNanos = timeoutMillis * 1000 * 1000;
        bufferStacks = new ByteBuffer[bufferTypeCount][];
        returnTimes = new long[bufferTypeCount][];
        for (int i = 0; i < bufferTypeCount; i++) {
            bufferStacks[i] = new ByteBuffer[maxCount[i]];
            returnTimes[i] = new long[maxCount[i]];
        }
        bottomIndices = new int[bufferTypeCount];
        topIndices = new int[bufferTypeCount];
        stackSizes = new int[bufferTypeCount];
        this.minCount = minCount;

        this.statBorrowCount = new long[bufferTypeCount];
        this.statBorrowHitCount = new long[bufferTypeCount];
        this.statReleaseCount = new long[bufferTypeCount];
        this.statReleaseHitCount = new long[bufferTypeCount];
    }

    private ByteBuffer allocate(int size) {
        return this.direct ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
    }

    @Override
    public ByteBuffer borrow(int requestSize) {
        if (requestSize < threshold) {
            statBorrowTooSmallCount++;
            return allocate(requestSize);
        }
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
            statBorrowTooLargeCount++;
            return allocate(requestSize);
        }
        statBorrowCount[stackIndex]++;

        int[] stackSizes = this.stackSizes;
        if (stackSizes[stackIndex] == 0) {
            // no buffer available
            return allocate(bufSizes[stackIndex]);
        }
        statBorrowHitCount[stackIndex]++;
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

    @Override
    public void release(ByteBuffer buf) {
        int capacity = buf.capacity();
        if (capacity < threshold) {
            return;
        }
        int[] bufSizes = this.bufSizes;
        int stackCount = bufSizes.length;
        int stackIndex = 0;
        for (; stackIndex < stackCount; stackIndex++) {
            if (bufSizes[stackIndex] == capacity) {
                break;
            }
        }
        if (stackIndex >= stackCount) {
            // buffer too large, release it without pool
            return;
        }
        statReleaseCount[stackIndex]++;
        int[] stackSizes = this.stackSizes;
        ByteBuffer[] bufferStack = this.bufferStacks[stackIndex];
        int stackCapacity = bufferStack.length;
        if (stackSizes[stackIndex] >= stackCapacity) {
            // too many buffer in pool
            return;
        }
        statReleaseHitCount[stackIndex]++;
        int[] topIndices = this.topIndices;
        int topIndex = topIndices[stackIndex];

        // return it to pool
        buf.clear();
        bufferStack[topIndex] = buf;
        this.returnTimes[stackIndex][topIndex] = currentNanos;

        topIndex = topIndex + 1 >= stackCapacity ? 0 : topIndex + 1;
        topIndices[stackIndex] = topIndex;
        stackSizes[stackIndex]++;
    }

    public void clean(Timestamp ts) {
        ByteBuffer[][] bufferStacks = this.bufferStacks;
        long[][] returnTimes = this.returnTimes;
        int[] bottomIndices = this.bottomIndices;
        int[] stackSizes = this.stackSizes;
        int stackCount = bufferStacks.length;
        long expireNanos = ts.getNanoTime() - this.timeoutNanos;
        for (int stackIndex = 0; stackIndex < stackCount; stackIndex++) {
            ByteBuffer[] stack = bufferStacks[stackIndex];
            long[] stackReturnTime = returnTimes[stackIndex];
            int size = stackSizes[stackIndex];
            int bottom = bottomIndices[stackIndex];
            int capacity = stack.length;
            int min = this.minCount[stackIndex];
            while (size > min) {
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

    public void refreshCurrentNanos(Timestamp ts) {
        this.currentNanos = ts.getNanoTime();
    }

    public String formatStat() {
        StringBuilder sb = new StringBuilder(512);
        DecimalFormat f1 = new DecimalFormat("#,###");
        NumberFormat f2 = NumberFormat.getPercentInstance();
        f2.setMaximumFractionDigits(1);
        long totalBorrow = 0;
        long totalBorrowHit = 0;
        long totalRelease = 0;
        long totalReleaseHit = 0;
        int bufferTypeCount = bufSizes.length;
        for (int i = 0; i < bufferTypeCount; i++) {
            totalBorrow += statBorrowCount[i];
            totalRelease += statReleaseCount[i];
            totalBorrowHit += statBorrowHitCount[i];
            totalReleaseHit += statReleaseHitCount[i];
        }
        sb.append("borrow ").append(f1.format(totalBorrow)).append('(');
        if (totalBorrow == 0) {
            sb.append("0%");
        } else {
            sb.append(f2.format((double) totalBorrowHit / totalBorrow));
        }
        sb.append("), release ").append(f1.format(totalRelease)).append('(');
        if (totalRelease == 0) {
            sb.append("0%");
        } else {
            sb.append(f2.format((double) totalReleaseHit / totalRelease));
        }
        sb.append("), borrow too small ").append(f1.format(statBorrowTooSmallCount))
                .append(", borrow too large ").append(f1.format(statBorrowTooLargeCount))
                .append('\n');
        for (int s : bufSizes) {
            if (s < 1024) {
                sb.append(s);
            } else {
                sb.append(s / 1024).append("KB, ");
            }
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.deleteCharAt(sb.length() - 1);
        sb.append("\nborrow ");

        appendDetail(bufferTypeCount, sb, f1, f2, statBorrowCount, statBorrowHitCount);
        sb.append("\nrelease ");
        appendDetail(bufferTypeCount, sb, f1, f2, statReleaseCount, statReleaseHitCount);
        return sb.toString();
    }

    private void appendDetail(int bufferTypeCount, StringBuilder sb, DecimalFormat f1, NumberFormat f2,
                              long[] count, long[] hitCount) {
        for (int i = 0; i < bufferTypeCount; i++) {
            sb.append(f1.format(count[i]));
            sb.append('(');
            if (count[i] == 0) {
                sb.append("0%");
            } else {
                sb.append(f2.format((double) hitCount[i] / count[i]));
            }
            sb.append("), ");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.deleteCharAt(sb.length() - 1);
    }

}
