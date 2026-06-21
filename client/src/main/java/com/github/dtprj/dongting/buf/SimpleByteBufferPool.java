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
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.common.VersionFactory;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Objects;

/**
 * Simple ByteBuffer pool, not thread safe.
 *
 * @author huangli
 */
public class SimpleByteBufferPool extends ByteBufferPool {
    static final VersionFactory VF = VersionFactory.getInstance();
    public static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    final int threshold;
    final int[] bufSizes;
    private final long timeoutNanos;
    private final boolean threadSafe;

    private long statBorrowTooSmallCount;
    private long statBorrowTooLargeCount;

    private Timestamp ts;
    private final FixSizeBufferPool[] pools;

    public SimpleByteBufferPool(SimpleByteBufferPoolConfig config) {
        super(config.direct);
        Objects.requireNonNull(config.bufSizes);
        Objects.requireNonNull(config.minCount);
        Objects.requireNonNull(config.maxCount);
        this.threadSafe = config.threadSafe;
        if (threadSafe) {
            // Thread safe pool should use a dedicated Timestamp
            this.ts = new Timestamp();
        } else {
            this.ts = config.ts;
        }
        this.threshold = config.threshold;
        this.bufSizes = config.bufSizes;
        this.timeoutNanos = config.timeoutMillis * 1000 * 1000;

        int[] bufSizes = this.bufSizes;
        int[] minCount = config.minCount;
        int[] maxCount = config.maxCount;

        int bufferTypeCount = bufSizes.length;
        if (bufferTypeCount != minCount.length || bufferTypeCount != maxCount.length) {
            throw new IllegalArgumentException();
        }
        if (config.timeoutMillis <= 0) {
            throw new IllegalArgumentException("timeout<=0. timeout=" + config.timeoutMillis);
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

        this.pools = new FixSizeBufferPool[bufferTypeCount];
        for (int i = 0; i < bufferTypeCount; i++) {
            this.pools[i] = new FixSizeBufferPool(config, direct, config.shareSize,
                    minCount[i], maxCount[i], bufSizes[i], config.weakRefThreshold);
        }
    }

    @Override
    public ByteBuffer allocate(int size) {
        return this.direct ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
    }

    @Override
    public RefBuffer borrow(boolean plain, int requestSize, int threshold) {
        return borrow0(plain, requestSize, threshold, this, true);
    }

    RefBuffer borrow0(boolean plain, int requestSize, int threshold,
                      ByteBufferPool returnPool, boolean allocateIfNotInPool) {
        if (requestSize < threshold) {
            return allocateIfNotInPool
                    ? new RefBuffer(plain, allocate(requestSize), null, !direct)
                    : null;
        }
        if (requestSize <= this.threshold) {
            incBorrowTooSmall();
            return allocateIfNotInPool
                    ? new RefBuffer(plain, allocate(requestSize), returnPool, false)
                    : null;
        }
        int[] bufSizes = this.bufSizes;
        int poolCount = bufSizes.length;
        int poolIndex = 0;
        for (; poolIndex < poolCount; poolIndex++) {
            if (bufSizes[poolIndex] >= requestSize) {
                break;
            }
        }
        if (poolIndex >= poolCount) {
            incBorrowTooLarge();
            return allocateIfNotInPool
                    ? new RefBuffer(plain, allocate(requestSize), returnPool, false)
                    : null;
        }
        ByteBuffer result;
        if (threadSafe) {
            synchronized (this) {
                result = pools[poolIndex].borrow();
            }
        } else {
            result = pools[poolIndex].borrow();
        }
        if (result == null) {
            return allocateIfNotInPool
                    ? new RefBuffer(plain, allocate(bufSizes[poolIndex]), returnPool, false)
                    : null;
        }
        return new RefBuffer(plain, result, returnPool, false);
    }

    private void incBorrowTooSmall() {
        if (threadSafe) {
            synchronized (this) {
                statBorrowTooSmallCount++;
            }
        } else {
            statBorrowTooSmallCount++;
        }
    }

    private void incBorrowTooLarge() {
        if (threadSafe) {
            synchronized (this) {
                statBorrowTooLargeCount++;
            }
        } else {
            statBorrowTooLargeCount++;
        }
    }

    @Override
    public void release(RefBuffer rb) {
        ByteBuffer buf = rb.buffer;
        if (buf != null) {
            releaseBuffer(buf);
        }
        rb.buffer = null;
    }

    void releaseBuffer(ByteBuffer buf) {
        boolean released;
        if (threadSafe) {
            synchronized (this) {
                ts.refresh(1);
                released = release0(buf);
            }
        } else {
            released = release0(buf);
        }
        if (!released && direct) {
            // buffer too small or too large, release it without pool
            VF.releaseDirectBuffer(buf);
        }
    }

    boolean release0(ByteBuffer buf) {
        if (buf.isDirect() != direct) {
            throw new DtException("the buffer not belong to this pool, direct=" + buf.isDirect());
        }
        int capacity = buf.capacity();
        if (capacity <= threshold) {
            return false;
        }
        int[] bufSizes = this.bufSizes;
        int poolCount = bufSizes.length;
        int poolIndex = 0;
        for (; poolIndex < poolCount; poolIndex++) {
            if (bufSizes[poolIndex] == capacity) {
                break;
            }
        }
        if (poolIndex >= poolCount) {
            if (buf.capacity() < bufSizes[bufSizes.length - 1]) {
                throw new DtException("the buffer not belong to this pool, capacity=" + buf.capacity());
            }
            return false;
        }
        return pools[poolIndex].release(buf, ts.nanoTime);
    }

    @Override
    public void clean() {
        if (threadSafe) {
            synchronized (this) {
                clean0();
            }
        } else {
            clean0();
        }
    }

    private void clean0() {
        long expireNanos = ts.nanoTime - this.timeoutNanos;
        for (FixSizeBufferPool pool : pools) {
            pool.clean(expireNanos);
        }
    }

    public void cleanAll() {
        for (FixSizeBufferPool pool : pools) {
            pool.cleanAll();
        }
    }

    public String formatStat() {
        if (threadSafe) {
            synchronized (this) {
                return formatStat0();
            }
        } else {
            return formatStat0();
        }
    }

    private String formatStat0() {
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
            FixSizeBufferPool p = pools[i];
            totalBorrow += p.statBorrowCount;
            totalRelease += p.statReleaseCount;
            totalBorrowHit += p.statBorrowHitCount;
            totalReleaseHit += p.statReleaseHitCount;
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
                sb.append(s).append("B, ");
            } else {
                sb.append(s / 1024).append("KB, ");
            }
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.deleteCharAt(sb.length() - 1);
        sb.append("\nborrow ");

        appendDetail(bufferTypeCount, sb, f1, f2, true);
        sb.append("\nrelease ");
        appendDetail(bufferTypeCount, sb, f1, f2, false);
        return sb.toString();
    }

    private void appendDetail(int bufferTypeCount, StringBuilder sb, DecimalFormat f1, NumberFormat f2, boolean borrow) {
        for (int i = 0; i < bufferTypeCount; i++) {
            FixSizeBufferPool p = pools[i];
            long count = borrow ? p.statBorrowCount : p.statReleaseCount;
            long hit = borrow ? p.statBorrowHitCount : p.statReleaseHitCount;
            sb.append(f1.format(count));
            sb.append('(');
            if (count == 0) {
                sb.append("0%");
            } else {
                sb.append(f2.format((double) hit / count));
            }
            sb.append("), ");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.deleteCharAt(sb.length() - 1);
    }

    public static long calcTotalSize(int[] bufSizes, int[] count) {
        long total = 0;
        for (int i = 0; i < bufSizes.length; i++) {
            total += (long) bufSizes[i] * count[i];
        }
        return total;
    }

    // for unit test
    void setTs(Timestamp ts) {
        this.ts = ts;
    }

    // for unit test
    Timestamp getTs() {
        return this.ts;
    }
}


