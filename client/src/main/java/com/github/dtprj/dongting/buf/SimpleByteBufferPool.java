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
import java.util.function.Consumer;

/**
 * Simple ByteBuffer pool, not thread safe.
 *
 * @author huangli
 */
public class SimpleByteBufferPool extends ByteBufferPool {
    static final VersionFactory VF = VersionFactory.getInstance();
    public static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    final int[] bufSizes;
    final int bufSizeMax;
    final ByteBufferPool next;
    private final long timeoutNanos;

    private long statBorrowTooSmallCount;
    private long statBorrowTooLargeCount;

    private final FixSizeBufferPool[] pools;

    private final Consumer<RefBuffer> defaultReleasor = this::release;

    public SimpleByteBufferPool(SimpleByteBufferPoolConfig config) {
        this(config, null);
    }

    public SimpleByteBufferPool(SimpleByteBufferPoolConfig config, ByteBufferPool next) {
        super(config.direct, config.threadSafe, config.threshold, config.threadSafe ? new Timestamp() : config.ts);
        Objects.requireNonNull(config.bufSizes);
        Objects.requireNonNull(config.minCount);
        Objects.requireNonNull(config.maxCount);
        this.bufSizes = config.bufSizes;
        this.bufSizeMax = bufSizes[bufSizes.length - 1];
        this.next = next;
        this.timeoutNanos = config.timeoutMillis * 1000 * 1000;

        int[] bufSizes = this.bufSizes;
        int[] minCount = config.minCount;
        int[] maxCount = config.maxCount;

        if (threshold > bufSizes[0]) {
            throw new IllegalArgumentException();
        }

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

        if (next instanceof SimpleByteBufferPool) {
            SimpleByteBufferPool sbNext = (SimpleByteBufferPool) next;
            for (int s : sbNext.bufSizes) {
                if (s > bufSizeMax) {
                    break;
                }
                boolean found = false;
                for (int smallSize : bufSizes) {
                    if (s == smallSize) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw new IllegalArgumentException();
                }
            }
        }
    }

    @Override
    public RefBuffer borrow(boolean plain, int requestSize, int threshold) {
        return borrow0(plain, requestSize, threshold, defaultReleasor);
    }

    @Override
    Consumer<RefBuffer> getDefaultReleasor() {
        return defaultReleasor;
    }

    @Override
    RefBuffer borrow0(boolean plain, int requestSize, int threshold, Consumer<RefBuffer> releasor) {
        if (requestSize > bufSizeMax) {
            if (next != null) {
                return borrowFromNext(plain, requestSize, threshold, releasor);
            }
            incBorrowTooLarge();
            return newUnpooledRefBuffer(plain, requestSize);
        }
        if (requestSize <= threshold) {
            return newUnpooledRefBuffer(plain, requestSize);
        }
        if (requestSize <= this.threshold) {
            incBorrowTooSmall();
            return newUnpooledRefBuffer(plain, requestSize);
        }
        int[] bufSizes = this.bufSizes;
        int poolCount = bufSizes.length;
        int poolIndex = 0;
        for (; poolIndex < poolCount; poolIndex++) {
            if (bufSizes[poolIndex] >= requestSize) {
                break;
            }
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
            if (next != null && requestSize > next.threshold) {
                return borrowFromNext(plain, requestSize, threshold, releasor);
            }
            // create new ByteBuffer
            return new RefBuffer(plain, allocate(bufSizes[poolIndex]), releasor, false);
        }
        // use pooled ByteBuffer
        return new RefBuffer(plain, result, releasor, false);
    }

    private RefBuffer borrowFromNext(boolean plain, int requestSize, int threshold, Consumer<RefBuffer> releasor) {
        // when next is thread-safe, bind next's releasor so the buffer returns to next (release
        // routing follows borrow origin, not capacity matching). A non-thread-safe next keeps the
        // caller's releasor: such buffers are released by the caller, so they won't be pooled by
        // next — a non-thread-safe next only saves allocation, not pooling.
        Consumer<RefBuffer> r = next.threadSafe ? next.getDefaultReleasor() : releasor;
        return next.borrow0(plain, requestSize, threshold, r);
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
    void releaseBuffer(ByteBuffer buf) {
        if (tryOffer(buf)) {
            return;
        }
        // bucket full: release directly. Each pool owns the buffers it lent (routed via the
        // releasor bound at borrow time), so never forward to next.
        if (direct) {
            VF.releaseDirectBuffer(buf);
        }
    }

    private boolean tryOffer(ByteBuffer buf) {
        int capacity = buf.capacity();
        if (capacity < bufSizes[0] || capacity > bufSizeMax) {
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
            throw new DtException("the buffer not belong to this pool, capacity=" + capacity);
        }
        if (threadSafe) {
            synchronized (this) {
                ts.refresh(1);
                return pools[poolIndex].release(buf, ts.nanoTime);
            }
        } else {
            return pools[poolIndex].release(buf, ts.nanoTime);
        }
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


