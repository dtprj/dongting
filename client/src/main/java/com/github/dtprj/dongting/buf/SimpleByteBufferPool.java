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
import com.github.dtprj.dongting.common.Timestamp;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Objects;
import java.util.Random;

/**
 * Simple ByteBuffer pool, not thread safe.
 *
 * @author huangli
 */
public class SimpleByteBufferPool extends ByteBufferPool {
    public static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0).asReadOnlyBuffer();
    public static final int DEFAULT_THRESHOLD = 128;

    private final int threshold;
    private final int[] bufSizes;
    private final long timeoutNanos;
    private final boolean direct;
    private final boolean threadSafe;
    private final long shareSize;

    private long statBorrowTooSmallCount;
    private long statBorrowTooLargeCount;

    private Timestamp ts;
    private final Pool[] pools;
    private long currentUsedShareSize;

    public static final int[] DEFAULT_BUF_SIZE = new int[]{1024, 2048, 4096, 8192, 16 * 1024,
            32 * 1024, 64 * 1024, 128 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024, 2 * 1024 * 1024,
            4 * 1024 * 1024};

    // 111,149,056 bytes
    public static final int[] DEFAULT_MAX_COUNT = new int[]{1024, 512, 256, 128, 128,
            128, 128, 64, 64, 32, 16, 8,
            4};

    // 3,129,344 bytes
    public static final int[] DEFAULT_MIN_COUNT = new int[]{16, 16, 16, 16, 16,
            16, 8, 4, 2, 1, 0, 0,
            0};

    public static final long DEFAULT_TIME_OUT_MILLIS = 10 * 1000;

    public SimpleByteBufferPool(Timestamp ts, boolean direct, int threshold) {
        this(new SimpleByteBufferPoolConfig(ts, direct, threshold, false));
    }

    public SimpleByteBufferPool(Timestamp ts, boolean direct) {
        this(new SimpleByteBufferPoolConfig(ts, direct));
    }

    public SimpleByteBufferPool(SimpleByteBufferPoolConfig config) {
        Objects.requireNonNull(config.getBufSizes());
        Objects.requireNonNull(config.getMinCount());
        Objects.requireNonNull(config.getMaxCount());
        this.threadSafe = config.isThreadSafe();
        if (threadSafe) {
            // Thread safe pool should use a dedicated Timestamp
            this.ts = new Timestamp();
        } else {
            this.ts = config.getTs();
        }
        this.direct = config.isDirect();
        this.threshold = config.getThreshold();
        this.bufSizes = config.getBufSizes();
        this.timeoutNanos = config.getTimeoutMillis() * 1000 * 1000;
        this.shareSize = config.getShareSize();

        int[] bufSizes = this.bufSizes;
        int[] minCount = config.getMinCount();
        int[] maxCount = config.getMaxCount();

        int bufferTypeCount = bufSizes.length;
        if (bufferTypeCount != minCount.length || bufferTypeCount != maxCount.length) {
            throw new IllegalArgumentException();
        }
        if (config.getTimeoutMillis() <= 0) {
            throw new IllegalArgumentException("timeout<=0. timeout=" + config.getTimeoutMillis());
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

        this.pools = new Pool[bufferTypeCount];
        Random r = new Random();
        for (int i = 0; i < bufferTypeCount; i++) {
            this.pools[i] = new Pool(minCount[i], maxCount[i], bufSizes[i], r.nextLong());
        }
    }

    @Override
    public ByteBuffer allocate(int size) {
        return this.direct ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
    }

    @Override
    public ByteBuffer borrow(int requestSize) {
        if (requestSize <= threshold) {
            if (threadSafe) {
                synchronized (this) {
                    statBorrowTooSmallCount++;
                }
            } else {
                statBorrowTooSmallCount++;
            }
            return allocate(requestSize);
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
            // request buffer too large, allocate without pool
            if (threadSafe) {
                synchronized (this) {
                    statBorrowTooLargeCount++;
                }
            } else {
                statBorrowTooLargeCount++;
            }
            return allocate(requestSize);
        }

        ByteBuffer result;
        if (threadSafe) {
            synchronized (this) {
                result = pools[poolIndex].borrow();
            }
        } else {
            result = pools[poolIndex].borrow();
        }
        if (result != null) {
            return result;
        } else {
            int size = bufSizes[poolIndex];
            return allocate(size);
        }
    }

    @Override
    public void release(ByteBuffer buf) {
        if (threadSafe) {
            synchronized (this) {
                release0(buf);
            }
        } else {
            release0(buf);
        }
    }

    private void release0(ByteBuffer buf) {
        int capacity = buf.capacity();
        if (capacity <= threshold) {
            return;
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
            // buffer too large, release it without pool
            return;
        }
        pools[poolIndex].release(buf, ts.getNanoTime());
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
        long expireNanos = ts.getNanoTime() - this.timeoutNanos;
        for (Pool pool : pools) {
            pool.clean(expireNanos);
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
            Pool p = pools[i];
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
            Pool p = pools[i];
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

    class Pool {
        private static final int MAGIC_INDEX = 0;
        private static final int RETURN_TIME_INDEX = 8;
        private final int bufferSize;
        private final int maxCount;
        private final int minCount;
        private final long magic;

        private final IndexedQueue<ByteBuffer> bufferStack;

        long statBorrowCount;
        long statBorrowHitCount;
        long statReleaseCount;
        long statReleaseHitCount;

        public Pool(int minCount, int maxCount, int bufferSize, long magic) {
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
            if (buf.getLong(MAGIC_INDEX) == magic) {
                // shit
                for (int i = 0, stackSize = bufferStack.size(); i < stackSize; i++) {
                    if (bufferStack.get(i) == buf) {
                        throw new DtException("A bug may exist where the buffer is released twice.");
                    }
                }
            }

            if (bufferStack.size() >= maxCount) {
                long newUsedShareSize = currentUsedShareSize + bufferSize;
                if (newUsedShareSize > shareSize) {
                    // too many buffer in pool
                    return;
                } else {
                    currentUsedShareSize = newUsedShareSize;
                }
            }
            statReleaseHitCount++;

            // return it to pool
            buf.clear();
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
}


