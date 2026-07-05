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

import java.util.function.BiFunction;
import java.util.function.Consumer;

import static com.github.dtprj.dongting.buf.SimpleByteBufferPool.calcTotalSize;

/**
 * @author huangli
 */
public class DefaultPoolFactory implements PoolFactory {

    public static final int[] DEFAULT_SMALL_SIZE = new int[]{128, 256, 512, 1024, 2048, 4096, 8192, 16384};
    // 557,056 bytes
    public static final int[] DEFAULT_SMALL_MIN_COUNT = new int[]{128, 64, 32, 16, 16, 16, 16, 16};
    // 18,874,368 bytes
    public static final int[] DEFAULT_SMALL_MAX_COUNT = new int[]{8192, 4096, 2048, 1024, 1024, 1024, 512, 256};

    public static final int DEFAULT_THRESHOLD = 64;

    private static final int DEFAULT_GLOBAL_MIN_CHUNK_COUNT = 4;
    private static final int DEFAULT_GLOBAL_MAX_CHUNK_COUNT = 8;
    private static final long DEFAULT_LARGE_SHARE = 400 * 1024 * 1024;

    private final ShareBudget largeShare = new ShareBudget(DEFAULT_LARGE_SHARE, true);

    @Override
    public Buffers createPool(Timestamp ts) {
        SimpleByteBufferPool heapSmallPool = createSmallPool(false);
        SimpleByteBufferPool directSmallPool = createSmallPool(true);
        BuddyBufferPool heapLargePool = createLargePool(ts, false);
        BuddyBufferPool directLargePool = createLargePool(ts, true);
        return new Buffers(heapSmallPool, directSmallPool, heapLargePool, directLargePool);
    }

    private SimpleByteBufferPool createSmallPool(boolean direct) {
        long maxMemory = Runtime.getRuntime().maxMemory();
        int[] minCount = calcByMem(maxMemory, DEFAULT_SMALL_MIN_COUNT);
        int[] maxCount = calcByMem(maxMemory, DEFAULT_SMALL_MAX_COUNT);
        SimpleByteBufferPoolConfig c = new SimpleByteBufferPoolConfig(direct,
                direct ? 0 : DEFAULT_THRESHOLD,
                DEFAULT_SMALL_SIZE, minCount, maxCount,
                calcTotalSize(DEFAULT_SMALL_SIZE, maxCount) / 2);
        return new SimpleByteBufferPool(c);
    }

    private BuddyBufferPool createLargePool(Timestamp ts, boolean direct) {
        long maxMemory = Runtime.getRuntime().maxMemory();
        int minChunk = calcByMem(maxMemory, DEFAULT_GLOBAL_MIN_CHUNK_COUNT);
        int maxChunk = calcByMem(maxMemory, DEFAULT_GLOBAL_MAX_CHUNK_COUNT);
        BuddyBufferPoolConfig c = new BuddyBufferPoolConfig(
                direct, false, ts, BuddyBufferPoolConfig.DEFAULT_CHUNK_SIZE,
                BuddyBufferPoolConfig.DEFAULT_MIN_BLOCK_SIZE,
                minChunk, maxChunk, 60000);
        return new BuddyBufferPool(c, largeShare);
    }

    private static int[] calcByMem(long maxMemory, int[] size) {
        int[] result = new int[size.length];
        for (int i = 0; i < size.length; i++) {
            result[i] = calcByMem(maxMemory, size[i]);
        }
        return result;
    }

    private static int calcByMem(long maxMemory, int size) {
        if (maxMemory < 1.01 * 1024 * 1024 * 1024) {
            return Math.max(size / 4, 1);
        } else if (maxMemory < 2.01 * 1024 * 1024 * 1024) {
            return Math.max(size / 2, 1);
        } else if (maxMemory >= 8L * 1000 * 1000 * 1000) {
            return size * 2;
        } else {
            return size;
        }
    }

    @Override
    public void initPool(Buffers buffers, Thread owner,
                         BiFunction<RefBuffer, Consumer<RefBuffer>, Boolean> crossThreadCallback) {
        buffers.init(owner, crossThreadCallback);
    }

    @Override
    public void destroyPool(Buffers pool) {
        pool.destroy();
    }
}
