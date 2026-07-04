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

import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static com.github.dtprj.dongting.buf.SimpleByteBufferPool.calcTotalSize;

/**
 * @author huangli
 */
public class DefaultPoolFactory implements PoolFactory {

    private static final DtLog log = DtLogs.getLogger(DefaultPoolFactory.class);

    public static final int[] DEFAULT_SMALL_SIZE = new int[]{128, 256, 512, 1024, 2048, 4096, 8192, 16384};
    // 557,056 bytes
    public static final int[] DEFAULT_SMALL_MIN_COUNT = new int[]{128, 64, 32, 16, 16, 16, 16, 16};
    // 18,874,368 bytes
    public static final int[] DEFAULT_SMALL_MAX_COUNT = new int[]{8192, 4096, 2048, 1024, 1024, 1024, 512, 256};

    public static final int DEFAULT_THRESHOLD = 64;

    public static final int[] DEFAULT_GLOBAL_MIN_CHUNK_COUNT = new int[]{1};
    public static final int[] DEFAULT_GLOBAL_MAX_CHUNK_COUNT = new int[]{16};

    private static final Timestamp GLOBAL_TS = new Timestamp();

    private static final BuddyBufferPool GLOBAL_DIRECT_POOL = createGlobalPool(true);
    private static final BuddyBufferPool GLOBAL_HEAP_POOL = createGlobalPool(false);

    static {
        Runnable r = () -> {
            GLOBAL_TS.refresh();
            GLOBAL_DIRECT_POOL.shrink();
            GLOBAL_HEAP_POOL.shrink();
        };
        DtUtil.LOW_PRIORITY_SCHEDULER.scheduleWithFixedDelay(r, 1, 1, TimeUnit.SECONDS);
    }

    private static BuddyBufferPool createGlobalPool(boolean direct) {
        int minChunk = calcByMem(DEFAULT_GLOBAL_MIN_CHUNK_COUNT)[0];
        int maxChunk = calcByMem(DEFAULT_GLOBAL_MAX_CHUNK_COUNT)[0];
        BuddyBufferPoolConfig c = new BuddyBufferPoolConfig(
                direct, true, GLOBAL_TS, BuddyBufferPoolConfig.DEFAULT_CHUNK_SIZE,
                BuddyBufferPoolConfig.DEFAULT_MIN_BLOCK_SIZE,
                minChunk, maxChunk, 60000);
        return new BuddyBufferPool(c);
    }

    @Override
    public Buffers createPool(Timestamp ts) {
        SimpleByteBufferPool heapPool = createPool(false);
        SimpleByteBufferPool directPool = createPool(true);
        return new Buffers(heapPool, directPool, GLOBAL_HEAP_POOL, GLOBAL_DIRECT_POOL);
    }

    private SimpleByteBufferPool createPool(boolean direct) {
        int[] minCount = calcByMem(DEFAULT_SMALL_MIN_COUNT);
        int[] maxCount = calcByMem(DEFAULT_SMALL_MAX_COUNT);
        SimpleByteBufferPoolConfig c = new SimpleByteBufferPoolConfig(direct,
                direct ? 0 : DEFAULT_THRESHOLD,
                DEFAULT_SMALL_SIZE, minCount, maxCount,
                calcTotalSize(DEFAULT_SMALL_SIZE, maxCount) / 2);
        return new SimpleByteBufferPool(c);
    }

    private static int[] calcByMem(int[] defaultSizes) {
        long max = Runtime.getRuntime().maxMemory();
        int[] sizes = new int[defaultSizes.length];
        for (int i = 0; i < defaultSizes.length; i++) {
            if (max < 1.01 * 1024 * 1024 * 1024) {
                sizes[i] = Math.max(defaultSizes[i] / 4, 1);
            } else if (max < 2.01 * 1024 * 1024 * 1024) {
                sizes[i] = Math.max(defaultSizes[i] / 2, 1);
            } else if (max >= 8L * 1000 * 1000 * 1000) {
                sizes[i] = defaultSizes[i] * 2;
            } else {
                sizes[i] = defaultSizes[i];
            }
        }
        return sizes;
    }

    @Override
    public void initPool(Buffers buffers, Thread owner,
                         BiFunction<RefBuffer, Consumer<RefBuffer>, Boolean> crossThreadCallback) {
        buffers.init(owner, crossThreadCallback);
    }

    @Override
    public void destroyPool(Buffers pool) {
        if (DtUtil.DEBUG >= 2) {
            log.info("direct pool stat: {}\nheap pool stat: {}",
                    pool.directPool.formatStat(), pool.heapPool.formatStat());
        }
        pool.heapPool.cleanAll();
        pool.directPool.cleanAll();
    }
}
