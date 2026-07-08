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

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * @author huangli
 */
public class DefaultPoolFactory implements PoolFactory {

    private static final DtLog log = DtLogs.getLogger(DefaultPoolFactory.class);

    static final int DEFAULT_THRESHOLD = 64;
    static final int[] DEFAULT_SMALL_SIZE = new int[]{128, 192, 256, 384, 512, 768, 1024, 1536,
            2 * 1024, 3 * 1024, 4 * 1024, 6 * 1024, 8 * 1024, 12 * 1024};

    private static final long SMALL_POOL_SLOT_MIN_SIZE = 768 * 1024; // 14 slots total 10752KB
    private static final long SMALL_POOL_SLOT_MAX_SIZE = SMALL_POOL_SLOT_MIN_SIZE * 2;
    private static final long SMALL_POOL_SHARE_SIZE = SMALL_POOL_SLOT_MIN_SIZE * DEFAULT_SMALL_SIZE.length;

    private static final int DEFAULT_GLOBAL_MIN_CHUNK_COUNT = 4;
    private static final int DEFAULT_GLOBAL_MAX_CHUNK_COUNT = 8;
    private static final long DEFAULT_LARGE_SHARE = 400 * 1024 * 1024;

    private static final long LARGE_POOL_TIMEOUT_MILLIS = 60_000;

    public static final int DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024;
    public static final int DEFAULT_MIN_BLOCK_SIZE = 16 * 1024;

    private final GlobalIdleChunkList heapChunkList =
            new GlobalIdleChunkList(DEFAULT_LARGE_SHARE, false, DEFAULT_CHUNK_SIZE,
                    DEFAULT_MIN_BLOCK_SIZE, LARGE_POOL_TIMEOUT_MILLIS);
    private final GlobalIdleChunkList directChunkList =
            new GlobalIdleChunkList(DEFAULT_LARGE_SHARE, true, DEFAULT_CHUNK_SIZE,
                    DEFAULT_MIN_BLOCK_SIZE, LARGE_POOL_TIMEOUT_MILLIS);

    public static final DefaultPoolFactory INSTANCE = new DefaultPoolFactory();

    private final ScheduledFuture<?> scheduledFuture;

    protected DefaultPoolFactory() {
        scheduledFuture = DtUtil.LOW_PRIORITY_SCHEDULER.scheduleAtFixedRate(() -> {
            try {
                heapChunkList.run();
                directChunkList.run();
            } catch (Throwable t) {
                log.error("", t);
            }
        }, 0, 10, TimeUnit.SECONDS);
    }

    @Override
    public Buffers createPool(Timestamp ts) {
        long maxMemory = Runtime.getRuntime().maxMemory();
        SimpleByteBufferPool heapSmallPool = createSmallPool(maxMemory, false);
        SimpleByteBufferPool directSmallPool = createSmallPool(maxMemory, true);
        BuddyBufferPool heapLargePool = createLargePool(ts, maxMemory, false);
        BuddyBufferPool directLargePool = createLargePool(ts, maxMemory, true);
        return new Buffers(heapSmallPool, directSmallPool, heapLargePool, directLargePool);
    }

    private SimpleByteBufferPool createSmallPool(long maxMemory, boolean direct) {
        int[] minCount = new int[DEFAULT_SMALL_SIZE.length];
        for (int i = 0; i < minCount.length; i++) {
            long count = SMALL_POOL_SLOT_MIN_SIZE / DEFAULT_SMALL_SIZE[i];
            minCount[i] = (int) calcByMem(maxMemory, count);
        }
        int[] maxCount = new int[DEFAULT_SMALL_SIZE.length];
        for (int i = 0; i < maxCount.length; i++) {
            long count = SMALL_POOL_SLOT_MAX_SIZE / DEFAULT_SMALL_SIZE[i];
            maxCount[i] = (int) calcByMem(maxMemory, count);
        }
        SimpleByteBufferPoolConfig c = new SimpleByteBufferPoolConfig(direct, direct ? 0 : DEFAULT_THRESHOLD,
                DEFAULT_SMALL_SIZE, minCount, maxCount, calcByMem(maxMemory, SMALL_POOL_SHARE_SIZE));
        return new SimpleByteBufferPool(c);
    }

    private BuddyBufferPool createLargePool(Timestamp ts, long maxMemory, boolean direct) {
        int minChunk = (int) calcByMem(maxMemory, DEFAULT_GLOBAL_MIN_CHUNK_COUNT);
        int maxChunk = (int) calcByMem(maxMemory, DEFAULT_GLOBAL_MAX_CHUNK_COUNT);
        BuddyBufferPoolConfig c = new BuddyBufferPoolConfig(
                direct, false, ts, DEFAULT_CHUNK_SIZE,
                DEFAULT_MIN_BLOCK_SIZE,
                minChunk, maxChunk, LARGE_POOL_TIMEOUT_MILLIS);
        return new BuddyBufferPool(c, direct ? directChunkList : heapChunkList);
    }

    private static long calcByMem(long maxMemory, long size) {
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

    public void close() {
        scheduledFuture.cancel(false);
        heapChunkList.clear();
        directChunkList.clear();
    }
}
