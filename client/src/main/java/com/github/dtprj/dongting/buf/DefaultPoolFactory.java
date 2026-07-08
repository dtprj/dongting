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

    public static final DefaultPoolFactory INSTANCE = new DefaultPoolFactory(new DefaultPoolFactoryConfig());

    private final DefaultPoolFactoryConfig config;
    private final GlobalIdleChunkList heapChunkList;
    private final GlobalIdleChunkList directChunkList;

    private final ScheduledFuture<?> scheduledFuture;

    protected DefaultPoolFactory(DefaultPoolFactoryConfig config) {
        this.config = config;
        this.heapChunkList = new GlobalIdleChunkList(config.largeShareSize, false, config.largeChunkSize,
                config.largeMinBlockSize, config.largeGlobalTimeoutMillis, config.largeGlobalIdleTargetSize);
        this.directChunkList = new GlobalIdleChunkList(config.largeShareSize, true, config.largeChunkSize,
                config.largeMinBlockSize, config.largeGlobalTimeoutMillis, config.largeGlobalIdleTargetSize);
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
        int[] sizes = config.smallSize;
        int[] minCount = new int[sizes.length];
        for (int i = 0; i < minCount.length; i++) {
            long count = config.smallSlotMinSize / sizes[i];
            minCount[i] = (int) calcByMem(maxMemory, count);
        }
        int[] maxCount = new int[sizes.length];
        for (int i = 0; i < maxCount.length; i++) {
            long count = config.smallSlotMaxSize / sizes[i];
            maxCount[i] = (int) calcByMem(maxMemory, count);
        }
        SimpleByteBufferPoolConfig c = new SimpleByteBufferPoolConfig(direct, direct ? 0 : config.threshold,
                sizes, minCount, maxCount, calcByMem(maxMemory, config.smallShareSize));
        return new SimpleByteBufferPool(c);
    }

    private BuddyBufferPool createLargePool(Timestamp ts, long maxMemory, boolean direct) {
        int minChunk = (int) calcByMem(maxMemory, config.largeMinChunkCount);
        int maxChunk = (int) calcByMem(maxMemory, config.largeMaxChunkCount);
        BuddyBufferPoolConfig c = new BuddyBufferPoolConfig(
                direct, false, ts, config.largeChunkSize,
                config.largeMinBlockSize,
                minChunk, maxChunk, config.largeTimeoutMillis);
        return new BuddyBufferPool(c, direct ? directChunkList : heapChunkList);
    }

    protected long calcByMem(long maxMemory, long size) {
        if (maxMemory < 1.01 * 1024 * 1024 * 1024) {
            return Math.max(size / 4, 1);
        } else if (maxMemory < 2.01 * 1024 * 1024 * 1024) {
            return Math.max(size / 2, 1);
        } else if (maxMemory >= 8L * 1000 * 1000 * 1000) {
            return size * 2;
        } else if (maxMemory >= 16L * 1000 * 1000 * 1000) {
            return size * 4;
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
