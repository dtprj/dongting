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

import java.util.concurrent.TimeUnit;

import static com.github.dtprj.dongting.buf.SimpleByteBufferPool.calcTotalSize;

/**
 * @author huangli
 */
public class DefaultPoolFactory implements PoolFactory {

    public static final int[] DEFAULT_GLOBAL_SIZE = new int[]{32 * 1024, 64 * 1024, 128 * 1024, 256 * 1024, 512 * 1024,
            1024 * 1024, 2 * 1024 * 1024, 4 * 1024 * 1024};
    // 18,874,368 bytes
    public static final int[] DEFAULT_GLOBAL_MIN_COUNT = new int[]{32, 32, 24, 16, 2, 1, 1, 1};
    // 104,857,600 bytes
    public static final int[] DEFAULT_GLOBAL_MAX_COUNT = new int[]{128, 128, 64, 64, 32, 16, 8, 4};

    public static final int[] DEFAULT_SMALL_SIZE = new int[]{128, 256, 512, 1024, 2048, 4096, 8192, 16384};
    // 557,056 bytes
    public static final int[] DEFAULT_SMALL_MIN_COUNT = new int[]{128, 64, 32, 16, 16, 16, 16, 16};
    // 18,874,368 bytes
    public static final int[] DEFAULT_SMALL_MAX_COUNT = new int[]{8192, 4096, 2048, 1024, 1024, 1024, 512, 256};

    private static final SimpleByteBufferPool GLOBAL_DIRECT_POOL = createGlobalPool(true);
    private static final SimpleByteBufferPool GLOBAL_HEAP_POOL = createGlobalPool(false);

    static {
        Runnable r = () -> {
            GLOBAL_DIRECT_POOL.clean();
            GLOBAL_HEAP_POOL.clean();
        };
        DtUtil.SCHEDULED_SERVICE.scheduleWithFixedDelay(r, 1, 1, TimeUnit.SECONDS);
    }

    private static SimpleByteBufferPool createGlobalPool(boolean direct) {
        // Thread safe pool should use a dedicated timestamp
        SimpleByteBufferPoolConfig c = new SimpleByteBufferPoolConfig(
                null, direct, 0, true, DEFAULT_GLOBAL_SIZE, DEFAULT_GLOBAL_MIN_COUNT,
                DEFAULT_GLOBAL_MAX_COUNT, 60000,
                calcTotalSize(DEFAULT_GLOBAL_SIZE, DEFAULT_GLOBAL_MAX_COUNT) / 2);
        return new SimpleByteBufferPool(c);
    }

    @Override
    public ByteBufferPool createPool(Timestamp ts, boolean direct) {
        SimpleByteBufferPoolConfig c = new SimpleByteBufferPoolConfig(ts, direct, direct ? 0 : 64, false,
                DEFAULT_SMALL_SIZE, DEFAULT_SMALL_MIN_COUNT, DEFAULT_SMALL_MAX_COUNT, 20000,
                calcTotalSize(DEFAULT_SMALL_SIZE, DEFAULT_SMALL_MAX_COUNT) / 2);
        SimpleByteBufferPool p1 = new SimpleByteBufferPool(c);
        return new TwoLevelPool(direct, p1, direct ? GLOBAL_DIRECT_POOL : GLOBAL_HEAP_POOL);
    }

    @Override
    public void destroyPool(ByteBufferPool pool) {
        TwoLevelPool p = (TwoLevelPool) pool;
        ((SimpleByteBufferPool) p.getSmallPool()).cleanAll();
    }
}
