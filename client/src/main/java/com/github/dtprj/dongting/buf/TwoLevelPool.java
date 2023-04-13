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
import java.util.function.BiFunction;

/**
 * @author huangli
 */
public class TwoLevelPool extends ByteBufferPool {
    private final SimpleByteBufferPool smallPool;
    private final SimpleByteBufferPool largePool;
    private final int threshold;

    private static SimpleByteBufferPool GLOBAL_POOL;

    private static BiFunction<Timestamp, Boolean, ByteBufferPool> DEFAULT_FACTORY = (ts, direct) -> {
        synchronized (TwoLevelPool.class) {
            if (GLOBAL_POOL == null) {
                int[] bufSize = new int[]{32 * 1024, 64 * 1024, 128 * 1024, 256 * 1024, 512 * 1024,
                        1024 * 1024, 2 * 1024 * 1024, 4 * 1024 * 1024};
                int[] minCount = new int[]{16, 8, 4, 2, 1, 0, 0, 0};
                int[] maxCount = new int[]{128, 128, 64, 64, 32, 16, 8, 4};
                // Thread safe pool should use a dedicated timestamp
                GLOBAL_POOL = new SimpleByteBufferPool(null, direct, 0, true,
                        bufSize, minCount, maxCount, 30000);
            }
        }
        int[] bufSize = new int[]{128, 256, 512, 1024, 2048, 4096, 8192, 16384};
        int[] minCount = new int[]{128, 64, 32, 16, 16, 16, 16, 16};
        int[] maxCount = new int[]{8192, 4096, 2048, 1024, 1024, 1024, 512, 256};
        SimpleByteBufferPool p1 = new SimpleByteBufferPool(ts, direct, 64, false,
                bufSize, minCount, maxCount, 10000);
        return new TwoLevelPool(p1, GLOBAL_POOL, 16 * 1024);
    };

    public TwoLevelPool(SimpleByteBufferPool smallPool, SimpleByteBufferPool largePool, int threshold) {
        this.smallPool = smallPool;
        this.largePool = largePool;
        this.threshold = threshold;
    }

    @Override
    public ByteBuffer borrow(int requestSize) {
        if (requestSize <= threshold) {
            return smallPool.borrow(requestSize);
        } else {
            return largePool.borrow(requestSize);
        }
    }

    @Override
    public void release(ByteBuffer buf) {
        if (buf.capacity() <= threshold) {
            smallPool.release(buf);
        } else {
            largePool.release(buf);
        }
    }

    @Override
    public ByteBuffer allocate(int requestSize) {
        if (requestSize <= threshold) {
            return smallPool.allocate(requestSize);
        } else {
            return largePool.allocate(requestSize);
        }
    }

    @Override
    public void clean() {
        smallPool.clean();
        largePool.clean();
    }

    @Override
    public String formatStat() {
        return smallPool.formatStat();
    }

    public static BiFunction<Timestamp, Boolean, ByteBufferPool> getDefaultFactory() {
        return DEFAULT_FACTORY;
    }

    public static void setDefaultFactory(BiFunction<Timestamp, Boolean, ByteBufferPool> defaultFactory) {
        GLOBAL_POOL = null;
        DEFAULT_FACTORY = defaultFactory;
    }
}
