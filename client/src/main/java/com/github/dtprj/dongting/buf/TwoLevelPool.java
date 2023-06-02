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

import java.nio.ByteBuffer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * @author huangli
 */
public class TwoLevelPool extends ByteBufferPool {
    private final boolean direct;
    private final ByteBufferPool smallPool;
    private final ByteBufferPool largePool;
    private final int threshold;
    private final boolean releaseInOtherThread;
    private final Consumer<ByteBuffer> releaseCallback;
    private final Thread owner;

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
        return new TwoLevelPool(direct, p1, GLOBAL_POOL, 16 * 1024);
    };

    public TwoLevelPool(boolean direct, ByteBufferPool smallPool, ByteBufferPool largePool, int threshold) {
        this(direct, smallPool, largePool, threshold, false, null, null);
    }

    private TwoLevelPool(boolean direct, ByteBufferPool smallPool, ByteBufferPool largePool,
                         int threshold, boolean releaseInOtherThread,
                         Consumer<ByteBuffer> releaseCallback, Thread owner) {
        this.direct = direct;
        this.smallPool = smallPool;
        this.largePool = largePool;
        this.threshold = threshold;
        this.releaseInOtherThread = releaseInOtherThread;
        this.releaseCallback = releaseCallback;
        this.owner = owner;
    }

    @Override
    public ByteBuffer borrow(int requestSize) {
        Thread owner = this.owner;
        if (owner != null && owner != Thread.currentThread()) {
            throw new DtException("borrow in other thread");
        }
        if (requestSize <= threshold) {
            return smallPool.borrow(requestSize);
        } else {
            return largePool.borrow(requestSize);
        }
    }

    @Override
    public void release(ByteBuffer buf) {
        int c = buf.capacity();
        if (c <= threshold) {
            if (releaseInOtherThread && owner != Thread.currentThread()) {
                releaseCallback.accept(buf);
            } else {
                smallPool.release(buf);
            }
        } else {
            largePool.release(buf);
        }
    }

    @Override
    public ByteBuffer allocate(int requestSize) {
        return direct ? ByteBuffer.allocateDirect(requestSize) : ByteBuffer.allocate(requestSize);
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

    public TwoLevelPool toReleaseInOtherThreadInstance(Thread owner, Consumer<ByteBuffer> releaseCallback) {
        return new TwoLevelPool(direct, smallPool, largePool,
                threshold, true, releaseCallback, owner);
    }

    @SuppressWarnings("unused")
    public ByteBufferPool getSmallPool() {
        return smallPool;
    }

    public ByteBufferPool getLargePool() {
        return largePool;
    }

    public static BiFunction<Timestamp, Boolean, ByteBufferPool> getDefaultFactory() {
        return DEFAULT_FACTORY;
    }

    @SuppressWarnings("unused")
    public static void setDefaultFactory(BiFunction<Timestamp, Boolean, ByteBufferPool> defaultFactory) {
        GLOBAL_POOL = null;
        DEFAULT_FACTORY = defaultFactory;
    }
}
