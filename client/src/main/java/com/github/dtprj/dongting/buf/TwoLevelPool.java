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

import java.nio.ByteBuffer;
import java.util.function.Consumer;

/**
 * @author huangli
 */
public class TwoLevelPool extends ByteBufferPool {
    private final ByteBufferPool smallPool;
    private final ByteBufferPool largePool;
    private final int threshold;
    private final boolean releaseInOtherThread;
    private final Consumer<ByteBuffer> releaseCallback;
    private final Thread owner;

    public TwoLevelPool(boolean direct, ByteBufferPool smallPool, ByteBufferPool largePool, int threshold) {
        this(direct, smallPool, largePool, threshold, false, null, null);
    }

    private TwoLevelPool(boolean direct, ByteBufferPool smallPool, ByteBufferPool largePool,
                         int threshold, boolean releaseInOtherThread,
                         Consumer<ByteBuffer> releaseCallback, Thread owner) {
        super(direct);
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
    }

    @Override
    public String formatStat() {
        return smallPool.formatStat();
    }

    public TwoLevelPool toReleaseInOtherThreadInstance(Thread owner, Consumer<ByteBuffer> releaseCallback) {
        return new TwoLevelPool(direct, smallPool, largePool,
                threshold, true, releaseCallback, owner);
    }

    public ByteBufferPool getSmallPool() {
        return smallPool;
    }

    public ByteBufferPool getLargePool() {
        return largePool;
    }

}
