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

import java.nio.ByteBuffer;
import java.util.function.Consumer;

/**
 * @author huangli
 */
public abstract class ByteBufferPool {

    protected final boolean direct;
    // a request with size <= threshold is not worth pooling in this pool
    protected final int threshold;

    public ByteBufferPool(boolean direct, int threshold) {
        this.direct = direct;
        this.threshold = threshold;
    }

    public abstract RefBuffer borrow(boolean plain, int requestSize, int threshold);

    public void release(RefBuffer rb) {
        releaseBuffer(rb.buffer);
        rb.buffer = null;
    }

    protected ByteBuffer allocate(int size) {
        return direct ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
    }

    // releasor=null, dummy=!direct: heap unpooled relies on GC, direct unpooled is released via
    // RefBuffer.doClean -> VF.releaseDirectBuffer
    protected RefBuffer newUnpooledRefBuffer(boolean plain, int requestSize) {
        return new RefBuffer(plain, allocate(requestSize), null, !direct);
    }

    public abstract void shrink();

    public abstract String formatStat();

    // borrow with an explicit releasor so the caller (Buffers) can control release routing
    abstract RefBuffer borrow0(boolean plain, int requestSize, int threshold, Consumer<RefBuffer> releasor);

    abstract void releaseBuffer(ByteBuffer buf);
}
