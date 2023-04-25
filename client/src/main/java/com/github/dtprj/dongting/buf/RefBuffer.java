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
import com.github.dtprj.dongting.common.RefCount;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class RefBuffer extends RefCount {

    private final ByteBuffer buffer;
    private final ByteBufferPool pool;

    RefBuffer(boolean plain, ByteBufferPool pool, int requestSize, int threshold) {
        super(plain);
        if (requestSize <= threshold) {
            this.buffer = pool.allocate(requestSize);
            this.pool = null;
        } else {
            this.buffer = pool.borrow(requestSize);
            this.pool = pool;
        }
    }

    private RefBuffer(int requestSize, boolean direct) {
        super(true);
        this.buffer = direct ? ByteBuffer.allocateDirect(requestSize) : ByteBuffer.allocate(requestSize);
        this.pool = null;
    }

    public static RefBuffer createUnpooled(int requestSize, boolean direct) {
        return new RefBuffer(requestSize, direct);
    }

    @Override
    public void retain(int increment) {
        if (pool == null) {
            return;
        }
        super.retain(increment);
    }

    @Override
    public boolean release(int decrement) {
        if (pool == null) {
            return false;
        }
        boolean result = super.release(decrement);
        if (result) {
            pool.release(buffer);
        }
        return result;
    }

    public ByteBuffer getBuffer() {
        if (DtUtil.DEBUG && isReleased()) {
            throw new IllegalStateException("buffer is released");
        }
        return buffer;
    }
}
