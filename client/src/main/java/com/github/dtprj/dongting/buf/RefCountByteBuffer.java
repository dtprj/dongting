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

import com.github.dtprj.dongting.common.RefCount;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class RefCountByteBuffer {

    private ByteBuffer buffer;
    private SimpleByteBufferPool pool;
    private RefCount refCount;

    protected RefCountByteBuffer() {
    }

    public static RefCountByteBuffer create(SimpleByteBufferPool pool, int requestSize, int threshold) {
        RefCountByteBuffer value = new RefCountByteBuffer();
        if (requestSize < threshold) {
            value.buffer = ByteBuffer.allocate(requestSize);
        } else {
            value.buffer = pool.borrow(requestSize);
            value.pool = pool;
            value.refCount = RefCount.newInstance();
        }
        return value;
    }

    public static RefCountByteBuffer createPlain(SimpleByteBufferPool pool, int requestSize, int threshold) {
        RefCountByteBuffer value = new RefCountByteBuffer();
        if (requestSize < threshold) {
            value.buffer = ByteBuffer.allocate(requestSize);
        } else {
            value.buffer = pool.borrow(requestSize);
            value.pool = pool;
            value.refCount = RefCount.newPlainInstance();
        }
        return value;
    }

    public void retain() {
        RefCount refCount = this.refCount;
        if (refCount != null) {
            refCount.retain();
        }
    }

    public void release() {
        RefCount refCount = this.refCount;
        if (refCount != null) {
            if (refCount.release()) {
                pool.release(buffer);
            }
        }
    }

}
