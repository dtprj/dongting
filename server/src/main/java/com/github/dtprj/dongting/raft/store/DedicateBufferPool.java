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
package com.github.dtprj.dongting.raft.store;

import com.github.dtprj.dongting.buf.ByteBufferPool;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
class DedicateBufferPool {
    private final ByteBufferPool pool;
    private final int size;
    private final ByteBuffer buffer;
    private boolean borrowed = false;

    public DedicateBufferPool(ByteBufferPool pool, int size) {
        this.pool = pool;
        this.size = size;
        this.buffer = pool.allocate(size);
    }

    public ByteBuffer borrow() {
        if (!borrowed) {
            borrowed = true;
            return buffer;
        } else {
            return pool.borrow(size);
        }
    }

    public void release(ByteBuffer buf) {
        if (buffer == buf) {
            if (borrowed) {
                borrowed = false;
            } else {
                throw new IllegalStateException("dedicate buffer is not borrowed");
            }
        } else {
            pool.release(buf);
        }
    }
}
