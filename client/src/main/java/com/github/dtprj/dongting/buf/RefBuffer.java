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

import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.common.RefCount;
import com.github.dtprj.dongting.net.ByteBufferWriteFrame;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class RefBuffer extends RefCount implements Encodable {

    private ByteBuffer buffer;
    private final ByteBufferPool pool;
    private final boolean direct;

    RefBuffer(boolean plain, ByteBufferPool pool, int requestSize, int threshold) {
        super(plain);
        this.direct = pool.isDirect();
        if (requestSize <= threshold) {
            this.buffer = pool.allocate(requestSize);
            this.pool = null;
        } else {
            this.buffer = pool.borrow(requestSize);
            this.pool = pool;
        }
    }

    @Override
    public void retain(int increment) {
        if (pool == null && !direct) {
            return;
        }
        super.retain(increment);
    }

    @Override
    public boolean release(int decrement) {
        if (pool == null && !direct) {
            return false;
        }
        return super.release(decrement);
    }

    @Override
    protected void doClean() {
        if (pool != null) {
            pool.release(buffer);
        } else {
            // should be direct
            SimpleByteBufferPool.VF.releaseDirectBuffer(buffer);
        }
        this.buffer = null;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    @Override
    public boolean encode(EncodeContext context, ByteBuffer destBuffer) {
        ByteBuffer src = this.buffer;
        if (src == null || src.remaining() == 0) {
            return true;
        }
        if (src.isDirect()) {
            ByteBuffer srcCopy = (ByteBuffer) context.getStatus();
            srcCopy = ByteBufferWriteFrame.copyFromDirectBuffer(src, destBuffer, srcCopy);
            if (srcCopy.remaining() == 0) {
                return true;
            } else {
                context.setStatus(srcCopy);
                return false;
            }
        } else {
            Integer s = (Integer) context.getStatus();
            int readBytes = 0;
            if (s != null) {
                readBytes = s;
            }
            readBytes = ByteBufferWriteFrame.copyFromHeapBuffer(src, destBuffer, readBytes);
            if (readBytes >= src.remaining()) {
                return true;
            } else {
                context.setStatus(readBytes);
                return false;
            }
        }
    }

    @Override
    public int actualSize() {
        return this.buffer == null ? 0 : this.buffer.remaining();
    }
}
