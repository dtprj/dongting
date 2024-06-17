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
public final class RefBuffer extends RefCount implements Encodable {

    private ByteBuffer buffer;
    private final ByteBufferPool pool;
    private final boolean direct;
    private final int size;

    private final RefBuffer root;

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
        this.size = this.buffer.remaining();
        this.root = null;
    }

    private RefBuffer(RefBuffer root, int absolutePos, int absoluteLimit) {
        super(false);
        this.root = root;
        this.pool = null;
        this.direct = root.direct;
        this.buffer = root.buffer.slice();
        this.size = absoluteLimit - absolutePos;
        this.buffer.limit(absoluteLimit);
        this.buffer.position(absolutePos);
    }

    public RefBuffer slice(int absolutePos, int absoluteLimit) {
        RefBuffer r = this.root == null ? this : this.root;
        return new RefBuffer(r, absolutePos, absoluteLimit);
    }

    @Override
    public void retain(int increment) {
        if (root != null) {
            root.retain(increment);
            return;
        }
        if (pool == null && !direct) {
            return;
        }
        super.retain(increment);
    }

    @Override
    public boolean release(int decrement) {
        if (root != null) {
            if (root.release(decrement)) {
                this.buffer = null;
                return true;
            } else {
                return false;
            }
        }
        if (pool == null && !direct) {
            return false;
        }
        return super.release(decrement);
    }

    @Override
    public void retain() {
        if (root != null) {
            root.retain();
        } else {
            super.retain();
        }
    }

    @Override
    public boolean release() {
        if (root != null) {
            return root.release();
        } else {
            return super.release();
        }
    }

    @Override
    protected boolean isReleased() {
        if (root != null) {
            return root.isReleased();
        } else {
            return super.isReleased();
        }
    }

    @Override
    protected void doClean() {
        // not called if this RefBuffer is a sliced buffer
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
        return size;
    }
}
