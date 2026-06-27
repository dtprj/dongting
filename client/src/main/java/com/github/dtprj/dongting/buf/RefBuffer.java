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
import com.github.dtprj.dongting.net.ByteBufferWritePacket;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

/**
 * @author huangli
 */
public final class RefBuffer extends RefCount implements Encodable {

    public static final RefBuffer EMPTY = new RefBuffer(ByteBuffer.allocate(0));

    ByteBuffer buffer;
    private final Consumer<RefBuffer> releasor;
    private final RefBuffer root;

    private int encodeSize;

    RefBuffer(boolean plain, ByteBuffer buffer, Consumer<RefBuffer> releasor, boolean dummy) {
        super(plain, dummy);
        this.buffer = buffer;
        this.releasor = releasor;
        this.encodeSize = -1;
        this.root = null;
    }

    private RefBuffer(RefBuffer root, int absolutePos, int absoluteLimit) {
        super(false, root.updater == null);
        this.root = root;
        this.releasor = null;
        this.encodeSize = absoluteLimit - absolutePos;

        ByteBuffer rootBuffer = root.buffer;
        int oldLimit = rootBuffer.limit();
        int oldPos = rootBuffer.position();
        if (oldLimit != absoluteLimit) {
            rootBuffer.limit(absoluteLimit);
        }
        if (oldPos != absolutePos) {
            rootBuffer.position(absolutePos);
        }
        this.buffer = rootBuffer.slice();
        if (oldLimit != absoluteLimit) {
            rootBuffer.limit(oldLimit);
        }
        if (oldPos != absolutePos) {
            rootBuffer.position(oldPos);
        }
    }

    private RefBuffer(ByteBuffer buf) {
        super(true, true);
        if (buf.isDirect()) {
            throw new IllegalArgumentException();
        }
        this.buffer = buf;
        this.releasor = null;
        this.encodeSize = buf.remaining();
        this.root = null;
    }

    public static RefBuffer wrap(ByteBuffer buf) {
        return new RefBuffer(buf);
    }

    /**
     * this method is preserved for biz code usage.
     */
    @SuppressWarnings("unused")
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
        super.retain(increment);
    }

    @Override
    public void release(int decrement) {
        if (root != null) {
            if (root.release0(decrement)) {
                this.buffer = null;
            }
        } else {
            super.release(decrement);
        }
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
    public void release() {
        release(1);
    }

    @Override
    public boolean isReleased() {
        if (root != null) {
            return root.isReleased();
        } else {
            return super.isReleased();
        }
    }

    /**
     * This method is not called if this RefBuffer is dummy or sliced.
     */
    @Override
    protected void doClean() {
        Consumer<RefBuffer> r = this.releasor;
        if (r != null) {
            r.accept(this);
        } else {
            // should be direct, SimpleByteBufferPool.newUnpooledRefBuffer(),
            // the heap buffer less than threshold is dummy and not call doClean
            SimpleByteBufferPool.VF.releaseDirectBuffer(buffer);
            this.buffer = null;
        }
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    @Override
    public boolean encode(EncodeContext context, ByteBuffer destBuffer) {
        if (encodeSize < 0) {
            throw new IllegalStateException("prepareForEncode() not called");
        }
        ByteBuffer src = this.buffer;
        if (src == null || src.remaining() == 0) {
            return true;
        }
        if (src.isDirect()) {
            ByteBuffer srcCopy = (ByteBuffer) context.status;
            srcCopy = ByteBufferWritePacket.copyFromDirectBuffer(src, destBuffer, srcCopy);
            if (srcCopy.remaining() == 0) {
                return true;
            } else {
                context.status = srcCopy;
                return false;
            }
        } else {
            Integer s = (Integer) context.status;
            int readBytes = 0;
            if (s != null) {
                readBytes = s;
            }
            readBytes = ByteBufferWritePacket.copyFromHeapBuffer(src, destBuffer, readBytes);
            if (readBytes >= src.remaining()) {
                return true;
            } else {
                context.status = readBytes;
                return false;
            }
        }
    }

    @Override
    public int actualSize() {
        if (encodeSize < 0) {
            throw new IllegalStateException("prepareForEncode() not called");
        }
        return encodeSize;
    }

    public void prepareForEncode() {
        this.encodeSize = this.buffer.remaining();
    }
}
