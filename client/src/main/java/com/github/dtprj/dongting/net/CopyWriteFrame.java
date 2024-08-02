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
package com.github.dtprj.dongting.net;

import com.github.dtprj.dongting.buf.RefBuffer;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public abstract class CopyWriteFrame extends WriteFrame {

    private RefBuffer tempRefBuffer;

    public CopyWriteFrame() {
    }

    @Override
    protected final boolean encodeBody(RpcEncodeContext context, ByteBuffer dest) {
        if (tempRefBuffer == null) {
            int bodySize = actualBodySize();
            if (dest.remaining() >= bodySize) {
                encodeBody(dest);
                return true;
            } else {
                tempRefBuffer = context.getHeapPool().create(bodySize);
                ByteBuffer tempBuf = tempRefBuffer.getBuffer();
                encodeBody(tempBuf);
                tempBuf.flip();

                dest.put(tempBuf);
                return tempBuf.hasRemaining();
            }
        } else {
            ByteBuffer tempBuf = tempRefBuffer.getBuffer();
            dest.put(tempBuf);
            return tempBuf.hasRemaining();
        }
    }

    protected abstract void encodeBody(ByteBuffer buf);

    @Override
    protected void doClean() {
        if (tempRefBuffer != null) {
            tempRefBuffer.release();
            tempRefBuffer = null;
        }
    }
}
