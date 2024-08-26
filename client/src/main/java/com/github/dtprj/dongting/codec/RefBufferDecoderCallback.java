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
package com.github.dtprj.dongting.codec;

import com.github.dtprj.dongting.buf.RefBuffer;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class RefBufferDecoderCallback extends DecoderCallback<RefBuffer> {

    public static final RefBufferDecoderCallback INSTANCE = new RefBufferDecoderCallback(false);
    public static final RefBufferDecoderCallback PLAIN_INSTANCE = new RefBufferDecoderCallback(true);

    private final boolean plain;
    private RefBuffer r;

    private RefBufferDecoderCallback(boolean plain) {
        this.plain = plain;
    }

    @Override
    public boolean doDecode(ByteBuffer buffer, int bodyLen, int currentPos) {
        boolean end = buffer.remaining() >= bodyLen - currentPos;
        if (currentPos == 0) {
            if (plain) {
                r = context.getHeapPool().createPlain(bodyLen);
            } else {
                r = context.getHeapPool().create(bodyLen);
            }
        }
        ByteBuffer bb = r.getBuffer();
        bb.put(buffer);
        if (end) {
            bb.flip();
            // release by user code
        }
        return true;
    }

    @Override
    public boolean end(boolean success) {
        if (!success && r != null) {
            r.release();
        }
        r = null;
        return success;
    }

    @Override
    protected RefBuffer getResult() {
        return r;
    }
}
