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
public abstract class CopyDecoder<T> implements Decoder<T> {
    @Override
    public T decode(DecodeContext context, ByteBuffer buffer, int bodyLen, int currentPos) {
        boolean start = currentPos == 0;
        boolean end = buffer.remaining() >= bodyLen - currentPos;
        if (start && end) {
            return decode(buffer);
        }
        RefBuffer temp;
        if (start) {
            temp = context.getHeapPool().create(bodyLen);
            context.setStatus(temp);
        } else {
            temp = (RefBuffer) context.getStatus();
        }
        ByteBuffer bb = temp.getBuffer();
        bb.put(buffer);
        if (end) {
            bb.flip();
            try {
                return decode(bb);
            } finally {
                temp.release();
            }
        }
        return null;
    }

    protected abstract T decode(ByteBuffer buffer);
}
