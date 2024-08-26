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

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public abstract class CopyDecoderCallback<T> extends DecoderCallback<T> {

    private ByteBuffer temp;

    @Override
    public final boolean doDecode(ByteBuffer buffer, int bodyLen, int currentPos) {
        boolean start = currentPos == 0;
        boolean end = buffer.remaining() >= bodyLen - currentPos;
        if (start && end) {
            return decode(buffer);
        }
        if (start) {
            temp = context.getHeapPool().getPool().borrow(bodyLen);
        }
        temp.put(buffer);
        if (end) {
            temp.flip();
            return decode(temp);
        }
        return true;
    }

    @Override
    public boolean end(boolean success) {
        if (temp != null) {
            context.getHeapPool().getPool().release(temp);
            temp = null;
        }
        return success;
    }

    protected abstract boolean decode(ByteBuffer buffer);
}
