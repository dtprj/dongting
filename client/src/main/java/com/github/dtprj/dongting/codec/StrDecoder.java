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
import java.nio.charset.StandardCharsets;

/**
 * This class can only be used in io threads.
 *
 * @author huangli
 */
public class StrDecoder implements Decoder<String> {

    public static final StrDecoder INSTANCE = new StrDecoder();

    private StrDecoder() {
    }

    @Override
    public String decode(DecodeContext decodeContext, ByteBuffer buf, int fieldLen, int currentPos) {
        boolean start = currentPos == 0;
        boolean end = buf.remaining() >= fieldLen - currentPos;
        if (start && end) {
            byte[] threadLocalBuffer = decodeContext.getThreadLocalBuffer();
            if (fieldLen <= threadLocalBuffer.length) {
                buf.get(threadLocalBuffer, 0, fieldLen);
                return new String(threadLocalBuffer, 0, fieldLen, StandardCharsets.UTF_8);
            }
        }
        ByteBuffer bufferFromPool;

        if (start) {
            bufferFromPool = decodeContext.getHeapPool().getPool().borrow(fieldLen);
        } else {
            bufferFromPool = (ByteBuffer) decodeContext.getStatus();
        }
        bufferFromPool.put(buf);

        if (end) {
            String s = new String(bufferFromPool.array(), 0, bufferFromPool.position(), StandardCharsets.UTF_8);
            decodeContext.getHeapPool().getPool().release(bufferFromPool);
            return s;
        } else {
            decodeContext.setStatus(bufferFromPool);
            return null;
        }
    }
}
