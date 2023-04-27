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

import com.github.dtprj.dongting.buf.ByteBufferPool;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * This class can only be used in io threads.
 *
 * @author huangli
 */
public class StrDecoder implements Decoder {

    private static final int THREAD_LOCAL_BUFFER_SIZE = 32 * 1024;
    private static final ThreadLocal<byte[]> THREAD_LOCAL_BUFFER = ThreadLocal.withInitial(() -> new byte[THREAD_LOCAL_BUFFER_SIZE]);
    private final byte[] threadLocalBuffer;
    private final ByteBufferPool pool;

    private ByteBuffer bufferFromPool;

    public StrDecoder(ByteBufferPool pool) {
        this.pool = pool;
        this.threadLocalBuffer = THREAD_LOCAL_BUFFER.get();
    }

    @Override
    public String decode(DecodeContext decodeContext, ByteBuffer buf, int fieldLen, boolean start, boolean end) {
        if (start && end && fieldLen <= THREAD_LOCAL_BUFFER_SIZE) {
            byte[] threadLocalBuffer = this.threadLocalBuffer;
            buf.get(threadLocalBuffer, 0, fieldLen);
            return new String(threadLocalBuffer, 0, fieldLen, StandardCharsets.UTF_8);
        }
        ByteBuffer bufferFromPool;
        if (start) {
            bufferFromPool = pool.borrow(fieldLen);
        } else {
            bufferFromPool = this.bufferFromPool;
        }
        bufferFromPool.put(buf);

        if (end) {
            String s = new String(bufferFromPool.array(), 0, bufferFromPool.position(), StandardCharsets.UTF_8);
            pool.release(bufferFromPool);
            this.bufferFromPool = null;
            return s;
        } else {
            this.bufferFromPool = bufferFromPool;
            return null;
        }
    }
}
