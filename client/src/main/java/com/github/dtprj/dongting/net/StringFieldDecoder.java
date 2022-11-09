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

import com.github.dtprj.dongting.buf.ByteBufferPool;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * This class can only be used in io threads.
 *
 * @author huangli
 */
public class StringFieldDecoder {

    private static final int THREAD_LOCAL_BUFFER_SIZE = 32 * 1024;
    private static final ThreadLocal<byte[]> THREAD_LOCAL_BUFFER = ThreadLocal.withInitial(() -> new byte[THREAD_LOCAL_BUFFER_SIZE]);
    private final byte[] threadLocalBuffer;
    private final ByteBufferPool pool;

    private ByteBuffer bufferFromPool;
    private int index;

    StringFieldDecoder(ByteBufferPool pool) {
        this.pool = pool;
        this.threadLocalBuffer = THREAD_LOCAL_BUFFER.get();
    }

    public String decodeUTF8(ByteBuffer buf, int fieldLen, boolean start, boolean end) {
        if (start && end && fieldLen <= THREAD_LOCAL_BUFFER_SIZE) {
            byte[] threadLocalBuffer = this.threadLocalBuffer;
            buf.get(threadLocalBuffer, 0, fieldLen);
            return new String(threadLocalBuffer, 0, fieldLen, StandardCharsets.UTF_8);
        }
        byte[] buffer;
        int index;
        ByteBuffer bufferFromPool = null;
        if (start) {
            index = 0;
            if (fieldLen <= THREAD_LOCAL_BUFFER_SIZE) {
                buffer = this.threadLocalBuffer;
            } else {
                bufferFromPool = pool.borrow(fieldLen);
                buffer = bufferFromPool.array();
                this.bufferFromPool = bufferFromPool;
            }
        } else {
            index = this.index;
            if (fieldLen <= THREAD_LOCAL_BUFFER_SIZE) {
                buffer = this.threadLocalBuffer;
            } else {
                bufferFromPool = this.bufferFromPool;
                buffer = bufferFromPool.array();
            }
        }
        int remain = buf.remaining();
        buf.get(buffer, index, remain);
        index += remain;

        if (end) {
            String s = new String(buffer, 0, index, StandardCharsets.UTF_8);
            if (bufferFromPool != null) {
                pool.release(bufferFromPool);
                this.bufferFromPool = null;
            }
            this.index = 0;
            return s;
        } else {
            this.index = index;
            return null;
        }
    }
}
