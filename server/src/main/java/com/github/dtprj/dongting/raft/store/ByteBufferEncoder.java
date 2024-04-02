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
package com.github.dtprj.dongting.raft.store;

import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.Encoder;
import com.github.dtprj.dongting.net.ByteBufferWriteFrame;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class ByteBufferEncoder implements Encoder<ByteBuffer> {

    public static final ByteBufferEncoder INSTANCE = new ByteBufferEncoder();

    private ByteBufferEncoder() {
    }

    @Override
    public boolean encode(EncodeContext context, ByteBuffer dest, ByteBuffer src) {
        if (src.isDirect()) {
            ByteBuffer srcCopy = (ByteBuffer) context.getStatus();
            srcCopy = ByteBufferWriteFrame.copyFromDirectBuffer(src, dest, srcCopy);
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
            readBytes = ByteBufferWriteFrame.copyFromHeapBuffer(src, dest, readBytes);
            if (readBytes >= src.remaining()) {
                return true;
            } else {
                context.setStatus(readBytes);
                return false;
            }
        }
    }

    @Override
    public int actualSize(ByteBuffer data) {
        return data.remaining();
    }

}
