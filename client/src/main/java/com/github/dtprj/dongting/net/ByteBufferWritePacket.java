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

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class ByteBufferWritePacket extends RetryableWritePacket {
    private final ByteBuffer data;
    private final int size;

    public ByteBufferWritePacket(ByteBuffer data) {
        this.size = data == null ? 0 : data.remaining();
        this.data = data;
    }

    @Override
    protected int calcActualBodySize() {
        return size;
    }

    public static int copyFromHeapBuffer(ByteBuffer src, ByteBuffer dest, int readBytes) {
        int len = Math.min(src.remaining() - readBytes, dest.remaining());
        dest.put(src.array(), src.position() + readBytes, len);
        return readBytes + len;
    }

    public static ByteBuffer copyFromDirectBuffer(ByteBuffer src, ByteBuffer dest, ByteBuffer srcCopy) {
        if (srcCopy == null) {
            srcCopy = src.slice();
        }
        if (srcCopy.remaining() > dest.remaining()) {
            int limit = srcCopy.limit();
            srcCopy.limit(srcCopy.position() + dest.remaining());
            dest.put(srcCopy);
            srcCopy.limit(limit);
        } else {
            dest.put(srcCopy);
        }
        return srcCopy;
    }

    static boolean encodeBody(RpcEncodeContext context,ByteBuffer src, ByteBuffer dest) {
        if (src == null) {
            return true;
        }
        if (src.isDirect()) {
            ByteBuffer srcCopy = (ByteBuffer) context.getStatus();
            srcCopy = copyFromDirectBuffer(src, dest, srcCopy);
            if (srcCopy.remaining() == 0) {
                return true;
            } else {
                context.setStatus(srcCopy);
                return false;
            }
        } else {
            Integer readBytes = (Integer) context.getStatus();
            readBytes = copyFromHeapBuffer(src, dest, readBytes == null ? 0 : readBytes);
            if(readBytes >= src.remaining()) {
                return true;
            } else {
                context.setStatus(readBytes);
                return false;
            }
        }
    }

    @Override
    protected boolean encodeBody(RpcEncodeContext context, ByteBuffer dest) {
        return encodeBody(context, data, dest);
    }
}
