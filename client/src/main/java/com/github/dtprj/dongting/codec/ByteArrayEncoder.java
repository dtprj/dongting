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
public class ByteArrayEncoder implements Encoder<byte[]> {

    public static final ByteArrayEncoder INSTANCE = new ByteArrayEncoder();

    private ByteArrayEncoder() {
    }

    @Override
    public boolean encode(EncodeContext context, ByteBuffer buffer, byte[] data) {
        if (data == null) {
            return true;
        }
        int totalLen = data.length;
        if (totalLen == 0) {
            return true;
        }
        Integer lastPos = (Integer) context.getStatus();
        int pos = lastPos == null ? 0 : lastPos;

        int count = Math.min(buffer.remaining(), totalLen - pos);
        buffer.put(data, pos, count);

        boolean finish = pos + count >= totalLen;
        if (!finish) {
            context.setStatus(pos + count);
        }
        return finish;
    }

    @Override
    public int actualSize(byte[] data) {
        if (data == null) {
            return 0;
        }
        return data.length;
    }
}
