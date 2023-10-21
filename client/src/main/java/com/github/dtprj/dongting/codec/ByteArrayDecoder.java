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
public class ByteArrayDecoder implements Decoder<byte[]> {
    public static final ByteArrayDecoder INSTANCE = new ByteArrayDecoder();

    private ByteArrayDecoder() {
    }

    @Override
    public byte[] decode(DecodeContext context, ByteBuffer buffer, int bodyLen, int currentPos) {
        return decodeToArray(context, buffer, bodyLen, currentPos);
    }

    public static byte[] decodeToArray(DecodeContext context, ByteBuffer buffer, int bodyLen, int currentPos) {
        boolean start = currentPos == 0;
        int remaining = buffer.remaining();
        boolean end = remaining >= bodyLen - currentPos;
        byte[] data;
        if (start) {
            data = new byte[bodyLen];
            context.setStatus(data);
        } else {
            data = (byte[]) context.getStatus();
        }
        buffer.get(data, currentPos, Math.min(remaining, bodyLen - currentPos));
        return end ? data : null;
    }

    @Override
    public void cancel(DecodeContext context) {
    }
}
