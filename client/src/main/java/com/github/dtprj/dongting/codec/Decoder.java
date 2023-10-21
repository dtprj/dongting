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
public interface Decoder<T> {

    T decode(DecodeContext context, ByteBuffer buffer, int bodyLen, int currentPos);

    void cancel(DecodeContext context);

    static ByteBuffer decodeToByteBuffer(ByteBuffer buffer, int bodyLen, int currentPos, ByteBuffer result) {
        if (currentPos == 0) {
            result = ByteBuffer.allocate(bodyLen);
        }
        boolean end = buffer.remaining() >= bodyLen - currentPos;
        result.put(buffer);

        if (end) {
            result.flip();
        }
        return result;
    }

    Decoder<Void> VOID_DECODER = new Decoder<Void>() {
        @Override
        public Void decode(DecodeContext context, ByteBuffer buffer, int bodyLen, int currentPos) {
            return null;
        }

        @Override
        public void cancel(DecodeContext context) {
        }
    };

}
