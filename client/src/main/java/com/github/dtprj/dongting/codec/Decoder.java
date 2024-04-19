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
public abstract class Decoder<T> {

    public final T decode(DecodeContext context, ByteBuffer buffer, int bodyLen, int currentPos) {
        int oldPos = buffer.position();
        int oldLimit = buffer.limit();
        int end = Math.min(oldLimit, oldPos - currentPos + bodyLen);
        try {
            if (oldLimit - oldPos > bodyLen - currentPos) {
                buffer.limit(end);
            }
            return doDecode(context, buffer, bodyLen, currentPos);
        } finally {
            buffer.limit(oldLimit);
            buffer.position(end);
        }
    }

    protected abstract T doDecode(DecodeContext context, ByteBuffer buffer, int bodyLen, int currentPos);

    public void finish(DecodeContext context) {
    }

    public static final Decoder<Void> VOID_DECODER = new Decoder<Void>() {
        @Override
        public Void doDecode(DecodeContext context, ByteBuffer buffer, int bodyLen, int currentPos) {
            return null;
        }

    };

}
