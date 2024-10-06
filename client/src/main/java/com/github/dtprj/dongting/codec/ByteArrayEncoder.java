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
public class ByteArrayEncoder implements Encodable {

    private final byte[] data;
    private final int size;

    public ByteArrayEncoder(byte[] data) {
        this.data = data;
        this.size = EncodeUtil.actualSize(data);
    }

    @Override
    public boolean encode(EncodeContext context, ByteBuffer buffer) {
        return EncodeUtil.encode(context, buffer, data);
    }

    public int actualSize() {
        return size;
    }

    public byte[] getData() {
        return data;
    }

    public static final class Callback extends DecoderCallback<ByteArrayEncoder> {

        private byte[] r;

        @Override
        public boolean doDecode(ByteBuffer buffer, int bodyLen, int currentPos) {
            r = parseBytes(buffer, bodyLen, currentPos);
            return true;
        }

        @Override
        protected ByteArrayEncoder getResult() {
            if (r == null) {
                return null;
            } else {
                return new ByteArrayEncoder(r);
            }
        }

        @Override
        protected boolean end(boolean success) {
            this.r = null;
            return success;
        }
    }
}
