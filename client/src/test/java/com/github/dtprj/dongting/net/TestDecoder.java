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
class BizByteBufferDecoder extends Decoder {
    @Override
    public boolean decodeInIoThread() {
        return false;
    }

    @Override
    public Object decode(ProcessContext context, ByteBuffer buffer, int bodyLen, boolean start, boolean end) {
        ByteBuffer buf = ByteBuffer.allocate(bodyLen);
        buf.put(buffer);
        buf.flip();
        return buf;
    }
}

class IoFullPackByteBufferDecoder extends Decoder {
    @Override
    public Object decode(ProcessContext context, ByteBuffer buffer, int bodyLen, boolean start, boolean end) {
        ByteBuffer buf = ByteBuffer.allocate(bodyLen);
        buf.put(buffer);
        buf.flip();
        return buf;
    }
}
