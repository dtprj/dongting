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
package com.github.dtprj.dongting.pb;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * @author huangli
 */
public class PbParser {

    public void parse(ByteBuffer buf, PbCallback callback) {
        ByteOrder old = buf.order();
        buf.order(ByteOrder.LITTLE_ENDIAN);
        int limit = buf.limit();
        while (buf.hasRemaining()) {
            int index = PbUtil.readVarUnsignedInt32(buf);
            int type = index & 0x07; // 0000 0111
            index >>>= 3;
            switch (type) {
                case PbUtil.TYPE_VAR_INT:
                    // TODO only support uint32 now
                    callback.readInt(index, PbUtil.readVarUnsignedInt32(buf));
                    break;
                case PbUtil.TYPE_FIX32:
                    callback.readInt(index, buf.getInt());
                    break;
                case PbUtil.TYPE_FIX64:
                    callback.readLong(index, buf.getLong());
                    break;
                case PbUtil.TYPE_LENGTH_DELIMITED:
                    int length = PbUtil.readVarUnsignedInt32(buf);
                    if (length > buf.remaining() || length < 0) {
                        throw new PbException("bad protobuf length: " + length);
                    }
                    int newLimit = buf.position() + length;
                    buf.limit(newLimit);
                    callback.readBytes(index, buf);
                    buf.limit(limit);
                    buf.position(newLimit);
                    break;
                case PbUtil.TYPE_START_GROUP:
                case PbUtil.TYPE_END_GROUP:
                default:
                    throw new PbException("protobuf type not support: " + type);
            }
        }
        buf.order(old);
    }
}
