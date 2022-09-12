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
 * encode/decode util for proto buffer 3.
 * <p>
 * see: https://developers.google.com/protocol-buffers/docs/encoding
 *
 * @author huangli
 */
public class PbUtil {

    public static final int MAX_SUPPORT_FIELD_INDEX = 536870911; // 29 bits

    public static final int TYPE_VAR_INT = 0;
    public static final int TYPE_FIX64 = 1;
    public static final int TYPE_LENGTH_DELIMITED = 2;
    public static final int TYPE_START_GROUP = 3;
    public static final int TYPE_END_GROUP = 4;
    public static final int TYPE_FIX32 = 5;

    public static void writeTag(ByteBuffer buf, int type, int index) {
        if (index > MAX_SUPPORT_FIELD_INDEX || index <= 0) {
            throw new IllegalArgumentException(String.valueOf(index));
        }
        int value = (index << 3) | type;

        writeVarUnsignedInt32(buf, value);
    }

    public static void writeVarUnsignedInt32(ByteBuffer buf, int value) {
        if (value == 0) {
            throw new IllegalArgumentException();
        }
        for (int i = 0; i < 5; i++) {
            int x = value & 0x7F;
            value >>>= 7;
            if (value != 0) {
                x |= 0x80;
                buf.put((byte) x);
            } else {
                buf.put((byte) x);
                return;
            }
        }
    }

    public static void writeVarUnsignedInt64(ByteBuffer buf, long value) {
        if (value == 0) {
            throw new IllegalArgumentException();
        }
        for (int i = 0; i < 10; i++) {
            long x = value & 0x7FL;
            value >>>= 7;
            if (value != 0) {
                x |= 0x80L;
                buf.put((byte) x);
            } else {
                buf.put((byte) x);
                return;
            }
        }
    }

    public static void parse(ByteBuffer buf, PbCallback callback) {
        ByteOrder old = buf.order();
        buf.order(ByteOrder.LITTLE_ENDIAN);
        int limit = buf.limit();
        while (buf.hasRemaining()) {
            int index = readVarUnsignedInt32(buf);
            int type = index & 0x07; // 0000 0111
            index >>>= 3;
            switch (type) {
                case TYPE_VAR_INT:
                    // TODO only support uint32 now
                    callback.readInt(index, readVarUnsignedInt32(buf));
                    break;
                case TYPE_FIX32:
                    callback.readInt(index, buf.getInt());
                    break;
                case TYPE_FIX64:
                    callback.readLong(index, buf.getLong());
                    break;
                case TYPE_LENGTH_DELIMITED:
                    int length = readVarUnsignedInt32(buf);
                    if (length > buf.remaining() || length < 0) {
                        throw new PbException("bad protobuf length: " + length);
                    }
                    int newLimit = buf.position() + length;
                    buf.limit(newLimit);
                    callback.readBytes(index, buf);
                    buf.limit(limit);
                    buf.position(newLimit);
                    break;
                case TYPE_START_GROUP:
                case TYPE_END_GROUP:
                default:
                    throw new PbException("protobuf type not support: " + type);
            }
        }
        buf.order(old);
    }

    public static int readVarUnsignedInt32(ByteBuffer buf) {
        int bitIndex = 0;
        int value = 0;
        for (int i = 0; i < 5; i++) {
            int x = buf.get();
            value |= (x & 0x7F) << bitIndex;
            if (x >= 0) {
                return value;
            }
            bitIndex += 7;
        }
        throw new PbException("bad protobuf var int input");
    }

    public static long readVarUnsignedInt64(ByteBuffer buf) {
        int bitIndex = 0;
        long value = 0;
        for (int i = 0; i < 10; i++) {
            long x = buf.get();
            value |= (x & 0x7F) << bitIndex;
            if (x >= 0) {
                return value;
            }
            bitIndex += 7;
        }
        throw new PbException("bad protobuf var int input");
    }

}
