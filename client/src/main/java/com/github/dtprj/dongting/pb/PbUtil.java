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
import java.nio.charset.StandardCharsets;

/**
 * encode/decode util for proto buffer 3.
 * <p>
 * see: https://developers.google.com/protocol-buffers/docs/encoding
 *
 * @author huangli
 */
public class PbUtil {

    private static final int MAX_SUPPORT_FIELD_INDEX = 536870911; // 29 bits
    private static final int MAX_TAG_LENGTH = 4;
    private static final int MAX_VAR_INT_LENGTH = 5;
    private static final int MAX_VAR_LONG_LENGTH = 10;

    public static final int TYPE_VAR_INT = 0;
    public static final int TYPE_FIX64 = 1;
    public static final int TYPE_LENGTH_DELIMITED = 2;
    public static final int TYPE_START_GROUP = 3;
    public static final int TYPE_END_GROUP = 4;
    public static final int TYPE_FIX32 = 5;

    static void writeTag(ByteBuffer buf, int type, int index) {
        if (index > MAX_SUPPORT_FIELD_INDEX || index <= 0) {
            throw new IllegalArgumentException(String.valueOf(index));
        }
        int value = (index << 3) | type;

        writeUnsignedInt32ValueOnly(buf, value);
    }

    public static void writeUnsignedInt32(ByteBuffer buf, int index, int value) {
        if (value == 0) {
            return;
        }
        writeTag(buf, TYPE_VAR_INT, index);
        writeUnsignedInt32ValueOnly(buf, value);
    }

    private static void writeUnsignedInt32ValueOnly(ByteBuffer buf, int value) {
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

    public static void writeFix32(ByteBuffer buf, int index, int value) {
        if (value == 0) {
            return;
        }
        writeTag(buf, TYPE_FIX32, index);
        buf.putInt(Integer.reverseBytes(value));
    }

    public static void writeUnsignedInt64(ByteBuffer buf, int index, long value) {
        if (value == 0) {
            return;
        }
        writeTag(buf, TYPE_VAR_INT, index);
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

    public static void writeUTF8(ByteBuffer buf, int index, String value) {
        if (value == null || value.length() == 0) {
            return;
        }
        writeTag(buf, TYPE_LENGTH_DELIMITED, index);
        byte[] bs = value.getBytes(StandardCharsets.UTF_8);
        writeUnsignedInt32ValueOnly(buf, bs.length);
        buf.put(bs);
    }

    public static void writeAscii(ByteBuffer buf, int index, String value) {
        if (value == null || value.length() == 0) {
            return;
        }
        writeTag(buf, TYPE_LENGTH_DELIMITED, index);
        byte[] bs = value.getBytes(StandardCharsets.ISO_8859_1);
        writeUnsignedInt32ValueOnly(buf, bs.length);
        buf.put(bs);
    }

    public static void writeLengthDelimitedPrefix(ByteBuffer buf, int index, int len) {
        if (len == 0) {
            throw new IllegalArgumentException(String.valueOf(len));
        }
        writeTag(buf, TYPE_LENGTH_DELIMITED, index);
        writeUnsignedInt32ValueOnly(buf, len);
    }

    public static int readUnsignedInt32(ByteBuffer buf) {
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

    public static long readUnsignedInt64(ByteBuffer buf) {
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

    public static int maxStrSizeUTF8(String str) {
        if (str == null) {
            return 0;
        }
        int len = str.length();
        if (len == 0) {
            return 0;
        }
        // tag, max 4 bytes
        // length, var int32, max 5 bytes
        return MAX_TAG_LENGTH + MAX_VAR_INT_LENGTH + len * 4;
    }

    public static int maxStrSizeAscii(String str) {
        if (str == null) {
            return 0;
        }
        int len = str.length();
        if (len == 0) {
            return 0;
        }
        // tag, max 4 bytes
        // length, var int32, max 5 bytes
        return MAX_TAG_LENGTH + MAX_VAR_INT_LENGTH + len;
    }

    public static int maxVarIntSize() {
        return MAX_TAG_LENGTH + MAX_VAR_INT_LENGTH;
    }

    public static int maxVarLongSize() {
        return MAX_TAG_LENGTH + MAX_VAR_LONG_LENGTH;
    }

    public static int maxFix32Size() {
        return MAX_TAG_LENGTH + 4;
    }

    public static int maxFix64Size() {
        return MAX_TAG_LENGTH + 8;
    }

    public static int maxLengthDelimitedSize(int bodyLen) {
        return MAX_TAG_LENGTH + MAX_VAR_INT_LENGTH + bodyLen;
    }
}
