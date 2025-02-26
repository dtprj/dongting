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
import java.util.Set;

/**
 * encode/decode util for proto buffer 3.
 * <p>
 * see: <a href="https://developers.google.com/protocol-buffers/docs/encoding">protobuf encoding docs</a>
 *
 * @author huangli
 */
public final class PbUtil {

    private static final int MAX_SUPPORT_FIELD_INDEX = 536870911; // 29 bits
    private static final int MAX_TAG_LENGTH = 5;
    private static final int MAX_INT32_LENGTH = 10;
    private static final int MAX_INT64_LENGTH = 10;

    static final int MAX_TAG_INT32_LEN = MAX_TAG_LENGTH + MAX_INT32_LENGTH;
    static final int MAX_TAG_INT64_LEN = MAX_TAG_LENGTH + MAX_INT64_LENGTH;
    static final int MAX_TAG_FIX32_LEN = MAX_TAG_LENGTH + 4;
    static final int MAX_TAG_FIX64_LEN = MAX_TAG_LENGTH + 8;

    private static final int MAX_1_BYTE_INT_VALUE = 0x0000007F;//7 bits 1
    private static final int MAX_2_BYTE_INT_VALUE = 0x00003FFF;//14 bits 1
    private static final int MAX_3_BYTE_INT_VALUE = 0x001FFFFF;//21 bits 1
    private static final int MAX_4_BYTE_INT_VALUE = 0x0FFFFFFF;//28 bits 1

    private static final long MAX_1_BYTE_LONG_VALUE = 0x00000000_0000007FL;//7 bits 1
    private static final long MAX_2_BYTE_LONG_VALUE = 0x00000000_00003FFFL;//14 bits 1
    private static final long MAX_3_BYTE_LONG_VALUE = 0x00000000_001FFFFFL;//21 bits 1
    private static final long MAX_4_BYTE_LONG_VALUE = 0x00000000_0FFFFFFFL;//28 bits 1
    private static final long MAX_5_BYTE_LONG_VALUE = 0x00000007_FFFFFFFFL;//35 bits 1
    private static final long MAX_6_BYTE_LONG_VALUE = 0x000003FF_FFFFFFFFL;//42 bits 1
    private static final long MAX_7_BYTE_LONG_VALUE = 0x0001FFFF_FFFFFFFFL;//49 bits 1
    private static final long MAX_8_BYTE_LONG_VALUE = 0x00FFFFFF_FFFFFFFFL;//56 bits 1
    // private static final long MAX_9_BYTE_LONG_VALUE = 0x7FFFFFFF_FFFFFFFFL;//63 bits 1

    public static final int TYPE_VAR_INT = 0;
    public static final int TYPE_FIX64 = 1;
    public static final int TYPE_LENGTH_DELIMITED = 2;
    // public static final int TYPE_START_GROUP = 3;
    // public static final int TYPE_END_GROUP = 4;
    public static final int TYPE_FIX32 = 5;

    static void writeTag(ByteBuffer buf, int type, int index) {
        if (index > MAX_SUPPORT_FIELD_INDEX || index <= 0) {
            throw new IllegalArgumentException(String.valueOf(index));
        }
        int value = (index << 3) | type;

        writeUnsignedInt32(buf, value);
    }

    public static void writeInt32Field(ByteBuffer buf, int index, int value) {
        if (value == 0) {
            return;
        }
        writeTag(buf, TYPE_VAR_INT, index);
        if (value < 0) {
            // negative number use 10 bytes
            writeUnsignedInt64(buf, value);
        } else {
            writeUnsignedInt32(buf, value);
        }
    }

    static void writeUnsignedInt32(ByteBuffer buf, int value) {
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

    static void writeInt32(ByteBuffer buf, int value) {
        if (value > 0) {
            writeUnsignedInt32(buf, value);
        } else {
            // negative number use 10 bytes
            writeUnsignedInt64(buf, value);
        }
    }

    public static void writeFix32Field(ByteBuffer buf, int index, int value) {
        if (value == 0) {
            return;
        }
        writeTag(buf, TYPE_FIX32, index);
        buf.putInt(Integer.reverseBytes(value));
    }

    public static void writeFix32Field(ByteBuffer buf, int index, Set<Integer> s) {
        if (s == null || s.isEmpty()) {
            return;
        }
        for (int id : s) {
            writeTag(buf, TYPE_FIX32, index);
            buf.putInt(Integer.reverseBytes(id));
        }
    }

    public static void writeFix64Field(ByteBuffer buf, int index, long value) {
        if (value == 0) {
            return;
        }
        writeTag(buf, TYPE_FIX64, index);
        buf.putLong(Long.reverseBytes(value));
    }

    public static void writeInt64Field(ByteBuffer buf, int index, long value) {
        if (value == 0) {
            return;
        }
        writeTag(buf, TYPE_VAR_INT, index);
        writeUnsignedInt64(buf, value);
    }

    static void writeUnsignedInt64(ByteBuffer buf, long value) {
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

    public static void writeAsciiField(ByteBuffer buf, int index, String value) {
        if (value == null) {
            return;
        }
        int len = value.length();
        if (len == 0) {
            return;
        }
        writeTag(buf, TYPE_LENGTH_DELIMITED, index);
        writeUnsignedInt32(buf, len);
        for (int i = 0; i < len; i++) {
            buf.put((byte) value.charAt(i));
        }
    }

    public static void writeLenFieldPrefix(ByteBuffer buf, int index, int len) {
        writeTag(buf, TYPE_LENGTH_DELIMITED, index);
        writeUnsignedInt32(buf, len);
    }

    public static void writeBytesField(ByteBuffer buf, int index, byte[] data) {
        if (data == null) {
            return;
        }
        if (data.length == 0) {
            return;
        }
        writeTag(buf, TYPE_LENGTH_DELIMITED, index);
        writeUnsignedInt32(buf, data.length);
        buf.put(data);
    }

    static int readUnsignedInt32(ByteBuffer buf) {
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

    static long readUnsignedInt64(ByteBuffer buf) {
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

    public static int sizeOfAscii(int index, String str) {
        if (str == null) {
            return 0;
        }
        int len = str.length();
        if (len == 0) {
            return 0;
        }
        return sizeOfTag(index) + sizeOfUnsignedInt32(len) + len;
    }

    public static int sizeOfTag(int index) {
        if (index > MAX_SUPPORT_FIELD_INDEX || index <= 0) {
            throw new IllegalArgumentException(String.valueOf(index));
        }
        return sizeOfUnsignedInt32(index << 3);
    }

    public static int sizeOfInt32Field(int index, int value) {
        if (value == 0) {
            return 0;
        }
        return sizeOfTag(index) + sizeOfInt32(value);
    }

    static int sizeOfInt32(int value) {
        if (value > 0) {
            return sizeOfUnsignedInt32(value);
        } else {
            // negative number use 10 bytes
            return sizeOfUnsignedInt64(value);
        }
    }

    static int sizeOfUnsignedInt32(int value) {
        if (value < 0) {
            return 5;
        } else if (value <= MAX_1_BYTE_INT_VALUE) {
            return 1;
        } else if (value <= MAX_2_BYTE_INT_VALUE) {
            return 2;
        } else if (value <= MAX_3_BYTE_INT_VALUE) {
            return 3;
        } else if (value <= MAX_4_BYTE_INT_VALUE) {
            return 4;
        } else {
            return 5;
        }
    }

    public static int sizeOfInt32Field(int index, int[] values) {
        if (values == null || values.length == 0) {
            return 0;
        }
        int size = 0;
        for (int value : values) {
            size += sizeOfTag(index);
            if (value > 0) {
                size += sizeOfUnsignedInt32(value);
            } else {
                // negative number use 10 bytes
                size += sizeOfUnsignedInt64(value);
            }
        }
        return size;
    }

    public static int sizeOfInt64Field(int index, long value) {
        if (value == 0L) {
            return 0;
        }
        return sizeOfTag(index) + sizeOfUnsignedInt64(value);
    }

    static int sizeOfUnsignedInt64(long value) {
        if (value < 0L) {
            return 10;
        } else if (value <= MAX_1_BYTE_LONG_VALUE) {
            return 1;
        } else if (value <= MAX_2_BYTE_LONG_VALUE) {
            return 2;
        } else if (value <= MAX_3_BYTE_LONG_VALUE) {
            return 3;
        } else if (value <= MAX_4_BYTE_LONG_VALUE) {
            return 4;
        } else if (value <= MAX_5_BYTE_LONG_VALUE) {
            return 5;
        } else if (value <= MAX_6_BYTE_LONG_VALUE) {
            return 6;
        } else if (value <= MAX_7_BYTE_LONG_VALUE) {
            return 7;
        } else if (value <= MAX_8_BYTE_LONG_VALUE) {
            return 8;
        } else {
            // MAX_9_BYTE_LONG_VALUE==Long.MAX_VALUE, so ...
            return 9;
        }
    }

    public static int sizeOfFix32Field(int index, int value) {
        if (value == 0) {
            return 0;
        }
        return sizeOfTag(index) + 4;
    }

    public static int sizeOfFix32Field(int index, Set<Integer> s) {
        if (s == null || s.isEmpty()) {
            return 0;
        }
        return s.size() * (sizeOfTag(index) + 4);
    }

    public static int sizeOfFix32Field(int index, int[] v) {
        if (v == null || v.length == 0) {
            return 0;
        }
        return v.length * (sizeOfTag(index) + 4);
    }

    public static int sizeOfFix64Field(int index, long value) {
        if (value == 0L) {
            return 0;
        }
        return sizeOfTag(index) + 8;
    }

    public static int sizeOfBytesField(int index, byte[] bs) {
        if (bs == null || bs.length == 0) {
            return 0;
        }
        return sizeOfTag(index) + sizeOfUnsignedInt32(bs.length) + bs.length;
    }

    public static int sizeOfLenFieldPrefix(int index, int bodyLen) {
        return sizeOfTag(index) + sizeOfUnsignedInt32(bodyLen);
    }
}
