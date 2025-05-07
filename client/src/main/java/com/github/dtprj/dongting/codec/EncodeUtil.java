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

import com.github.dtprj.dongting.common.ByteArray;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author huangli
 */
public class EncodeUtil {

    /**
     * not encode null and empty object
     */
    private static final int MODE_CUT_0BYTE = 1;

    /**
     * not encode null object, but encode empty object
     */
    private static final int MODE_ENCODE_EMPTY_NOT_ENCODE_NULL = 2;

    /**
     * encode null and empty object
     */
    private static final int MODE_ENCODE_ALL = 3;

    // ----------------------------------------------------------------------------

    public static int sizeOf(int pbIndex, SimpleEncodable o) {
        if (o == null) {
            return 0;
        }
        int s = o.actualSize();
        return PbUtil.sizeOfLenFieldPrefix(pbIndex, s) + s;
    }

    public static void encode(ByteBuffer destBuffer, int pbIndex, SimpleEncodable o) {
        if (o == null) {
            return;
        }
        PbUtil.writeLenFieldPrefix(destBuffer, pbIndex, o.actualSize());
        o.encode(destBuffer);
    }

    // ----------------------------------------------------------------------------

    public static int sizeOf(int pbIndex, Encodable o) {
        if (o == null) {
            return 0;
        }
        int s = o.actualSize();
        return PbUtil.sizeOfLenFieldPrefix(pbIndex, s) + s;
    }

    public static boolean encode(EncodeContext c, ByteBuffer destBuffer, int pbIndex, Encodable o) {
        if (encode(c, destBuffer, pbIndex, o, MODE_ENCODE_EMPTY_NOT_ENCODE_NULL)) {
            c.stage = pbIndex;
            return true;
        } else {
            return false;
        }
    }

    // ----------------------------------------------------------------------------

    public static int sizeOf(int pbIndex, ByteArray o) {
        if (o == null || o.actualSize() == 0) {
            return 0;
        }
        int s = o.actualSize();
        return PbUtil.sizeOfLenFieldPrefix(pbIndex, s) + s;
    }

    public static boolean encode(EncodeContext c, ByteBuffer destBuffer, int pbIndex, ByteArray o) {
        if (encode(c, destBuffer, pbIndex, o, MODE_CUT_0BYTE)) {
            c.stage = pbIndex;
            return true;
        } else {
            return false;
        }
    }

    // ----------------------------------------------------------------------------

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private static boolean writeObjPrefix(ByteBuffer dest, int pbIndex, int objSize) {
        if (dest.remaining() >= PbUtil.MAX_TAG_INT32_LEN) {
            PbUtil.writeLenFieldPrefix(dest, pbIndex, objSize);
        } else {
            if (dest.remaining() < PbUtil.sizeOfLenFieldPrefix(pbIndex, objSize)) {
                return false;
            } else {
                PbUtil.writeLenFieldPrefix(dest, pbIndex, objSize);
            }
        }
        return true;
    }

    private static EncodeContext sub(EncodeContext c, int p1, int p2) {
        if (c.pending == p1) {
            c.pending = p2;
            return c.createOrGetNestedContext(true);
        } else if (c.pending == p2) {
            return c.createOrGetNestedContext(false);
        } else {
            throw new CodecException(c);
        }
    }

    private static boolean encode(EncodeContext c, ByteBuffer destBuffer, int pbIndex, Encodable o, int mode) {
        int size = o == null ? 0 : o.actualSize();
        if (size == 0) {
            if (mode == MODE_CUT_0BYTE) {
                c.pending = 0;
                return true;
            }
            if (o == null && mode == MODE_ENCODE_EMPTY_NOT_ENCODE_NULL) {
                c.pending = 0;
                return true;
            }
        }

        if (c.pending == 0) {
            if (!writeObjPrefix(destBuffer, pbIndex, size)) {
                return false;
            }
            if (size == 0) {
                c.pending = 0;
                return true;
            }
            c.pending = 1;
        }
        // assert o is not null
        //noinspection DataFlowIssue
        if (o.encode(sub(c, 1, 2), destBuffer)) {
            c.pending = 0;
            return true;
        } else {
            return false;
        }
    }

    // ----------------------------------------------------------------------------

    public static int sizeOf(int pbIndex, byte[] o) {
        return PbUtil.sizeOfBytesField(pbIndex, o);
    }

    public static boolean encode(EncodeContext context, ByteBuffer destBuffer, int pbIndex, byte[] o) {
        if (encode(context, destBuffer, pbIndex, o, MODE_CUT_0BYTE)) {
            context.stage = pbIndex;
            return true;
        } else {
            return false;
        }
    }

    // ----------------------------------------------------------------------------

    private static boolean encode(EncodeContext c, ByteBuffer destBuffer, int pbIndex, byte[] o, int mode) {
        int size = o == null ? 0 : o.length;
        if (size == 0) {
            if (mode == MODE_CUT_0BYTE) {
                c.pending = 0;
                return true;
            }
            if (o == null && mode == MODE_ENCODE_EMPTY_NOT_ENCODE_NULL) {
                c.pending = 0;
                return true;
            }
        }
        if (c.pending == 0) {
            if (!writeObjPrefix(destBuffer, pbIndex, size)) {
                return false;
            }
            if (size == 0) {
                c.pending = 0;
                return true;
            }
            c.pending = 1;
        }
        int arrOffset = c.pending - 1;
        if (arrOffset < 0 || arrOffset >= size) {
            throw new CodecException(c);
        }
        int r = destBuffer.remaining();
        int needWrite = size - arrOffset;
        if (r >= needWrite) {
            destBuffer.put(o, arrOffset, needWrite);
            c.pending = 0;
            return true;
        } else {
            destBuffer.put(o, arrOffset, r);
            c.pending += r;
            return false;
        }
    }

    // ----------------------------------------------------------------------------

    public static int sizeOfBytesList(int pbIndex, List<byte[]> list) {
        return PbUtil.sizeOfBytesListField(pbIndex, list);
    }

    public static boolean encodeBytesList(EncodeContext c, ByteBuffer dest, int pbIndex, List<byte[]> list) {
        if (list == null || list.isEmpty()) {
            c.stage = pbIndex;
            c.pending = 0;
            return true;
        }
        EncodeContext sub = sub(c, 0, 1);
        for (int count = list.size(), i = sub.stage; i < count; i++) {
            byte[] bs = list.get(i);
            if (!encode(sub, dest, pbIndex, bs, MODE_ENCODE_ALL)) {
                sub.stage = i;
                return false;
            }
        }
        sub.stage = EncodeContext.STAGE_END;
        c.stage = pbIndex;
        c.pending = 0;
        return true;
    }

    // ----------------------------------------------------------------------------

    public static int sizeOfList(int pbIndex, List<? extends Encodable> list) {
        if (list == null || list.isEmpty()) {
            return 0;
        }
        int size = 0;
        for (int len = list.size(), i = 0; i < len; i++) {
            Encodable e = list.get(i);
            int s = e == null ? 0 : e.actualSize();
            size += PbUtil.sizeOfLenFieldPrefix(pbIndex, s) + s;
        }
        return size;
    }

    public static boolean encodeList(EncodeContext c, ByteBuffer dest, int pbIndex, List<? extends Encodable> list) {
        if (list == null || list.isEmpty()) {
            c.stage = pbIndex;
            c.pending = 0;
            return true;
        }
        EncodeContext sub = sub(c, 0, 1);
        for (int count = list.size(), i = sub.stage; i < count; i++) {
            Encodable o = list.get(i);
            if (!encode(sub, dest, pbIndex, o, MODE_ENCODE_ALL)) {
                sub.stage = i;
                return false;
            }
        }
        sub.stage = EncodeContext.STAGE_END;
        c.stage = pbIndex;
        c.pending = 0;
        return true;
    }

    // ----------------------------------------------------------------------------

    public static boolean encodeFix32(EncodeContext c, ByteBuffer dest, int pbIndex, int value) {
        int r = dest.remaining();
        if (r >= PbUtil.MAX_TAG_FIX32_LEN) {
            PbUtil.writeFix32Field(dest, pbIndex, value);
            c.stage = pbIndex;
            return true;
        } else {
            if (r < PbUtil.sizeOfFix32Field(pbIndex, value)) {
                return false;
            } else {
                PbUtil.writeFix32Field(dest, pbIndex, value);
                c.stage = pbIndex;
                return true;
            }
        }
    }

    public static boolean encodeFix64(EncodeContext c, ByteBuffer dest, int pbIndex, long value) {
        int r = dest.remaining();
        if (r >= PbUtil.MAX_TAG_FIX64_LEN) {
            PbUtil.writeFix64Field(dest, pbIndex, value);
            c.stage = pbIndex;
            return true;
        } else {
            if (r < PbUtil.sizeOfFix64Field(pbIndex, value)) {
                return false;
            } else {
                PbUtil.writeFix64Field(dest, pbIndex, value);
                c.stage = pbIndex;
                return true;
            }
        }
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public static boolean encodeInt32(EncodeContext c, ByteBuffer dest, int pbIndex, int value) {
        int r = dest.remaining();
        if (r >= PbUtil.MAX_TAG_INT32_LEN) {
            PbUtil.writeInt32Field(dest, pbIndex, value);
            c.stage = pbIndex;
            return true;
        } else {
            if (r < PbUtil.sizeOfInt32Field(pbIndex, value)) {
                return false;
            } else {
                PbUtil.writeInt32Field(dest, pbIndex, value);
                c.stage = pbIndex;
                return true;
            }
        }
    }

    public static boolean encodeInt64(EncodeContext c, ByteBuffer dest, int pbIndex, long value) {
        int r = dest.remaining();
        if (r >= PbUtil.MAX_TAG_INT64_LEN) {
            PbUtil.writeInt64Field(dest, pbIndex, value);
            c.stage = pbIndex;
            return true;
        } else {
            if (r < PbUtil.sizeOfInt64Field(pbIndex, value)) {
                return false;
            } else {
                PbUtil.writeInt64Field(dest, pbIndex, value);
                c.stage = pbIndex;
                return true;
            }
        }
    }

    public static boolean encodeInt32s(EncodeContext c, ByteBuffer dest, int pbIndex, int[] values) {
        if (values == null || values.length == 0) {
            c.stage = pbIndex;
            c.pending = 0;
            return true;
        }
        int i = c.pending;
        for (int l = values.length; i < l; i++) {
            int r = dest.remaining();
            if (r >= PbUtil.MAX_TAG_INT32_LEN) {
                PbUtil.writeTag(dest, PbUtil.TYPE_VAR_INT, pbIndex);
                PbUtil.writeInt32(dest, values[i]);
            } else {
                if (r < PbUtil.sizeOfTag(pbIndex) + PbUtil.sizeOfInt32(values[i])) {
                    c.pending = i;
                    return false;
                } else {
                    PbUtil.writeTag(dest, PbUtil.TYPE_VAR_INT, pbIndex);
                    PbUtil.writeInt32(dest, values[i]);
                }
            }
        }
        c.stage = pbIndex;
        c.pending = 0;
        return true;
    }

    public static boolean encodeFix32s(EncodeContext c, ByteBuffer dest, int pbIndex, int[] values) {
        if (values == null || values.length == 0) {
            c.stage = pbIndex;
            c.pending = 0;
            return true;
        }
        int i = c.pending;
        for (int l = values.length; i < l; i++) {
            int r = dest.remaining();
            if (r >= PbUtil.MAX_TAG_FIX32_LEN) {
                PbUtil.writeTag(dest, PbUtil.TYPE_FIX32, pbIndex);
                dest.putInt(Integer.reverseBytes(values[i]));
            } else {
                if (r < PbUtil.sizeOfTag(pbIndex) + 4) {
                    c.pending = i;
                    return false;
                } else {
                    PbUtil.writeTag(dest, PbUtil.TYPE_FIX32, pbIndex);
                    dest.putInt(Integer.reverseBytes(values[i]));
                }
            }
        }
        c.stage = pbIndex;
        c.pending = 0;
        return true;
    }

    public static boolean encodeInt64s(EncodeContext c, ByteBuffer dest, int pbIndex, long[] values) {
        if (values == null || values.length == 0) {
            c.stage = pbIndex;
            c.pending = 0;
            return true;
        }
        int i = c.pending;
        for (int l = values.length; i < l; i++) {
            int r = dest.remaining();
            if (r >= PbUtil.MAX_TAG_INT64_LEN) {
                PbUtil.writeTag(dest, PbUtil.TYPE_VAR_INT, pbIndex);
                PbUtil.writeUnsignedInt64(dest, values[i]);
            } else {
                if (r < PbUtil.sizeOfTag(pbIndex) + PbUtil.sizeOfUnsignedInt64(values[i])) {
                    c.pending = i;
                    return false;
                } else {
                    PbUtil.writeTag(dest, PbUtil.TYPE_VAR_INT, pbIndex);
                    PbUtil.writeUnsignedInt64(dest, values[i]);
                }
            }
        }
        c.stage = pbIndex;
        c.pending = 0;
        return true;
    }

    public static boolean encodeFix64s(EncodeContext c, ByteBuffer dest, int pbIndex, long[] values) {
        if (values == null || values.length == 0) {
            c.stage = pbIndex;
            c.pending = 0;
            return true;
        }
        int i = c.pending;
        for (int l = values.length; i < l; i++) {
            int r = dest.remaining();
            if (r >= PbUtil.MAX_TAG_FIX64_LEN) {
                PbUtil.writeTag(dest, PbUtil.TYPE_FIX64, pbIndex);
                dest.putLong(Long.reverseBytes(values[i]));
            } else {
                if (r < PbUtil.sizeOfTag(pbIndex) + 8) {
                    c.pending = i;
                    return false;
                } else {
                    PbUtil.writeTag(dest, PbUtil.TYPE_FIX64, pbIndex);
                    dest.putLong(Long.reverseBytes(values[i]));
                }
            }
        }
        c.stage = pbIndex;
        c.pending = 0;
        return true;
    }

}
