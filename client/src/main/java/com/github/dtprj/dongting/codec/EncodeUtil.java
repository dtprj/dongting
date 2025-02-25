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
import java.util.Objects;

/**
 * @author huangli
 */
public class EncodeUtil {

    public static int actualSize(int pbIndex, SimpleEncodable o) {
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

    public static int actualSize(int pbIndex, Encodable o) {
        if (o == null) {
            return 0;
        }
        int s = o.actualSize();
        return PbUtil.sizeOfLenFieldPrefix(pbIndex, s) + s;
    }

    public static int actualSize(int pbIndex, ByteArray o) {
        if (o == null || o.actualSize() == 0) {
            return 0;
        }
        int s = o.actualSize();
        return PbUtil.sizeOfLenFieldPrefix(pbIndex, s) + s;
    }

    public static boolean encode(EncodeContext c, ByteBuffer destBuffer, int pbIndex, Encodable o) {
        return encode(c, destBuffer, pbIndex, o, true, true);
    }

    public static boolean encode(EncodeContext c, ByteBuffer destBuffer, int pbIndex, ByteArray o) {
        return encode(c, destBuffer, pbIndex, o, false, true);
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private static boolean writeObjPrefix(EncodeContext c, ByteBuffer dest, int pbIndex, int objSize) {
        if (dest.remaining() >= PbUtil.MAX_TAG_INT32_LEN) {
            PbUtil.writeLenFieldPrefix(dest, pbIndex, objSize);
        } else {
            if (dest.remaining() < PbUtil.sizeOfLenFieldPrefix(pbIndex, objSize)) {
                return false;
            } else {
                PbUtil.writeLenFieldPrefix(dest, pbIndex, objSize);
            }
        }
        c.pending = 1;
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

    private static boolean encode(EncodeContext c, ByteBuffer destBuffer, int pbIndex, Encodable o,
                                  boolean encodeEmpty, boolean updateStage) {
        if (o == null) {
            if (updateStage) {
                c.stage = pbIndex;
            }
            c.pending = 0;
            return true;
        }
        int size = o.actualSize();
        if (size == 0 && !encodeEmpty) {
            if (updateStage) {
                c.stage = pbIndex;
            }
            c.pending = 0;
            return true;
        }
        if (c.pending == 0) {
            if (!writeObjPrefix(c, destBuffer, pbIndex, size)) {
                return false;
            }
            if (size == 0) {
                if (updateStage) {
                    c.stage = pbIndex;
                }
                c.pending = 0;
                return true;
            }
        }
        if (o.encode(sub(c, 1, 2), destBuffer)) {
            if (updateStage) {
                c.stage = pbIndex;
            }
            c.pending = 0;
            return true;
        } else {
            return false;
        }
    }

    public static int actualSize(int pbIndex, byte[] o) {
        if (o == null || o.length == 0) {
            return 0;
        }
        int s = o.length;
        return PbUtil.sizeOfLenFieldPrefix(pbIndex, s) + s;
    }

    public static boolean encode(EncodeContext context, ByteBuffer destBuffer, int pbIndex, byte[] o) {
        return encode(context, destBuffer, pbIndex, o, false, true);
    }

    private static boolean encode(EncodeContext c, ByteBuffer destBuffer, int pbIndex, byte[] o,
                                  boolean encodeEmpty, boolean updateStage) {
        if (o == null) {
            if (updateStage) {
                c.stage = pbIndex;
            }
            c.pending = 0;
            return true;
        }
        if (o.length == 0 && !encodeEmpty) {
            if (updateStage) {
                c.stage = pbIndex;
            }
            c.pending = 0;
            return true;
        }
        if (c.pending == 0) {
            if (!writeObjPrefix(c, destBuffer, pbIndex, o.length)) {
                return false;
            }
            if (o.length == 0) {
                if (updateStage) {
                    c.stage = pbIndex;
                }
                c.pending = 0;
                return true;
            }
        }
        int arrOffset = c.pending - 1;
        if (arrOffset < 0 || arrOffset >= o.length) {
            throw new CodecException(c);
        }
        int r = destBuffer.remaining();
        int needWrite = o.length - arrOffset;
        if (r >= needWrite) {
            destBuffer.put(o, arrOffset, needWrite);
            if (updateStage) {
                c.stage = pbIndex;
            }
            c.pending = 0;
            return true;
        } else {
            destBuffer.put(o, arrOffset, r);
            c.pending += r;
            return false;
        }
    }

    public static int actualSizeOfBytes(int pbIndex, List<byte[]> list) {
        if (list == null || list.isEmpty()) {
            return 0;
        }
        int size = 0;
        for (int len = list.size(), i = 0; i < len; i++) {
            byte[] e = list.get(i);
            Objects.requireNonNull(e);
            int s = e.length;
            size += PbUtil.sizeOfLenFieldPrefix(pbIndex, s) + s;
        }
        return size;
    }

    public static boolean encodeBytes(EncodeContext c, ByteBuffer dest, int pbIndex, List<byte[]> list) {
        if (list == null || list.isEmpty()) {
            c.stage = pbIndex;
            c.pending = 0;
            return true;
        }
        EncodeContext sub = sub(c, 0, 1);
        for (int count = list.size(), i = sub.stage; i < count; i++) {
            byte[] bs = list.get(i);
            Objects.requireNonNull(bs);
            if (!encode(sub, dest, pbIndex, bs, true, false)) {
                sub.stage = i;
                return false;
            }
        }
        sub.stage = EncodeContext.STAGE_END;
        c.stage = pbIndex;
        c.pending = 0;
        return true;
    }

    public static int actualSizeOfObjs(int pbIndex, List<? extends Encodable> list) {
        if (list == null || list.isEmpty()) {
            return 0;
        }
        int size = 0;
        for (int len = list.size(), i = 0; i < len; i++) {
            Encodable e = list.get(i);
            Objects.requireNonNull(e);
            int s = e.actualSize();
            size += PbUtil.sizeOfLenFieldPrefix(pbIndex, s) + s;
        }
        return size;
    }

    public static boolean encodeObjs(EncodeContext c, ByteBuffer dest, int pbIndex, List<? extends Encodable> list) {
        if (list == null || list.isEmpty()) {
            c.stage = pbIndex;
            c.pending = 0;
            return true;
        }
        EncodeContext sub = sub(c, 0, 1);
        for (int count = list.size(), i = sub.stage; i < count; i++) {
            Encodable o = list.get(i);
            Objects.requireNonNull(o);
            if (!encode(sub, dest, pbIndex, o, true, false)) {
                sub.stage = i;
                return false;
            }
        }
        sub.stage = EncodeContext.STAGE_END;
        c.stage = pbIndex;
        c.pending = 0;
        return true;
    }

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
                PbUtil.writeUnsignedInt32(dest, values[i]);
            } else {
                if (r < PbUtil.sizeOfTag(pbIndex) + PbUtil.sizeOfUnsignedInt32(values[i])) {
                    c.pending = i;
                    return false;
                } else {
                    PbUtil.writeTag(dest, PbUtil.TYPE_VAR_INT, pbIndex);
                    PbUtil.writeUnsignedInt32(dest, values[i]);
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

}
