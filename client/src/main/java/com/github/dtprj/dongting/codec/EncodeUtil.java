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
        return PbUtil.accurateLengthDelimitedPrefixSize(pbIndex, s) + s;
    }

    public static void encode(ByteBuffer destBuffer, int pbIndex, SimpleEncodable o) {
        if (o == null) {
            return;
        }
        PbUtil.writeLengthDelimitedPrefix(destBuffer, pbIndex, o.actualSize());
        o.encode(destBuffer);
    }

    public static int actualSize(int pbIndex, Encodable o) {
        if (o == null) {
            return 0;
        }
        int s = o.actualSize();
        return PbUtil.accurateLengthDelimitedPrefixSize(pbIndex, s) + s;
    }

    public static int actualSize(int pbIndex, ByteArray o) {
        if (o == null || o.actualSize() == 0) {
            return 0;
        }
        int s = o.actualSize();
        return PbUtil.accurateLengthDelimitedPrefixSize(pbIndex, s) + s;
    }

    public static boolean encode(EncodeContext c, ByteBuffer destBuffer, int pbIndex, Encodable o) {
        return encode(c, destBuffer, pbIndex, o, true);
    }

    public static boolean encode(EncodeContext c, ByteBuffer destBuffer, int pbIndex, ByteArray o) {
        return encode(c, destBuffer, pbIndex, o, false);
    }

    private static boolean encode(EncodeContext c, ByteBuffer destBuffer, int pbIndex, Encodable o, boolean encodeEmpty) {
        if (o == null) {
            return true;
        }
        int actualSize = o.actualSize();
        if (actualSize == 0 && !encodeEmpty) {
            return true;
        }
        if (c.pending == 0) {
            int r = destBuffer.remaining();
            int prefixSize = PbUtil.accurateLengthDelimitedPrefixSize(pbIndex, actualSize);
            if (r < prefixSize) {
                return false;
            }
            PbUtil.writeLengthDelimitedPrefix(destBuffer, pbIndex, actualSize);
            if (actualSize == 0) {
                return true;
            }
            c.pending = 1;
        }
        EncodeContext sub;
        if (c.pending == 1) {
            c.pending = 2;
            sub = c.createOrGetNestedContext(true);
        } else if (c.pending == 2) {
            sub = c.createOrGetNestedContext(false);
        } else {
            throw new CodecException(c);
        }
        if (o.encode(sub, destBuffer)) {
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
        return PbUtil.accurateLengthDelimitedPrefixSize(pbIndex, s) + s;
    }

    public static boolean encode(EncodeContext context, ByteBuffer destBuffer, int pbIndex, byte[] o) {
        return encode(context, destBuffer, pbIndex, o, false);
    }

    private static boolean encode(EncodeContext context, ByteBuffer destBuffer, int pbIndex, byte[] o, boolean encodeEmpty) {
        if (o == null) {
            return true;
        }
        int size = o.length;
        if (size == 0 && !encodeEmpty) {
            return true;
        }
        if (context.pending == 0) {
            int r = destBuffer.remaining();
            int prefixSize = PbUtil.accurateLengthDelimitedPrefixSize(pbIndex, size);
            if (r < prefixSize) {
                return false;
            }
            PbUtil.writeLengthDelimitedPrefix(destBuffer, pbIndex, size);
            if (size == 0) {
                return true;
            }
            context.pending = 1;
        }
        int arrOffset = context.pending - 1;
        if (arrOffset < 0 || arrOffset >= o.length) {
            throw new CodecException(context);
        }
        int remaining = destBuffer.remaining();
        int needWrite = o.length - arrOffset;
        if (remaining >= needWrite) {
            destBuffer.put(o, arrOffset, needWrite);
            context.pending = 0;
            return true;
        } else {
            destBuffer.put(o, arrOffset, remaining);
            context.pending += remaining;
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
            size += PbUtil.accurateLengthDelimitedPrefixSize(pbIndex, s) + s;
        }
        return size;
    }

    public static boolean encodeBytes(EncodeContext c, ByteBuffer dest, int pbIndex, List<byte[]> list) {
        if (list == null || list.isEmpty()) {
            return true;
        }
        EncodeContext sub;
        if (c.pending == 0) {
            sub = c.createOrGetNestedContext(true);
            c.pending = 1;
        } else if (c.pending == 1) {
            sub = c.createOrGetNestedContext(false);
        } else {
            throw new CodecException(c);
        }
        int count = list.size();
        int i = sub.stage;
        for (; i < count; i++) {
            byte[] bs = list.get(i);
            Objects.requireNonNull(bs);
            if (!encode(sub, dest, pbIndex, bs, true)) {
                sub.stage = i;
                return false;
            }
        }
        sub.stage = EncodeContext.STAGE_END;
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
            size += PbUtil.accurateLengthDelimitedPrefixSize(pbIndex, s) + s;
        }
        return size;
    }

    public static boolean encodeObjs(EncodeContext c, ByteBuffer dest, int pbIndex, List<? extends Encodable> list) {
        if (list == null || list.isEmpty()) {
            return true;
        }
        EncodeContext sub;
        if (c.pending == 0) {
            sub = c.createOrGetNestedContext(true);
            c.pending = 1;
        } else if (c.pending == 1) {
            sub = c.createOrGetNestedContext(false);
        } else {
            throw new CodecException(c);
        }
        int count = list.size();
        int i = sub.stage;
        for (; i < count; i++) {
            Encodable o = list.get(i);
            Objects.requireNonNull(o);
            if (!encode(sub, dest, pbIndex, o, true)) {
                sub.stage = i;
                return false;
            }
        }
        sub.stage = EncodeContext.STAGE_END;
        c.pending = 0;
        return true;
    }

}
