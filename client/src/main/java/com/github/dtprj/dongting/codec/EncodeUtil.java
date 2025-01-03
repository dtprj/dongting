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
import java.util.List;

/**
 * @author huangli
 */
public class EncodeUtil {

    public static int actualSize(int pbIndex, Encodable o) {
        if (o == null) {
            return 0;
        }
        return PbUtil.accurateLengthDelimitedSize(pbIndex, o.actualSize());
    }

    public static boolean encode(EncodeContext c, ByteBuffer destBuffer, int pbIndex, Encodable o) {
        if (o == null) {
            return true;
        }
        if (c.pending == 0) {
            int r = destBuffer.remaining();
            int actualSize = o.actualSize();
            if (actualSize == 0) {
                return true;
            }
            int prefixSize = PbUtil.accurateLengthDelimitedPrefixSize(pbIndex, actualSize);
            if (r < prefixSize) {
                return false;
            }
            PbUtil.writeLengthDelimitedPrefix(destBuffer, pbIndex, actualSize);
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
        if (o == null) {
            return 0;
        }
        return PbUtil.accurateLengthDelimitedSize(pbIndex, o.length);
    }

    public static boolean encode(EncodeContext context, ByteBuffer destBuffer, int pbIndex, byte[] o) {
        if (o == null || o.length == 0) {
            return true;
        }
        if (context.pending == 0) {
            int r = destBuffer.remaining();
            int prefixSize = PbUtil.accurateLengthDelimitedPrefixSize(pbIndex, o.length);
            if (r < prefixSize) {
                return false;
            }
            PbUtil.writeLengthDelimitedPrefix(destBuffer, pbIndex, o.length);
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
            if (e != null) {
                size += PbUtil.accurateLengthDelimitedSize(pbIndex, e.length);
            }
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
            if (!encode(sub, dest, pbIndex, bs)) {
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
            if (e != null) {
                size += PbUtil.accurateLengthDelimitedSize(pbIndex, e.actualSize());
            }
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
            if (!encode(sub, dest, pbIndex, o)) {
                sub.stage = i;
                return false;
            }
        }
        sub.stage = EncodeContext.STAGE_END;
        c.pending = 0;
        return true;
    }

}
