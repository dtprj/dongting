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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author huangli
 */
class EncodeUtilTest {

    EncodeContext c;
    ByteBuffer buf = ByteBuffer.allocate(128);

    @BeforeEach
    public void init() {
        c = CodecTestUtil.createEncodeContext();
        buf.clear();
    }

    private void encodeUseSmallBuf(Function<ByteBuffer, Boolean> encodeCallback) {
        ByteBuffer smallBuf = ByteBuffer.allocate(1);
        ByteBuffer b = smallBuf;
        while (!encodeCallback.apply(b)) {
            if (b.position() == 0) {
                b = ByteBuffer.allocate(b.capacity() + 1);
            } else {
                b.flip();
                buf.put(b);
                b = smallBuf;
                b.clear();
            }
        }
        b.flip();
        buf.put(b);
    }

    @Test
    public void testEncodeForBytes() {
        buf.clear();
        EncodeUtil.encode(c, buf, 1, new byte[]{});
        assertEquals(0, buf.position());

        buf.clear();
        EncodeUtil.encode(c, buf, 1, (byte[]) null);
        assertEquals(0, buf.position());

        buf.clear();
        byte[] data = new byte[]{1, 2, 3};
        EncodeUtil.encode(c, buf, 1, data);
        assertEquals(EncodeUtil.actualSize(1, data), buf.position());
        check(1, data);

        buf.clear();
        encodeUseSmallBuf(b -> EncodeUtil.encode(c, b, 1, data));
        check(1, data);
    }

    private void check(int index, byte[] data) {
        buf.flip();
        ByteBuffer buf2 = ByteBuffer.allocate(buf.remaining());
        PbUtil.writeLengthDelimitedPrefix(buf2, index, data.length);
        buf2.put(data);
        buf2.flip();
        assertEquals(buf2, buf);
    }

    @Test
    public void testEncodeBytesList() {
        ArrayList<byte[]> list = new ArrayList<>();
        list.add(new byte[]{1, 2, 3});
        list.add(new byte[]{});
        list.add(new byte[]{4, 5, 6});
        EncodeUtil.encodeBytes(c, buf, 1, list);
        assertEquals(EncodeUtil.actualSizeOfBytes(1, list), buf.position());
        check(1, list);

        buf.clear();
        encodeUseSmallBuf(b -> EncodeUtil.encodeBytes(c, b, 1, list));
        assertEquals(EncodeUtil.actualSizeOfBytes(1, list), buf.position());
        check(1, list);
    }

    private void check(int index, ArrayList<byte[]> list) {
        buf.flip();
        ByteBuffer buf2 = ByteBuffer.allocate(buf.remaining());
        for (byte[] data : list) {
            PbUtil.writeLengthDelimitedPrefix(buf2, index, data.length);
            buf2.put(data);
        }
        buf2.flip();
        assertEquals(buf2, buf);
    }
}
