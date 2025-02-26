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

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.common.ByteArray;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
class EncodeUtilTest {

    EncodeContext c;
    ByteBuffer buf;

    public void init(int size) {
        c = CodecTestUtil.createEncodeContext();
        buf = ByteBuffer.allocate(size);
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
    public void testEncodeBytes() {
        init(0);
        assertTrue(EncodeUtil.encodeBytes(c, buf, 1, new byte[]{}));
        assertEquals(0, buf.position());
        assertEquals(1, c.stage);
        assertEquals(0, c.pending);

        init(0);
        assertTrue(EncodeUtil.encodeBytes(c, buf, 1, null));
        assertEquals(0, buf.position());
        assertEquals(1, c.stage);
        assertEquals(0, c.pending);

        byte[] data = new byte[]{1, 2, 3};
        init(EncodeUtil.sizeOfBytesField(1, data));
        assertTrue(EncodeUtil.encodeBytes(c, buf, 1, data));
        assertFalse(buf.hasRemaining());
        assertEquals(1, c.stage);
        assertEquals(0, c.pending);
        check(1, data);

        init(EncodeUtil.sizeOfBytesField(1, data));
        encodeUseSmallBuf(b -> EncodeUtil.encodeBytes(c, b, 1, data));
        assertFalse(buf.hasRemaining());
        assertEquals(1, c.stage);
        assertEquals(0, c.pending);
        check(1, data);
    }

    private void check(int index, byte[] data) {
        buf.flip();
        ByteBuffer buf2 = ByteBuffer.allocate(buf.remaining());
        PbUtil.writeLenFieldPrefix(buf2, index, data.length);
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
        init(EncodeUtil.sizeOfBytesListField(1, list));
        assertTrue(EncodeUtil.encodeBytesList(c, buf, 1, list));
        assertFalse(buf.hasRemaining());
        assertEquals(1, c.stage);
        assertEquals(0, c.pending);
        check(1, list);

        init(EncodeUtil.sizeOfBytesListField(1, list));
        encodeUseSmallBuf(b -> EncodeUtil.encodeBytesList(c, b, 1, list));
        assertFalse(buf.hasRemaining());
        assertEquals(1, c.stage);
        assertEquals(0, c.pending);
        check(1, list);
    }

    private void check(int index, ArrayList<byte[]> list) {
        buf.flip();
        ByteBuffer buf2 = ByteBuffer.allocate(buf.remaining());
        for (byte[] data : list) {
            PbUtil.writeLenFieldPrefix(buf2, index, data.length);
            buf2.put(data);
        }
        buf2.flip();
        assertEquals(buf2, buf);
    }

    @Test
    public void testEncodeFix32s() {
        int[] data = new int[]{1, 2, 3, 0, -1, 100};
        int size = PbUtil.sizeOfFix32Field(1, data);
        init(size);
        assertTrue(EncodeUtil.encodeFix32s(c, buf, 1, data));
        assertFalse(buf.hasRemaining());
        assertEquals(1, c.stage);
        assertEquals(0, c.pending);
        check(1, data);

        init(size);
        encodeUseSmallBuf(b -> EncodeUtil.encodeFix32s(c, b, 1, data));
        assertFalse(buf.hasRemaining());
        assertEquals(1, c.stage);
        assertEquals(0, c.pending);
        check(1, data);
    }

    private void check(int index, int[] data) {
        buf.flip();
        ByteBuffer buf2 = ByteBuffer.allocate(buf.remaining());
        for (int value : data) {
            PbUtil.writeTag(buf2, PbUtil.TYPE_FIX32, index);
            buf2.putInt(Integer.reverseBytes(value));
        }
        buf2.flip();
        assertEquals(buf2, buf);
    }

    @Test
    public void testEncodeInt32s() {
        int[] data = new int[]{1, 2, 3, 0, -1, 100};
        int size = PbUtil.sizeOfInt32Field(1, data);
        init(size);
        assertTrue(EncodeUtil.encodeInt32s(c, buf, 1, data));
        assertFalse(buf.hasRemaining());
        assertEquals(1, c.stage);
        assertEquals(0, c.pending);
        checkInt32s(1, data);

        init(size);
        encodeUseSmallBuf(b -> EncodeUtil.encodeInt32s(c, b, 1, data));
        assertFalse(buf.hasRemaining());
        assertEquals(1, c.stage);
        assertEquals(0, c.pending);
        checkInt32s(1, data);
    }

    private void checkInt32s(int index, int[] data) {
        buf.flip();
        ByteBuffer buf2 = ByteBuffer.allocate(buf.remaining());
        for (int value : data) {
            PbUtil.writeTag(buf2, PbUtil.TYPE_VAR_INT, index);
            PbUtil.writeInt32(buf2, value);
        }
        buf2.flip();
        assertEquals(buf2, buf);
    }

    @Test
    public void testEncodeEncodable() {
        RefBuffer encodable = RefBuffer.wrap(ByteBuffer.wrap(new byte[]{10, 20, 30}));
        int size = EncodeUtil.sizeOfEncodableField(1, encodable);
        init(size);
        assertTrue(EncodeUtil.encode(c, buf, 1, encodable));
        assertFalse(buf.hasRemaining());
        assertEquals(1, c.stage);
        assertEquals(0, c.pending);
        check(1, encodable);

        init(size);
        encodeUseSmallBuf(b -> EncodeUtil.encode(c, b, 1, encodable));
        assertFalse(buf.hasRemaining());
        assertEquals(1, c.stage);
        assertEquals(0, c.pending);
        check(1, encodable);
    }

    private void check(int index, RefBuffer o) {
        buf.flip();
        ByteBuffer buf2 = ByteBuffer.allocate(buf.remaining());
        PbUtil.writeBytesField(buf2, index, o.getBuffer().array());
        buf2.flip();
        assertEquals(buf2, buf);
    }


    @Test
    public void testEncodeByteArray() {
        ByteArray data = new ByteArray(new byte[]{1, 2, 3, 0, -1});
        int size = EncodeUtil.sizeOfByteArrayField(1, data);
        init(size);
        assertTrue(EncodeUtil.encode(c, buf, 1, data));
        assertFalse(buf.hasRemaining());
        assertEquals(1, c.stage);
        assertEquals(0, c.pending);
        check(1, data);

        init(size);
        encodeUseSmallBuf(b -> EncodeUtil.encode(c, b, 1, data));
        assertFalse(buf.hasRemaining());
        assertEquals(1, c.stage);
        assertEquals(0, c.pending);
        check(1, data);
    }

    private void check(int index, ByteArray data) {
        buf.flip();
        ByteBuffer buf2 = ByteBuffer.allocate(buf.remaining());
        PbUtil.writeLenFieldPrefix(buf2, index, data.getData().length);
        buf2.put(data.getData());
        buf2.flip();
        assertEquals(buf2, buf);
    }

    @Test
    public void testEncodeEncodableList() {
        List<ByteArray> list = new ArrayList<>();
        list.add(new ByteArray(new byte[]{1, 2, 3}));
        list.add(new ByteArray(new byte[]{}));
        list.add(new ByteArray(new byte[]{-1, -2, -3}));
        int size = EncodeUtil.sizeOfEncodableListField(1, list);
        init(size);
        assertTrue(EncodeUtil.encodeList(c, buf, 1, list));
        assertFalse(buf.hasRemaining());
        assertEquals(1, c.stage);
        assertEquals(0, c.pending);
        check(1, list);

        init(size);
        encodeUseSmallBuf(b -> EncodeUtil.encodeList(c, b, 1, list));
        assertFalse(buf.hasRemaining());
        assertEquals(1, c.stage);
        assertEquals(0, c.pending);
        check(1, list);
    }

    private void check(int index, List<ByteArray> list) {
        buf.flip();
        ByteBuffer buf2 = ByteBuffer.allocate(buf.remaining());
        for (ByteArray data : list) {
            PbUtil.writeLenFieldPrefix(buf2, index, data.getData().length);
            buf2.put(data.getData());
        }
        buf2.flip();
        assertEquals(buf2, buf);
    }

    @Test
    public void testEncodeFix32() {
        testEncodeFix32(123456789);
        testEncodeFix32(0);
        testEncodeFix32(-123456789);
    }

    private void testEncodeFix32(int value) {
        int size = PbUtil.sizeOfFix32Field(1, value);
        init(size);
        assertTrue(EncodeUtil.encodeFix32(c, buf, 1, value));
        assertFalse(buf.hasRemaining());
        assertEquals(1, c.stage);
        checkFix32(1, value);

        init(size);
        encodeUseSmallBuf(b -> EncodeUtil.encodeFix32(c, b, 1, value));
        assertFalse(buf.hasRemaining());
        assertEquals(1, c.stage);
        checkFix32(1, value);
    }

    private void checkFix32(int index, int value) {
        buf.flip();
        ByteBuffer buf2 = ByteBuffer.allocate(buf.remaining());
        PbUtil.writeFix32Field(buf2, index, value);
        buf2.flip();
        assertEquals(buf2, buf);
    }

    @Test
    public void testEncodeFix64() {
        testEncodeFix64(1234567890123456789L);
        testEncodeFix64(0);
        testEncodeFix64(-1234567890123456789L);
    }

    private void testEncodeFix64(long value) {
        int size = PbUtil.sizeOfFix64Field(1, value);
        init(size);
        assertTrue(EncodeUtil.encodeFix64(c, buf, 1, value));
        assertFalse(buf.hasRemaining());
        assertEquals(1, c.stage);
        checkFix64(1, value);

        init(size);
        encodeUseSmallBuf(b -> EncodeUtil.encodeFix64(c, b, 1, value));
        assertFalse(buf.hasRemaining());
        assertEquals(1, c.stage);
        checkFix64(1, value);
    }

    private void checkFix64(int index, long value) {
        buf.flip();
        ByteBuffer buf2 = ByteBuffer.allocate(buf.remaining());
        PbUtil.writeFix64Field(buf2, index, value);
        buf2.flip();
        assertEquals(buf2, buf);
    }

    @Test
    public void testEncodeInt32() {
        testEncodeInt32(123456789);
        testEncodeInt32(0);
        testEncodeInt32(-123456789);
    }

    private void testEncodeInt32(int value) {
        int size = PbUtil.sizeOfInt32Field(1, value);
        init(size);
        assertTrue(EncodeUtil.encodeInt32(c, buf, 1, value));
        assertFalse(buf.hasRemaining());
        assertEquals(1, c.stage);
        checkInt32(1, value);

        init(size);
        encodeUseSmallBuf(b -> EncodeUtil.encodeInt32(c, b, 1, value));
        assertFalse(buf.hasRemaining());
        assertEquals(1, c.stage);
        checkInt32(1, value);
    }

    private void checkInt32(int index, int value) {
        buf.flip();
        ByteBuffer buf2 = ByteBuffer.allocate(buf.remaining());
        PbUtil.writeInt32Field(buf2, index, value);
        buf2.flip();
        assertEquals(buf2, buf);
    }

    @Test
    public void testEncodeInt64() {
        testEncodeInt64(1234567890123456789L);
        testEncodeInt64(0);
        testEncodeInt64(-1234567890123456789L);
    }

    private void testEncodeInt64(long value) {
        int size = PbUtil.sizeOfInt64Field(1, value);
        init(size);
        assertTrue(EncodeUtil.encodeInt64(c, buf, 1, value));
        assertFalse(buf.hasRemaining());
        assertEquals(1, c.stage);
        checkInt64(1, value);

        init(size);
        encodeUseSmallBuf(b -> EncodeUtil.encodeInt64(c, b, 1, value));
        assertFalse(buf.hasRemaining());
        assertEquals(1, c.stage);
        checkInt64(1, value);
    }

    private void checkInt64(int index, long value) {
        buf.flip();
        ByteBuffer buf2 = ByteBuffer.allocate(buf.remaining());
        PbUtil.writeInt64Field(buf2, index, value);
        buf2.flip();
        assertEquals(buf2, buf);
    }

}
