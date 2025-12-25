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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static com.github.dtprj.dongting.codec.PbUtil.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
public class PbUtilTest {

    @Test
    public void testWriteTag() {
        DtPbTest.TestTag pb = DtPbTest.TestTag.newBuilder().setF1(1).build();
        compare(pb, 1);
        pb = DtPbTest.TestTag.newBuilder().setF2(1).build();
        compare(pb, 2);
        pb = DtPbTest.TestTag.newBuilder().setF15(1).build();
        compare(pb, 15);
        pb = DtPbTest.TestTag.newBuilder().setF16(1).build();
        compare(pb, 16);
        pb = DtPbTest.TestTag.newBuilder().setF2047(1).build();
        compare(pb, 2047);
        pb = DtPbTest.TestTag.newBuilder().setF2048(1).build();
        compare(pb, 2048);
        pb = DtPbTest.TestTag.newBuilder().setF262143(1).build();
        compare(pb, 262143);
        pb = DtPbTest.TestTag.newBuilder().setF262144(1).build();
        compare(pb, 262144);
        pb = DtPbTest.TestTag.newBuilder().setF33554431(1).build();
        compare(pb, 33554431);
        pb = DtPbTest.TestTag.newBuilder().setF33554432(1).build();
        compare(pb, 33554432);
        pb = DtPbTest.TestTag.newBuilder().setF536870911(1).build();
        compare(pb, 536870911);
    }

    private void compare(DtPbTest.TestTag pb, int index) {
        byte[] bs = pb.toByteArray();
        int len = bs.length - 4;
        assertTrue(len >= 1);
        assertTrue(len <= 5);
        byte[] expect = new byte[len];
        System.arraycopy(bs, 0, expect, 0, len);

        ByteBuffer buf = ByteBuffer.allocate(20);
        writeTag(buf, PbUtil.TYPE_FIX32, index);
        assertEquals(len, buf.position());
        byte[] actual = new byte[len];
        System.arraycopy(buf.array(), 0, actual, 0, len);

        assertArrayEquals(expect, actual);
    }

    @Test
    public void testWriteInt32Field() {
        PbUtil.writeInt32Field(null, 0, 0);
        testWriteInt32Field(1);
        testWriteInt32Field(-1);
        testWriteInt32Field(10000);
        testWriteInt32Field(-10000);
        testWriteInt32Field(Integer.MAX_VALUE);
        testWriteInt32Field(Integer.MIN_VALUE);
        testWriteInt32Field(1 << 7);
        testWriteInt32Field((1 << 7) + 1);
        testWriteInt32Field((1 << 7) - 1);
        testWriteInt32Field((1 << 7) - 2);
        testWriteInt32Field(1 << 14);
        testWriteInt32Field((1 << 14) + 1);
        testWriteInt32Field((1 << 14) - 1);
        testWriteInt32Field((1 << 14) - 2);
        testWriteInt32Field(1 << 21);
        testWriteInt32Field((1 << 21) + 1);
        testWriteInt32Field((1 << 21) - 1);
        testWriteInt32Field((1 << 21) - 2);
        testWriteInt32Field(1 << 28);
        testWriteInt32Field((1 << 28) + 1);
        testWriteInt32Field((1 << 28) - 1);
        testWriteInt32Field((1 << 28) - 2);
    }

    private void testWriteInt32Field(int v) {
        DtPbTest.TestInt pb = DtPbTest.TestInt.newBuilder().setFInt32(v).build();
        byte[] bs = pb.toByteArray();
        int len = bs.length;
        assertTrue(len >= 2);
        assertTrue(len <= 11);

        ByteBuffer buf = ByteBuffer.allocate(20);
        PbUtil.writeInt32Field(buf, 1, v);
        assertEquals(len, buf.position());
        byte[] actual = new byte[len];
        System.arraycopy(buf.array(), 0, actual, 0, len);

        assertArrayEquals(bs, actual);
    }

    @Test
    public void testWriteInt64Field() {
        PbUtil.writeInt64Field(null, 1, 0);
        testWriteInt64Field(1);
        testWriteInt64Field(-1);
        testWriteInt64Field(10000);
        testWriteInt64Field(-10000);
        testWriteInt64Field(Long.MAX_VALUE);
        testWriteInt64Field(Long.MIN_VALUE);
        testWriteInt64Field(1L << 7);
        testWriteInt64Field((1L << 7) + 1);
        testWriteInt64Field((1L << 7) - 1);
        testWriteInt64Field((1L << 7) - 2);
        testWriteInt64Field(1L << 14);
        testWriteInt64Field((1L << 14) + 1);
        testWriteInt64Field((1L << 14) - 1);
        testWriteInt64Field((1L << 14) - 2);
        testWriteInt64Field(1L << 21);
        testWriteInt64Field((1L << 21) + 1);
        testWriteInt64Field((1L << 21) - 1);
        testWriteInt64Field((1L << 21) - 2);
        testWriteInt64Field(1L << 28);
        testWriteInt64Field((1L << 28) + 1);
        testWriteInt64Field((1L << 28) - 1);
        testWriteInt64Field((1L << 28) - 2);
        testWriteInt64Field(1L << 35);
        testWriteInt64Field((1L << 35) + 1);
        testWriteInt64Field((1L << 35) - 1);
        testWriteInt64Field((1L << 35) - 2);
        testWriteInt64Field(1L << 42);
        testWriteInt64Field((1L << 42) + 1);
        testWriteInt64Field((1L << 42) - 1);
        testWriteInt64Field((1L << 42) - 2);
        testWriteInt64Field(1L << 49);
        testWriteInt64Field((1L << 49) + 1);
        testWriteInt64Field((1L << 49) - 1);
        testWriteInt64Field((1L << 49) - 2);
        testWriteInt64Field(1L << 56);
        testWriteInt64Field((1L << 56) + 1);
        testWriteInt64Field((1L << 56) - 1);
        testWriteInt64Field((1L << 56) - 2);
        testWriteInt64Field(1L << 63);
        testWriteInt64Field((1L << 63) + 1);
        testWriteInt64Field((1L << 63) - 1);
        testWriteInt64Field((1L << 63) - 2);
    }

    private void testWriteInt64Field(long v) {
        DtPbTest.TestInt pb = DtPbTest.TestInt.newBuilder().setFInt64(v).build();
        byte[] bs = pb.toByteArray();
        int len = bs.length;
        assertTrue(len >= 2);
        assertTrue(len <= 11);

        ByteBuffer buf = ByteBuffer.allocate(20);
        PbUtil.writeInt64Field(buf, 2, v);
        assertEquals(len, buf.position());
        byte[] actual = new byte[len];
        System.arraycopy(buf.array(), 0, actual, 0, len);

        assertArrayEquals(bs, actual);
    }

    @Test
    public void testReadUnsignedInt32() {
        testReadUnsignedInt32(1);
        testReadUnsignedInt32(-1);
        testReadUnsignedInt32(10000);
        testReadUnsignedInt32(-10000);
        testReadUnsignedInt32(Integer.MAX_VALUE);
        testReadUnsignedInt32(Integer.MIN_VALUE);
        testReadUnsignedInt32(1 << 7);
        testReadUnsignedInt32((1 << 7) + 1);
        testReadUnsignedInt32((1 << 7) - 1);
        testReadUnsignedInt32((1 << 7) - 2);
        testReadUnsignedInt32(1 << 14);
        testReadUnsignedInt32((1 << 14) + 1);
        testReadUnsignedInt32((1 << 14) - 1);
        testReadUnsignedInt32((1 << 14) - 2);
        testReadUnsignedInt32(1 << 21);
        testReadUnsignedInt32((1 << 21) + 1);
        testReadUnsignedInt32((1 << 21) - 1);
        testReadUnsignedInt32((1 << 21) - 2);
        testReadUnsignedInt32(1 << 28);
        testReadUnsignedInt32((1 << 28) + 1);
        testReadUnsignedInt32((1 << 28) - 1);
        testReadUnsignedInt32((1 << 28) - 2);

        ByteBuffer buf = ByteBuffer.allocate(10);
        buf.limit(1);
        assertEquals(0, PbUtil.readUnsignedInt32(buf));
    }

    private void testReadUnsignedInt32(int v) {
        ByteBuffer buf = ByteBuffer.allocate(10);
        PbUtil.writeUnsignedInt32(buf, v);
        buf.flip();
        int v2 = PbUtil.readUnsignedInt32(buf);
        assertEquals(v, v2);
    }

    @Test
    public void testReadUnsignedInt64() {
        testReadUnsignedInt64(1);
        testReadUnsignedInt64(-1);
        testReadUnsignedInt64(10000);
        testReadUnsignedInt64(-10000);
        testReadUnsignedInt64(Long.MAX_VALUE);
        testReadUnsignedInt64(Long.MIN_VALUE);
        testReadUnsignedInt64(1L << 7);
        testReadUnsignedInt64((1L << 7) + 1);
        testReadUnsignedInt64((1L << 7) - 1);
        testReadUnsignedInt64((1L << 7) - 2);
        testReadUnsignedInt64(1L << 14);
        testReadUnsignedInt64((1L << 14) + 1);
        testReadUnsignedInt64((1L << 14) - 1);
        testReadUnsignedInt64((1L << 14) - 2);
        testReadUnsignedInt64(1L << 21);
        testReadUnsignedInt64((1L << 21) + 1);
        testReadUnsignedInt64((1L << 21) - 1);
        testReadUnsignedInt64((1L << 21) - 2);
        testReadUnsignedInt64(1L << 28);
        testReadUnsignedInt64((1L << 28) + 1);
        testReadUnsignedInt64((1L << 28) - 1);
        testReadUnsignedInt64((1L << 28) - 2);
        testReadUnsignedInt64(1L << 35);
        testReadUnsignedInt64((1L << 35) + 1);
        testReadUnsignedInt64((1L << 35) - 1);
        testReadUnsignedInt64((1L << 35) - 2);
        testReadUnsignedInt64(1L << 42);
        testReadUnsignedInt64((1L << 42) + 1);
        testReadUnsignedInt64((1L << 42) - 1);
        testReadUnsignedInt64((1L << 42) - 2);
        testReadUnsignedInt64(1L << 49);
        testReadUnsignedInt64((1L << 49) + 1);
        testReadUnsignedInt64((1L << 49) - 1);
        testReadUnsignedInt64((1L << 49) - 2);
        testReadUnsignedInt64(1L << 56);
        testReadUnsignedInt64((1L << 56) + 1);
        testReadUnsignedInt64((1L << 56) - 1);
        testReadUnsignedInt64((1L << 56) - 2);
        testReadUnsignedInt64(1L << 63);
        testReadUnsignedInt64((1L << 63) + 1);
        testReadUnsignedInt64((1L << 63) - 1);
        testReadUnsignedInt64((1L << 63) - 2);

        ByteBuffer buf = ByteBuffer.allocate(10);
        buf.limit(1);
        assertEquals(0, PbUtil.readUnsignedInt64(buf));
    }

    private void testReadUnsignedInt64(long v) {
        ByteBuffer buf = ByteBuffer.allocate(10);
        PbUtil.writeUnsignedInt64(buf, v);
        buf.flip();
        long v2 = PbUtil.readUnsignedInt64(buf);
        assertEquals(v, v2);
    }

    @Test
    public void testSizeOfTag() {
        assertThrows(IllegalArgumentException.class, () -> sizeOfTag(0));
        assertThrows(IllegalArgumentException.class, () -> sizeOfTag(-1));
        assertThrows(IllegalArgumentException.class, () -> sizeOfTag(536870911 + 1));
        testSizeOfTag(1);

        testSizeOfTag(1 << 4);
        testSizeOfTag((1 << 4) + 1);
        testSizeOfTag((1 << 4) - 1);
        testSizeOfTag((1 << 4) - 2);

        testSizeOfTag(1 << 11);
        testSizeOfTag((1 << 11) + 1);
        testSizeOfTag((1 << 11) - 1);
        testSizeOfTag((1 << 11) - 2);

        testSizeOfTag(1 << 18);
        testSizeOfTag((1 << 18) + 1);
        testSizeOfTag((1 << 18) - 1);
        testSizeOfTag((1 << 18) - 2);

        testSizeOfTag(1 << 25);
        testSizeOfTag((1 << 25) + 1);
        testSizeOfTag((1 << 25) - 1);
        testSizeOfTag((1 << 25) - 2);

        assertThrows(IllegalArgumentException.class, () -> testSizeOfTag(1 << 29));
        testSizeOfTag((1 << 29) - 1);
        testSizeOfTag((1 << 29) - 2);
    }

    private void testSizeOfTag(int index) {
        ByteBuffer buf = ByteBuffer.allocate(5);
        writeTag(buf, TYPE_VAR_INT, index);
        assertEquals(buf.position(), sizeOfTag(index));
    }

    @Test
    public void testSizeOfUnsignedInt32() {
        assertEquals(1, sizeOfUnsignedInt32(0));
        testSizeOfUnsignedInt32(-1);
        testSizeOfUnsignedInt32(1);
        testSizeOfUnsignedInt32(Integer.MAX_VALUE);
        testSizeOfUnsignedInt32(Integer.MAX_VALUE - 1);
        testSizeOfUnsignedInt32(Integer.MAX_VALUE + 1);
        testSizeOfUnsignedInt32(Integer.MIN_VALUE);
        testSizeOfUnsignedInt32(Integer.MIN_VALUE - 1);
        testSizeOfUnsignedInt32(Integer.MIN_VALUE + 1);

        testSizeOfUnsignedInt32(1 << 7);
        testSizeOfUnsignedInt32((1 << 7) + 1);
        testSizeOfUnsignedInt32((1 << 7) - 1);
        testSizeOfUnsignedInt32((1 << 7) - 2);

        testSizeOfUnsignedInt32(1 << 14);
        testSizeOfUnsignedInt32((1 << 14) + 1);
        testSizeOfUnsignedInt32((1 << 14) - 1);
        testSizeOfUnsignedInt32((1 << 14) - 2);

        testSizeOfUnsignedInt32(1 << 21);
        testSizeOfUnsignedInt32((1 << 21) + 1);
        testSizeOfUnsignedInt32((1 << 21) - 1);
        testSizeOfUnsignedInt32((1 << 21) - 2);

        testSizeOfUnsignedInt32(1 << 28);
        testSizeOfUnsignedInt32((1 << 28) + 1);
        testSizeOfUnsignedInt32((1 << 28) - 1);
        testSizeOfUnsignedInt32((1 << 28) - 2);
    }

    private void testSizeOfUnsignedInt32(int value) {
        ByteBuffer buf = ByteBuffer.allocate(5);
        writeUnsignedInt32(buf, value);
        assertEquals(buf.position(), sizeOfUnsignedInt32(value));
    }

    @Test
    public void testSizeOfInt32Field() {
        assertEquals(0, sizeOfInt32Field(100, 0));
        assertTrue(sizeOfInt32Field(536870911, Integer.MAX_VALUE) <= MAX_TAG_INT32_LEN);
        assertEquals(MAX_TAG_INT32_LEN, sizeOfInt32Field(536870911, Integer.MIN_VALUE));
        assertEquals(MAX_TAG_INT32_LEN, sizeOfInt32Field(536870911, -1));
    }

    @Test
    public void testSizeOfUnsignedInt64() {
        assertEquals(1L, sizeOfUnsignedInt64(0L));
        testSizeOfUnsignedInt64(-1L);
        testSizeOfUnsignedInt64(1L);
        testSizeOfUnsignedInt64(Long.MAX_VALUE);
        testSizeOfUnsignedInt64(Long.MAX_VALUE - 1);
        testSizeOfUnsignedInt64(Long.MAX_VALUE + 1);
        testSizeOfUnsignedInt64(Long.MIN_VALUE);
        testSizeOfUnsignedInt64(Long.MIN_VALUE - 1);
        testSizeOfUnsignedInt64(Long.MIN_VALUE + 1);

        testSizeOfUnsignedInt64(1L << 7);
        testSizeOfUnsignedInt64((1L << 7) + 1);
        testSizeOfUnsignedInt64((1L << 7) - 1);
        testSizeOfUnsignedInt64((1L << 7) - 2);

        testSizeOfUnsignedInt64(1L << 14);
        testSizeOfUnsignedInt64((1L << 14) + 1);
        testSizeOfUnsignedInt64((1L << 14) - 1);
        testSizeOfUnsignedInt64((1L << 14) - 2);

        testSizeOfUnsignedInt64(1L << 21);
        testSizeOfUnsignedInt64((1L << 21) + 1);
        testSizeOfUnsignedInt64((1L << 21) - 1);
        testSizeOfUnsignedInt64((1L << 21) - 2);

        testSizeOfUnsignedInt64(1L << 28);
        testSizeOfUnsignedInt64((1L << 28) + 1);
        testSizeOfUnsignedInt64((1L << 28) - 1);
        testSizeOfUnsignedInt64((1L << 28) - 2);

        testSizeOfUnsignedInt64(1L << 35);
        testSizeOfUnsignedInt64((1L << 35) + 1);
        testSizeOfUnsignedInt64((1L << 35) - 1);
        testSizeOfUnsignedInt64((1L << 35) - 2);

        testSizeOfUnsignedInt64(1L << 42);
        testSizeOfUnsignedInt64((1L << 42) + 1);
        testSizeOfUnsignedInt64((1L << 42) - 1);
        testSizeOfUnsignedInt64((1L << 42) - 2);

        testSizeOfUnsignedInt64(1L << 49);
        testSizeOfUnsignedInt64((1L << 49) + 1);
        testSizeOfUnsignedInt64((1L << 49) - 1);
        testSizeOfUnsignedInt64((1L << 49) - 2);

        testSizeOfUnsignedInt64(1L << 56);
        testSizeOfUnsignedInt64((1L << 56) + 1);
        testSizeOfUnsignedInt64((1L << 56) - 1);
        testSizeOfUnsignedInt64((1L << 56) - 2);
    }

    private void testSizeOfUnsignedInt64(long value) {
        ByteBuffer buf = ByteBuffer.allocate(10);
        writeUnsignedInt64(buf, value);
        assertEquals(buf.position(), sizeOfUnsignedInt64(value));
    }

    @Test
    public void testSizeOfInt64Field() {
        assertEquals(0, sizeOfInt64Field(100, 0L));
        assertEquals(MAX_TAG_INT64_LEN - 1, sizeOfInt64Field(536870911, Long.MAX_VALUE));
        assertEquals(MAX_TAG_INT64_LEN, sizeOfInt64Field(536870911, Long.MIN_VALUE));
        assertEquals(MAX_TAG_INT64_LEN, sizeOfInt64Field(536870911, -1));
    }

    @Test
    public void testWriteFixNumber() {
        assertEquals(0, sizeOfFix32Field(1, 0));
        assertEquals(0, sizeOfFix64Field(1, 0));

        ByteBuffer buf = ByteBuffer.allocate(MAX_TAG_FIX32_LEN);
        writeFix32Field(buf, 1, 0);
        writeFix64Field(buf, 1, 0);
        assertEquals(0, buf.position());

        writeFix32Field(buf, 536870911, 123456);
        assertEquals(sizeOfFix32Field(536870911, 123456), buf.position());
        buf.position(5);
        assertEquals(123456, Integer.reverseBytes(buf.getInt()));

        buf = ByteBuffer.allocate(MAX_TAG_FIX64_LEN);
        writeFix64Field(buf, 536870911, 12345689012345L);
        assertEquals(sizeOfFix64Field(536870911, 12345689012345L), buf.position());
        buf.position(5);
        assertEquals(12345689012345L, Long.reverseBytes(buf.getLong()));
    }

    @Test
    public void testString() {
        assertEquals(0, sizeOfAscii(1, null));
        assertEquals(0, sizeOfAscii(1, ""));

        String asciiStr = "12345678";
        ByteBuffer buf = ByteBuffer.allocate(sizeOfAscii(536870911, asciiStr));
        writeAsciiField(buf, 536870911, asciiStr);
        assertEquals(0, buf.remaining());
    }

    @Test
    public void testUTF8() {
        assertEquals(0, sizeOfUTF8(1, null));
        assertEquals(0, sizeOfUTF8(1, ""));

        String utf8Str = "中文abc";
        ByteBuffer buf = ByteBuffer.allocate(sizeOfUTF8(536870911, utf8Str));
        writeUTF8Field(buf, 536870911, utf8Str);
        assertEquals(0, buf.remaining());
    }

    @Test
    public void testWriteUnsignedInt32() {
        testWriteUnsignedInt32(0);
        testWriteUnsignedInt32(1);
        testWriteUnsignedInt32(-1);
        testWriteUnsignedInt32(Integer.MAX_VALUE);
        testWriteUnsignedInt32(Integer.MIN_VALUE);
    }

    private void testWriteUnsignedInt32(int v) {
        ByteBuffer buf = ByteBuffer.allocate(10);

        PbUtil.writeUnsignedInt32(buf, v);
        buf.flip();
        assertEquals(v, PbUtil.readUnsignedInt32(buf));
    }

    @Test
    public void testWriteUnsignedInt64() {
        // testWriteUnsignedInt64ValueOnly(0);
        testWriteUnsignedInt64(1);
        testWriteUnsignedInt64(-1);
        testWriteUnsignedInt64(Long.MAX_VALUE);
        testWriteUnsignedInt64(Long.MIN_VALUE);
    }

    private void testWriteUnsignedInt64(long v) {
        ByteBuffer buf = ByteBuffer.allocate(10);

        PbUtil.writeUnsignedInt64(buf, v);
        buf.flip();
        assertEquals(v, PbUtil.readUnsignedInt64(buf));
    }
}
