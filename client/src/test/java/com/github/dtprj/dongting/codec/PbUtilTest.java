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
import java.nio.ByteOrder;

import static com.github.dtprj.dongting.codec.PbUtil.TYPE_VAR_INT;
import static com.github.dtprj.dongting.codec.PbUtil.accurateFix32Size;
import static com.github.dtprj.dongting.codec.PbUtil.accurateFix64Size;
import static com.github.dtprj.dongting.codec.PbUtil.accurateLengthDelimitedSize;
import static com.github.dtprj.dongting.codec.PbUtil.accurateStrSizeAscii;
import static com.github.dtprj.dongting.codec.PbUtil.accurateTagSize;
import static com.github.dtprj.dongting.codec.PbUtil.accurateUnsignedIntSize;
import static com.github.dtprj.dongting.codec.PbUtil.accurateUnsignedLongSize;
import static com.github.dtprj.dongting.codec.PbUtil.maxFix32Size;
import static com.github.dtprj.dongting.codec.PbUtil.maxFix64Size;
import static com.github.dtprj.dongting.codec.PbUtil.maxLengthDelimitedSize;
import static com.github.dtprj.dongting.codec.PbUtil.maxStrSizeAscii;
import static com.github.dtprj.dongting.codec.PbUtil.maxStrSizeUTF8;
import static com.github.dtprj.dongting.codec.PbUtil.maxUnsignedIntSize;
import static com.github.dtprj.dongting.codec.PbUtil.maxUnsignedLongSize;
import static com.github.dtprj.dongting.codec.PbUtil.writeAscii;
import static com.github.dtprj.dongting.codec.PbUtil.writeFix32;
import static com.github.dtprj.dongting.codec.PbUtil.writeFix64;
import static com.github.dtprj.dongting.codec.PbUtil.writeTag;
import static com.github.dtprj.dongting.codec.PbUtil.writeUTF8;
import static com.github.dtprj.dongting.codec.PbUtil.writeUnsignedInt32ValueOnly;
import static com.github.dtprj.dongting.codec.PbUtil.writeUnsignedInt64ValueOnly;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    public void testWriteUnsignedInt32() {
        PbUtil.writeUnsignedInt32(null, 0, 0);
        testWriteUnsignedInt32(1);
        testWriteUnsignedInt32(-1);
        testWriteUnsignedInt32(10000);
        testWriteUnsignedInt32(-10000);
        testWriteUnsignedInt32(Integer.MAX_VALUE);
        testWriteUnsignedInt32(Integer.MIN_VALUE);
        testWriteUnsignedInt32(1 << 7);
        testWriteUnsignedInt32((1 << 7) + 1);
        testWriteUnsignedInt32((1 << 7) - 1);
        testWriteUnsignedInt32((1 << 7) - 2);
        testWriteUnsignedInt32(1 << 14);
        testWriteUnsignedInt32((1 << 14) + 1);
        testWriteUnsignedInt32((1 << 14) - 1);
        testWriteUnsignedInt32((1 << 14) - 2);
        testWriteUnsignedInt32(1 << 21);
        testWriteUnsignedInt32((1 << 21) + 1);
        testWriteUnsignedInt32((1 << 21) - 1);
        testWriteUnsignedInt32((1 << 21) - 2);
        testWriteUnsignedInt32(1 << 28);
        testWriteUnsignedInt32((1 << 28) + 1);
        testWriteUnsignedInt32((1 << 28) - 1);
        testWriteUnsignedInt32((1 << 28) - 2);
    }

    private void testWriteUnsignedInt32(int v) {
        DtPbTest.TestInt pb = DtPbTest.TestInt.newBuilder().setFUint32(v).build();
        byte[] bs = pb.toByteArray();
        int len = bs.length;
        assertTrue(len >= 2);
        assertTrue(len <= 6);

        ByteBuffer buf = ByteBuffer.allocate(20);
        PbUtil.writeUnsignedInt32(buf, 1, v);
        assertEquals(len, buf.position());
        byte[] actual = new byte[len];
        System.arraycopy(buf.array(), 0, actual, 0, len);

        assertArrayEquals(bs, actual);
    }

    @Test
    public void testWriteUnsignedInt64() {
        PbUtil.writeUnsignedInt64(null, 1, 0);
        testWriteUnsignedInt64(1);
        testWriteUnsignedInt64(-1);
        testWriteUnsignedInt64(10000);
        testWriteUnsignedInt64(-10000);
        testWriteUnsignedInt64(Long.MAX_VALUE);
        testWriteUnsignedInt64(Long.MIN_VALUE);
        testWriteUnsignedInt64(1L << 7);
        testWriteUnsignedInt64((1L << 7) + 1);
        testWriteUnsignedInt64((1L << 7) - 1);
        testWriteUnsignedInt64((1L << 7) - 2);
        testWriteUnsignedInt64(1L << 14);
        testWriteUnsignedInt64((1L << 14) + 1);
        testWriteUnsignedInt64((1L << 14) - 1);
        testWriteUnsignedInt64((1L << 14) - 2);
        testWriteUnsignedInt64(1L << 21);
        testWriteUnsignedInt64((1L << 21) + 1);
        testWriteUnsignedInt64((1L << 21) - 1);
        testWriteUnsignedInt64((1L << 21) - 2);
        testWriteUnsignedInt64(1L << 28);
        testWriteUnsignedInt64((1L << 28) + 1);
        testWriteUnsignedInt64((1L << 28) - 1);
        testWriteUnsignedInt64((1L << 28) - 2);
        testWriteUnsignedInt64(1L << 35);
        testWriteUnsignedInt64((1L << 35) + 1);
        testWriteUnsignedInt64((1L << 35) - 1);
        testWriteUnsignedInt64((1L << 35) - 2);
        testWriteUnsignedInt64(1L << 42);
        testWriteUnsignedInt64((1L << 42) + 1);
        testWriteUnsignedInt64((1L << 42) - 1);
        testWriteUnsignedInt64((1L << 42) - 2);
        testWriteUnsignedInt64(1L << 49);
        testWriteUnsignedInt64((1L << 49) + 1);
        testWriteUnsignedInt64((1L << 49) - 1);
        testWriteUnsignedInt64((1L << 49) - 2);
        testWriteUnsignedInt64(1L << 56);
        testWriteUnsignedInt64((1L << 56) + 1);
        testWriteUnsignedInt64((1L << 56) - 1);
        testWriteUnsignedInt64((1L << 56) - 2);
        testWriteUnsignedInt64(1L << 63);
        testWriteUnsignedInt64((1L << 63) + 1);
        testWriteUnsignedInt64((1L << 63) - 1);
        testWriteUnsignedInt64((1L << 63) - 2);
    }

    private void testWriteUnsignedInt64(long v) {
        DtPbTest.TestInt pb = DtPbTest.TestInt.newBuilder().setFUint64(v).build();
        byte[] bs = pb.toByteArray();
        int len = bs.length;
        assertTrue(len >= 2);
        assertTrue(len <= 11);

        ByteBuffer buf = ByteBuffer.allocate(20);
        PbUtil.writeUnsignedInt64(buf, 2, v);
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
        PbUtil.writeUnsignedInt32(buf, 1, v);
        buf.flip();
        buf.position(1);
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
        ByteBuffer buf = ByteBuffer.allocate(11);
        PbUtil.writeUnsignedInt64(buf, 1, v);
        buf.flip();
        buf.position(1);
        long v2 = PbUtil.readUnsignedInt64(buf);
        assertEquals(v, v2);
    }

    @Test
    public void testAccurateTagSize() {
        assertThrows(IllegalArgumentException.class, () -> accurateTagSize(0));
        assertThrows(IllegalArgumentException.class, () -> accurateTagSize(-1));
        assertThrows(IllegalArgumentException.class, () -> accurateTagSize(536870911 + 1));
        testAccurateTagSize(1);

        testAccurateTagSize(1 << 4);
        testAccurateTagSize((1 << 4) + 1);
        testAccurateTagSize((1 << 4) - 1);
        testAccurateTagSize((1 << 4) - 2);

        testAccurateTagSize(1 << 11);
        testAccurateTagSize((1 << 11) + 1);
        testAccurateTagSize((1 << 11) - 1);
        testAccurateTagSize((1 << 11) - 2);

        testAccurateTagSize(1 << 18);
        testAccurateTagSize((1 << 18) + 1);
        testAccurateTagSize((1 << 18) - 1);
        testAccurateTagSize((1 << 18) - 2);

        testAccurateTagSize(1 << 25);
        testAccurateTagSize((1 << 25) + 1);
        testAccurateTagSize((1 << 25) - 1);
        testAccurateTagSize((1 << 25) - 2);

        assertThrows(IllegalArgumentException.class, () -> testAccurateTagSize(1 << 29));
        testAccurateTagSize((1 << 29) - 1);
        testAccurateTagSize((1 << 29) - 2);
    }

    private void testAccurateTagSize(int index) {
        ByteBuffer buf = ByteBuffer.allocate(5);
        writeTag(buf, TYPE_VAR_INT, index);
        assertEquals(buf.position(), accurateTagSize(index));
    }

    @Test
    public void testAccurateUnsignedIntSize() {
        assertEquals(1, accurateUnsignedIntSize(0));
        testAccurateUnsignedIntSize(-1);
        testAccurateUnsignedIntSize(1);
        testAccurateUnsignedIntSize(Integer.MAX_VALUE);
        testAccurateUnsignedIntSize(Integer.MAX_VALUE - 1);
        testAccurateUnsignedIntSize(Integer.MAX_VALUE + 1);
        testAccurateUnsignedIntSize(Integer.MIN_VALUE);
        testAccurateUnsignedIntSize(Integer.MIN_VALUE - 1);
        testAccurateUnsignedIntSize(Integer.MIN_VALUE + 1);

        testAccurateUnsignedIntSize(1 << 7);
        testAccurateUnsignedIntSize((1 << 7) + 1);
        testAccurateUnsignedIntSize((1 << 7) - 1);
        testAccurateUnsignedIntSize((1 << 7) - 2);

        testAccurateUnsignedIntSize(1 << 14);
        testAccurateUnsignedIntSize((1 << 14) + 1);
        testAccurateUnsignedIntSize((1 << 14) - 1);
        testAccurateUnsignedIntSize((1 << 14) - 2);

        testAccurateUnsignedIntSize(1 << 21);
        testAccurateUnsignedIntSize((1 << 21) + 1);
        testAccurateUnsignedIntSize((1 << 21) - 1);
        testAccurateUnsignedIntSize((1 << 21) - 2);

        testAccurateUnsignedIntSize(1 << 28);
        testAccurateUnsignedIntSize((1 << 28) + 1);
        testAccurateUnsignedIntSize((1 << 28) - 1);
        testAccurateUnsignedIntSize((1 << 28) - 2);
    }

    private void testAccurateUnsignedIntSize(int value) {
        ByteBuffer buf = ByteBuffer.allocate(5);
        writeUnsignedInt32ValueOnly(buf, value);
        assertEquals(buf.position(), accurateUnsignedIntSize(value));
    }

    @Test
    public void testAccurateUnsignedIntSizeWithTag() {
        assertEquals(0, accurateUnsignedIntSize(100, 0));
        assertEquals(maxUnsignedIntSize(), accurateUnsignedIntSize(536870911, Integer.MAX_VALUE));
        assertEquals(maxUnsignedIntSize(), accurateUnsignedIntSize(536870911, Integer.MIN_VALUE));
        assertEquals(maxUnsignedIntSize(), accurateUnsignedIntSize(536870911, -1));
    }

    @Test
    public void testAccurateUnsignedLongSize() {
        assertEquals(0L, accurateUnsignedLongSize(0L));
        testAccurateUnsignedLongSize(-1L);
        testAccurateUnsignedLongSize(1L);
        testAccurateUnsignedLongSize(Long.MAX_VALUE);
        testAccurateUnsignedLongSize(Long.MAX_VALUE - 1);
        testAccurateUnsignedLongSize(Long.MAX_VALUE + 1);
        testAccurateUnsignedLongSize(Long.MIN_VALUE);
        testAccurateUnsignedLongSize(Long.MIN_VALUE - 1);
        testAccurateUnsignedLongSize(Long.MIN_VALUE + 1);

        testAccurateUnsignedLongSize(1L << 7);
        testAccurateUnsignedLongSize((1L << 7) + 1);
        testAccurateUnsignedLongSize((1L << 7) - 1);
        testAccurateUnsignedLongSize((1L << 7) - 2);

        testAccurateUnsignedLongSize(1L << 14);
        testAccurateUnsignedLongSize((1L << 14) + 1);
        testAccurateUnsignedLongSize((1L << 14) - 1);
        testAccurateUnsignedLongSize((1L << 14) - 2);

        testAccurateUnsignedLongSize(1L << 21);
        testAccurateUnsignedLongSize((1L << 21) + 1);
        testAccurateUnsignedLongSize((1L << 21) - 1);
        testAccurateUnsignedLongSize((1L << 21) - 2);

        testAccurateUnsignedLongSize(1L << 28);
        testAccurateUnsignedLongSize((1L << 28) + 1);
        testAccurateUnsignedLongSize((1L << 28) - 1);
        testAccurateUnsignedLongSize((1L << 28) - 2);

        testAccurateUnsignedLongSize(1L << 35);
        testAccurateUnsignedLongSize((1L << 35) + 1);
        testAccurateUnsignedLongSize((1L << 35) - 1);
        testAccurateUnsignedLongSize((1L << 35) - 2);

        testAccurateUnsignedLongSize(1L << 42);
        testAccurateUnsignedLongSize((1L << 42) + 1);
        testAccurateUnsignedLongSize((1L << 42) - 1);
        testAccurateUnsignedLongSize((1L << 42) - 2);

        testAccurateUnsignedLongSize(1L << 49);
        testAccurateUnsignedLongSize((1L << 49) + 1);
        testAccurateUnsignedLongSize((1L << 49) - 1);
        testAccurateUnsignedLongSize((1L << 49) - 2);

        testAccurateUnsignedLongSize(1L << 56);
        testAccurateUnsignedLongSize((1L << 56) + 1);
        testAccurateUnsignedLongSize((1L << 56) - 1);
        testAccurateUnsignedLongSize((1L << 56) - 2);
    }

    private void testAccurateUnsignedLongSize(long value) {
        ByteBuffer buf = ByteBuffer.allocate(10);
        writeUnsignedInt64ValueOnly(buf, value);
        assertEquals(buf.position(), accurateUnsignedLongSize(value));
    }

    @Test
    public void testAccurateUnsignedLongSizeWithTag() {
        assertEquals(0, accurateUnsignedLongSize(100, 0L));
        assertEquals(maxUnsignedLongSize() - 1, accurateUnsignedLongSize(536870911, Long.MAX_VALUE));
        assertEquals(maxUnsignedLongSize(), accurateUnsignedLongSize(536870911, Integer.MIN_VALUE));
        assertEquals(maxUnsignedLongSize(), accurateUnsignedLongSize(536870911, -1));
    }

    @Test
    public void testWriteFixNumber() {
        assertEquals(0, accurateFix32Size(1, 0));
        assertEquals(0, accurateFix64Size(1, 0));

        ByteBuffer buf = ByteBuffer.allocate(maxFix32Size());
        writeFix32(buf, 1, 0);
        writeFix64(buf, 1, 0);
        assertEquals(0, buf.position());

        writeFix32(buf, 536870911, 123456);
        assertEquals(accurateFix32Size(536870911, 123456), buf.position());
        buf.position(5);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(123456, buf.getInt());

        buf = ByteBuffer.allocate(maxFix64Size());
        writeFix64(buf, 536870911, 123456L);
        assertEquals(accurateFix64Size(536870911, 123456L), buf.position());
        buf.position(5);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(123456L, buf.getLong());
    }

    @Test
    public void testString() {
        assertEquals(0, maxStrSizeUTF8(null));
        assertEquals(0, maxStrSizeUTF8(""));
        assertEquals(0, maxStrSizeAscii(null));
        assertEquals(0, maxStrSizeAscii(""));
        assertEquals(0, accurateStrSizeAscii(1, null));
        assertEquals(0, accurateStrSizeAscii(1, ""));

        String chineseStr = "汉字";
        ByteBuffer buf = ByteBuffer.allocate(maxStrSizeUTF8(chineseStr));
        writeUTF8(buf, 536870911, chineseStr);
        assertEquals(4, buf.remaining());

        String asciiStr = "12345678";
        buf = ByteBuffer.allocate(maxStrSizeAscii(asciiStr));
        writeAscii(buf, 536870911, asciiStr);
        assertEquals(4, buf.remaining());
        assertEquals(buf.capacity() - 4, accurateStrSizeAscii(536870911, asciiStr));
    }

    @Test
    public void testLengthDelimitedSize() {
        assertEquals(0, accurateLengthDelimitedSize(1, 0));
        assertEquals(accurateLengthDelimitedSize(536870911, Integer.MAX_VALUE), maxLengthDelimitedSize(Integer.MAX_VALUE));
        assertTrue(accurateLengthDelimitedSize(10000, Integer.MAX_VALUE) < maxLengthDelimitedSize(Integer.MAX_VALUE));
        assertTrue(accurateLengthDelimitedSize(536870911, 10000) < maxLengthDelimitedSize(10000));
    }

    @Test
    public void testWriteUnsignedInt32ValueOnly(){
        testWriteUnsignedInt32ValueOnly(0);
        testWriteUnsignedInt32ValueOnly(1);
        testWriteUnsignedInt32ValueOnly(-1);
        testWriteUnsignedInt32ValueOnly(Integer.MAX_VALUE);
        testWriteUnsignedInt32ValueOnly(Integer.MIN_VALUE);
    }

    private void testWriteUnsignedInt32ValueOnly(int v) {
        ByteBuffer buf = ByteBuffer.allocate(10);

        PbUtil.writeUnsignedInt32ValueOnly(buf, v);
        buf.flip();
        assertEquals(v, PbUtil.readUnsignedInt32(buf));
    }

    @Test
    public void testWriteUnsignedInt64ValueOnly() {
        // testWriteUnsignedInt64ValueOnly(0);
        testWriteUnsignedInt64ValueOnly(1);
        testWriteUnsignedInt64ValueOnly(-1);
        testWriteUnsignedInt64ValueOnly(Long.MAX_VALUE);
        testWriteUnsignedInt64ValueOnly(Long.MIN_VALUE);
    }

    private void testWriteUnsignedInt64ValueOnly(long v) {
        ByteBuffer buf = ByteBuffer.allocate(10);

        PbUtil.writeUnsignedInt64ValueOnly(buf, v);
        buf.flip();
        assertEquals(v, PbUtil.readUnsignedInt64(buf));
    }
}
