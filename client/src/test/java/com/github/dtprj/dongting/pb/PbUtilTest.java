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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
        PbUtil.writeTag(buf, PbUtil.TYPE_FIX32, index);
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
        testWriteUnsignedInt32((1 << 7) - 1);
        testWriteUnsignedInt32(1 << 14);
        testWriteUnsignedInt32((1 << 14) - 1);
        testWriteUnsignedInt32(1 << 21);
        testWriteUnsignedInt32((1 << 21) - 1);
        testWriteUnsignedInt32(1 << 28);
        testWriteUnsignedInt32((1 << 28) - 1);
    }

    private void testWriteUnsignedInt32(int v) {
        DtPbTest.TestInt pb = DtPbTest.TestInt.newBuilder().setFUint32(v).build();
        byte[] bs = pb.toByteArray();
        int len = bs.length;
        assertTrue(len >= 2);
        assertTrue(len <= 6);

        ByteBuffer buf = ByteBuffer.allocate(20);
        PbUtil.writeUnsignedInt32(buf,1, v);
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
        testWriteUnsignedInt64((1L << 7) - 1);
        testWriteUnsignedInt64(1L << 14);
        testWriteUnsignedInt64((1L << 14) - 1);
        testWriteUnsignedInt64(1L << 21);
        testWriteUnsignedInt64((1L << 21) - 1);
        testWriteUnsignedInt64(1L << 28);
        testWriteUnsignedInt64((1L << 28) - 1);
        testWriteUnsignedInt64(1L << 35);
        testWriteUnsignedInt64((1L << 35) - 1);
        testWriteUnsignedInt64(1L << 42);
        testWriteUnsignedInt64((1L << 42) - 1);
        testWriteUnsignedInt64(1L << 49);
        testWriteUnsignedInt64((1L << 49) - 1);
        testWriteUnsignedInt64(1L << 56);
        testWriteUnsignedInt64((1L << 56) - 1);
        testWriteUnsignedInt64(1L << 63);
        testWriteUnsignedInt64((1L << 63) - 1);
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
        testReadUnsignedInt32((1 << 7) - 1);
        testReadUnsignedInt32(1 << 14);
        testReadUnsignedInt32((1 << 14) - 1);
        testReadUnsignedInt32(1 << 21);
        testReadUnsignedInt32((1 << 21) - 1);
        testReadUnsignedInt32(1 << 28);
        testReadUnsignedInt32((1 << 28) - 1);

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
        testReadUnsignedInt64((1L << 7) - 1);
        testReadUnsignedInt64(1L << 14);
        testReadUnsignedInt64((1L << 14) - 1);
        testReadUnsignedInt64(1L << 21);
        testReadUnsignedInt64((1L << 21) - 1);
        testReadUnsignedInt64(1L << 28);
        testReadUnsignedInt64((1L << 28) - 1);
        testReadUnsignedInt64(1L << 35);
        testReadUnsignedInt64((1L << 35) - 1);
        testReadUnsignedInt64(1L << 42);
        testReadUnsignedInt64((1L << 42) - 1);
        testReadUnsignedInt64(1L << 49);
        testReadUnsignedInt64((1L << 49) - 1);
        testReadUnsignedInt64(1L << 56);
        testReadUnsignedInt64((1L << 56) - 1);
        testReadUnsignedInt64(1L << 63);
        testReadUnsignedInt64((1L << 63) - 1);

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

}
