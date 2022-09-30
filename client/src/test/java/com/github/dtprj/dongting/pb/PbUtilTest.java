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
        PbUtil.writeTag(buf, PbUtil.TYPE_FIX32, index);
        assertEquals(len, buf.position());
        byte[] actual = new byte[len];
        System.arraycopy(buf.array(), 0, actual, 0, len);

        assertArrayEquals(expect, actual);
    }

    @Test
    public void testWriteUnsignedInt32() {
        assertThrows(IllegalArgumentException.class, () -> PbUtil.writeVarUnsignedInt32(null, 0));
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
        int len = bs.length - 1;
        assertTrue(len >= 1);
        assertTrue(len <= 5);
        byte[] expect = new byte[len];

        System.arraycopy(bs, 1, expect, 0, len);
        ByteBuffer buf = ByteBuffer.allocate(20);
        PbUtil.writeVarUnsignedInt32(buf, v);
        assertEquals(len, buf.position());
        byte[] actual = new byte[len];
        System.arraycopy(buf.array(), 0, actual, 0, len);

        assertArrayEquals(expect, actual);
    }

    @Test
    public void testWriteUnsignedInt64() {
        assertThrows(IllegalArgumentException.class, () -> PbUtil.writeVarUnsignedInt32(null, 0));
        testWriteUnsignedInt64(1);
        testWriteUnsignedInt64(-1);
        testWriteUnsignedInt64(10000);
        testWriteUnsignedInt64(-10000);
        testWriteUnsignedInt64(Long.MAX_VALUE);
        testWriteUnsignedInt64(Long.MIN_VALUE);
        testWriteUnsignedInt64(1 << 7);
        testWriteUnsignedInt64((1 << 7) - 1);
        testWriteUnsignedInt64(1 << 14);
        testWriteUnsignedInt64((1 << 14) - 1);
        testWriteUnsignedInt64(1 << 21);
        testWriteUnsignedInt64((1 << 21) - 1);
        testWriteUnsignedInt64(1 << 28);
        testWriteUnsignedInt64((1 << 28) - 1);
        testWriteUnsignedInt64(1 << 35);
        testWriteUnsignedInt64((1 << 35) - 1);
        testWriteUnsignedInt64(1 << 42);
        testWriteUnsignedInt64((1 << 42) - 1);
        testWriteUnsignedInt64(1 << 49);
        testWriteUnsignedInt64((1 << 49) - 1);
        testWriteUnsignedInt64(1 << 56);
        testWriteUnsignedInt64((1 << 56) - 1);
        testWriteUnsignedInt64(1 << 63);
        testWriteUnsignedInt64((1 << 63) - 1);
    }

    private void testWriteUnsignedInt64(long v) {
        DtPbTest.TestInt pb = DtPbTest.TestInt.newBuilder().setFUint64(v).build();
        byte[] bs = pb.toByteArray();
        int len = bs.length - 1;
        assertTrue(len >= 1);
        assertTrue(len <= 10);
        byte[] expect = new byte[len];

        System.arraycopy(bs, 1, expect, 0, len);
        ByteBuffer buf = ByteBuffer.allocate(20);
        PbUtil.writeVarUnsignedInt64(buf, v);
        assertEquals(len, buf.position());
        byte[] actual = new byte[len];
        System.arraycopy(buf.array(), 0, actual, 0, len);

        assertArrayEquals(expect, actual);
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
        assertEquals(0, PbUtil.readVarUnsignedInt32(buf));
    }

    private void testReadUnsignedInt32(int v) {
        ByteBuffer buf = ByteBuffer.allocate(10);
        PbUtil.writeVarUnsignedInt32(buf, v);
        buf.flip();
        int v2 = PbUtil.readVarUnsignedInt32(buf);
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
        testReadUnsignedInt64(1 << 7);
        testReadUnsignedInt64((1 << 7) - 1);
        testReadUnsignedInt64(1 << 14);
        testReadUnsignedInt64((1 << 14) - 1);
        testReadUnsignedInt64(1 << 21);
        testReadUnsignedInt64((1 << 21) - 1);
        testReadUnsignedInt64(1 << 28);
        testReadUnsignedInt64((1 << 28) - 1);
        testReadUnsignedInt64(1 << 35);
        testReadUnsignedInt64((1 << 35) - 1);
        testReadUnsignedInt64(1 << 42);
        testReadUnsignedInt64((1 << 42) - 1);
        testReadUnsignedInt64(1 << 49);
        testReadUnsignedInt64((1 << 49) - 1);
        testReadUnsignedInt64(1 << 56);
        testReadUnsignedInt64((1 << 56) - 1);
        testReadUnsignedInt64(1 << 63);
        testReadUnsignedInt64((1 << 63) - 1);

        ByteBuffer buf = ByteBuffer.allocate(10);
        buf.limit(1);
        assertEquals(0, PbUtil.readVarUnsignedInt64(buf));
    }

    private void testReadUnsignedInt64(long v) {
        ByteBuffer buf = ByteBuffer.allocate(10);
        PbUtil.writeVarUnsignedInt64(buf, v);
        buf.flip();
        long v2 = PbUtil.readVarUnsignedInt64(buf);
        assertEquals(v, v2);
    }

}
