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

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PbUtilTest {

    @Test
    public void testToTag() {
        DtTest.TestToTag pb = DtTest.TestToTag.newBuilder().setF1(1).build();
        compare(pb, 1);
        pb = DtTest.TestToTag.newBuilder().setF2(1).build();
        compare(pb, 2);
        pb = DtTest.TestToTag.newBuilder().setF15(1).build();
        compare(pb, 15);
        pb = DtTest.TestToTag.newBuilder().setF16(1).build();
        compare(pb, 16);
        pb = DtTest.TestToTag.newBuilder().setF2047(1).build();
        compare(pb, 2047);
        pb = DtTest.TestToTag.newBuilder().setF2048(1).build();
        compare(pb, 2048);
        pb = DtTest.TestToTag.newBuilder().setF262143(1).build();
        compare(pb, 262143);
        pb = DtTest.TestToTag.newBuilder().setF262144(1).build();
        compare(pb, 262144);
        pb = DtTest.TestToTag.newBuilder().setF33554431(1).build();
        compare(pb, 33554431);
        assertThrows(IllegalArgumentException.class, () -> PbUtil.toTag(PbUtil.TYPE_FIX32, 33554432));
    }

    private void compare(DtTest.TestToTag pb, int index) {
        byte[] bs = pb.toByteArray();
        int len = bs.length - 4;
        assertTrue(len >= 1);
        assertTrue(len <= 4);
        int expect = 0;
        for (int i = 0; i < len; i++) {
            expect |= bs[i] & 0xFF;
            if (i < len - 1) {
                expect <<= 8;
            }
        }
        assertEquals(expect, PbUtil.toTag(PbUtil.TYPE_FIX32, index));
    }

    @Test
    public void testParse() {
        Frame.RpcHeader h = Frame.RpcHeader.newBuilder()
                .setFrameType(100)
                .setCommand(200)
                .setSeq(300)
                .setRespCode(400)
                .setRespMsg("5")
                .setBody(ByteString.copyFrom(new byte[]{'6'}))
                .build();
        h.toByteArray();
        PbUtil.parse(ByteBuffer.wrap(h.toByteArray()), new PbCallback() {
            @Override
            public void begin() {

            }

            @Override
            public void end() {

            }

            @Override
            public void readInt(int index, int value) {
                assertEquals(index * 100, value);
            }

            @Override
            public void readLong(int index, long value) {
                throw new AssertionError();
            }

            @Override
            public void readBytes(int index, ByteBuffer buf) {
                assertEquals(String.valueOf(index), Character.valueOf((char) buf.get()).toString());
            }
        });
    }
}
