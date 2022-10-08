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

/**
 * @author huangli
 */
public class PbParserTest {
    @Test
    public void testParse() {
        DtFrame.Frame h = DtFrame.Frame.newBuilder()
                .setFrameType(100)
                .setCommand(200)
                .setSeq(300)
                .setRespCode(400)
                .setRespMsg("msg")
                .setBody(ByteString.copyFrom("body".getBytes()))
                .build();
        h.toByteArray();
        //TODO finish test
//        new PbParser(50000).parse(ByteBuffer.wrap(h.toByteArray()), new PbCallback() {
//            @Override
//            public void readVarInt(int index, long value) {
//                assertEquals(index * 100, value);
//            }
//
//            @Override
//            public void readBytes(int index, ByteBuffer buf) {
//                byte[] bs = new byte[buf.remaining()];
//                buf.get(bs);
//                String s = new String(bs);
//                if (index == 5) {
//                    assertEquals("msg", s);
//                } else {
//                    assertEquals("body", s);
//                }
//            }
//        });
    }
}
