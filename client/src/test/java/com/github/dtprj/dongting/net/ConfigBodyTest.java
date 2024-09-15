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
package com.github.dtprj.dongting.net;

import com.github.dtprj.dongting.codec.CodecTestUtil;
import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.DtPacket;
import com.github.dtprj.dongting.codec.PbParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class ConfigBodyTest {

    @Test
    public void testDecode() {
        DtPacket.Config c = DtPacket.Config.newBuilder()
                .setMaxPacketSize(100)
                .setMaxBodySize(200)
                .setMaxInPending(300)
                .setMaxInPendingBytes(Integer.MAX_VALUE + 400L)
                .setMaxOutPending(500)
                .setMaxOutPendingBytes(Integer.MAX_VALUE + 600L)
                .build();
        byte[] bs = c.toByteArray();
        ConfigBody.Callback callback = new ConfigBody.Callback();
        PbParser parser = new PbParser();
        DecodeContext context = CodecTestUtil.createContext();
        parser.prepareNext(context, callback, bs.length);
        parser.parse(ByteBuffer.wrap(bs));
        ConfigBody result = callback.getResult();
        Assertions.assertEquals(c.getMaxPacketSize(), result.maxPacketSize);
        Assertions.assertEquals(c.getMaxBodySize(), result.maxBodySize);
        Assertions.assertEquals(c.getMaxInPending(), result.maxInPending);
        Assertions.assertEquals(c.getMaxInPendingBytes(), result.maxInPendingBytes);
        Assertions.assertEquals(c.getMaxOutPending(), result.maxOutPending);
        Assertions.assertEquals(c.getMaxOutPendingBytes(), result.maxOutPendingBytes);
    }

    @Test
    public void testEncode() throws Exception {
        ConfigBody c = new ConfigBody();
        c.maxPacketSize = 100;
        c.maxBodySize = 200;
        c.maxInPending = 300;
        c.maxInPendingBytes = Integer.MAX_VALUE + 400;
        c.maxOutPending = 500;
        c.maxOutPendingBytes = Integer.MAX_VALUE + 600;

        ByteBuffer buf = ByteBuffer.allocate(128);
        c.encodeBody(buf);
        buf.flip();

        Assertions.assertEquals(c.calcActualBodySize(), buf.remaining());

        DtPacket.Config result = DtPacket.Config.parseFrom(buf);

        Assertions.assertEquals(c.maxPacketSize, result.getMaxPacketSize());
        Assertions.assertEquals(c.maxBodySize, result.getMaxBodySize());
        Assertions.assertEquals(c.maxInPending, result.getMaxInPending());
        Assertions.assertEquals(c.maxInPendingBytes, result.getMaxInPendingBytes());
        Assertions.assertEquals(c.maxOutPending, result.getMaxOutPending());
        Assertions.assertEquals(c.maxOutPendingBytes, result.getMaxOutPendingBytes());
    }
}
