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
public class HandshakeBodyTest {
    @Test
    public void testDecode() {
        DtPacket.Handshake h = DtPacket.Handshake.newBuilder()
                .setMagic1(HandshakeBody.MAGIC1)
                .setMagic2(HandshakeBody.MAGIC2)
                .setMajorVersion(1)
                .setMinorVersion(2)
                .setConfig(DtPacket.Config.newBuilder()
                        .setMaxPacketSize(100)
                        .build())
                .build();
        byte[] bs = h.toByteArray();
        HandshakeBody.Callback callback = new HandshakeBody.Callback();
        PbParser parser = new PbParser();
        DecodeContext context = CodecTestUtil.createContext();
        parser.prepareNext(context, callback, bs.length);
        parser.parse(ByteBuffer.wrap(bs));

        HandshakeBody result = callback.getResult();
        Assertions.assertEquals(h.getMagic1(), result.MAGIC1);
        Assertions.assertEquals(h.getMagic2(), result.MAGIC2);
        Assertions.assertEquals(h.getMajorVersion(), result.majorVersion);
        Assertions.assertEquals(h.getMinorVersion(), result.minorVersion);
        Assertions.assertEquals(h.getConfig().getMaxPacketSize(), result.config.maxPacketSize);
    }

    @Test
    public void testEncode() throws Exception {
        HandshakeBody h = new HandshakeBody();
        h.majorVersion = 1;
        h.minorVersion = 2;
        h.config = new ConfigBody();
        h.config.maxPacketSize = 100;

        HandshakeBody.WritePacket p = new HandshakeBody.WritePacket(h);
        ByteBuffer buf = ByteBuffer.allocate(128);
        p.encodeBody(buf);
        buf.flip();
        Assertions.assertEquals(buf.remaining(), p.calcActualBodySize());

        DtPacket.Handshake result = DtPacket.Handshake.parseFrom(buf);
        Assertions.assertEquals(h.MAGIC1, result.getMagic1());
        Assertions.assertEquals(h.MAGIC2, result.getMagic2());
        Assertions.assertEquals(h.majorVersion, result.getMajorVersion());
        Assertions.assertEquals(h.minorVersion, result.getMinorVersion());
        Assertions.assertEquals(h.config.maxPacketSize, result.getConfig().getMaxPacketSize());
    }
}
