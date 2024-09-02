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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class PbNoCopyDecoderCallbackTest {

    @Test
    public void test() {
        DecodeContext c = CodecTestUtil.createContext();
        Decoder decoder = new Decoder();
        decoder.prepareNext(c, c.getOrCreatePbNoCopyDecoderCallback(new PbNoCopyDecoderCallback.IntCallback()));

        ByteBuffer buf = ByteBuffer.allocate(30);
        PbUtil.writeFix32(buf, 1, 2000);
        buf.flip();

        Object r = decoder.decode(buf, buf.remaining(), 0);
        Assertions.assertEquals(2000, r);

        buf.clear();
        PbUtil.writeFix32(buf, 1, 0);
        buf.flip();

        decoder.prepareNext(c, c.getOrCreatePbNoCopyDecoderCallback(new PbNoCopyDecoderCallback.IntCallback()));
        r = decoder.decode(buf, buf.remaining(), 0);
        Assertions.assertEquals(0, r);
    }
}
