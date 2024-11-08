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
package com.github.dtprj.dongting.dtkv.server;

import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.PbParser;
import com.github.dtprj.dongting.config.DtKv;
import com.github.dtprj.dongting.dtkv.KvResult;
import com.github.dtprj.dongting.util.CodecTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Random;

/**
 * @author huangli
 */
public class KvResultTest {

    public static KvResult buildResult() {
        return new KvResult(new Random().nextInt(), KvNodeTest.buildNode());
    }

    @Test
    public void testFullBuffer() throws Exception {
        KvResult result = buildResult();
        ByteBuffer buf = ByteBuffer.allocate(256);
        EncodeContext encodeContext = CodecTestUtil.encodeContext();
        Assertions.assertTrue(result.encode(encodeContext, buf));
        buf.flip();
        DtKv.KvResult protoResult = DtKv.KvResult.parseFrom(buf);
        compare1(result, protoResult);
        buf.position(0);

        PbParser p = new PbParser();
        KvResult.Callback callback = new KvResult.Callback();
        p.prepareNext(CodecTestUtil.decodeContext(), callback, buf.limit());
        KvResult r = (KvResult) p.parse(buf);
        compare2(result, r);
    }

    @Test
    public void testSmallBuffer() {
        KvResult result = buildResult();
        ByteBuffer smallBuf = ByteBuffer.allocate(1);
        ByteBuffer bigBuf = ByteBuffer.allocate(256);
        EncodeContext encodeContext = CodecTestUtil.encodeContext();

        PbParser p = new PbParser();
        KvResult.Callback callback = new KvResult.Callback();
        p.prepareNext(CodecTestUtil.decodeContext(), callback, result.actualSize());

        KvResult r = (KvResult) KvReqTest.encodeAndParse(smallBuf, bigBuf, result, encodeContext, p);
        compare2(result, r);
    }

    public static void compare1(KvResult expect, DtKv.KvResult result) {
        Assertions.assertEquals(expect.getBizCode(), result.getBizCode());
        KvNodeTest.compare1(expect.getNode(), result.getNode());
    }

    public static void compare2(KvResult expect, KvResult r) {
        Assertions.assertEquals(expect.getBizCode(), r.getBizCode());
        KvNodeTest.compare2(expect.getNode(), r.getNode());
    }
}
