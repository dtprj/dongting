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
import com.github.dtprj.dongting.dtkv.KvResp;
import com.github.dtprj.dongting.util.CodecTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author huangli
 */
public class KvRespTest {

    private KvResp buildResp() {
        return new KvResp(Arrays.asList(KvResultTest.buildResult(), KvResultTest.buildResult()));
    }

    @Test
    public void testFullBuffer() throws Exception {
        KvResp resp = buildResp();
        ByteBuffer buf = ByteBuffer.allocate(256);
        EncodeContext encodeContext = CodecTestUtil.encodeContext();
        Assertions.assertTrue(resp.encode(encodeContext, buf));
        buf.flip();
        DtKv.KvResp protoResp = DtKv.KvResp.parseFrom(buf);
        compare1(resp, protoResp);

        buf.position(0);
        PbParser p = new PbParser();
        KvResp.Callback callback = new KvResp.Callback();
        p.prepareNext(CodecTestUtil.decodeContext(), callback, buf.limit());
        KvResp r = (KvResp) p.parse(buf);
        compare2(resp, r);
    }

    @Test
    public void testSmallBuffer() {
        KvResp resp = buildResp();
        ByteBuffer smallBuf = ByteBuffer.allocate(1);
        ByteBuffer bigBuf = ByteBuffer.allocate(256);
        EncodeContext encodeContext = CodecTestUtil.encodeContext();

        PbParser p = new PbParser();
        KvResp.Callback callback = new KvResp.Callback();
        p.prepareNext(CodecTestUtil.decodeContext(), callback, resp.actualSize());

        KvResp r = (KvResp) KvReqTest.encodeAndParse(smallBuf, bigBuf, resp, encodeContext, p);
        compare2(resp, r);
    }

    private void compare1(KvResp expect, DtKv.KvResp resp) {
        for (int i = 0; i < expect.getResults().size(); i++) {
            KvResultTest.compare1(expect.getResults().get(i), resp.getResults(i));
        }
    }

    private void compare2(KvResp expect, KvResp r) {
        for (int i = 0; i < expect.getResults().size(); i++) {
            KvResultTest.compare2(expect.getResults().get(i), r.getResults().get(i));
        }
    }
}
