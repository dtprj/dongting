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

import com.github.dtprj.dongting.config.DtKv;
import com.github.dtprj.dongting.dtkv.KvResp;
import com.github.dtprj.dongting.util.CodecTestUtil;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author huangli
 */
public class KvRespTest {

    @Test
    public void testFullBuffer() throws Exception {
        KvResp resp = new KvResp(100, Arrays.asList(KvResultTest.buildResult(), KvResultTest.buildResult()));
        ByteBuffer buf = CodecTestUtil.fullBufferEncode(resp);
        DtKv.KvResp protoResp = DtKv.KvResp.parseFrom(buf);
        compare1(resp, protoResp);

        KvResp.Callback callback = new KvResp.Callback();
        KvResp r = CodecTestUtil.fullBufferDecode(buf, callback);
        compare2(resp, r);
    }

    @Test
    public void testSmallBuffer() {
        KvResp resp = new KvResp(100, Arrays.asList(KvResultTest.buildResult(), KvResultTest.buildResult()));
        KvResp r = (KvResp) CodecTestUtil.smallBufferEncodeAndParse(resp, new KvResp.Callback());
        compare2(resp, r);
    }

    private void compare1(KvResp expect, DtKv.KvResp resp) {
        assertEquals(expect.raftIndex, resp.getRaftIndex());
        if (expect.results != null) {
            for (int i = 0; i < expect.results.size(); i++) {
                KvResultTest.compare1(expect.results.get(i), resp.getResults(i));
            }
        }
    }

    private void compare2(KvResp expect, KvResp r) {
        assertEquals(expect.raftIndex, r.raftIndex);
        if (expect.results != null) {
            for (int i = 0; i < expect.results.size(); i++) {
                KvResultTest.compare2(expect.results.get(i), r.results.get(i));
            }
        }
    }
}
