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
import com.github.dtprj.dongting.dtkv.WatchNotifyResp;
import com.github.dtprj.dongting.util.CodecTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
class WatchNotifyRespTest {
    private static WatchNotifyResp buildTestData() {
        return new WatchNotifyResp(new int[]{0, 1, 2});
    }

    @Test
    public void testFullBuffer() throws Exception {
        WatchNotifyResp data = buildTestData();
        ByteBuffer buf = CodecTestUtil.fullBufferEncode(data);
        DtKv.WatchNotifyResp protoResult = DtKv.WatchNotifyResp.parseFrom(buf);
        compare1(data, protoResult);

        WatchNotifyRespCallback callback = new WatchNotifyRespCallback(data.results.length);
        WatchNotifyRespCallback r = CodecTestUtil.fullBufferDecode(buf, callback);
        compare2(data, r);
    }

    @Test
    public void testSmallBuffer() {
        WatchNotifyResp data = buildTestData();
        WatchNotifyRespCallback r = (WatchNotifyRespCallback) CodecTestUtil.smallBufferEncodeAndParse(
                data, new WatchNotifyRespCallback(data.results.length));
        compare2(data, r);
    }

    private void compare1(WatchNotifyResp data, DtKv.WatchNotifyResp protoResult) {
        for (int i = 0; i < data.results.length; i++) {
            Assertions.assertEquals(data.results[i], protoResult.getResultsList().get(i).intValue());
        }
    }


    public static void compare2(WatchNotifyResp expect, WatchNotifyRespCallback actual) {
        Assertions.assertArrayEquals(expect.results, actual.results);
    }
}
