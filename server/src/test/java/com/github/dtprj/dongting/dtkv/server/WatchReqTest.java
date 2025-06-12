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
import com.github.dtprj.dongting.dtkv.WatchReq;
import com.github.dtprj.dongting.util.CodecTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * @author huangli
 */
class WatchReqTest {
    public static WatchReq buildTestData() {
        return new WatchReq(100, true,
                Arrays.asList("key1".getBytes(), "key2".getBytes()), new long[]{10000, 20000});
    }

    @Test
    public void testFullBuffer() throws Exception {
        WatchReq data = buildTestData();
        ByteBuffer buf = CodecTestUtil.fullBufferEncode(data);
        DtKv.WatchReq protoResult = DtKv.WatchReq.parseFrom(buf);
        compare1(data, protoResult);

        WatchReqCallback callback = new WatchReqCallback();
        WatchReqCallback r = CodecTestUtil.fullBufferDecode(buf, callback);
        compare2(data, r);
    }

    @Test
    public void testSmallBuffer() {
        WatchReq data = buildTestData();
        WatchReqCallback r = (WatchReqCallback) CodecTestUtil.smallBufferEncodeAndParse(data, new WatchReqCallback());
        compare2(data, r);
    }

    static void compare1(WatchReq expect, DtKv.WatchReq protoResult) {
        Assertions.assertEquals(expect.groupId, protoResult.getGroupId());
        Assertions.assertEquals(expect.syncAll, protoResult.getSyncAll());
        for (int i = 0; i < expect.keys.size(); i++) {
            Assertions.assertArrayEquals(expect.keys.get(i), protoResult.getKeys(i).getBytes(StandardCharsets.UTF_8));
            Assertions.assertEquals(expect.knownRaftIndexes[i], protoResult.getKnownRaftIndex(i));
        }
    }

    static void compare2(WatchReq expect, WatchReqCallback parseResult) {
        Assertions.assertEquals(expect.groupId, parseResult.groupId);
        Assertions.assertEquals(expect.syncAll, parseResult.syncAll);
        for (int i = 0; i < expect.keys.size(); i++) {
            Assertions.assertArrayEquals(expect.keys.get(i), parseResult.keys[i].getData());
            Assertions.assertEquals(expect.knownRaftIndexes[i], parseResult.knownRaftIndexes[i]);
        }
    }


}
