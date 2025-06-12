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
import com.github.dtprj.dongting.dtkv.WatchNotify;
import com.github.dtprj.dongting.dtkv.WatchNotifyReq;
import com.github.dtprj.dongting.util.CodecTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author huangli
 */
class WatchNotifyReqTest {
    public static WatchNotifyReq buildTestData() {
        return new WatchNotifyReq(123456789, Arrays.asList(
                new WatchNotify(100, 0, "key1".getBytes(), "value1".getBytes()),
                new WatchNotify(200, 1, "key2".getBytes(), "value2".getBytes()),
                new WatchNotify(300, 2, "key3".getBytes(), "value3".getBytes())
        ));
    }

    @Test
    public void testFullBuffer() throws Exception {
        WatchNotifyReq data = buildTestData();
        ByteBuffer buf = CodecTestUtil.fullBufferEncode(data);
        DtKv.WatchNotifyReq protoResult = DtKv.WatchNotifyReq.parseFrom(buf);
        compare1(data, protoResult);

        WatchNotifyReq.Callback callback = new WatchNotifyReq.Callback();
        WatchNotifyReq r = CodecTestUtil.fullBufferDecode(buf, callback);
        compare2(data, r);
    }

    @Test
    public void testSmallBuffer() {
        WatchNotifyReq data = buildTestData();
        WatchNotifyReq r = (WatchNotifyReq) CodecTestUtil.smallBufferEncodeAndParse(data, new WatchNotifyReq.Callback());
        compare2(data, r);
    }


    private void compare1(WatchNotifyReq data, DtKv.WatchNotifyReq protoResult) {
        Assertions.assertEquals(data.groupId, protoResult.getGroupId());
        Assertions.assertEquals(data.notifyList.size(), protoResult.getNotifyListCount());
        for (int i = 0; i < data.notifyList.size(); i++) {
            WatchNotifyTest.compare1(data.notifyList.get(i), protoResult.getNotifyList(i));
        }
    }

    private void compare2(WatchNotifyReq data, WatchNotifyReq parseResult) {
        Assertions.assertEquals(data.groupId, parseResult.groupId);
        Assertions.assertEquals(data.notifyList.size(), parseResult.notifyList.size());
        for (int i = 0; i < data.notifyList.size(); i++) {
            WatchNotifyTest.compare2(data.notifyList.get(i), parseResult.notifyList.get(i));
        }
    }

}
