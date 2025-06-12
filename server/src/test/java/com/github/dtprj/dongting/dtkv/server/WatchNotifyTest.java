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
import com.github.dtprj.dongting.util.CodecTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;

/**
 * @author huangli
 */
class WatchNotifyTest {

    public static WatchNotify buildTestData() {
        Random r = new Random();
        return new WatchNotify(r.nextLong(), r.nextInt(), "key".getBytes(), "value".getBytes());
    }

    @Test
    public void testFullBuffer() throws Exception {
        WatchNotify data = buildTestData();
        ByteBuffer buf = CodecTestUtil.fullBufferEncode(data);
        DtKv.WatchNotify protoResult = DtKv.WatchNotify.parseFrom(buf);
        compare1(data, protoResult);

        WatchNotify.Callback callback = new WatchNotify.Callback();
        WatchNotify r = CodecTestUtil.fullBufferDecode(buf, callback);
        compare2(data, r);
    }

    @Test
    public void testSmallBuffer() {
        WatchNotify data = buildTestData();
        WatchNotify r = (WatchNotify) CodecTestUtil.smallBufferEncodeAndParse(data, new WatchNotify.Callback());
        compare2(data, r);
    }

    static void compare1(WatchNotify expect, DtKv.WatchNotify protoResult) {
        Assertions.assertEquals(expect.raftIndex, protoResult.getRaftIndex());
        Assertions.assertEquals(expect.state, protoResult.getState());
        Assertions.assertArrayEquals(expect.key, protoResult.getKey().getBytes(StandardCharsets.UTF_8));
        Assertions.assertArrayEquals(expect.value, protoResult.getValue().toByteArray());
    }

    static void compare2(WatchNotify expect, WatchNotify parseResult) {
        Assertions.assertEquals(expect.raftIndex, parseResult.raftIndex);
        Assertions.assertEquals(expect.state, parseResult.state);
        Assertions.assertArrayEquals(expect.key, parseResult.key);
        Assertions.assertArrayEquals(expect.value, parseResult.value);
    }
}
