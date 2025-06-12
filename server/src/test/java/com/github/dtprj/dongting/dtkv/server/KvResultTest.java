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

import com.github.dtprj.dongting.common.ByteArray;
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
        return new KvResult(new Random().nextInt(), KvNodeTest.buildNode(), new ByteArray("keyInDir".getBytes()));
    }

    @Test
    public void testFullBuffer() throws Exception {
        KvResult result = buildResult();
        ByteBuffer buf = CodecTestUtil.fullBufferEncode(result);
        DtKv.KvResult protoResult = DtKv.KvResult.parseFrom(buf);
        compare1(result, protoResult);

        KvResult.Callback callback = new KvResult.Callback();
        KvResult r = CodecTestUtil.fullBufferDecode(buf, callback);
        compare2(result, r);
    }

    @Test
    public void testSmallBuffer() {
        KvResult result = buildResult();
        KvResult r = (KvResult) CodecTestUtil.smallBufferEncodeAndParse(result, new KvResult.Callback());
        compare2(result, r);
    }

    public static void compare1(KvResult expect, DtKv.KvResult result) {
        Assertions.assertEquals(expect.getBizCode(), result.getBizCode());
        KvNodeTest.compare1(expect.getNode(), result.getNode());
        Assertions.assertEquals(expect.getKeyInDir().toString(), result.getKeyInDir());
    }

    public static void compare2(KvResult expect, KvResult r) {
        Assertions.assertEquals(expect.getBizCode(), r.getBizCode());
        KvNodeTest.compare2(expect.getNode(), r.getNode());
        Assertions.assertEquals(expect.getKeyInDir().toString(), r.getKeyInDir().toString());
    }
}
