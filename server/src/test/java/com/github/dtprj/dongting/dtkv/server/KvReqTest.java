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
import com.github.dtprj.dongting.dtkv.KvReq;
import com.github.dtprj.dongting.util.CodecTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.UUID;

/**
 * @author huangli
 */
public class KvReqTest {

    private KvReq buildReq() {
        ArrayList<byte[]> keys = new ArrayList<>();
        ArrayList<byte[]> values = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            keys.add(("test_key" + i).getBytes());
            values.add(("test_value" + i).getBytes());
        }
        KvReq req = new KvReq();
        req.groupId = 1;
        req.key = "test_key".getBytes();
        req.value = "test_value".getBytes();
        req.expectValue = "test_expect_value".getBytes();
        req.ownerUuid = UUID.randomUUID();
        req.ttlMillis = 10000;
        req.keys = keys;
        req.values = values;
        return req;
    }

    @Test
    public void testFullBuffer() throws Exception {
        KvReq req = buildReq();
        ByteBuffer buf = CodecTestUtil.fullBufferEncode(req);
        DtKv.KvReq protoReq = DtKv.KvReq.parseFrom(buf);
        compare1(req, protoReq);

        KvReqCallback callback = new KvReqCallback();
        KvReq r = CodecTestUtil.fullBufferDecode(buf, callback);
        compare2(req, r);
    }

    @Test
    public void testSmallBuffer() {
        KvReq req = buildReq();
        KvReq r = (KvReq) CodecTestUtil.smallBufferEncodeAndParse(req, new KvReqCallback());
        compare2(req, r);
    }

    private void compare1(KvReq expect, DtKv.KvReq req) {
        Assertions.assertEquals(expect.groupId, req.getGroupId());
        Assertions.assertEquals(new String(expect.key), req.getKey());
        Assertions.assertEquals(new String(expect.value), req.getValue().toStringUtf8());
        Assertions.assertEquals(new String(expect.expectValue), req.getExpectValue().toStringUtf8());
        Assertions.assertEquals(expect.ownerUuid.getMostSignificantBits(), req.getOwnerUuid1());
        Assertions.assertEquals(expect.ownerUuid.getLeastSignificantBits(), req.getOwnerUuid2());
        Assertions.assertEquals(expect.ttlMillis, req.getTtlMillis());
        for (int i = 0; i < expect.keys.size(); i++) {
            Assertions.assertEquals(new String(expect.keys.get(i)), req.getKeys(i));
            byte[] s = expect.values.get(i);
            Assertions.assertEquals(new String(s), req.getValues(i).toStringUtf8());
        }
    }

    private void compare2(KvReq expect, KvReq r) {
        Assertions.assertEquals(expect.groupId, r.groupId);
        Assertions.assertArrayEquals(expect.key, r.key);
        Assertions.assertArrayEquals(expect.value, r.value);
        Assertions.assertArrayEquals(expect.expectValue, r.expectValue);
        Assertions.assertEquals(expect.ownerUuid, r.ownerUuid);
        Assertions.assertEquals(expect.ttlMillis, r.ttlMillis);
        for (int i = 0; i < expect.keys.size(); i++) {
            Assertions.assertArrayEquals(expect.keys.get(i), r.keys.get(i));
            Assertions.assertArrayEquals(expect.values.get(i), r.values.get(i));
        }
    }

}
