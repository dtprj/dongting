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

import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.PbParser;
import com.github.dtprj.dongting.config.DtKv;
import com.github.dtprj.dongting.dtkv.KvReq;
import com.github.dtprj.dongting.util.CodecTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * @author huangli
 */
public class KvReqTest {

    static Object encodeAndParse(Encodable e, EncodeContext c, PbParser p) {
        ByteBuffer fullBuffer = ByteBuffer.allocate(e.actualSize());
        ByteBuffer smallBuf = ByteBuffer.allocate(1);
        ByteBuffer buf = smallBuf;
        while (!e.encode(c, buf)) {
            if (buf.position() == 0) {
                buf = ByteBuffer.allocate(buf.capacity() + 1);
                continue;
            }
            buf.flip();
            fullBuffer.put(buf);
            buf = smallBuf;
            buf.clear();
        }
        buf.flip();
        fullBuffer.put(buf);
        fullBuffer.flip();
        Object o = p.parse(fullBuffer);
        Assertions.assertNotNull(o);
        return o;
    }

    private KvReq buildReq() {
        ArrayList<byte[]> keys = new ArrayList<>();
        ArrayList<byte[]> values = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            keys.add(("test_key" + i).getBytes());
            values.add(("test_value" + i).getBytes());
        }
        return new KvReq(1, "test_key".getBytes(), "test_value".getBytes(),
                "test_expect_value".getBytes(), keys, values);
    }

    @Test
    public void testFullBuffer() throws Exception {
        KvReq req = buildReq();
        ByteBuffer buf = ByteBuffer.allocate(256);
        EncodeContext encodeContext = CodecTestUtil.encodeContext();
        Assertions.assertTrue(req.encode(encodeContext, buf));
        buf.flip();
        DtKv.KvReq protoReq = DtKv.KvReq.parseFrom(buf);
        compare1(req, protoReq);

        buf.position(0);
        PbParser p = new PbParser();
        KvReqCallback callback = new KvReqCallback();
        p.prepareNext(CodecTestUtil.decodeContext(), callback, buf.limit());
        KvReq r = (KvReq) p.parse(buf);
        compare2(req, r);
    }

    @Test
    public void testSmallBuffer() {
        KvReq req = buildReq();
        EncodeContext encodeContext = CodecTestUtil.encodeContext();

        PbParser p = new PbParser();
        KvReqCallback callback = new KvReqCallback();
        p.prepareNext(CodecTestUtil.decodeContext(), callback, req.actualSize());

        KvReq r = (KvReq) KvReqTest.encodeAndParse(req, encodeContext, p);
        compare2(req, r);
    }

    private void compare1(KvReq expect, DtKv.KvReq req) {
        Assertions.assertEquals(expect.groupId, req.getGroupId());
        Assertions.assertEquals(new String(expect.key), req.getKey());
        Assertions.assertEquals(new String(expect.value), req.getValue().toStringUtf8());
        Assertions.assertEquals(new String(expect.expectValue), req.getExpectValue().toStringUtf8());
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
        for (int i = 0; i < expect.keys.size(); i++) {
            Assertions.assertArrayEquals(expect.keys.get(i), r.keys.get(i));
            Assertions.assertArrayEquals(expect.values.get(i), r.values.get(i));
        }
    }

}
