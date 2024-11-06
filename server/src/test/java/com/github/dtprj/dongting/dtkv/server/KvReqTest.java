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
import com.github.dtprj.dongting.codec.StrEncoder;
import com.github.dtprj.dongting.common.ByteArray;
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

    static Object encodeAndParse(ByteBuffer smallBuf, ByteBuffer bigBuf, Encodable e, EncodeContext c, PbParser p) {
        ByteBuffer buf = smallBuf;
        while (!e.encode(c, buf)) {
            if (buf.position() == 0) {
                buf = bigBuf;
                if (e.encode(c, buf)) {
                    break;
                }
            }
            buf.flip();
            Assertions.assertNull(p.parse(buf));
            buf.clear();
            buf = smallBuf;
        }
        buf.flip();
        Object o = p.parse(buf);
        Assertions.assertNotNull(o);
        return o;
    }

    private KvReq buildReq() {
        ArrayList<byte[]> keys = new ArrayList<>();
        ArrayList<StrEncoder> values = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            keys.add(("test_key" + i).getBytes());
            values.add(new StrEncoder("test_value" + i));
        }
        return new KvReq(1, "test_key".getBytes(), new StrEncoder("test_value"),
                keys, values, new StrEncoder("test_expect_value"));
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
        ByteBuffer smallBuf = ByteBuffer.allocate(1);
        ByteBuffer bigBuf = ByteBuffer.allocate(256);
        EncodeContext encodeContext = CodecTestUtil.encodeContext();

        PbParser p = new PbParser();
        KvReqCallback callback = new KvReqCallback();
        p.prepareNext(CodecTestUtil.decodeContext(), callback, req.actualSize());

        KvReq r = (KvReq) KvReqTest.encodeAndParse(smallBuf, bigBuf, req, encodeContext, p);
        compare2(req, r);
    }

    private void compare1(KvReq expect, DtKv.KvReq req) {
        Assertions.assertEquals(expect.getGroupId(), req.getGroupId());
        Assertions.assertEquals(new String(expect.getKey()), req.getKey());
        Assertions.assertEquals(((StrEncoder) expect.getValue()).getStr(), req.getValue().toStringUtf8());
        Assertions.assertEquals(((StrEncoder) expect.getExpectValue()).getStr(), req.getExpectValue().toStringUtf8());
        for (int i = 0; i < expect.getKeys().size(); i++) {
            Assertions.assertEquals(new String(expect.getKeys().get(i)), req.getKeys(i));
            StrEncoder s = (StrEncoder) expect.getValues().get(i);
            Assertions.assertEquals(s.getStr(), req.getValues(i).toStringUtf8());
        }
    }

    private void compare2(KvReq expect, KvReq r) {
        Assertions.assertEquals(expect.getGroupId(), r.getGroupId());
        Assertions.assertArrayEquals(expect.getKey(), r.getKey());
        Assertions.assertArrayEquals(((StrEncoder) expect.getValue()).getStr().getBytes(), ((ByteArray) r.getValue()).getData());
        Assertions.assertArrayEquals(((StrEncoder) expect.getExpectValue()).getStr().getBytes(), ((ByteArray) r.getExpectValue()).getData());
        for (int i = 0; i < expect.getKeys().size(); i++) {
            Assertions.assertArrayEquals(expect.getKeys().get(i), r.getKeys().get(i));
            Assertions.assertArrayEquals(((StrEncoder) expect.getValues().get(i)).getStr().getBytes(), ((ByteArray) r.getValues().get(i)).getData());
        }
    }

}
