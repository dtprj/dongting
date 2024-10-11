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

import com.github.dtprj.dongting.codec.ByteArrayEncoder;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.PbParser;
import com.github.dtprj.dongting.codec.StrEncoder;
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

    private final String KEY = "test_key";
    private final String VALUE = "test_value";
    private final String EXPECT_VALUE = "test_expect_value";
    private final int LOOP = 3;

    private KvReq buildReq() {
        ArrayList<byte[]> keys = new ArrayList<>();
        ArrayList<StrEncoder> values = new ArrayList<>();
        for (int i = 0; i < LOOP; i++) {
            keys.add((KEY + i).getBytes());
            values.add(new StrEncoder(VALUE + i));
        }
        return new KvReq(1, KEY.getBytes(), new StrEncoder(VALUE),
                keys, values, new StrEncoder(EXPECT_VALUE));
    }

    @Test
    public void testFullBuffer() throws Exception {
        KvReq req = buildReq();
        ByteBuffer buf = ByteBuffer.allocate(1024);
        EncodeContext encodeContext = CodecTestUtil.encodeContext();
        Assertions.assertTrue(req.encode(encodeContext, buf));
        buf.flip();
        check1(buf);
        buf.position(0);
        check2(buf);
    }

    @Test
    public void testSmallBuffer() {
        KvReq req = buildReq();
        ByteBuffer smallBuf = ByteBuffer.allocate(1);
        ByteBuffer bigBuf = ByteBuffer.allocate(32);
        EncodeContext encodeContext = CodecTestUtil.encodeContext();

        PbParser p = new PbParser();
        KvReqCallback callback = new KvReqCallback();
        p.prepareNext(CodecTestUtil.decodeContext(), callback, req.actualSize());

        ByteBuffer buf = smallBuf;
        while (!req.encode(encodeContext, buf)) {
            if (buf.position() == 0) {
                buf = bigBuf;
                if(req.encode(encodeContext, buf)) {
                    break;
                }
            }
            buf.flip();
            Assertions.assertNull(p.parse(buf));
            buf.clear();
            buf = smallBuf;
        }
        buf.flip();
        KvReq r = (KvReq) p.parse(buf);
        compare(r);
    }

    private void check1(ByteBuffer buf) throws Exception {
        DtKv.KvReq req = DtKv.KvReq.parseFrom(buf);
        Assertions.assertEquals(1, req.getGroupId());
        Assertions.assertEquals(KEY, req.getKey());
        Assertions.assertEquals(VALUE, new String(req.getValue().toByteArray()));
        Assertions.assertEquals(EXPECT_VALUE, new String(req.getExpectValue().toByteArray()));
        for (int i = 0; i < LOOP; i++) {
            Assertions.assertEquals(KEY + i, req.getKeys(i));
            Assertions.assertEquals(VALUE + i, new String(req.getValues(i).toByteArray()));
        }
    }

    private void check2(ByteBuffer buf) {
        PbParser p = new PbParser();
        KvReqCallback callback = new KvReqCallback();
        p.prepareNext(CodecTestUtil.decodeContext(), callback, buf.limit());
        KvReq r = (KvReq) p.parse(buf);
        compare(r);
    }

    private void compare(KvReq r) {
        Assertions.assertEquals(1, r.getGroupId());
        Assertions.assertEquals(KEY, new String(r.getKey()));
        Assertions.assertEquals(VALUE, new String(((ByteArrayEncoder) r.getValue()).getData()));
        Assertions.assertEquals(EXPECT_VALUE, new String(((ByteArrayEncoder) r.getExpectValue()).getData()));
        for (int i = 0; i < LOOP; i++) {
            Assertions.assertEquals(KEY + i, new String(r.getKeys().get(i)));
            Assertions.assertEquals(VALUE + i, new String(((ByteArrayEncoder) r.getValues().get(i)).getData()));
        }
    }

}
