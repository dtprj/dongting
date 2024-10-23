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

import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.PbParser;
import com.github.dtprj.dongting.config.DtKv;
import com.github.dtprj.dongting.dtkv.KvNode;
import com.github.dtprj.dongting.raft.test.TestUtil;
import com.github.dtprj.dongting.util.CodecTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Random;

/**
 * @author huangli
 */
public class KvNodeTest {

    public static KvNode buildNode() {
        Random r = new Random();
        return new KvNode(r.nextLong(), r.nextLong(), r.nextLong(), r.nextLong(), TestUtil.randomStr(5).getBytes());
    }

    @Test
    public void testFullBuffer() throws Exception {
        KvNode node = buildNode();
        ByteBuffer buf = ByteBuffer.allocate(256);
        EncodeContext encodeContext = CodecTestUtil.encodeContext();
        Assertions.assertTrue(node.encode(encodeContext, buf));
        buf.flip();
        DtKv.KvNode protoNode = DtKv.KvNode.parseFrom(buf);
        compare1(node, protoNode);

        buf.position(0);
        PbParser p = new PbParser();
        KvNode.Callback callback = new KvNode.Callback();
        p.prepareNext(CodecTestUtil.decodeContext(), callback, buf.limit());
        KvNode n = (KvNode) p.parse(buf);
        compare2(node, n);
    }

    @Test
    public void testSmallBuffer() {
        KvNode node = buildNode();
        ByteBuffer smallBuf = ByteBuffer.allocate(1);
        ByteBuffer bigBuf = ByteBuffer.allocate(64);
        EncodeContext encodeContext = CodecTestUtil.encodeContext();

        PbParser p = new PbParser();
        KvNode.Callback callback = new KvNode.Callback();
        p.prepareNext(CodecTestUtil.decodeContext(), callback, node.actualSize());

        KvNode n = (KvNode) KvReqTest.encodeAndParse(smallBuf, bigBuf, node, encodeContext, p);
        compare2(node, n);
    }

    public static void compare1(KvNode expect, DtKv.KvNode node) {
        Assertions.assertEquals(expect.getCreateIndex(), node.getCreateIndex());
        Assertions.assertEquals(expect.getCreateTime(), node.getCreateTime());
        Assertions.assertEquals(expect.getUpdateIndex(), node.getUpdateIndex());
        Assertions.assertEquals(expect.getUpdateTime(), node.getUpdateTime());
        Assertions.assertArrayEquals(expect.getData(), node.getValue().toByteArray());
    }

    public static void compare2(KvNode expect, KvNode n) {
        Assertions.assertEquals(expect.getCreateIndex(), n.getCreateIndex());
        Assertions.assertEquals(expect.getCreateTime(), n.getCreateTime());
        Assertions.assertEquals(expect.getUpdateIndex(), n.getUpdateIndex());
        Assertions.assertEquals(expect.getUpdateTime(), n.getUpdateTime());
        Assertions.assertArrayEquals(expect.getData(), n.getData());
    }
}
