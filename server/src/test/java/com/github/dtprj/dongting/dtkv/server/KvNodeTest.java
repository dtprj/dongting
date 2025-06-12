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
        return new KvNode(r.nextLong(), r.nextLong(), r.nextLong(), r.nextLong(), false, TestUtil.randomStr(5).getBytes());
    }

    @Test
    public void testFullBuffer() throws Exception {
        KvNode node = buildNode();
        ByteBuffer buf = CodecTestUtil.fullBufferEncode(node);
        DtKv.KvNode protoNode = DtKv.KvNode.parseFrom(buf);
        compare1(node, protoNode);

        KvNode.Callback callback = new KvNode.Callback();
        KvNode n = CodecTestUtil.fullBufferDecode(buf, callback);
        compare2(node, n);
    }

    @Test
    public void testSmallBuffer() {
        KvNode node = buildNode();
        KvNode n = (KvNode) CodecTestUtil.smallBufferEncodeAndParse(node, new KvNode.Callback());
        compare2(node, n);
    }

    public static void compare1(KvNode expect, DtKv.KvNode node) {
        Assertions.assertEquals(expect.createIndex, node.getCreateIndex());
        Assertions.assertEquals(expect.createTime, node.getCreateTime());
        Assertions.assertEquals(expect.updateIndex, node.getUpdateIndex());
        Assertions.assertEquals(expect.updateTime, node.getUpdateTime());
        Assertions.assertArrayEquals(expect.data, node.getValue().toByteArray());
    }

    public static void compare2(KvNode expect, KvNode n) {
        Assertions.assertEquals(expect.createIndex, n.createIndex);
        Assertions.assertEquals(expect.createTime, n.createTime);
        Assertions.assertEquals(expect.updateIndex, n.updateIndex);
        Assertions.assertEquals(expect.updateTime, n.updateTime);
        Assertions.assertArrayEquals(expect.data, n.data);
    }
}
