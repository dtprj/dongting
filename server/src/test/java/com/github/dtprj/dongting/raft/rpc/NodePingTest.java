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
package com.github.dtprj.dongting.raft.rpc;

import com.github.dtprj.dongting.raft.impl.DtRaftServer;
import com.github.dtprj.dongting.util.CodecTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

public class NodePingTest {

    @Test
    public void testFullBuffer() throws Exception {
        NodePing ping = new NodePing(1, 2, UUID.randomUUID());
        ByteBuffer buf = CodecTestUtil.simpleEncode(ping);
        DtRaftServer.NodePing protoPing = DtRaftServer.NodePing.parseFrom(buf);
        compare(ping, protoPing);

        NodePing result = CodecTestUtil.fullBufferDecode(buf, new NodePing());
        compare(ping, result);
    }

    private void compare(NodePing expect, DtRaftServer.NodePing proto) {
        Assertions.assertEquals(expect.localNodeId, proto.getLocalNodeId());
        Assertions.assertEquals(expect.remoteNodeId, proto.getRemoteNodeId());
        Assertions.assertEquals(expect.uuidHigh, proto.getUuidHigh());
        Assertions.assertEquals(expect.uuidLow, proto.getUuidLow());
    }

    private void compare(NodePing expect, NodePing result) {
        Assertions.assertEquals(expect.localNodeId, result.localNodeId);
        Assertions.assertEquals(expect.remoteNodeId, result.remoteNodeId);
        Assertions.assertEquals(expect.uuidHigh, result.uuidHigh);
        Assertions.assertEquals(expect.uuidLow, result.uuidLow);
    }
}
