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

import com.github.dtprj.dongting.raft.RaftNode;
import com.github.dtprj.dongting.raft.impl.DtRaftServer;
import com.github.dtprj.dongting.util.CodecTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class AdminListNodesRespTest {

    @Test
    public void testFullBuffer() throws Exception {
        AdminListNodesResp resp = new AdminListNodesResp();
        resp.nodes = buildNodes();

        ByteBuffer buf = CodecTestUtil.fullBufferEncode(resp);
        DtRaftServer.AdminListNodesResp protoResp = DtRaftServer.AdminListNodesResp.parseFrom(buf);
        compare(resp, protoResp);

        AdminListNodesResp result = CodecTestUtil.fullBufferDecode(buf, new AdminListNodesResp());
        compare(resp, result);
    }

    @Test
    public void testSmallBuffer() {
        AdminListNodesResp resp = new AdminListNodesResp();
        resp.nodes = buildNodes();

        AdminListNodesResp result = (AdminListNodesResp) CodecTestUtil.smallBufferEncodeAndParse(resp, new AdminListNodesResp());
        compare(resp, result);
    }

    private List<RaftNode> buildNodes() {
        List<RaftNode> nodes = new ArrayList<>();
        nodes.add(new RaftNode(1, new com.github.dtprj.dongting.net.HostPort("localhost", 9331)));
        nodes.add(new RaftNode(2, new com.github.dtprj.dongting.net.HostPort("127.0.0.1", 9332)));
        return nodes;
    }

    private void compare(AdminListNodesResp expect, DtRaftServer.AdminListNodesResp proto) {
        Assertions.assertEquals(expect.nodes.size(), proto.getSize());
        for (int i = 0; i < expect.nodes.size(); i++) {
            RaftNode n = expect.nodes.get(i);
            DtRaftServer.RaftNode pn = proto.getNodes(i);
            Assertions.assertEquals(n.nodeId, pn.getNodeId());
            Assertions.assertEquals(n.hostPort.getHost(), pn.getHost());
            Assertions.assertEquals(n.hostPort.getPort(), pn.getPort());
        }
    }

    private void compare(AdminListNodesResp expect, AdminListNodesResp result) {
        Assertions.assertEquals(expect.nodes.size(), result.nodes.size());
        for (int i = 0; i < expect.nodes.size(); i++) {
            RaftNode n = expect.nodes.get(i);
            RaftNode r = result.nodes.get(i);
            Assertions.assertEquals(n.nodeId, r.nodeId);
            Assertions.assertEquals(n.hostPort.getHost(), r.hostPort.getHost());
            Assertions.assertEquals(n.hostPort.getPort(), r.hostPort.getPort());
        }
    }
}
