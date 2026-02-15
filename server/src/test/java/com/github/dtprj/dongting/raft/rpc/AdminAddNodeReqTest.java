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

public class AdminAddNodeReqTest {

    @Test
    public void testFullBuffer() throws Exception {
        AdminAddNodeReq req = new AdminAddNodeReq();
        req.nodeId = 1;
        req.host = "localhost";
        req.port = 9331;

        ByteBuffer buf = CodecTestUtil.simpleEncode(req);
        DtRaftServer.AdminAddNodeReq protoReq = DtRaftServer.AdminAddNodeReq.parseFrom(buf);
        compare(req, protoReq);

        AdminAddNodeReq result = CodecTestUtil.fullBufferDecode(buf, new AdminAddNodeReq());
        compare(req, result);
    }

    private void compare(AdminAddNodeReq expect, DtRaftServer.AdminAddNodeReq proto) {
        Assertions.assertEquals(expect.nodeId, proto.getNodeId());
        Assertions.assertEquals(expect.host, proto.getHost());
        Assertions.assertEquals(expect.port, proto.getPort());
    }

    private void compare(AdminAddNodeReq expect, AdminAddNodeReq result) {
        Assertions.assertEquals(expect.nodeId, result.nodeId);
        Assertions.assertEquals(expect.host, result.host);
        Assertions.assertEquals(expect.port, result.port);
    }
}
