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

public class AdminAddGroupReqTest {

    @Test
    public void testFullBuffer() throws Exception {
        AdminAddGroupReq req = new AdminAddGroupReq();
        req.groupId = 1;
        req.nodeIdOfMembers = "1,2,3";
        req.nodeIdOfObservers = "4,5";

        ByteBuffer buf = CodecTestUtil.simpleEncode(req);
        DtRaftServer.AdminAddGroupReq protoReq = DtRaftServer.AdminAddGroupReq.parseFrom(buf);
        compare(req, protoReq);

        AdminAddGroupReq result = CodecTestUtil.fullBufferDecode(buf, new AdminAddGroupReq());
        compare(req, result);
    }

    private void compare(AdminAddGroupReq expect, DtRaftServer.AdminAddGroupReq proto) {
        Assertions.assertEquals(expect.groupId, proto.getGroupId());
        Assertions.assertEquals(expect.nodeIdOfMembers, proto.getNodeIdOfMembers());
        Assertions.assertEquals(expect.nodeIdOfObservers, proto.getNodeIdOfObservers());
    }

    private void compare(AdminAddGroupReq expect, AdminAddGroupReq result) {
        Assertions.assertEquals(expect.groupId, result.groupId);
        Assertions.assertEquals(expect.nodeIdOfMembers, result.nodeIdOfMembers);
        Assertions.assertEquals(expect.nodeIdOfObservers, result.nodeIdOfObservers);
    }
}
