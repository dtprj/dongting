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

public class AdminCommitOrAbortReqTest {

    @Test
    public void testFullBuffer() throws Exception {
        AdminCommitOrAbortReq req = new AdminCommitOrAbortReq();
        req.groupId = 1;
        req.prepareIndex = 100;

        ByteBuffer buf = CodecTestUtil.simpleEncode(req);
        DtRaftServer.AdminCommitConfigChangeReq protoReq = DtRaftServer.AdminCommitConfigChangeReq.parseFrom(buf);
        compare(req, protoReq);

        AdminCommitOrAbortReq result = CodecTestUtil.fullBufferDecode(buf, new AdminCommitOrAbortReq());
        compare(req, result);
    }

    private void compare(AdminCommitOrAbortReq expect, DtRaftServer.AdminCommitConfigChangeReq proto) {
        Assertions.assertEquals(expect.groupId, proto.getGroupId());
        Assertions.assertEquals(expect.prepareIndex, proto.getPrepareIndex());
    }

    private void compare(AdminCommitOrAbortReq expect, AdminCommitOrAbortReq result) {
        Assertions.assertEquals(expect.groupId, result.groupId);
        Assertions.assertEquals(expect.prepareIndex, result.prepareIndex);
    }
}
