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

public class TransferLeaderReqTest {

    @Test
    public void testFullBuffer() throws Exception {
        TransferLeaderReq req = new TransferLeaderReq();
        req.groupId = 1;
        req.term = 10;
        req.oldLeaderId = 2;
        req.newLeaderId = 3;
        req.logIndex = 100;

        ByteBuffer buf = CodecTestUtil.simpleEncode(req);
        DtRaftServer.TransferLeaderReq protoReq = DtRaftServer.TransferLeaderReq.parseFrom(buf);
        compare(req, protoReq);

        TransferLeaderReq result = CodecTestUtil.fullBufferDecode(buf, new TransferLeaderReq.Callback());
        compare(req, result);
    }

    private void compare(TransferLeaderReq expect, DtRaftServer.TransferLeaderReq proto) {
        Assertions.assertEquals(expect.groupId, proto.getGroupId());
        Assertions.assertEquals(expect.term, proto.getTerm());
        Assertions.assertEquals(expect.oldLeaderId, proto.getOldLeaderId());
        Assertions.assertEquals(expect.newLeaderId, proto.getNewLeaderId());
        Assertions.assertEquals(expect.logIndex, proto.getLogIndex());
    }

    private void compare(TransferLeaderReq expect, TransferLeaderReq result) {
        Assertions.assertEquals(expect.groupId, result.groupId);
        Assertions.assertEquals(expect.term, result.term);
        Assertions.assertEquals(expect.oldLeaderId, result.oldLeaderId);
        Assertions.assertEquals(expect.newLeaderId, result.newLeaderId);
        Assertions.assertEquals(expect.logIndex, result.logIndex);
    }
}
