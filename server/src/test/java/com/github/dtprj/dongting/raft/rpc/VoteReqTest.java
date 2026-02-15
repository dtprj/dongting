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

public class VoteReqTest {

    @Test
    public void testFullBuffer() throws Exception {
        VoteReq req = new VoteReq();
        req.groupId = 1;
        req.term = 10;
        req.candidateId = 2;
        req.lastLogIndex = 100;
        req.lastLogTerm = 5;
        req.preVote = true;

        ByteBuffer buf = CodecTestUtil.simpleEncode(req);
        DtRaftServer.RequestVoteReq protoReq = DtRaftServer.RequestVoteReq.parseFrom(buf);
        compare(req, protoReq);

        VoteReq result = CodecTestUtil.fullBufferDecode(buf, new VoteReq.Callback());
        compare(req, result);
    }

    private void compare(VoteReq expect, DtRaftServer.RequestVoteReq proto) {
        Assertions.assertEquals(expect.groupId, proto.getGroupId());
        Assertions.assertEquals(expect.term, proto.getTerm());
        Assertions.assertEquals(expect.candidateId, proto.getCandidateId());
        Assertions.assertEquals(expect.lastLogIndex, proto.getLastLogIndex());
        Assertions.assertEquals(expect.lastLogTerm, proto.getLastLogTerm());
        Assertions.assertEquals(expect.preVote ? 1 : 0, proto.getPreVote());
    }

    private void compare(VoteReq expect, VoteReq result) {
        Assertions.assertEquals(expect.groupId, result.groupId);
        Assertions.assertEquals(expect.term, result.term);
        Assertions.assertEquals(expect.candidateId, result.candidateId);
        Assertions.assertEquals(expect.lastLogIndex, result.lastLogIndex);
        Assertions.assertEquals(expect.lastLogTerm, result.lastLogTerm);
        Assertions.assertEquals(expect.preVote, result.preVote);
    }
}
