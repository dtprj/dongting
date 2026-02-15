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

public class VoteRespTest {

    @Test
    public void testFullBuffer() throws Exception {
        VoteResp resp = new VoteResp();
        resp.groupId = 1;
        resp.term = 10;
        resp.voteGranted = true;

        ByteBuffer buf = CodecTestUtil.simpleEncode(resp);
        DtRaftServer.RequestVoteResp protoResp = DtRaftServer.RequestVoteResp.parseFrom(buf);
        compare(resp, protoResp);

        VoteResp result = CodecTestUtil.fullBufferDecode(buf, new VoteResp.Callback());
        compare(resp, result);
    }

    private void compare(VoteResp expect, DtRaftServer.RequestVoteResp proto) {
        Assertions.assertEquals(expect.term, proto.getTerm());
        Assertions.assertEquals(expect.voteGranted ? 1 : 0, proto.getVoteGranted());
    }

    private void compare(VoteResp expect, VoteResp result) {
        Assertions.assertEquals(expect.term, result.term);
        Assertions.assertEquals(expect.voteGranted, result.voteGranted);
    }
}
