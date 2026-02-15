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

public class RaftPingTest {

    @Test
    public void testFullBuffer() throws Exception {
        RaftPing ping = new RaftPing();
        ping.groupId = 1;
        ping.nodeId = 2;
        ping.members = "1,2,3";
        ping.observers = "4,5";
        ping.preparedMembers = "6";
        ping.preparedObservers = "7";

        ByteBuffer buf = CodecTestUtil.simpleEncode(ping);
        DtRaftServer.RaftPing protoPing = DtRaftServer.RaftPing.parseFrom(buf);
        compare(ping, protoPing);

        RaftPing result = CodecTestUtil.fullBufferDecode(buf, new RaftPing());
        compare(ping, result);
    }

    private void compare(RaftPing expect, DtRaftServer.RaftPing proto) {
        Assertions.assertEquals(expect.groupId, proto.getGroupId());
        Assertions.assertEquals(expect.nodeId, proto.getNodeId());
        Assertions.assertEquals(expect.members, proto.getServers());
        Assertions.assertEquals(expect.observers, proto.getObservers());
        Assertions.assertEquals(expect.preparedMembers, proto.getPreparedMembers());
        Assertions.assertEquals(expect.preparedObservers, proto.getPreparedObservers());
    }

    private void compare(RaftPing expect, RaftPing result) {
        Assertions.assertEquals(expect.groupId, result.groupId);
        Assertions.assertEquals(expect.nodeId, result.nodeId);
        Assertions.assertEquals(expect.members, result.members);
        Assertions.assertEquals(expect.observers, result.observers);
        Assertions.assertEquals(expect.preparedMembers, result.preparedMembers);
        Assertions.assertEquals(expect.preparedObservers, result.preparedObservers);
    }
}
