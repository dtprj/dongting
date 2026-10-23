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
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.dtprj.dongting.raft.rpc;

import com.github.dtprj.dongting.raft.RaftServerEventPush;
import com.github.dtprj.dongting.raft.impl.DtRaftServer;
import com.github.dtprj.dongting.util.CodecTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

public class RaftServerEventPushTest {

    @Test
    public void testFullBuffer() throws Exception {
        RaftServerEventPush push = new RaftServerEventPush(2, 3, RaftServerEventPush.EVENT_GROUP_STOP);
        ByteBuffer buf = CodecTestUtil.simpleEncode(push);
        DtRaftServer.RaftServerEventPush protoPush = DtRaftServer.RaftServerEventPush.parseFrom(buf);
        compare(push, protoPush);

        RaftServerEventPush result = CodecTestUtil.fullBufferDecode(buf, new RaftServerEventPush.Callback());
        compare(push, result);
    }

    private void compare(RaftServerEventPush expect, DtRaftServer.RaftServerEventPush proto) {
        Assertions.assertEquals(expect.groupId, proto.getGroupId());
        Assertions.assertEquals(expect.nodeId, proto.getNodeId());
        Assertions.assertEquals(expect.eventType, proto.getEventType());
    }

    private void compare(RaftServerEventPush expect, RaftServerEventPush result) {
        Assertions.assertEquals(expect.groupId, result.groupId);
        Assertions.assertEquals(expect.nodeId, result.nodeId);
        Assertions.assertEquals(expect.eventType, result.eventType);
    }
}
