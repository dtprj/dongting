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
import java.util.HashSet;

public class AdminPrepareConfigChangeReqTest {

    @Test
    public void testFullBuffer() throws Exception {
        AdminPrepareConfigChangeReq req = new AdminPrepareConfigChangeReq();
        req.groupId = 1;
        req.members = new HashSet<>();
        req.members.add(1);
        req.members.add(2);
        req.members.add(3);
        req.observers = new HashSet<>();
        req.observers.add(4);
        req.observers.add(5);
        req.preparedMembers = new HashSet<>();
        req.preparedMembers.add(6);
        req.preparedObservers = new HashSet<>();
        req.preparedObservers.add(7);

        ByteBuffer buf = CodecTestUtil.simpleEncode(req);
        DtRaftServer.AdminPrepareConfigChangeReq protoReq = DtRaftServer.AdminPrepareConfigChangeReq.parseFrom(buf);
        compare(req, protoReq);

        AdminPrepareConfigChangeReq result = CodecTestUtil.fullBufferDecode(buf, new AdminPrepareConfigChangeReq.Callback());
        compare(req, result);
    }

    private void compare(AdminPrepareConfigChangeReq expect, DtRaftServer.AdminPrepareConfigChangeReq proto) {
        Assertions.assertEquals(expect.groupId, proto.getGroupId());
        if (expect.members != null) {
            Assertions.assertEquals(expect.members.size(), proto.getMembersCount());
            for (Integer m : expect.members) {
                Assertions.assertTrue(proto.getMembersList().contains(m));
            }
        }
        if (expect.observers != null) {
            Assertions.assertEquals(expect.observers.size(), proto.getObserversCount());
            for (Integer o : expect.observers) {
                Assertions.assertTrue(proto.getObserversList().contains(o));
            }
        }
    }

    private void compare(AdminPrepareConfigChangeReq expect, AdminPrepareConfigChangeReq result) {
        Assertions.assertEquals(expect.groupId, result.groupId);
        if (expect.members != null) {
            Assertions.assertEquals(expect.members, result.members);
        }
        if (expect.observers != null) {
            Assertions.assertEquals(expect.observers, result.observers);
        }
    }
}
