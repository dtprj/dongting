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
package com.github.dtprj.dongting.dtkv.server;

import com.github.dtprj.dongting.raft.impl.DtRaftServer;
import com.github.dtprj.dongting.raft.QueryStatusResp;
import com.github.dtprj.dongting.util.CodecTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashSet;

public class QueryStatusRespTest {

    public static QueryStatusResp buildQueryStatusResp() {
        QueryStatusResp status = new QueryStatusResp();
        status.groupId = 1;
        status.nodeId = 2;
        status.setFlag(true, false, true, false);
        status.term = 10;
        status.leaderId = 2;
        status.commitIndex = 1000;
        status.lastApplied = 995;
        status.lastApplyTimeToNowMillis = 5000;
        status.lastLogIndex = 1000;
        status.applyLagMillis = 100;
        status.members = new HashSet<>();
        status.members.add(1);
        status.members.add(2);
        status.members.add(3);
        status.observers = new HashSet<>();
        status.observers.add(4);
        status.preparedMembers = new HashSet<>();
        status.preparedMembers.add(5);
        status.preparedObservers = new HashSet<>();
        status.preparedObservers.add(6);
        status.lastConfigChangeIndex = 500;
        status.lastError = "test error";
        return status;
    }

    @Test
    public void testFullBuffer() throws Exception {
        QueryStatusResp status = buildQueryStatusResp();
        ByteBuffer buf = CodecTestUtil.fullBufferEncode(status);
        DtRaftServer.QueryStatusResp protoStatus = DtRaftServer.QueryStatusResp.parseFrom(buf);
        compare1(status, protoStatus);

        QueryStatusResp r = CodecTestUtil.fullBufferDecode(buf, new QueryStatusResp.Callback());
        compare2(status, r);
    }

    @Test
    public void testSmallBuffer() {
        QueryStatusResp status = buildQueryStatusResp();
        QueryStatusResp r = (QueryStatusResp) CodecTestUtil.smallBufferEncodeAndParse(status, new QueryStatusResp.Callback());
        compare2(status, r);
    }

    public static void compare1(QueryStatusResp expect, DtRaftServer.QueryStatusResp protoStatus) {
        Assertions.assertEquals(expect.groupId, protoStatus.getGroupId());
        Assertions.assertEquals(expect.nodeId, protoStatus.getNodeId());
        Assertions.assertEquals(expect.isInitFinished(), protoStatus.getFlag() != 0);
        Assertions.assertEquals(expect.term, protoStatus.getTerm());
        Assertions.assertEquals(expect.leaderId, protoStatus.getLeaderId());
        Assertions.assertEquals(expect.commitIndex, protoStatus.getCommitIndex());
        Assertions.assertEquals(expect.lastApplied, protoStatus.getLastApplied());
        Assertions.assertEquals(expect.lastApplyTimeToNowMillis, protoStatus.getLastApplyTimeToNowMillis());
        Assertions.assertEquals(expect.lastLogIndex, protoStatus.getLastLogIndex());
        Assertions.assertEquals(expect.applyLagMillis, protoStatus.getApplyLagMillis());
        Assertions.assertEquals(expect.lastConfigChangeIndex, protoStatus.getLastConfigChangeIndex());
        Assertions.assertEquals(expect.lastError, protoStatus.getLastError());
    }

    public static void compare2(QueryStatusResp expect, QueryStatusResp r) {
        Assertions.assertEquals(expect.groupId, r.groupId);
        Assertions.assertEquals(expect.nodeId, r.nodeId);
        Assertions.assertEquals(expect.isInitFinished(), r.isInitFinished());
        Assertions.assertEquals(expect.isInitFailed(), r.isInitFailed());
        Assertions.assertEquals(expect.isGroupReady(), r.isGroupReady());
        Assertions.assertEquals(expect.term, r.term);
        Assertions.assertEquals(expect.leaderId, r.leaderId);
        Assertions.assertEquals(expect.commitIndex, r.commitIndex);
        Assertions.assertEquals(expect.lastApplied, r.lastApplied);
        Assertions.assertEquals(expect.lastApplyTimeToNowMillis, r.lastApplyTimeToNowMillis);
        Assertions.assertEquals(expect.lastLogIndex, r.lastLogIndex);
        Assertions.assertEquals(expect.applyLagMillis, r.applyLagMillis);
        Assertions.assertEquals(expect.lastConfigChangeIndex, r.lastConfigChangeIndex);
        Assertions.assertEquals(expect.lastError, r.lastError);
    }
}
