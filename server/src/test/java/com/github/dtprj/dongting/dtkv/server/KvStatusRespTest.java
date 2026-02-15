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

import com.github.dtprj.dongting.config.DtKv;
import com.github.dtprj.dongting.dtkv.KvStatusResp;
import com.github.dtprj.dongting.raft.QueryStatusResp;
import com.github.dtprj.dongting.util.CodecTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashSet;

/**
 * @author huangli
 */
public class KvStatusRespTest {

    public static KvStatusResp buildStatusResp() {
        KvStatusResp resp = new KvStatusResp();
        resp.raftServerStatus = buildQueryStatusResp();
        resp.watchCount = 100;
        return resp;
    }

    private static QueryStatusResp buildQueryStatusResp() {
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
        status.lastConfigChangeIndex = 500;
        status.lastError = "test error";
        return status;
    }

    @Test
    public void testFullBuffer() throws Exception {
        KvStatusResp resp = buildStatusResp();
        ByteBuffer buf = CodecTestUtil.fullBufferEncode(resp);
        DtKv.KvStatusResp protoResp = DtKv.KvStatusResp.parseFrom(buf);
        compare1(resp, protoResp);

        KvStatusResp r = CodecTestUtil.fullBufferDecode(buf, new KvStatusResp());
        compare2(resp, r);
    }

    @Test
    public void testSmallBuffer() {
        KvStatusResp resp = buildStatusResp();
        KvStatusResp r = (KvStatusResp) CodecTestUtil.smallBufferEncodeAndParse(resp, new KvStatusResp());
        compare2(resp, r);
    }

    private void compare1(KvStatusResp expect, DtKv.KvStatusResp protoResp) {
        Assertions.assertEquals(expect.watchCount, protoResp.getWatchCount());
        QueryStatusRespTest.compare1(expect.raftServerStatus, protoResp.getRaftServerStatus());
    }

    private void compare2(KvStatusResp expect, KvStatusResp r) {
        Assertions.assertEquals(expect.watchCount, r.watchCount);
        QueryStatusRespTest.compare2(expect.raftServerStatus, r.raftServerStatus);
    }
}
