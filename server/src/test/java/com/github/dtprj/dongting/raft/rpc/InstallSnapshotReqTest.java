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

public class InstallSnapshotReqTest {

    @Test
    public void testDecodeFromProto() throws Exception {
        InstallSnapshotReq req = buildReq();
        ByteBuffer buf = encodeToBuf(req);
        DtRaftServer.InstallSnapshotReq.parseFrom(buf);

        buf.position(0);
        InstallSnapshotReq.Callback callback = new InstallSnapshotReq.Callback();
        InstallSnapshotReq result = CodecTestUtil.fullBufferDecode(buf, callback);
        compare(req, result);
    }

    private InstallSnapshotReq buildReq() {
        InstallSnapshotReq req = new InstallSnapshotReq();
        req.groupId = 1;
        req.term = 10;
        req.leaderId = 2;
        req.lastIncludedIndex = 1000;
        req.lastIncludedTerm = 5;
        req.offset = 0;
        req.done = false;
        req.nextWritePos = 2000;
        req.members = new HashSet<>();
        req.members.add(1);
        req.members.add(2);
        req.observers = new HashSet<>();
        req.observers.add(3);
        req.preparedMembers = new HashSet<>();
        req.preparedObservers = new HashSet<>();
        req.lastConfigChangeIndex = 500;
        return req;
    }

    private ByteBuffer encodeToBuf(InstallSnapshotReq req) {
        DtRaftServer.InstallSnapshotReq.Builder builder = DtRaftServer.InstallSnapshotReq.newBuilder();
        builder.setGroupId(req.groupId);
        builder.setTerm(req.term);
        builder.setLeaderId(req.leaderId);
        builder.setLastIncludedIndex(req.lastIncludedIndex);
        builder.setLastIncludedTerm(req.lastIncludedTerm);
        builder.setOffset(req.offset);
        builder.setDone(req.done);
        builder.setNextWritePos(req.nextWritePos);
        builder.setLastConfigChangeIndex(req.lastConfigChangeIndex);
        if (req.members != null) {
            for (Integer m : req.members) {
                builder.addMembers(m);
            }
        }
        if (req.observers != null) {
            for (Integer o : req.observers) {
                builder.addObservers(o);
            }
        }
        return builder.build().toByteString().asReadOnlyByteBuffer();
    }

    private void compare(InstallSnapshotReq expect, InstallSnapshotReq result) {
        Assertions.assertEquals(expect.groupId, result.groupId);
        Assertions.assertEquals(expect.term, result.term);
        Assertions.assertEquals(expect.leaderId, result.leaderId);
        Assertions.assertEquals(expect.lastIncludedIndex, result.lastIncludedIndex);
        Assertions.assertEquals(expect.lastIncludedTerm, result.lastIncludedTerm);
        Assertions.assertEquals(expect.offset, result.offset);
        Assertions.assertEquals(expect.done, result.done);
        Assertions.assertEquals(expect.nextWritePos, result.nextWritePos);
        Assertions.assertEquals(expect.lastConfigChangeIndex, result.lastConfigChangeIndex);
    }
}
