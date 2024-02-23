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

import com.github.dtprj.dongting.codec.Decoder;
import com.github.dtprj.dongting.codec.PbNoCopyDecoder;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.RaftServer;
import com.github.dtprj.dongting.raft.server.ReqInfo;

/**
 * @author huangli
 */
public class QueryStatusProcessor extends RaftSequenceProcessor<Integer> {

    public QueryStatusProcessor(RaftServer raftServer) {
        super(raftServer);
    }

    @Override
    public Decoder<Integer> createDecoder(int command) {
        return PbNoCopyDecoder.SIMPLE_INT_DECODER;
    }

    @Override
    protected int getGroupId(ReadFrame<Integer> frame) {
        return frame.getBody();
    }

    @Override
    protected FiberFrame<Void> processInFiberGroup(ReqInfo<Integer> reqInfo) {
        RaftStatusImpl raftStatus = reqInfo.getRaftGroup().getGroupComponents().getRaftStatus();
        QueryStatusResp resp = new QueryStatusResp();
        resp.groupId = reqInfo.getRaftGroup().getGroupId();
        resp.leaderId = raftStatus.getCurrentLeader() == null ? 0 : raftStatus.getCurrentLeader().getNode().getNodeId();
        resp.term = raftStatus.getCurrentTerm();
        resp.commitIndex = raftStatus.getCommitIndex();
        resp.lastApplied = raftStatus.getLastApplied();
        resp.lastLogIndex = raftStatus.getLastLogIndex();

        QueryStatusResp.QueryStatusRespWriteFrame wf = new QueryStatusResp.QueryStatusRespWriteFrame(resp);
        wf.setRespCode(CmdCodes.SUCCESS);
        writeResp(reqInfo, wf);
        return FiberFrame.voidCompletedFrame();
    }
}
