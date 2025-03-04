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

import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.SimpleWritePacket;
import com.github.dtprj.dongting.raft.QueryStatusResp;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.RaftServer;

/**
 * @author huangli
 */
public class QueryStatusProcessor extends RaftSequenceProcessor<Integer> {

    public QueryStatusProcessor(RaftServer raftServer) {
        super(raftServer);
    }

    @Override
    public DecoderCallback<Integer> createDecoderCallback(int command, DecodeContext context) {
        return context.toDecoderCallback(context.cachedPbIntCallback());
    }

    @Override
    protected int getGroupId(ReadPacket<Integer> frame) {
        Integer x = frame.getBody();
        return x == null ? 0 : x;
    }

    @Override
    protected FiberFrame<Void> processInFiberGroup(ReqInfoEx<Integer> reqInfo) {
        RaftStatusImpl raftStatus = reqInfo.raftGroup.getGroupComponents().getRaftStatus();
        QueryStatusResp resp = new QueryStatusResp();
        resp.groupId = reqInfo.raftGroup.getGroupId();
        resp.setLeaderId(raftStatus.getCurrentLeader() == null ? 0 : raftStatus.getCurrentLeader().getNode().getNodeId());
        resp.term = raftStatus.currentTerm;
        resp.setCommitIndex(raftStatus.commitIndex);
        resp.setLastApplied(raftStatus.getLastApplied());
        resp.setLastLogIndex(raftStatus.lastLogIndex);
        resp.members = raftStatus.nodeIdOfMembers;
        resp.observers = raftStatus.nodeIdOfObservers;
        resp.preparedMembers = raftStatus.nodeIdOfPreparedMembers;
        resp.preparedObservers = raftStatus.nodeIdOfPreparedObservers;

        SimpleWritePacket wf = new SimpleWritePacket(resp);
        wf.setRespCode(CmdCodes.SUCCESS);
        reqInfo.reqContext.writeRespInBizThreads(wf);
        return FiberFrame.voidCompletedFrame();
    }
}
