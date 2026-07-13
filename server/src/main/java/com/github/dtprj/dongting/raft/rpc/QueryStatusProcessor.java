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
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.EncodableBodyWritePacket;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.raft.QueryStatusResp;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.RaftServer;

/**
 * @author huangli
 */
public class QueryStatusProcessor extends RaftSequenceProcessor<Integer> {

    public QueryStatusProcessor(RaftServer raftServer) {
        // query status enabled on replicate port and service port
        super(raftServer, true, true);
    }

    @Override
    public DecoderCallback<Integer> createDecoderCallback(int command, DecodeContext context) {
        return context.toDecoderCallback(context.cachedPbIntCallback());
    }

    @Override
    protected int getGroupId(ReadPacket<Integer> frame) {
        return frame.getBody();
    }

    @Override
    protected FiberFrame<Void> processInFiberGroup(ReqInfoEx<Integer> reqInfo) {
        RaftStatusImpl raftStatus = reqInfo.raftGroup.groupComponents.raftStatus;
        QueryStatusResp resp = buildQueryStatusResp(raftServer.getServerConfig().nodeId, raftStatus);

        EncodableBodyWritePacket wf = new EncodableBodyWritePacket(resp);
        wf.respCode = CmdCodes.SUCCESS;
        reqInfo.reqContext.writeRespInBizThreads(wf);
        return FiberFrame.voidCompletedFrame();
    }

    public static QueryStatusResp buildQueryStatusResp(int nodeId, RaftStatusImpl raftStatus) {
        QueryStatusResp resp = new QueryStatusResp();
        resp.groupId = raftStatus.groupId;
        resp.nodeId = nodeId;
        resp.setFlag(raftStatus.isInitFinished(), raftStatus.isInitFailed(),
                raftStatus.isGroupReady(), BugLog.getCount() != 0);
        resp.leaderId = raftStatus.getCurrentLeader() == null ? 0 : raftStatus.getCurrentLeader().node.nodeId;
        resp.term = raftStatus.currentTerm;
        resp.commitIndex = raftStatus.commitIndex;
        resp.lastApplied = raftStatus.getLastApplied();
        resp.lastApplyTimeToNowMillis = (raftStatus.ts.nanoTime - raftStatus.lastApplyNanos) / 1_000_000L;
        resp.lastLogIndex = raftStatus.lastLogIndex;
        resp.applyLagMillis = raftStatus.applyLagNanos / 1_000_000L;
        resp.members = raftStatus.nodeIdOfMembers;
        resp.observers = raftStatus.nodeIdOfObservers;
        resp.preparedMembers = raftStatus.nodeIdOfPreparedMembers;
        resp.preparedObservers = raftStatus.nodeIdOfPreparedObservers;
        resp.lastConfigChangeIndex = raftStatus.lastConfigChangeIndex;
        resp.lastError = BugLog.getFirstError();
        return resp;
    }
}
