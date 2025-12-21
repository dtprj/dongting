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
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.EmptyBodyRespPacket;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.GroupComponents;
import com.github.dtprj.dongting.raft.impl.RaftRole;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.RaftServer;

/**
 * @author huangli
 */
public class TransferLeaderProcessor extends RaftSequenceProcessor<TransferLeaderReq> {

    private static final DtLog log = DtLogs.getLogger(TransferLeaderProcessor.class);

    public TransferLeaderProcessor(RaftServer raftServer) {
        super(raftServer, false, true);
    }

    @Override
    protected int getGroupId(ReadPacket<TransferLeaderReq> frame) {
        return frame.getBody().groupId;
    }

    @Override
    protected FiberFrame<Void> processInFiberGroup(ReqInfoEx<TransferLeaderReq> reqInfo) {
        ReadPacket<TransferLeaderReq> frame = reqInfo.reqFrame;
        TransferLeaderReq req = frame.getBody();
        GroupComponents gc = reqInfo.raftGroup.groupComponents;
        RaftStatusImpl raftStatus = gc.raftStatus;
        if (raftStatus.getRole() != RaftRole.follower) {
            log.error("not follower, groupId={}, role={}", req.groupId, raftStatus.getRole());
            throw new RaftException("not follower");
        }
        if (req.newLeaderId != gc.serverConfig.nodeId) {
            log.error("new leader id mismatch, groupId={}, newLeaderId={}, localId={}",
                    req.groupId, req.newLeaderId, gc.serverConfig.nodeId);
            throw new RaftException("new leader id mismatch");
        }
        if (!gc.memberManager.isValidCandidate(req.oldLeaderId) || !gc.memberManager.isValidCandidate(req.newLeaderId)) {
            log.error("old leader or new leader is not valid candidate, groupId={}, old={}, new={}", req.groupId, req.oldLeaderId, req.newLeaderId);
            throw new RaftException("old leader or new leader is not valid candidate");
        }

        if (raftStatus.currentTerm != req.term) {
            log.error("term check fail, groupId={}, reqTerm={}, localTerm={}",
                    req.groupId, req.term, raftStatus.currentTerm);
            throw new RaftException("term check fail");
        }
        if (raftStatus.lastLogIndex != req.logIndex) {
            log.error("logIndex check fail, groupId={}, reqIndex={}, lastIndex={}", req.groupId,
                    req.logIndex, raftStatus.lastLogIndex);
            throw new RaftException("logIndex check fail");
        }
        if (req.logIndex != (gc.groupConfig.syncForce ? raftStatus.lastForceLogIndex : raftStatus.lastWriteLogIndex)) {
            log.error("persist index check fail, groupId={}, reqIndex={}, sync={}, lastForce={}, lastWrite={}",
                    req.groupId, req.logIndex, gc.groupConfig.syncForce,
                    raftStatus.lastForceLogIndex, raftStatus.lastWriteLogIndex);
            throw new RaftException("persist index check fail");
        }
        raftStatus.commitIndex = req.logIndex;
        gc.applyManager.wakeupApply();

        RaftUtil.changeToLeader(raftStatus);
        gc.voteManager.cancelVote("transfer leader");
        gc.linearTaskRunner.issueHeartBeat();
        reqInfo.reqContext.writeRespInBizThreads(new EmptyBodyRespPacket(CmdCodes.SUCCESS));
        return FiberFrame.voidCompletedFrame();
    }

    @Override
    public DecoderCallback<TransferLeaderReq> createDecoderCallback(int command, DecodeContext context) {
        return context.toDecoderCallback(new TransferLeaderReq.Callback());
    }
}
