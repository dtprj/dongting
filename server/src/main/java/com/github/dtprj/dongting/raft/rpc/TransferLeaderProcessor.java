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
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.ChannelContext;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.EmptyBodyRespFrame;
import com.github.dtprj.dongting.net.NetCodeException;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.net.WriteFrame;
import com.github.dtprj.dongting.raft.impl.RaftGroupImpl;
import com.github.dtprj.dongting.raft.impl.RaftRole;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.RaftGroup;

/**
 * @author huangli
 */
public class TransferLeaderProcessor extends RaftGroupProcessor<TransferLeaderReq> {

    private static final DtLog log = DtLogs.getLogger(TransferLeaderProcessor.class);

    private static final Decoder<TransferLeaderReq> DECODER = new PbNoCopyDecoder<>(context -> new TransferLeaderReq.Callback());

    @Override
    protected int getGroupId(ReadFrame<TransferLeaderReq> frame) {
        return frame.getBody().groupId;
    }

    @Override
    protected WriteFrame doProcess(ReadFrame<TransferLeaderReq> frame, ChannelContext channelContext,
                                   ReqContext reqContext, RaftGroup rg) {
        TransferLeaderReq req = frame.getBody();
        RaftGroupImpl gc = (RaftGroupImpl) rg;
        RaftStatusImpl raftStatus = gc.getRaftStatus();
        if (raftStatus.isError()) {
            log.error("transfer leader fail, error state, groupId={}", req.groupId);
            throw new NetCodeException(CmdCodes.BIZ_ERROR, "in error state");
        }
        if (raftStatus.getRole() != RaftRole.follower) {
            log.error("transfer leader fail, not follower, groupId={}, role={}", req.groupId, raftStatus.getRole());
            throw new NetCodeException(CmdCodes.BIZ_ERROR, "transfer leader fail, not follower");
        }
        if (!gc.getMemberManager().checkLeader(req.oldLeaderId)) {
            log.error("transfer leader fail, leader check fail, groupId={}", req.groupId);
            throw new NetCodeException(CmdCodes.BIZ_ERROR, "transfer leader fail, leader check fail");
        }
        if (raftStatus.getCurrentTerm() != req.term) {
            log.error("transfer leader fail, term check fail, groupId={}, reqTerm={}, localTerm={}",
                    req.groupId, req.term, raftStatus.getCurrentTerm());
            throw new NetCodeException(CmdCodes.BIZ_ERROR, "transfer leader fail, term check fail");
        }
        if (raftStatus.getLastLogIndex() != req.logIndex) {
            log.error("transfer leader fail, logIndex check fail, groupId={}, reqIndex={}, localIndex={}",
                    req.groupId, req.logIndex, raftStatus.getLastLogIndex());
            throw new NetCodeException(CmdCodes.BIZ_ERROR, "transfer leader fail, logIndex check fail");
        }
        raftStatus.setCommitIndex(req.logIndex);
        gc.getApplyManager().apply(raftStatus);

        RaftUtil.changeToLeader(raftStatus);
        gc.getVoteManager().cancelVote();
        gc.getRaft().sendHeartBeat();
        return new EmptyBodyRespFrame(CmdCodes.SUCCESS);
    }

    @Override
    public Decoder<TransferLeaderReq> createDecoder() {
        return DECODER;
    }
}
