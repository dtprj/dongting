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

import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.ChannelContext;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.Decoder;
import com.github.dtprj.dongting.net.PbZeroCopyDecoder;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.net.WriteFrame;
import com.github.dtprj.dongting.raft.impl.GroupComponents;
import com.github.dtprj.dongting.raft.impl.GroupComponentsMap;
import com.github.dtprj.dongting.raft.impl.MemberManager;
import com.github.dtprj.dongting.raft.impl.RaftStatus;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.impl.StatusUtil;

/**
 * @author huangli
 */
public class VoteProcessor extends AbstractProcessor {
    private static final DtLog log = DtLogs.getLogger(VoteProcessor.class);

    private static final PbZeroCopyDecoder decoder = new PbZeroCopyDecoder(c -> new VoteReq.Callback());

    public VoteProcessor(GroupComponentsMap groupComponentsMap) {
        super(groupComponentsMap);
    }

    @Override
    protected int getGroupId(ReadFrame frame) {
        return ((VoteReq) frame.getBody()).getGroupId();
    }

    @Override
    protected WriteFrame doProcess(ReadFrame rf, ChannelContext channelContext, GroupComponents gc) {
        VoteReq voteReq = (VoteReq) rf.getBody();
        VoteResp resp = new VoteResp();
        RaftStatus raftStatus = gc.getRaftStatus();
        if (MemberManager.validCandidate(raftStatus, voteReq.getCandidateId())) {
            int localTerm = raftStatus.getCurrentTerm();
            if (voteReq.isPreVote()) {
                processPreVote(raftStatus, voteReq, resp, localTerm);
            } else {
                RaftUtil.resetElectTimer(raftStatus);
                processVote(raftStatus, voteReq, resp, localTerm);
            }
            log.info("receive {} request. granted={}. reqTerm={}, localTerm={}",
                    voteReq.isPreVote() ? "pre-vote" : "vote", resp.isVoteGranted(), voteReq.getTerm(), localTerm);
        } else {
            resp.setVoteGranted(false);
            log.warn("receive vote request from unknown member. remoteId={}, group={}, remote={}",
                    voteReq.getCandidateId(), voteReq.getGroupId(), channelContext.getRemoteAddr());
        }

        resp.setTerm(raftStatus.getCurrentTerm());
        VoteResp.WriteFrame wf = new VoteResp.WriteFrame(resp);
        wf.setRespCode(CmdCodes.SUCCESS);
        return wf;
    }

    private void processPreVote(RaftStatus raftStatus, VoteReq voteReq, VoteResp resp, int localTerm) {
        if (shouldGrant(raftStatus, voteReq, localTerm)) {
            resp.setVoteGranted(true);
        }
    }

    private void processVote(RaftStatus raftStatus, VoteReq voteReq, VoteResp resp, int localTerm) {
        if (voteReq.getTerm() > localTerm) {
            RaftUtil.incrTerm(voteReq.getTerm(), raftStatus, -1);
        }

        if (shouldGrant(raftStatus, voteReq, localTerm)) {
            raftStatus.setVotedFor(voteReq.getCandidateId());
            resp.setVoteGranted(true);
            StatusUtil.persist(raftStatus);
        }
    }

    private boolean shouldGrant(RaftStatus raftStatus, VoteReq voteReq, int localTerm) {
        if (voteReq.getTerm() < localTerm) {
            return false;
        } else {
            if (raftStatus.getVotedFor() == 0 || raftStatus.getVotedFor() == voteReq.getCandidateId()) {
                if (voteReq.getLastLogTerm() > raftStatus.getLastLogTerm()) {
                    return true;
                } else {
                    return voteReq.getLastLogTerm() == raftStatus.getLastLogTerm()
                            && voteReq.getLastLogIndex() >= raftStatus.getLastLogIndex();
                }
            } else {
                return false;
            }
        }
    }

    @Override
    public Decoder getDecoder() {
        return decoder;
    }
}

