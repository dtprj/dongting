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
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.net.ReqProcessor;
import com.github.dtprj.dongting.net.WriteFrame;
import com.github.dtprj.dongting.raft.impl.RaftStatus;
import com.github.dtprj.dongting.raft.impl.RaftUtil;

/**
 * @author huangli
 */
public class VoteProcessor extends ReqProcessor {
    private static final DtLog log = DtLogs.getLogger(VoteProcessor.class);

    private final RaftStatus raftStatus;

    private static final PbZeroCopyDecoder decoder = new PbZeroCopyDecoder(c -> new VoteReq.Callback());

    public VoteProcessor(RaftStatus raftStatus) {
        this.raftStatus = raftStatus;
    }

    @Override
    public WriteFrame process(ReadFrame rf, ChannelContext channelContext, ReqContext reqContext) {
        RaftUtil.resetElectTimer(raftStatus);

        VoteReq voteReq = (VoteReq) rf.getBody();
        VoteResp resp = new VoteResp();
        int localTerm = raftStatus.getCurrentTerm();

        if (voteReq.isPreVote()) {
            processPreVote(voteReq, resp, localTerm);
        } else {
            processVote(voteReq, resp, localTerm);
        }

        log.info("receive {} request. granted={}. reqTerm={}, localTerm={}",
                voteReq.isPreVote() ? "pre-vote" : "vote", resp.isVoteGranted(), voteReq.getTerm(), localTerm);
        resp.setTerm(raftStatus.getCurrentTerm());
        VoteResp.WriteFrame wf = new VoteResp.WriteFrame(resp);
        wf.setRespCode(CmdCodes.SUCCESS);
        return wf;
    }

    private void processPreVote(VoteReq voteReq, VoteResp resp, int localTerm) {
        if (shouldGrant(voteReq, localTerm)) {
            resp.setVoteGranted(true);
        }
    }

    private void processVote(VoteReq voteReq, VoteResp resp, int localTerm) {
        if (voteReq.getTerm() > localTerm) {
            RaftUtil.incrTermAndConvertToFollower(voteReq.getTerm(), raftStatus, -1);
        }

        if (shouldGrant(voteReq, localTerm)) {
            // TODO persist
            raftStatus.setVotedFor(voteReq.getCandidateId());
            resp.setVoteGranted(true);
        }
    }

    private boolean shouldGrant(VoteReq voteReq, int localTerm) {
        if (voteReq.getTerm() < localTerm) {
            return false;
        } else {
            if (raftStatus.getVotedFor() == 0 || raftStatus.getVotedFor() == voteReq.getCandidateId()) {
                if (voteReq.getLastLogTerm() > raftStatus.getLastLogTerm()) {
                    return true;
                } else if (voteReq.getLastLogTerm() == raftStatus.getLastLogTerm()
                        && voteReq.getLastLogIndex() >= raftStatus.getLastLogIndex()) {
                    return true;
                } else {
                    return false;
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

