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
import com.github.dtprj.dongting.pb.PbCallback;
import com.github.dtprj.dongting.raft.impl.RaftStatus;
import com.github.dtprj.dongting.raft.impl.RaftUtil;

/**
 * @author huangli
 */
public class VoteProcessor extends ReqProcessor {
    private static final DtLog log = DtLogs.getLogger(VoteProcessor.class);

    private final RaftStatus raftStatus;

    private final PbZeroCopyDecoder decoder = new PbZeroCopyDecoder() {
        @Override
        protected PbCallback createCallback(ChannelContext context) {
            return new VoteReq.Callback();
        }
    };

    public VoteProcessor(RaftStatus raftStatus) {
        this.raftStatus = raftStatus;
    }

    @Override
    public WriteFrame process(ReadFrame rf, ChannelContext channelContext, ReqContext reqContext) {
        VoteReq voteReq = (VoteReq) rf.getBody();
        VoteResp resp = new VoteResp();
        int localTerm = raftStatus.getCurrentTerm();
        if (voteReq.getTerm() > localTerm) {
            RaftUtil.incrTermAndConvertToFollower(voteReq.getTerm(), raftStatus);
        }

        if (voteReq.getTerm() < localTerm) {
            resp.setVoteGranted(false);
        } else {
            RaftUtil.resetElectTimer(raftStatus);
            if (raftStatus.getVoteFor() == 0 || raftStatus.getVoteFor() == voteReq.getCandidateId()) {
                // TODO persist
                if (voteReq.getLastLogTerm() > raftStatus.getLastLogTerm()) {
                    raftStatus.setVoteFor(voteReq.getCandidateId());
                    resp.setVoteGranted(true);
                } else if (voteReq.getLastLogTerm() == raftStatus.getLastLogTerm()
                        && voteReq.getLastLogIndex() >= raftStatus.getLastLogIndex()) {
                    raftStatus.setVoteFor(voteReq.getCandidateId());
                    resp.setVoteGranted(true);
                } else {
                    resp.setVoteGranted(false);
                }
            } else {
                resp.setVoteGranted(false);
            }
        }
        log.info("receive vote request. granted={}. remoteTerm={}, localTerm={}",
                resp.isVoteGranted(), voteReq.getTerm(), localTerm);
        resp.setTerm(raftStatus.getCurrentTerm());
        VoteResp.WriteFrame wf = new VoteResp.WriteFrame(resp);
        wf.setRespCode(CmdCodes.SUCCESS);
        return wf;
    }

    @Override
    public Decoder getDecoder() {
        return decoder;
    }
}

