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
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.raft.impl.MemberManager;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.RaftServer;
import com.github.dtprj.dongting.raft.store.StatusManager;

/**
 * @author huangli
 */
public class VoteProcessor extends RaftSequenceProcessor<VoteReq> {
    private static final DtLog log = DtLogs.getLogger(VoteProcessor.class);

    public VoteProcessor(RaftServer raftServer) {
        super(raftServer);
    }

    @Override
    protected int getGroupId(ReadPacket<VoteReq> frame) {
        return frame.getBody().getGroupId();
    }

    @Override
    protected FiberFrame<Void> processInFiberGroup(ReqInfoEx<VoteReq> reqInfo) {
        return new VoteFiberFrame(reqInfo);
    }

    private class VoteFiberFrame extends FiberFrame<Void> {

        private final ReqInfoEx<VoteReq> reqInfo;
        private final VoteReq voteReq;
        private final VoteResp resp = new VoteResp();
        private final RaftStatusImpl raftStatus;

        private VoteFiberFrame(ReqInfoEx<VoteReq> reqInfo) {
            this.reqInfo = reqInfo;
            this.raftStatus = reqInfo.getRaftGroup().getGroupComponents().getRaftStatus();
            this.voteReq = reqInfo.getReqFrame().getBody();
        }

        @Override
        public FrameCallResult execute(Void input) {
            if (MemberManager.validCandidate(raftStatus, voteReq.getCandidateId())) {
                if(raftStatus.isInstallSnapshot()) {
                    resp.setVoteGranted(false);
                    log.info("receive vote/preVote request during install snapshot. remoteId={}, group={}",
                            voteReq.getCandidateId(), voteReq.getGroupId());
                } else if (voteReq.isPreVote()) {
                    processPreVote();
                    log.info("receive pre-vote request from node {}, granted={}.",
                            voteReq.getCandidateId(), resp.isVoteGranted());
                } else {
                    if (RaftUtil.writeNotFinished(raftStatus)) {
                        return RaftUtil.waitWriteFinish(raftStatus, this);
                    }
                    return processVote();
                }
                return writeVoteResp();
            } else {
                resp.setVoteGranted(false);
                log.warn("receive vote request from unknown member. remoteId={}, group={}, remote={}",
                        voteReq.getCandidateId(), voteReq.getGroupId(),
                        reqInfo.getReqContext().getDtChannel().getRemoteAddr());
                // don't write response
                return Fiber.frameReturn();
            }
        }

        private FrameCallResult writeVoteResp() {
            resp.setTerm(raftStatus.getCurrentTerm());
            VoteResp.VoteRespWritePacket wf = new VoteResp.VoteRespWritePacket(resp);
            wf.setRespCode(CmdCodes.SUCCESS);
            writeResp(reqInfo, wf);
            return Fiber.frameReturn();
        }

        private void processPreVote() {
            if (shouldGrant()) {
                RaftUtil.resetElectTimer(raftStatus);
                resp.setVoteGranted(true);
            }
        }

        private FrameCallResult processVote() {
            boolean needPersist = false;
            if (voteReq.getTerm() > raftStatus.getCurrentTerm()) {
                RaftUtil.incrTerm(voteReq.getTerm(), raftStatus, -1);
                needPersist = true;
            }

            if (shouldGrant()) {
                RaftUtil.resetElectTimer(raftStatus);
                raftStatus.setVotedFor(voteReq.getCandidateId());
                resp.setVoteGranted(true);
                needPersist = true;
            }
            if (needPersist) {
                StatusManager statusManager = reqInfo.getRaftGroup().getGroupComponents().getStatusManager();
                statusManager.persistAsync(true);
                int expectTerm = raftStatus.getCurrentTerm();
                return statusManager.waitUpdateFinish(v -> postProcessVote(expectTerm));
            } else {
                return postProcessVote(raftStatus.getCurrentTerm());
            }
        }

        private FrameCallResult postProcessVote(int expectTerm) {
            if (expectTerm != raftStatus.getCurrentTerm()) {
                log.warn("localTerm changed, ignore vote response. expectTerm={}, currentTerm={}",
                        expectTerm, raftStatus.getCurrentTerm());
                return Fiber.frameReturn();
            }
            if (resp.isVoteGranted()) {
                RaftUtil.updateLeader(raftStatus, voteReq.getCandidateId());
            }
            log.info("receive vote request from node {}. granted={}. reqTerm={}, currentTerm={}",
                    voteReq.getCandidateId(), resp.isVoteGranted(), voteReq.getTerm(), raftStatus.getCurrentTerm());
            return writeVoteResp();
        }

        private boolean shouldGrant() {
            boolean result;
            if (voteReq.getTerm() < raftStatus.getCurrentTerm()) {
                result = false;
            } else {
                // pre-vote not save voteFor state, so not check it
                if (voteReq.isPreVote() || raftStatus.getVotedFor() == 0
                        || raftStatus.getVotedFor() == voteReq.getCandidateId()) {
                    if (voteReq.getLastLogTerm() > raftStatus.getLastLogTerm()) {
                        result = true;
                    } else {
                        result = voteReq.getLastLogTerm() == raftStatus.getLastLogTerm()
                                && voteReq.getLastLogIndex() >= raftStatus.getLastLogIndex();
                    }
                } else {
                    result = false;
                }
            }
            String voteType = voteReq.isPreVote() ? "pre-vote" : "vote";
            log.info("[{}] grant check {}. candidateId={}, reqTerm={}, currentLocalTerm={}, voteFor={}," +
                            " reqLastLogTerm={}, reqLastIndex={}, localLastLogTerm={}, localLastIndex={}",
                    voteType, result, voteReq.getCandidateId(), voteReq.getTerm(),
                    raftStatus.getCurrentTerm(), raftStatus.getVotedFor(), voteReq.getLastLogTerm(),
                    voteReq.getLastLogIndex(), raftStatus.getLastLogTerm(), raftStatus.getLastLogIndex());
            return result;
        }

    }

    @Override
    public DecoderCallback<VoteReq> createDecoderCallback(int command, DecodeContext context) {
        return context.toDecoderCallback(new VoteReq.Callback());
    }
}

