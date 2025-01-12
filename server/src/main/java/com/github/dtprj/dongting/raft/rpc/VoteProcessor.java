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
import com.github.dtprj.dongting.net.SimpleWritePacket;
import com.github.dtprj.dongting.raft.impl.MemberManager;
import com.github.dtprj.dongting.raft.impl.RaftRole;
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
        private final RaftStatusImpl raftStatus;
        private boolean logReceiveInfo;

        private boolean termUpdated;

        private VoteFiberFrame(ReqInfoEx<VoteReq> reqInfo) {
            this.reqInfo = reqInfo;
            this.raftStatus = reqInfo.getRaftGroup().getGroupComponents().getRaftStatus();
            this.voteReq = reqInfo.getReqFrame().getBody();
        }

        @Override
        public FrameCallResult execute(Void input) {
            if (!MemberManager.validCandidate(raftStatus, voteReq.getCandidateId())) {
                log.warn("receive vote request from unknown member. remoteId={}, group={}, remote={}",
                        voteReq.getCandidateId(), voteReq.getGroupId(),
                        reqInfo.getReqContext().getDtChannel().getRemoteAddr());
                // don't write response
                return Fiber.frameReturn();
            }
            if (!logReceiveInfo) {
                log.info("receive {} request from node {}. groupId={}, voteFor={}, reqTerm={}, currentTerm={}, " +
                                "reqLastLogTerm={}, localLastLogTerm={}, reqIndex={}, localLastLogIndex={}",
                        voteReq.isPreVote() ? "pre-vote" : "vote", voteReq.getCandidateId(), voteReq.getGroupId(),
                        raftStatus.getVotedFor(), voteReq.getTerm(), raftStatus.getCurrentTerm(), voteReq.getLastLogTerm(),
                        raftStatus.getLastLogTerm(), voteReq.getLastLogIndex(), raftStatus.getLastLogIndex());
                logReceiveInfo = true;
            }
            if (isGroupShouldStopPlain()) {
                // RaftSequenceProcessor checked, however the fiber may suspend to for wait write finish,
                // the stop flag may be changed, so we should re-check it
                log.warn("raft group is stopping. ignore vote/pre-vote request");
                return Fiber.frameReturn();
            }
            if (voteReq.getTerm() > raftStatus.getCurrentTerm()) {
                String msg = (voteReq.isPreVote() ? "pre-vote" : "vote") + " request term greater than local";
                RaftUtil.incrTerm(voteReq.getTerm(), raftStatus, -1, msg);
                termUpdated = true;
            }
            RaftUtil.resetElectTimer(raftStatus);
            if (raftStatus.isInstallSnapshot()) {
                log.info("receive vote/preVote request during install snapshot. remoteId={}, group={}",
                        voteReq.getCandidateId(), voteReq.getGroupId());
                return updateStatusFile(false);
            } else {
                if (!voteReq.isPreVote() && RaftUtil.writeNotFinished(raftStatus)) {
                    return RaftUtil.waitWriteFinish(raftStatus, this);
                }
                if (shouldGrant()) {
                    return updateStatusFile(true);
                } else {
                    return updateStatusFile(false);
                }
            }
        }

        private FrameCallResult updateStatusFile(boolean grant) {
            StatusManager statusManager = reqInfo.getRaftGroup().getGroupComponents().getStatusManager();
            int expectTerm = raftStatus.getCurrentTerm();

            boolean notPreVoteAndGrant = !voteReq.isPreVote() && grant;
            if (notPreVoteAndGrant) {
                raftStatus.setVotedFor(voteReq.getCandidateId());
                reqInfo.getRaftGroup().getGroupComponents().getVoteManager().cancelVote(
                        "vote for node " + voteReq.getCandidateId());
            }
            if (termUpdated || notPreVoteAndGrant) {
                statusManager.persistAsync(notPreVoteAndGrant);
            }
            if (notPreVoteAndGrant) {
                return statusManager.waitUpdateFinish(v -> afterStatusFileUpdated(expectTerm, grant));
            } else {
                return afterStatusFileUpdated(expectTerm, grant);
            }
        }

        private FrameCallResult afterStatusFileUpdated(int expectTerm, boolean grant) {
            if (expectTerm != raftStatus.getCurrentTerm()) {
                log.warn("localTerm changed, ignore vote response. expectTerm={}, currentTerm={}",
                        expectTerm, raftStatus.getCurrentTerm());
                return Fiber.frameReturn();
            }
            RaftUtil.resetElectTimer(raftStatus);

            VoteResp resp = new VoteResp();
            resp.setVoteGranted(grant);
            resp.setTerm(raftStatus.getCurrentTerm());
            SimpleWritePacket wf = new SimpleWritePacket(resp);
            wf.setRespCode(CmdCodes.SUCCESS);
            writeResp(reqInfo, wf);
            log.info("receive {} request from node {}. granted={}", voteReq.isPreVote() ? "pre-vote" : "vote",
                    voteReq.getCandidateId(), resp.isVoteGranted());
            return Fiber.frameReturn();
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
                    } else if (voteReq.getLastLogTerm() == raftStatus.getLastLogTerm()) {
                        if (voteReq.isPreVote() && raftStatus.getRole() == RaftRole.leader) {
                            result = voteReq.getLastLogIndex() > raftStatus.getLastLogIndex();
                        } else {
                            result = voteReq.getLastLogIndex() >= raftStatus.getLastLogIndex();
                        }
                    } else {
                        result = false;
                    }
                } else {
                    result = false;
                }
            }
            log.info("{} grant check {}. candidateId={}, groupId={}, voteFor={}, reqTerm={}, currentTerm={}, " +
                            "reqLastLogTerm={}, localLastLogTerm={}, reqIndex={}, localLastLogIndex={}",
                    voteReq.isPreVote() ? "pre-vote" : "vote", result, voteReq.getCandidateId(), voteReq.getGroupId(),
                    raftStatus.getVotedFor(), voteReq.getTerm(), raftStatus.getCurrentTerm(), voteReq.getLastLogTerm(),
                    raftStatus.getLastLogTerm(), voteReq.getLastLogIndex(), raftStatus.getLastLogIndex());
            return result;
        }
    }

    @Override
    public DecoderCallback<VoteReq> createDecoderCallback(int command, DecodeContext context) {
        return context.toDecoderCallback(new VoteReq.Callback());
    }
}

