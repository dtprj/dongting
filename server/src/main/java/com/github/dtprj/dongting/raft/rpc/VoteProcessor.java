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
import com.github.dtprj.dongting.net.EmptyBodyRespPacket;
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
        super(raftServer, false, true);
    }

    @Override
    protected int getGroupId(ReadPacket<VoteReq> frame) {
        return frame.getBody().groupId;
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
            this.raftStatus = reqInfo.raftGroup.groupComponents.raftStatus;
            this.voteReq = reqInfo.reqFrame.getBody();
        }

        @Override
        public FrameCallResult execute(Void input) {
            if (!MemberManager.validCandidate(raftStatus, voteReq.candidateId)) {
                log.warn("receive vote request from unknown member. remoteId={}, group={}, remote={}",
                        voteReq.candidateId, voteReq.groupId,
                        reqInfo.reqContext.getDtChannel().getRemoteAddr());
                EmptyBodyRespPacket resp = new EmptyBodyRespPacket(CmdCodes.SYS_ERROR);
                resp.msg = "receive vote request from unknown member";
                reqInfo.reqContext.writeRespInBizThreads(resp);
                return Fiber.frameReturn();
            }
            if (!MemberManager.validCandidate(raftStatus, raftServer.getServerConfig().nodeId)) {
                log.warn("current node is not members and can't process vote. remoteId={}, group={}, remote={}",
                        voteReq.candidateId, voteReq.groupId,
                        reqInfo.reqContext.getDtChannel().getRemoteAddr());
                EmptyBodyRespPacket resp = new EmptyBodyRespPacket(CmdCodes.SYS_ERROR);
                resp.msg = "current node is not members and can't process vote";
                reqInfo.reqContext.writeRespInBizThreads(resp);
                return Fiber.frameReturn();
            }
            if (!logReceiveInfo) {
                log.info("receive {} request from node {}. groupId={}, voteFor={}, reqTerm={}, currentTerm={}, " +
                                "reqLastLogTerm={}, localLastLogTerm={}, reqIndex={}, localLastLogIndex={}",
                        voteReq.preVote ? "pre-vote" : "vote", voteReq.candidateId, voteReq.groupId,
                        raftStatus.votedFor, voteReq.term, raftStatus.currentTerm, voteReq.lastLogTerm,
                        raftStatus.lastLogTerm, voteReq.lastLogIndex, raftStatus.lastLogIndex);
                logReceiveInfo = true;
            }
            if (isGroupShouldStopPlain()) {
                // RaftSequenceProcessor checked, however the fiber may suspend to for wait write finish,
                // the stop flag may be changed, so we should re-check it
                log.warn("raft group is stopping. ignore vote/pre-vote request");
                reqInfo.reqContext.writeRespInBizThreads(createStoppedResp(voteReq.groupId));
                return Fiber.frameReturn();
            }
            if (voteReq.term > raftStatus.currentTerm) {
                String msg = (voteReq.preVote ? "pre-vote" : "vote") + " request term greater than local";
                RaftUtil.incrTerm(voteReq.term, raftStatus, -1, msg);
                termUpdated = true;
            }
            if (raftStatus.installSnapshot) {
                log.info("receive vote/preVote request during install snapshot. remoteId={}, group={}",
                        voteReq.candidateId, voteReq.groupId);
                return updateStatusFile(false);
            } else {
                if (!voteReq.preVote && RaftUtil.writeNotFinished(raftStatus)) {
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
            StatusManager statusManager = reqInfo.raftGroup.groupComponents.statusManager;
            int expectTerm = raftStatus.currentTerm;

            boolean notPreVoteAndGrant = !voteReq.preVote && grant;
            if (notPreVoteAndGrant) {
                RaftUtil.resetElectTimer(raftStatus);
                raftStatus.votedFor = voteReq.candidateId;
                reqInfo.raftGroup.groupComponents.voteManager.cancelVote(
                        "vote for node " + voteReq.candidateId);
            }
            if (termUpdated || notPreVoteAndGrant) {
                statusManager.persistAsync(notPreVoteAndGrant);
            }
            if (notPreVoteAndGrant) {
                return statusManager.waitUpdateFinish(v -> afterStatusFileUpdated(expectTerm, true));
            } else {
                return afterStatusFileUpdated(expectTerm, grant);
            }
        }

        private FrameCallResult afterStatusFileUpdated(int expectTerm, boolean grant) {
            if (expectTerm != raftStatus.currentTerm) {
                log.warn("localTerm changed, ignore vote response. expectTerm={}, currentTerm={}",
                        expectTerm, raftStatus.currentTerm);
                return Fiber.frameReturn();
            }

            VoteResp resp = new VoteResp();
            resp.voteGranted = grant;
            resp.term = raftStatus.currentTerm;
            SimpleWritePacket wf = new SimpleWritePacket(resp);
            wf.respCode = CmdCodes.SUCCESS;
            reqInfo.reqContext.writeRespInBizThreads(wf);
            log.info("receive {} request from node {}. granted={}", voteReq.preVote ? "pre-vote" : "vote",
                    voteReq.candidateId, resp.voteGranted);
            return Fiber.frameReturn();
        }

        private boolean shouldGrant() {
            boolean result;
            if (voteReq.term < raftStatus.currentTerm) {
                result = false;
            } else {
                // pre-vote not save voteFor state, so not check it
                if (voteReq.preVote || raftStatus.votedFor == 0
                        || raftStatus.votedFor == voteReq.candidateId) {
                    if (voteReq.lastLogTerm > raftStatus.lastLogTerm) {
                        result = true;
                    } else if (voteReq.lastLogTerm == raftStatus.lastLogTerm) {
                        if (voteReq.preVote && raftStatus.getRole() == RaftRole.leader) {
                            result = voteReq.lastLogIndex > raftStatus.lastLogIndex;
                        } else {
                            result = voteReq.lastLogIndex >= raftStatus.lastLogIndex;
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
                    voteReq.preVote ? "pre-vote" : "vote", result, voteReq.candidateId, voteReq.groupId,
                    raftStatus.votedFor, voteReq.term, raftStatus.currentTerm, voteReq.lastLogTerm,
                    raftStatus.lastLogTerm, voteReq.lastLogIndex, raftStatus.lastLogIndex);
            return result;
        }
    }

    @Override
    public DecoderCallback<VoteReq> createDecoderCallback(int command, DecodeContext context) {
        return context.toDecoderCallback(new VoteReq.Callback());
    }
}

