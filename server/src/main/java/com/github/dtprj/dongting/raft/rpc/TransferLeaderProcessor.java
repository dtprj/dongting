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
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberTimeoutException;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.fiber.FutureFrame;
import com.github.dtprj.dongting.fiber.SimpleFrame;
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

import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class TransferLeaderProcessor extends RaftSequenceProcessor<TransferLeaderReq> {

    private static final DtLog log = DtLogs.getLogger(TransferLeaderProcessor.class);

    public TransferLeaderProcessor(RaftServer raftServer) {
        super(raftServer, false, true);
    }

    @Override
    protected FiberFrame<Void> processInFiberGroup(ReqInfoEx<TransferLeaderReq> reqInfo) {
        return new TransferLeaderFiberFrame(reqInfo);
    }

    private class TransferLeaderFiberFrame extends FiberFrame<Void> {
        private final ReqInfoEx<TransferLeaderReq> reqInfo;
        private final TransferLeaderReq req;
        private final GroupComponents gc;
        private final RaftStatusImpl raftStatus;

        TransferLeaderFiberFrame(ReqInfoEx<TransferLeaderReq> reqInfo) {
            this.reqInfo = reqInfo;
            this.req = reqInfo.reqFrame.getBody();
            this.gc = reqInfo.raftGroup.groupComponents;
            this.raftStatus = gc.raftStatus;
        }

        @Override
        protected FrameCallResult handle(Throwable ex) {
            if (DtUtil.rootCause(ex) instanceof FiberTimeoutException) {
                log.error("transfer leader wait timeout, the transfer may have taken effect. " +
                        "groupId={}, term={}", req.groupId, req.term);
                return Fiber.frameReturn();
            }
            writeErrorResp(reqInfo, ex);
            return Fiber.frameReturn();
        }

        @Override
        public FrameCallResult execute(Void input) {
            if (req.raftClusterId != raftStatus.raftClusterId
                    && (raftStatus.lastLogIndex > 0 || raftStatus.installSnapshot)) {
                log.error("raft cluster id not match, ignore transfer leader request. localId={}, reqId={}, " +
                                "groupId={}, remote={}",
                        raftStatus.raftClusterId, req.raftClusterId, req.groupId,
                        reqInfo.reqContext.getDtChannel().getRemoteAddr());
                EmptyBodyRespPacket resp = new EmptyBodyRespPacket(CmdCodes.CLIENT_ERROR);
                resp.msg = "raft cluster id not match";
                reqInfo.reqContext.writeRespInBizThreads(resp);
                return Fiber.frameReturn();
            }
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
            return Fiber.call(gc.applyManager.waitApply(req.logIndex, reqInfo.reqContext.getTimeout()),
                    this::afterApply);
        }

        private FrameCallResult afterApply(Void v) {
            // a higher term append or role change may happen during wait apply
            if (raftStatus.currentTerm != req.term || raftStatus.getRole() != RaftRole.follower) {
                log.error("term or role changed during wait apply, groupId={}, reqTerm={}, currentTerm={}, role={}",
                        req.groupId, req.term, raftStatus.currentTerm, raftStatus.getRole());
                throw new RaftException("term or role changed during wait apply");
            }
            RaftUtil.changeToLeader(raftStatus, true);
            boolean persistVote = raftStatus.votedFor != gc.serverConfig.nodeId;
            if (persistVote) {
                raftStatus.votedFor = gc.serverConfig.nodeId;
                gc.statusManager.persistAsync();
            }
            gc.voteManager.cancelVote("transfer leader");
            long currentRaftIndex = raftStatus.lastLogIndex;
            gc.linearTaskRunner.issueHeartBeat();

            FiberFuture<Void> persistFuture;
            if (persistVote) {
                persistFuture = FutureFrame.startWaitFiber("transferPersistVote", gc.fiberGroup,
                        new SimpleFrame<>("transferPersistVote",
                                frame -> gc.statusManager.waitUpdateFinish(frame::justReturn)));
            } else {
                persistFuture = FiberFuture.completedFuture(gc.fiberGroup, null);
            }
            FiberFuture<Void> applyFuture = FutureFrame.startWaitFiber("transferWaitHeartBeat", gc.fiberGroup,
                    gc.applyManager.waitApply(currentRaftIndex + 1, reqInfo.reqContext.getTimeout()));
            long restMillis = reqInfo.reqContext.getTimeout().rest(TimeUnit.MILLISECONDS);
            if (restMillis <= 0) {
                log.error("transfer leader wait timeout, the transfer may have taken effect. " +
                        "groupId={}, term={}", req.groupId, req.term);
                return Fiber.frameReturn();
            }
            return FiberFuture.allOf("transferLeaderFinish", persistFuture, applyFuture)
                    .await(restMillis, this::afterHeartBeat);
        }

        private FrameCallResult afterHeartBeat(Void unused) {
            // a higher term append or role change may happen during the waits
            if (raftStatus.currentTerm != req.term || raftStatus.getRole() != RaftRole.leader) {
                log.error("term or role changed during wait. groupId={}, reqTerm={}, currentTerm={}, role={}",
                        req.groupId, req.term, raftStatus.currentTerm, raftStatus.getRole());
                throw new RaftException("term or role changed during wait");
            }
            reqInfo.reqContext.writeRespInBizThreads(new EmptyBodyRespPacket(CmdCodes.SUCCESS));
            return Fiber.frameReturn();
        }
    }

    @Override
    public DecoderCallback<TransferLeaderReq> createDecoderCallback(int command, DecodeContext context) {
        return context.toDecoderCallback(new TransferLeaderReq.Callback());
    }

    @Override
    protected int getGroupId(ReadPacket<TransferLeaderReq> frame) {
        return frame.getBody().groupId;
    }
}
