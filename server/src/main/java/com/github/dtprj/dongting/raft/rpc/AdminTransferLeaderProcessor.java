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
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.EmptyBodyRespPacket;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.GroupComponents;
import com.github.dtprj.dongting.raft.impl.RaftRole;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.NotLeaderException;
import com.github.dtprj.dongting.raft.server.RaftServer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class AdminTransferLeaderProcessor extends RaftSequenceProcessor<TransferLeaderReq> {

    private static final DtLog log = DtLogs.getLogger(AdminTransferLeaderProcessor.class);

    public AdminTransferLeaderProcessor(RaftServer raftServer) {
        super(raftServer);
    }

    @Override
    protected int getGroupId(ReadPacket<TransferLeaderReq> frame) {
        return frame.getBody().groupId;
    }

    @Override
    protected FiberFrame<Void> processInFiberGroup(ReqInfoEx<TransferLeaderReq> reqInfo) {
        ReadPacket<TransferLeaderReq> frame = reqInfo.getReqFrame();
        TransferLeaderReq req = frame.getBody();
        GroupComponents gc = reqInfo.getRaftGroup().getGroupComponents();
        RaftStatusImpl raftStatus = gc.getRaftStatus();
        if (raftStatus.getRole() != RaftRole.leader) {
            log.error("not leader, groupId={}, role={}", req.groupId, raftStatus.getRole());
            throw new NotLeaderException(raftStatus.getCurrentLeader().getNode());
        }
        if (req.oldLeaderId != gc.getServerConfig().getNodeId()) {
            log.error("old leader id mismatch, groupId={}, oldLeaderId={}, localId={}",
                    req.groupId, req.oldLeaderId, gc.getServerConfig().getNodeId());
            throw new RaftException("new leader id mismatch");
        }
        if (!gc.getMemberManager().isValidCandidate(req.oldLeaderId) || !gc.getMemberManager().isValidCandidate(req.newLeaderId)) {
            log.error("old leader or new leader is not valid candidate, groupId={}, old={}, new={}", req.groupId, req.oldLeaderId, req.newLeaderId);
            throw new RaftException("old leader or new leader is not valid candidate");
        }
        return new FiberFrame<>() {
            final FiberFuture<Void> fiberFuture = getFiberGroup().newFuture("admin transfer leader");

            @Override
            protected FrameCallResult handle(Throwable ex) {
                log.error("admin transfer leader fail", ex);
                writeErrorResp(reqInfo, ex);
                return Fiber.frameReturn();
            }

            @Override
            public FrameCallResult execute(Void input) {
                log.info("admin transfer leader begin, groupId={}, old={}, new={}", req.groupId, req.oldLeaderId, req.newLeaderId);
                long timeout = reqInfo.getReqContext().getTimeout().getTimeout(TimeUnit.MILLISECONDS);
                CompletableFuture<Void> f = reqInfo.getRaftGroup().transferLeadership(req.newLeaderId, timeout);
                f.whenComplete((v, ex) -> {
                    if (ex != null) {
                        fiberFuture.completeExceptionally(ex);
                    } else {
                        fiberFuture.complete(null);
                    }
                });
                return fiberFuture.await(timeout, this::afterTransfer);
            }

            private FrameCallResult afterTransfer(Void unused) {
                EmptyBodyRespPacket resp = new EmptyBodyRespPacket(CmdCodes.SUCCESS);
                writeResp(reqInfo, resp);
                log.info("admin transfer leader success, groupId={}", req.groupId);
                return Fiber.frameReturn();
            }
        };
    }


    @Override
    public DecoderCallback<TransferLeaderReq> createDecoderCallback(int command, DecodeContext context) {
        return context.toDecoderCallback(new TransferLeaderReq());
    }
}
