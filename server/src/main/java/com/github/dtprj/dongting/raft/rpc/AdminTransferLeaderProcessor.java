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
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.EmptyBodyRespPacket;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.server.RaftProcessor;
import com.github.dtprj.dongting.raft.server.RaftServer;
import com.github.dtprj.dongting.raft.server.ReqInfo;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class AdminTransferLeaderProcessor extends RaftProcessor<TransferLeaderReq> {

    private static final DtLog log = DtLogs.getLogger(AdminTransferLeaderProcessor.class);

    public AdminTransferLeaderProcessor(RaftServer raftServer) {
        super(raftServer, false, true);
    }

    @Override
    protected int getGroupId(ReadPacket<TransferLeaderReq> frame) {
        return frame.getBody().groupId;
    }

    @Override
    protected WritePacket doProcess(ReqInfo<TransferLeaderReq> reqInfo) {
        ReadPacket<TransferLeaderReq> frame = reqInfo.reqFrame;
        TransferLeaderReq req = frame.getBody();
        if (req.oldLeaderId != raftServer.getServerConfig().nodeId) {
            log.error("old leader id mismatch, groupId={}, oldLeaderId={}, localId={}",
                    req.groupId, req.oldLeaderId, raftServer.getServerConfig().nodeId);
            throw new RaftException("new leader id mismatch");
        }
        CompletableFuture<Void> f = reqInfo.raftGroup.transferLeadership(req.newLeaderId,
                reqInfo.reqContext.getTimeout().getTimeout(TimeUnit.MILLISECONDS));
        f.whenComplete((v, ex) -> {
            if (ex != null) {
                log.error("transferLeadership failed, groupId={}, newLeaderId={}",
                        req.groupId, req.newLeaderId, ex);
                writeErrorResp(reqInfo, ex);
            } else {
                log.info("transferLeadership success, groupId={}, newLeaderId={}",
                        req.groupId, req.newLeaderId);
                reqInfo.reqContext.writeRespInBizThreads(new EmptyBodyRespPacket(CmdCodes.SUCCESS));
            }
        });
        return null;
    }


    @Override
    public DecoderCallback<TransferLeaderReq> createDecoderCallback(int command, DecodeContext context) {
        return context.toDecoderCallback(new TransferLeaderReq.Callback());
    }
}
