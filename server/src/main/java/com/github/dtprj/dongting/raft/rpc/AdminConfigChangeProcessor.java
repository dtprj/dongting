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
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.PbLongWritePacket;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.raft.server.NotLeaderException;
import com.github.dtprj.dongting.raft.server.RaftGroup;
import com.github.dtprj.dongting.raft.server.RaftProcessor;
import com.github.dtprj.dongting.raft.server.RaftServer;
import com.github.dtprj.dongting.raft.server.ReqInfo;

import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
public class AdminConfigChangeProcessor extends RaftProcessor<Object> {

    private static final DtLog log = DtLogs.getLogger(AdminConfigChangeProcessor.class);

    public AdminConfigChangeProcessor(RaftServer raftServer) {
        super(raftServer, false, true);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    protected int getGroupId(ReadPacket frame) {
        if (frame.command == Commands.RAFT_ADMIN_PREPARE_CHANGE) {
            ReadPacket<AdminPrepareConfigChangeReq> f = (ReadPacket<AdminPrepareConfigChangeReq>) frame;
            AdminPrepareConfigChangeReq req = f.getBody();
            return req.groupId;
        } else {
            ReadPacket<AdminCommitOrAbortReq> f = (ReadPacket<AdminCommitOrAbortReq>) frame;
            AdminCommitOrAbortReq req = f.getBody();
            return req.groupId;
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public DecoderCallback createDecoderCallback(int command, DecodeContext context) {
        if (command == Commands.RAFT_ADMIN_PREPARE_CHANGE) {
            return context.toDecoderCallback(new AdminPrepareConfigChangeReq.Callback());
        } else {
            return context.toDecoderCallback(new AdminCommitOrAbortReq());
        }
    }

    @SuppressWarnings({"rawtypes"})
    @Override
    protected WritePacket doProcess(ReqInfo reqInfo) {
        ReadPacket reqFrame = reqInfo.reqFrame;
        RaftGroup rg = reqInfo.raftGroup;
        String type;
        CompletableFuture<Long> f;
        if (reqFrame.command == Commands.RAFT_ADMIN_PREPARE_CHANGE) {
            type = "prepare";
            AdminPrepareConfigChangeReq req = (AdminPrepareConfigChangeReq) reqFrame.getBody();
            f = rg.leaderPrepareJointConsensus(req.members, req.observers, req.preparedMembers, req.preparedObservers);
        } else if (reqFrame.command == Commands.RAFT_ADMIN_COMMIT_CHANGE) {
            type = "commit";
            AdminCommitOrAbortReq req = (AdminCommitOrAbortReq) reqFrame.getBody();
            f = rg.leaderCommitJointConsensus(req.prepareIndex);
        } else {
            type = "abort";
            f = rg.leaderAbortJointConsensus();
        }
        f.whenComplete((index, ex) -> {
            if (ex != null) {
                if (ex instanceof NotLeaderException) {
                    log.warn("Admin {} config change failed, groupId={}, not leader", type, rg.getGroupId());
                } else {
                    log.error("Admin {} config change failed, groupId={}", type, rg.getGroupId(), ex);
                }
                writeErrorResp(reqInfo, ex);
            } else {
                log.info("Admin {} config change success, groupId={}, resultIndex={}", type, rg.getGroupId(), index);
                reqInfo.reqContext.writeRespInBizThreads(new PbLongWritePacket(index));
            }
        });
        return null;
    }
}
