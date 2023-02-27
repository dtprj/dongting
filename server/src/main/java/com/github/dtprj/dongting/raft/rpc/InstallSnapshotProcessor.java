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

import com.github.dtprj.dongting.log.BugLog;
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
import com.github.dtprj.dongting.raft.impl.RaftRole;
import com.github.dtprj.dongting.raft.impl.RaftStatus;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.StateMachine;

/**
 * @author huangli
 */
public class InstallSnapshotProcessor extends ReqProcessor {

    private static final DtLog log = DtLogs.getLogger(InstallSnapshotProcessor.class);

    private static final Decoder DECODER = new PbZeroCopyDecoder(c -> new InstallSnapshotReq.Callback());
    private final RaftStatus raftStatus;
    private final StateMachine stateMachine;

    public InstallSnapshotProcessor(RaftStatus raftStatus, StateMachine stateMachine) {
        this.raftStatus = raftStatus;
        this.stateMachine = stateMachine;
    }

    @Override
    public WriteFrame process(ReadFrame frame, ChannelContext channelContext, ReqContext reqContext) {
        InstallSnapshotReq req = (InstallSnapshotReq) frame.getBody();
        InstallSnapshotResp resp = new InstallSnapshotResp();
        InstallSnapshotResp.WriteFrame respFrame = new InstallSnapshotResp.WriteFrame(resp);
        int remoteTerm = req.term;
        RaftStatus raftStatus = this.raftStatus;
        int localTerm = raftStatus.getCurrentTerm();
        if (remoteTerm == localTerm) {
            if (raftStatus.getRole() == RaftRole.follower) {
                RaftUtil.resetElectTimer(raftStatus);
                RaftUtil.updateLeader(raftStatus, req.leaderId);
                installSnapshot(req, resp);
            } else if (raftStatus.getRole() == RaftRole.candidate) {
                RaftUtil.changeToFollower(raftStatus, req.leaderId);
                installSnapshot(req, resp);
            } else {
                BugLog.getLog().error("leader receive raft install snapshot request. term={}, remote={}",
                        remoteTerm, channelContext.getRemoteAddr());
                resp.success = false;
            }
        } else if (remoteTerm > localTerm) {
            RaftUtil.incrTermAndConvertToFollower(remoteTerm, raftStatus, req.leaderId);
            installSnapshot(req, resp);
        } else {
            log.debug("receive raft install snapshot request with a smaller term, ignore, remoteTerm={}, localTerm={}", remoteTerm, localTerm);
            resp.success = false;
        }
        resp.term = raftStatus.getCurrentTerm();
        respFrame.setRespCode(CmdCodes.SUCCESS);
        return respFrame;
    }

    private void installSnapshot(InstallSnapshotReq req, InstallSnapshotResp resp) {
        boolean start = req.offset == 0;
        boolean finish = req.done;
        if (start) {
            raftStatus.setInstallSnapshot(true);
            raftStatus.setLastLogTerm(req.lastIncludedTerm);
            raftStatus.setLastLogIndex(req.lastIncludedIndex);
        }
        try {
            stateMachine.installSnapshot(start, finish, req.data);
            resp.success = true;
            if (finish) {
                raftStatus.setInstallSnapshot(false);
                raftStatus.setLastApplied(req.lastIncludedIndex);
            }
        } catch (Exception e) {
            log.error("install snapshot error", e);
            resp.success = false;
        }
    }

    @Override
    public Decoder getDecoder() {
        return DECODER;
    }
}
