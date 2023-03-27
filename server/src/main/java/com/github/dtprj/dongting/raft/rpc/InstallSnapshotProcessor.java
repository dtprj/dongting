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
import com.github.dtprj.dongting.net.WriteFrame;
import com.github.dtprj.dongting.raft.impl.GroupComponents;
import com.github.dtprj.dongting.raft.impl.GroupComponentsMap;
import com.github.dtprj.dongting.raft.impl.RaftRole;
import com.github.dtprj.dongting.raft.impl.RaftStatus;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.StateMachine;

/**
 * @author huangli
 */
public class InstallSnapshotProcessor extends AbstractProcessor {

    private static final DtLog log = DtLogs.getLogger(InstallSnapshotProcessor.class);

    private static final Decoder DECODER = new PbZeroCopyDecoder(c -> new InstallSnapshotReq.Callback());

    public InstallSnapshotProcessor(GroupComponentsMap groupComponentsMap) {
        super(groupComponentsMap);
    }

    @Override
    protected int getGroupId(ReadFrame frame) {
        return ((InstallSnapshotReq) frame.getBody()).groupId;
    }

    @Override
    protected WriteFrame doProcess(ReadFrame frame, ChannelContext channelContext, GroupComponents gc) {
        InstallSnapshotReq req = (InstallSnapshotReq) frame.getBody();
        InstallSnapshotResp resp = new InstallSnapshotResp();
        InstallSnapshotResp.WriteFrame respFrame = new InstallSnapshotResp.WriteFrame(resp);
        int remoteTerm = req.term;
        RaftStatus raftStatus = gc.getRaftStatus();

        if (raftStatus.isError()) {
            resp.success = false;
        } else if (gc.getMemberManager().checkLeader(req.leaderId)) {
            int localTerm = raftStatus.getCurrentTerm();
            if (remoteTerm == localTerm) {
                if (raftStatus.getRole() == RaftRole.follower) {
                    RaftUtil.resetElectTimer(raftStatus);
                    RaftUtil.updateLeader(raftStatus, req.leaderId);
                    installSnapshot(raftStatus, gc.getStateMachine(), req, resp);
                } else if (raftStatus.getRole() == RaftRole.observer) {
                    RaftUtil.updateLeader(raftStatus, req.leaderId);
                    installSnapshot(raftStatus, gc.getStateMachine(), req, resp);
                } else if (raftStatus.getRole() == RaftRole.candidate) {
                    RaftUtil.changeToFollower(raftStatus, req.leaderId);
                    installSnapshot(raftStatus, gc.getStateMachine(), req, resp);
                } else {
                    BugLog.getLog().error("leader receive raft install snapshot request. term={}, remote={}",
                            remoteTerm, channelContext.getRemoteAddr());
                    resp.success = false;
                }
            } else if (remoteTerm > localTerm) {
                RaftUtil.incrTerm(remoteTerm, raftStatus, req.leaderId);
                installSnapshot(raftStatus, gc.getStateMachine(), req, resp);
            } else {
                log.debug("receive raft install snapshot request with a smaller term, ignore, remoteTerm={}, localTerm={}", remoteTerm, localTerm);
                resp.success = false;
            }
        } else {
            resp.success = false;
            log.warn("receive raft install snapshot request from a non-member, ignore. remoteId={}, group={}, remote={}",
                    req.leaderId, req.groupId, channelContext.getRemoteAddr());
        }

        resp.term = raftStatus.getCurrentTerm();
        respFrame.setRespCode(CmdCodes.SUCCESS);
        return respFrame;
    }

    private void installSnapshot(RaftStatus raftStatus, StateMachine stateMachine, InstallSnapshotReq req, InstallSnapshotResp resp) {
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
