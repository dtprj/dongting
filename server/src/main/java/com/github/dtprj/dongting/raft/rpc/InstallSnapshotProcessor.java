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

import com.github.dtprj.dongting.codec.Decoder;
import com.github.dtprj.dongting.codec.PbNoCopyDecoder;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.ChannelContext;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.net.WriteFrame;
import com.github.dtprj.dongting.raft.impl.GroupComponents;
import com.github.dtprj.dongting.raft.impl.RaftGroupImpl;
import com.github.dtprj.dongting.raft.impl.RaftRole;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.RaftGroup;
import com.github.dtprj.dongting.raft.server.RaftServer;
import com.github.dtprj.dongting.raft.sm.StateMachine;

/**
 * @author huangli
 */
public class InstallSnapshotProcessor extends RaftGroupProcessor<InstallSnapshotReq> {

    private static final DtLog log = DtLogs.getLogger(InstallSnapshotProcessor.class);

    private static final Decoder<InstallSnapshotReq> DECODER = new PbNoCopyDecoder<>(c -> new InstallSnapshotReq.Callback(c.getHeapPool()));

    public InstallSnapshotProcessor(RaftServer raftServer) {
        super(false, raftServer);
    }

    @Override
    protected int getGroupId(ReadFrame<InstallSnapshotReq> frame) {
        return frame.getBody().groupId;
    }

    @Override
    protected WriteFrame doProcess(ReadFrame<InstallSnapshotReq> frame, ChannelContext channelContext,
                                   ReqContext reqContext, RaftGroup rg) {
        GroupComponents gc = ((RaftGroupImpl) rg).getGroupComponents();
        RaftStatusImpl raftStatus = gc.getRaftStatus();
        Runnable reprocess = () -> doProcess(frame, channelContext, reqContext, rg);
        if (raftStatus.isWaitAppend()) {
            raftStatus.getWaitAppendQueue().addLast(reprocess);
            return null;
        }
        InstallSnapshotReq req = frame.getBody();
        try {
            InstallSnapshotResp resp = new InstallSnapshotResp();
            InstallSnapshotResp.InstallRespWriteFrame respFrame = new InstallSnapshotResp.InstallRespWriteFrame(resp);
            int remoteTerm = req.term;

            if (gc.getMemberManager().checkLeader(req.leaderId)) {
                int localTerm = raftStatus.getCurrentTerm();
                if (remoteTerm == localTerm) {
                    if (raftStatus.getRole() == RaftRole.follower) {
                        RaftUtil.resetElectTimer(raftStatus);
                        RaftUtil.updateLeader(raftStatus, req.leaderId);
                        if (AppendProcessor.hangIfWriting(raftStatus, reprocess)) {
                            return null;
                        }
                        installSnapshot(raftStatus, gc.getStateMachine(), req, resp);
                    } else if (raftStatus.getRole() == RaftRole.observer) {
                        RaftUtil.updateLeader(raftStatus, req.leaderId);
                        if (AppendProcessor.hangIfWriting(raftStatus, reprocess)) {
                            return null;
                        }
                        installSnapshot(raftStatus, gc.getStateMachine(), req, resp);
                    } else if (raftStatus.getRole() == RaftRole.candidate) {
                        RaftUtil.changeToFollower(raftStatus, req.leaderId);
                        if (AppendProcessor.hangIfWriting(raftStatus, reprocess)) {
                            return null;
                        }
                        installSnapshot(raftStatus, gc.getStateMachine(), req, resp);
                    } else {
                        BugLog.getLog().error("leader receive raft install snapshot request. term={}, remote={}",
                                remoteTerm, channelContext.getRemoteAddr());
                        resp.success = false;
                    }
                } else if (remoteTerm > localTerm) {
                    RaftUtil.incrTerm(remoteTerm, raftStatus, req.leaderId);
                    gc.getStatusManager().persistSync();
                    if (AppendProcessor.hangIfWriting(raftStatus, reprocess)) {
                        return null;
                    }
                    installSnapshot(raftStatus, gc.getStateMachine(), req, resp);
                } else {
                    log.info("receive raft install snapshot request with a smaller term, ignore, remoteTerm={}, localTerm={}", remoteTerm, localTerm);
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
        } finally {
            if (req.data != null) {
                req.data.release();
            }
        }
    }

    private void installSnapshot(RaftStatusImpl raftStatus, StateMachine stateMachine,
                                 InstallSnapshotReq req, InstallSnapshotResp resp) {
        boolean start = req.offset == 0;
        boolean finish = req.done;
        if (start) {
            raftStatus.setInstallSnapshot(true);
        }
        try {
            stateMachine.installSnapshot(req.lastIncludedIndex, req.lastIncludedTerm, req.offset, finish, req.data);
            raftStatus.setLastLogTerm(req.lastIncludedTerm);
            raftStatus.setLastLogIndex(req.lastIncludedIndex);
            raftStatus.setLastPersistLogIndex(req.lastIncludedIndex);
            resp.success = true;
            if (finish) {
                raftStatus.setInstallSnapshot(false);
                raftStatus.setLastApplied(req.lastIncludedIndex);
                raftStatus.setCommitIndex(req.lastIncludedIndex);
            }
        } catch (Exception e) {
            log.error("install snapshot error", e);
            resp.success = false;
        } finally {
            if (req.data != null) {
                req.data.release();
            }
        }
    }

    @Override
    public Decoder<InstallSnapshotReq> createDecoder() {
        return DECODER;
    }
}
