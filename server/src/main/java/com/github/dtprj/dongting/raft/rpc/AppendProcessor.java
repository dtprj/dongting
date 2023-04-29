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

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.codec.Decoder;
import com.github.dtprj.dongting.codec.PbZeroCopyDecoder;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.ChannelContext;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.net.WriteFrame;
import com.github.dtprj.dongting.raft.impl.RaftGroupImpl;
import com.github.dtprj.dongting.raft.impl.RaftGroups;
import com.github.dtprj.dongting.raft.impl.RaftRole;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftInput;

import java.util.ArrayList;

/**
 * @author huangli
 */
public class AppendProcessor extends AbstractProcessor<AppendReqCallback> {
    private static final DtLog log = DtLogs.getLogger(AppendProcessor.class);

    public static final int CODE_LOG_NOT_MATCH = 1;
    public static final int CODE_PREV_LOG_INDEX_LESS_THAN_LOCAL_COMMIT = 2;
    public static final int CODE_CLIENT_REQ_ERROR = 3;
    public static final int CODE_INSTALL_SNAPSHOT = 4;
    public static final int CODE_NOT_MEMBER_IN_GROUP = 5;
    public static final int CODE_ERROR_STATE = 6;

    private static final PbZeroCopyDecoder<AppendReqCallback> decoder = new PbZeroCopyDecoder<>(AppendReqCallback::new);

    public AppendProcessor(RaftGroups raftGroups) {
        super(raftGroups);
    }

    @Override
    protected int getGroupId(ReadFrame<AppendReqCallback> frame) {
        return frame.getBody().getGroupId();
    }

    @Override
    protected WriteFrame doProcess(ReadFrame<AppendReqCallback> rf, ChannelContext channelContext, RaftGroupImpl gc) {
        AppendRespWriteFrame resp = new AppendRespWriteFrame();
        AppendReqCallback req = rf.getBody();
        RaftStatusImpl raftStatus = gc.getRaftStatus();
        if (raftStatus.isError()) {
            resp.setSuccess(false);
            resp.setAppendCode(CODE_ERROR_STATE);
        } else if (gc.getMemberManager().checkLeader(req.getLeaderId())) {
            int remoteTerm = req.getTerm();
            int localTerm = raftStatus.getCurrentTerm();
            if (remoteTerm == localTerm) {
                if (raftStatus.getRole() == RaftRole.follower) {
                    RaftUtil.resetElectTimer(raftStatus);
                    RaftUtil.updateLeader(raftStatus, req.getLeaderId());
                    append(gc, raftStatus, req, resp);
                } else if (raftStatus.getRole() == RaftRole.observer) {
                    RaftUtil.updateLeader(raftStatus, req.getLeaderId());
                    append(gc, raftStatus, req, resp);
                } else if (raftStatus.getRole() == RaftRole.candidate) {
                    RaftUtil.changeToFollower(raftStatus, req.getLeaderId());
                    append(gc, raftStatus, req, resp);
                } else {
                    BugLog.getLog().error("leader receive raft append request. term={}, remote={}, groupId={}",
                            remoteTerm, channelContext.getRemoteAddr(), raftStatus.getGroupId());
                    resp.setSuccess(false);
                }
            } else if (remoteTerm > localTerm) {
                RaftUtil.incrTerm(remoteTerm, raftStatus, req.getLeaderId());
                append(gc, raftStatus, req, resp);
            } else {
                log.debug("receive append request with a smaller term, ignore, remoteTerm={}, localTerm={}, groupId={}",
                        remoteTerm, localTerm, raftStatus.getGroupId());
                resp.setSuccess(false);
            }
        } else {
            log.warn("receive append request from a non-member, ignore, remoteId={}, groupId={}, remote={}",
                    req.getLeaderId(), req.getGroupId(), channelContext.getRemoteAddr());
            resp.setSuccess(false);
            resp.setAppendCode(CODE_NOT_MEMBER_IN_GROUP);
        }

        resp.setTerm(raftStatus.getCurrentTerm());
        resp.setRespCode(CmdCodes.SUCCESS);
        ArrayList<LogItem> logs = req.getLogs();
        if (logs != null) {
            //noinspection ForLoopReplaceableByForEach
            for (int i = 0; i < logs.size(); i++) {
                RefBuffer b = logs.get(i).getBuffer();
                if (b != null) {
                    b.release();
                }
            }
        }
        return resp;
    }

    @SuppressWarnings("ForLoopReplaceableByForEach")
    private void append(RaftGroupImpl gc, RaftStatusImpl raftStatus, AppendReqCallback req, AppendRespWriteFrame resp) {
        if (raftStatus.isInstallSnapshot()) {
            resp.setSuccess(false);
            resp.setAppendCode(CODE_INSTALL_SNAPSHOT);
            return;
        }
        gc.getVoteManager().cancelVote();
        if (req.getPrevLogIndex() != raftStatus.getLastLogIndex() || req.getPrevLogTerm() != raftStatus.getLastLogTerm()) {
            log.info("log not match. prevLogIndex={}, localLastLogIndex={}, prevLogTerm={}, localLastLogTerm={}, leaderId={}, groupId={}",
                    req.getPrevLogIndex(), raftStatus.getLastLogIndex(), req.getPrevLogTerm(),
                    raftStatus.getLastLogTerm(), req.getLeaderId(), raftStatus.getGroupId());
            resp.setSuccess(false);
            resp.setAppendCode(CODE_LOG_NOT_MATCH);
            resp.setMaxLogIndex(raftStatus.getLastLogIndex());
            resp.setMaxLogTerm(raftStatus.getLastLogTerm());
            return;
        }
        if (req.getPrevLogIndex() < raftStatus.getCommitIndex()) {
            BugLog.getLog().error("leader append request prevLogIndex less than local commit index. leaderId={}, prevLogIndex={}, commitIndex={}, groupId={}",
                    req.getLeaderId(), req.getPrevLogIndex(), raftStatus.getCommitIndex(), raftStatus.getGroupId());
            resp.setSuccess(false);
            resp.setAppendCode(CODE_PREV_LOG_INDEX_LESS_THAN_LOCAL_COMMIT);
            return;
        }
        ArrayList<LogItem> logs = req.getLogs();
        if (logs == null || logs.size() == 0) {
            log.error("bad request: no logs");
            resp.setSuccess(false);
            resp.setAppendCode(CODE_CLIENT_REQ_ERROR);
            return;
        }

        RaftUtil.append(gc.getRaftLog(), raftStatus, logs);

        for (int i = 0; i < logs.size(); i++) {
            LogItem li = logs.get(i);
            RaftInput raftInput = new RaftInput(li.getBuffer(), null, null, false);
            RaftTask task = new RaftTask(raftStatus.getTs(), li.getType(), raftInput, null);
            RaftUtil.retain(raftInput);
            raftStatus.getPendingRequests().put(li.getIndex(), task);
        }

        long newIndex = req.getPrevLogIndex() + logs.size();
        raftStatus.setLastLogIndex(newIndex);
        raftStatus.setLastLogTerm(logs.get(logs.size() - 1).getTerm());
        if (req.getLeaderCommit() > raftStatus.getCommitIndex()) {
            raftStatus.setCommitIndex(Math.min(newIndex, req.getLeaderCommit()));
            gc.getApplyManager().apply(raftStatus);
        } else if (req.getLeaderCommit() < raftStatus.getCommitIndex()) {
            log.info("leader commitIndex less than local, maybe leader restart recently. leaderId={}, leaderTerm={}, leaderCommitIndex={}, localCommitIndex={}, groupId={}",
                    req.getLeaderId(), req.getTerm(), req.getLeaderCommit(), raftStatus.getCommitIndex(), raftStatus.getGroupId());
        }
        resp.setSuccess(true);
    }

    @Override
    public Decoder<AppendReqCallback> getDecoder() {
        return decoder;
    }
}
