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

import com.github.dtprj.dongting.common.IntObjMap;
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
import com.github.dtprj.dongting.raft.impl.PendingMap;
import com.github.dtprj.dongting.raft.impl.RaftRole;
import com.github.dtprj.dongting.raft.impl.RaftStatus;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.GroupComponents;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.StateMachine;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * @author huangli
 */
public class AppendProcessor extends ReqProcessor {
    private static final DtLog log = DtLogs.getLogger(AppendProcessor.class);

    public static final int CODE_LOG_NOT_MATCH = 1;
    public static final int CODE_PREV_LOG_INDEX_LESS_THAN_LOCAL_COMMIT = 2;
    public static final int CODE_CLIENT_REQ_ERROR = 3;
    public static final int CODE_INSTALL_SNAPSHOT = 4;

    private final IntObjMap<GroupComponents> groupComponents;

    private static final PbZeroCopyDecoder decoder = new PbZeroCopyDecoder(c -> new AppendReqCallback());

    public AppendProcessor(IntObjMap<GroupComponents> groupComponents) {
        this.groupComponents = groupComponents;
    }

    @Override
    public WriteFrame process(ReadFrame rf, ChannelContext channelContext, ReqContext reqContext) {
        AppendRespWriteFrame resp = new AppendRespWriteFrame();
        AppendReqCallback req = (AppendReqCallback) rf.getBody();
        GroupComponents gc = RaftUtil.getGroupComponents(groupComponents, req.getGroupId());
        int remoteTerm = req.getTerm();
        RaftStatus raftStatus = gc.getRaftStatus();
        int localTerm = raftStatus.getCurrentTerm();
        if (remoteTerm == localTerm) {
            if (raftStatus.getRole() == RaftRole.follower) {
                RaftUtil.resetElectTimer(raftStatus);
                RaftUtil.updateLeader(raftStatus, req.getLeaderId());
                append(gc, raftStatus, req, resp);
            } else if (raftStatus.getRole() == RaftRole.candidate) {
                RaftUtil.changeToFollower(raftStatus, req.getLeaderId());
                append(gc, raftStatus, req, resp);
            } else {
                BugLog.getLog().error("leader receive raft append request. term={}, remote={}",
                        remoteTerm, channelContext.getRemoteAddr());
                resp.setSuccess(false);
            }
        } else if (remoteTerm > localTerm) {
            RaftUtil.incrTermAndConvertToFollower(remoteTerm, raftStatus, req.getLeaderId(), true);
            append(gc, raftStatus, req, resp);
        } else {
            log.debug("receive append request with a smaller term, ignore, remoteTerm={}, localTerm={}", remoteTerm, localTerm);
            resp.setSuccess(false);
        }
        resp.setTerm(raftStatus.getCurrentTerm());
        resp.setRespCode(CmdCodes.SUCCESS);
        return resp;
    }

    private void append(GroupComponents gc, RaftStatus raftStatus, AppendReqCallback req, AppendRespWriteFrame resp) {
        if (raftStatus.isInstallSnapshot()) {
            resp.setSuccess(false);
            resp.setAppendCode(CODE_INSTALL_SNAPSHOT);
            return;
        }
        gc.getVoteManager().cancelVote();
        if (req.getPrevLogIndex() != raftStatus.getLastLogIndex() || req.getPrevLogTerm() != raftStatus.getLastLogTerm()) {
            log.info("log not match. prevLogIndex={}, localLastLogIndex={}, prevLogTerm={}, localLastLogTerm={}, leaderId={}",
                    req.getPrevLogIndex(), raftStatus.getLastLogIndex(), req.getPrevLogTerm(), raftStatus.getLastLogTerm(), req.getLeaderId());
            resp.setSuccess(false);
            resp.setAppendCode(CODE_LOG_NOT_MATCH);
            resp.setMaxLogIndex(raftStatus.getLastLogIndex());
            resp.setMaxLogTerm(raftStatus.getLastLogTerm());
            return;
        }
        if (req.getPrevLogIndex() < raftStatus.getCommitIndex()) {
            BugLog.getLog().error("leader append request prevLogIndex less than local commit index. leaderId={}, prevLogIndex={}, commitIndex={}",
                    req.getLeaderId(), req.getPrevLogIndex(), raftStatus.getCommitIndex());
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

        RaftUtil.append(gc.getRaftLog(), raftStatus, req.getPrevLogIndex(), req.getPrevLogTerm(), logs);

        for (LogItem li : logs) {
            RaftInput raftInput = new RaftInput(li.getBuffer(), null, null, false);
            RaftTask task = new RaftTask(raftStatus.getTs(), li.getType(), raftInput, null);
            raftStatus.getPendingRequests().put(li.getIndex(), task);
        }

        long newIndex = req.getPrevLogIndex() + logs.size();
        raftStatus.setLastLogIndex(newIndex);
        raftStatus.setLastLogTerm(logs.get(logs.size() - 1).getTerm());
        if (req.getLeaderCommit() > raftStatus.getCommitIndex()) {
            raftStatus.setCommitIndex(Math.min(newIndex, req.getLeaderCommit()));
            apply(gc, raftStatus);
        } else if (req.getLeaderCommit() < raftStatus.getCommitIndex()) {
            log.warn("leader commitIndex less than local. leaderId={}, leaderTerm={}, leaderCommitIndex={}, localCommitIndex={}",
                    req.getLeaderId(), req.getTerm(), req.getLeaderCommit(), raftStatus.getCommitIndex());
        }
        resp.setSuccess(true);
    }

    private void apply(GroupComponents gc, RaftStatus raftStatus) {
        long diff = raftStatus.getCommitIndex() - raftStatus.getLastApplied();
        PendingMap pendingRequests = raftStatus.getPendingRequests();
        long lastApplied = raftStatus.getLastApplied();
        while (diff > 0) {
            long index = lastApplied+1;
            RaftTask rt = pendingRequests.get(index);
            if (rt != null) {
                apply(gc.getStateMachine(), rt.type, index, rt.input.getLogData());
                lastApplied++;
                diff--;
            } else {
                int limit = (int) Math.min(diff, 100L);
                LogItem[] items = RaftUtil.load(gc.getRaftLog(), raftStatus,
                        index, limit, 16 * 1024 * 1024);
                int readCount = items.length;
                for (LogItem item : items) {
                    apply(gc.getStateMachine(), item.getType(), index++, item.getBuffer());
                }
                lastApplied += readCount;
                diff -= readCount;
            }
        }
        raftStatus.setLastApplied(lastApplied);
    }

    private void apply(StateMachine stateMachine, int type, long index, ByteBuffer buffer) {
        if (type != LogItem.TYPE_HEARTBEAT) {
            Object decodedObj = stateMachine.decode(buffer);
            stateMachine.exec(index, decodedObj);
        }
    }

    @Override
    public Decoder getDecoder() {
        return decoder;
    }
}
