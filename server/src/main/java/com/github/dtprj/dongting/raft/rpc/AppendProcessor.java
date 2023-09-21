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
import com.github.dtprj.dongting.common.Pair;
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
import com.github.dtprj.dongting.raft.impl.RaftGroups;
import com.github.dtprj.dongting.raft.impl.RaftRole;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroup;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftServer;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class AppendProcessor extends RaftGroupProcessor<AppendReqCallback> {
    private static final DtLog log = DtLogs.getLogger(AppendProcessor.class);

    public static final int CODE_LOG_NOT_MATCH = 1;
    public static final int CODE_PREV_LOG_INDEX_LESS_THAN_LOCAL_COMMIT = 2;
    public static final int CODE_REQ_ERROR = 3;
    public static final int CODE_INSTALL_SNAPSHOT = 4;
    public static final int CODE_NOT_MEMBER_IN_GROUP = 5;
    public static final int CODE_SERVER_ERROR = 6;

    private final PbNoCopyDecoder<AppendReqCallback> decoder;

    public static String getCodeStr(int code) {
        switch (code) {
            case CODE_LOG_NOT_MATCH:
                return "CODE_LOG_NOT_MATCH";
            case CODE_PREV_LOG_INDEX_LESS_THAN_LOCAL_COMMIT:
                return "CODE_PREV_LOG_INDEX_LESS_THAN_LOCAL_COMMIT";
            case CODE_REQ_ERROR:
                return "CODE_REQ_ERROR";
            case CODE_INSTALL_SNAPSHOT:
                return "CODE_INSTALL_SNAPSHOT";
            case CODE_NOT_MEMBER_IN_GROUP:
                return "CODE_NOT_MEMBER_IN_GROUP";
            case CODE_SERVER_ERROR:
                return "CODE_SERVER_ERROR";
            default:
                return "CODE_UNKNOWN_" + code;
        }
    }

    public AppendProcessor(RaftServer raftServer, RaftGroups raftGroups) {
        super(false, raftServer);
        decoder = new PbNoCopyDecoder<>(decodeContext -> new AppendReqCallback(decodeContext, raftGroups));
    }

    @Override
    protected int getGroupId(ReadFrame<AppendReqCallback> frame) {
        return frame.getBody().getGroupId();
    }

    @Override
    protected WriteFrame doProcess(ReadFrame<AppendReqCallback> rf, ChannelContext channelContext,
                                   ReqContext reqContext, RaftGroup rg) {
        AppendRespWriteFrame resp = new AppendRespWriteFrame();
        AppendReqCallback req = rf.getBody();
        GroupComponents gc = ((RaftGroupImpl) rg).getGroupComponents();
        RaftStatusImpl raftStatus = gc.getRaftStatus();
        if (gc.getMemberManager().checkLeader(req.getLeaderId())) {
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
                gc.getStatusManager().persistSync();
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
        return resp;
    }

    @SuppressWarnings("ForLoopReplaceableByForEach")
    private void append(GroupComponents gc, RaftStatusImpl raftStatus, AppendReqCallback req, AppendRespWriteFrame resp) {
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
            int currentTerm = raftStatus.getCurrentTerm();
            Supplier<Boolean> cancelIndicator = () -> raftStatus.getCurrentTerm() != currentTerm;
            CompletableFuture<Pair<Integer, Long>> replicatePos = gc.getRaftLog().tryFindMatchPos(
                    req.getPrevLogTerm(), req.getPrevLogIndex(), cancelIndicator);
            try {
                Pair<Integer, Long> pos = replicatePos.get();
                if (pos == null) {
                    log.info("follower has no suggest index, will install snapshot. groupId={}", raftStatus.getGroupId());
                    resp.setSuccess(false);
                    resp.setAppendCode(CODE_LOG_NOT_MATCH);
                    resp.setSuggestTerm(0);
                    resp.setSuggestIndex(0);
                    return;
                } else if (pos.getLeft() == req.getPrevLogTerm() && pos.getRight() == req.getPrevLogIndex()) {
                    log.info("local log truncate to prevLogIndex={}, prevLogTerm={}, groupId={}",
                            req.getPrevLogIndex(), req.getPrevLogTerm(), raftStatus.getGroupId());
                    // not return here
                } else {
                    log.info("follower suggest term={}, index={}, groupId={}", pos.getLeft(), pos.getRight(), raftStatus.getGroupId());
                    resp.setSuccess(false);
                    resp.setAppendCode(CODE_LOG_NOT_MATCH);
                    resp.setSuggestTerm(pos.getLeft());
                    resp.setSuggestIndex(pos.getRight());
                    return;
                }
            } catch (Exception ex) {
                log.error("find replicate pos error", ex);
                resp.setSuccess(false);
                resp.setAppendCode(CODE_SERVER_ERROR);
                return;
            }
        }
        if (req.getPrevLogIndex() < raftStatus.getCommitIndex()) {
            BugLog.getLog().error("leader append request prevLogIndex less than local commit index. leaderId={}, prevLogIndex={}, commitIndex={}, groupId={}",
                    req.getLeaderId(), req.getPrevLogIndex(), raftStatus.getCommitIndex(), raftStatus.getGroupId());
            resp.setSuccess(false);
            resp.setAppendCode(CODE_PREV_LOG_INDEX_LESS_THAN_LOCAL_COMMIT);
            return;
        }
        ArrayList<LogItem> logs = req.getLogs();
        if (logs == null || logs.isEmpty()) {
            log.error("bad request: no logs");
            resp.setSuccess(false);
            resp.setAppendCode(CODE_REQ_ERROR);
            return;
        }

        RaftUtil.append(gc.getRaftLog(), raftStatus, logs);

        for (int i = 0; i < logs.size(); i++) {
            LogItem li = logs.get(i);
            RaftInput raftInput = new RaftInput(li.getBizType(), li.getHeader(), li.getBody(), null, li.getActualBodySize());
            RaftTask task = new RaftTask(raftStatus.getTs(), li.getType(), raftInput, null);
            task.setItem(li);
            raftStatus.getTailCache().put(li.getIndex(), task);
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
    public Decoder<AppendReqCallback> createDecoder() {
        return decoder;
    }
}
