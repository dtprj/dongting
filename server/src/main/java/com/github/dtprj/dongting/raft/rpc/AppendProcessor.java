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
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.ChannelContext;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.raft.impl.GroupComponents;
import com.github.dtprj.dongting.raft.impl.LinearTaskRunner;
import com.github.dtprj.dongting.raft.impl.RaftGroupImpl;
import com.github.dtprj.dongting.raft.impl.RaftGroups;
import com.github.dtprj.dongting.raft.impl.RaftRole;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.impl.TailCache;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftServer;

import java.util.ArrayList;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class AppendProcessor extends RaftGroupProcessor<AppendReqCallback> {
    private static final DtLog log = DtLogs.getLogger(AppendProcessor.class);

    public static final int CODE_SUCCESS = 0;
    public static final int CODE_LOG_NOT_MATCH = 1;
    public static final int CODE_PREV_LOG_INDEX_LESS_THAN_LOCAL_COMMIT = 2;
    public static final int CODE_REQ_ERROR = 3;
    public static final int CODE_INSTALL_SNAPSHOT = 4;
    public static final int CODE_NOT_MEMBER_IN_GROUP = 5;
    public static final int CODE_SERVER_ERROR = 6;

    private final PbNoCopyDecoder<AppendReqCallback> decoder;

    public AppendProcessor(RaftServer raftServer, RaftGroups raftGroups) {
        super(raftServer);
        this.decoder = new PbNoCopyDecoder<>(decodeContext -> new AppendReqCallback(decodeContext, raftGroups));
    }

    public static String getCodeStr(int code) {
        switch (code) {
            case CODE_SUCCESS:
                return "CODE_SUCCESS";
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

    @Override
    protected int getGroupId(ReadFrame<AppendReqCallback> frame) {
        return frame.getBody().getGroupId();
    }

    @Override
    public Decoder<AppendReqCallback> createDecoder() {
        return decoder;
    }

    private FrameCallResult writeResp(AppendContext ctx, int code) {
        AppendRespWriteFrame resp = new AppendRespWriteFrame();
        resp.setTerm(ctx.gc.getRaftStatus().getCurrentTerm());
        if (code == CODE_SUCCESS) {
            resp.setSuccess(true);
        } else {
            resp.setSuccess(false);
            resp.setAppendCode(code);
        }
        resp.setRespCode(CmdCodes.SUCCESS);
        resp.setSuggestTerm(ctx.suggestTerm);
        resp.setSuggestIndex(ctx.suggestIndex);
        writeResp(ctx, resp);
        return Fiber.frameReturn();
    }


    @Override
    protected ReqInfo<AppendReqCallback> createReqInfo(ReadFrame<AppendReqCallback> reqFrame, ChannelContext channelContext, ReqContext reqContext, RaftGroupImpl raftGroup) {
        return new AppendContext(reqFrame, channelContext, reqContext, raftGroup);
    }

    @Override
    protected FiberFrame<Void> doProcess(ReqInfo<AppendReqCallback> reqInfo) {
        return new AppendFiberFrame((AppendContext) reqInfo);
    }

    class AppendContext extends RaftGroupProcessor.ReqInfo<AppendReqCallback> {
        final GroupComponents gc;
        int suggestTerm;
        long suggestIndex;

        AppendContext(ReadFrame<AppendReqCallback> reqFrame, ChannelContext channelContext,
                      ReqContext reqContext, RaftGroupImpl raftGroup) {
            super(reqFrame, channelContext, reqContext, raftGroup);
            this.gc = raftGroup.getGroupComponents();
        }
    }

    class AppendFiberFrame extends FiberFrame<Void> {

        private final AppendContext ctx;

        public AppendFiberFrame(AppendContext ctx) {
            this.ctx = ctx;
        }

        @Override
        protected FrameCallResult handle(Throwable ex) {
            log.error("find replicate pos error", ex);
            writeResp(ctx, CODE_SERVER_ERROR);
            return Fiber.frameReturn();
        }

        @Override
        public FrameCallResult execute(Void input) {
            AppendReqCallback req = ctx.getReqFrame().getBody();
            GroupComponents gc = ctx.gc;
            RaftStatusImpl raftStatus = gc.getRaftStatus();
            if (gc.getMemberManager().checkLeader(req.getLeaderId())) {
                int remoteTerm = req.getTerm();
                int localTerm = raftStatus.getCurrentTerm();
                if (remoteTerm == localTerm) {
                    if (raftStatus.getRole() == RaftRole.follower) {
                        RaftUtil.resetElectTimer(raftStatus);
                        RaftUtil.updateLeader(raftStatus, req.getLeaderId());
                        return append(ctx);
                    } else if (raftStatus.getRole() == RaftRole.observer) {
                        RaftUtil.updateLeader(raftStatus, req.getLeaderId());
                        return append(ctx);
                    } else if (raftStatus.getRole() == RaftRole.candidate) {
                        RaftUtil.changeToFollower(raftStatus, req.getLeaderId());
                        return append(ctx);
                    } else {
                        req.clean();
                        log.error("leader receive raft append request. term={}, remote={}, groupId={}",
                                remoteTerm, ctx.getChannelContext().getRemoteAddr(), raftStatus.getGroupId());
                        return writeResp(ctx, CODE_REQ_ERROR);
                    }
                } else if (remoteTerm > localTerm) {
                    RaftUtil.incrTerm(remoteTerm, raftStatus, req.getLeaderId());
                    gc.getStatusManager().persistSync();
                    return append(ctx);
                } else {
                    log.debug("receive append request with a smaller term, ignore, remoteTerm={}, localTerm={}, groupId={}",
                            remoteTerm, localTerm, raftStatus.getGroupId());
                    return writeResp(ctx, CODE_REQ_ERROR);
                }
            } else {
                log.warn("receive append request from a non-member, ignore, remoteId={}, groupId={}, remote={}",
                        req.getLeaderId(), req.getGroupId(), ctx.getChannelContext().getRemoteAddr());
                return writeResp(ctx, CODE_NOT_MEMBER_IN_GROUP);
            }
        }

        private FrameCallResult append(AppendContext ctx) {
            AppendReqCallback req = ctx.getReqFrame().getBody();
            GroupComponents gc = ctx.gc;
            RaftStatusImpl raftStatus = gc.getRaftStatus();
            if (ctx.getReqContext().getTimeout().isTimeout(raftStatus.getTs())) {
                // not generate response
                return Fiber.frameReturn();
            }
            gc.getVoteManager().cancelVote();
            if (raftStatus.isInstallSnapshot()) {
                return writeResp(ctx, CODE_INSTALL_SNAPSHOT);
            }
            if (req.getPrevLogIndex() != raftStatus.getLastLogIndex() || req.getPrevLogTerm() != raftStatus.getLastLogTerm()) {
                log.info("log not match. prevLogIndex={}, localLastLogIndex={}, prevLogTerm={}, localLastLogTerm={}, leaderId={}, groupId={}",
                        req.getPrevLogIndex(), raftStatus.getLastLogIndex(), req.getPrevLogTerm(),
                        raftStatus.getLastLogTerm(), req.getLeaderId(), raftStatus.getGroupId());
                int currentTerm = raftStatus.getCurrentTerm();
                Supplier<Boolean> cancelIndicator = () -> raftStatus.getCurrentTerm() != currentTerm;
                FiberFrame<Pair<Integer, Long>> replicatePosFrame = gc.getRaftLog().tryFindMatchPos(
                        req.getPrevLogTerm(), req.getPrevLogIndex(), cancelIndicator);
                return Fiber.call(replicatePosFrame, pos -> resumeWhenFindReplicatePosFinish(pos, currentTerm));
            }
            return doAppend(ctx);
        }

        private FrameCallResult doAppend(AppendContext ctx) {
            AppendReqCallback req = ctx.getReqFrame().getBody();
            GroupComponents gc = ctx.gc;
            RaftStatusImpl raftStatus = gc.getRaftStatus();
            if (req.getPrevLogIndex() < raftStatus.getCommitIndex()) {
                BugLog.getLog().error("leader append request prevLogIndex less than local commit index. leaderId={}, prevLogIndex={}, commitIndex={}, groupId={}",
                        req.getLeaderId(), req.getPrevLogIndex(), raftStatus.getCommitIndex(), raftStatus.getGroupId());
                return writeResp(ctx, CODE_PREV_LOG_INDEX_LESS_THAN_LOCAL_COMMIT);
            }
            ArrayList<LogItem> logs = req.getLogs();
            if (logs == null || logs.isEmpty()) {
                log.error("bad request: no logs");
                return writeResp(ctx, CODE_REQ_ERROR);
            }

            long index = LinearTaskRunner.lastIndex(raftStatus);

            TailCache tailCache = raftStatus.getTailCache();
            for (int i = 0; i < logs.size(); i++) {
                LogItem li = logs.get(i);
                if (++index != li.getIndex()) {
                    log.error("bad request: log index not match. index={}, expectIndex={}, leaderId={}, groupId={}",
                            li.getIndex(), index, req.getLeaderId(), raftStatus.getGroupId());
                    return writeResp(ctx, CODE_REQ_ERROR);
                }
                RaftInput raftInput = new RaftInput(li.getBizType(), li.getHeader(), li.getBody(), null, li.getActualBodySize());
                RaftTask task = new RaftTask(raftStatus.getTs(), li.getType(), raftInput, null);
                task.setItem(li);
                tailCache.put(li.getIndex(), task);
                if (i == logs.size() - 1) {
                    raftStatus.setLastLogIndex(li.getIndex());
                    raftStatus.setLastLogTerm(li.getTerm());
                    int term = raftStatus.getCurrentTerm();
                    long itemIndex = li.getIndex();
                    gc.getCommitManager().registerRespWriter(lastPersistIndex -> {
                        if (raftStatus.getCurrentTerm() == term) {
                            if (lastPersistIndex >= itemIndex) {
                                writeResp(ctx, CODE_SUCCESS);
                                return true;
                            } else {
                                return false;
                            }
                        } else {
                            return true;
                        }
                    });
                }
            }
            raftStatus.getDataArrivedCondition().signalAll();

            if (req.getLeaderCommit() < raftStatus.getCommitIndex()) {
                log.info("leader commitIndex less than local, maybe leader restart recently. leaderId={}, leaderTerm={}, leaderCommitIndex={}, localCommitIndex={}, groupId={}",
                        req.getLeaderId(), req.getTerm(), req.getLeaderCommit(), raftStatus.getCommitIndex(), raftStatus.getGroupId());
            }
            if (req.getLeaderCommit() > raftStatus.getLeaderCommit()) {
                raftStatus.setLeaderCommit(req.getLeaderCommit());
            }

            // success response write in callback
            return Fiber.frameReturn();
        }

        private FrameCallResult resumeWhenFindReplicatePosFinish(Pair<Integer, Long> pos, int oldTerm) {
            RaftStatusImpl raftStatus = ctx.gc.getRaftStatus();
            if (oldTerm != raftStatus.getCurrentTerm()) {
                log.info("term changed when find replicate pos, ignore result. oldTerm={}, newTerm={}, groupId={}",
                        oldTerm, raftStatus.getCurrentTerm(), raftStatus.getGroupId());
                return writeResp(ctx, CODE_REQ_ERROR);
            }
            if (ctx.getReqContext().getTimeout().isTimeout(raftStatus.getTs())) {
                // not generate response
                return Fiber.frameReturn();
            }
            AppendReqCallback req = ctx.getReqFrame().getBody();
            if (pos == null) {
                log.info("follower has no suggest index, will install snapshot. groupId={}", raftStatus.getGroupId());
                return writeResp(ctx, CODE_LOG_NOT_MATCH);
            } else if (pos.getLeft() == req.getPrevLogTerm() && pos.getRight() == req.getPrevLogIndex()) {
                log.info("local log truncate to prevLogIndex={}, prevLogTerm={}, groupId={}",
                        req.getPrevLogIndex(), req.getPrevLogTerm(), raftStatus.getGroupId());
                long truncateIndex = req.getPrevLogIndex() + 1;
                if (RaftUtil.writeNotFinished(raftStatus)) {
                    return waitWriteFinish(raftStatus);
                } else {
                    ctx.gc.getRaftLog().truncateTail(truncateIndex);
                    return doAppend(ctx);
                }
            } else {
                log.info("follower suggest term={}, index={}, groupId={}", pos.getLeft(), pos.getRight(), raftStatus.getGroupId());
                ctx.suggestTerm = pos.getLeft();
                ctx.suggestIndex = pos.getRight();
                return writeResp(ctx, CODE_LOG_NOT_MATCH);
            }
        }

        private FrameCallResult waitWriteFinish(RaftStatusImpl raftStatus) {
            // TODO not finish
            return null;
        }

    }// end of AppendFiberFrame
}






