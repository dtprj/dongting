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
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.SimpleWritePacket;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.DecodeContextEx;
import com.github.dtprj.dongting.raft.impl.GroupComponents;
import com.github.dtprj.dongting.raft.impl.LinearTaskRunner;
import com.github.dtprj.dongting.raft.impl.MemberManager;
import com.github.dtprj.dongting.raft.impl.RaftRole;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.impl.TailCache;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroup;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftServer;
import com.github.dtprj.dongting.raft.sm.RaftCodecFactory;
import com.github.dtprj.dongting.raft.store.StatusManager;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class AppendProcessor extends RaftSequenceProcessor<Object> {

    public static final int APPEND_SUCCESS = 0;
    public static final int APPEND_LOG_NOT_MATCH = 1;
    public static final int APPEND_PREV_LOG_INDEX_LESS_THAN_LOCAL_COMMIT = 2;
    public static final int APPEND_REQ_ERROR = 3;
    public static final int APPEND_INSTALL_SNAPSHOT = 4;
    public static final int APPEND_NOT_MEMBER_IN_GROUP = 5;
    public static final int APPEND_SERVER_ERROR = 6;

    private final Function<Integer, RaftCodecFactory> decoderFactory;

    public AppendProcessor(RaftServer raftServer) {
        super(raftServer, false, true);
        this.decoderFactory = groupId -> {
            RaftGroup g = raftServer.getRaftGroup(groupId);
            return g == null ? null : g.getStateMachine();
        };
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    protected int getGroupId(ReadPacket frame) {
        if (frame.command == Commands.RAFT_APPEND_ENTRIES) {
            ReadPacket<AppendReq> f = (ReadPacket<AppendReq>) frame;
            return f.getBody().groupId;
        } else {
            ReadPacket<InstallSnapshotReq> f = (ReadPacket<InstallSnapshotReq>) frame;
            return f.getBody().groupId;
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public DecoderCallback createDecoderCallback(int command, DecodeContext context) {
        if (command == Commands.RAFT_APPEND_ENTRIES) {
            AppendReq.Callback c = ((DecodeContextEx) context).appendReqCallback(decoderFactory);
            return context.toDecoderCallback(c);
        } else {
            return context.toDecoderCallback(new InstallSnapshotReq.Callback());
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    protected FiberFrame<Void> processInFiberGroup(ReqInfoEx reqInfo) {
        if (reqInfo.reqFrame.command == Commands.RAFT_APPEND_ENTRIES) {
            return new AppendFiberFrame(reqInfo, this);
        } else {
            return new InstallFiberFrame(reqInfo, this);
        }
    }

    public static String getAppendResultStr(int code) {
        switch (code) {
            case APPEND_SUCCESS:
                return "CODE_SUCCESS";
            case APPEND_LOG_NOT_MATCH:
                return "CODE_LOG_NOT_MATCH";
            case APPEND_PREV_LOG_INDEX_LESS_THAN_LOCAL_COMMIT:
                return "CODE_PREV_LOG_INDEX_LESS_THAN_LOCAL_COMMIT";
            case APPEND_REQ_ERROR:
                return "CODE_REQ_ERROR";
            case APPEND_INSTALL_SNAPSHOT:
                return "CODE_INSTALL_SNAPSHOT";
            case APPEND_NOT_MEMBER_IN_GROUP:
                return "CODE_NOT_MEMBER_IN_GROUP";
            case APPEND_SERVER_ERROR:
                return "CODE_SERVER_ERROR";
            default:
                return "CODE_UNKNOWN_" + code;
        }
    }
}

abstract class AbstractAppendFrame<C> extends FiberFrame<Void> {
    private static final DtLog log = DtLogs.getLogger(AbstractAppendFrame.class);

    private final String appendType;
    protected final GroupComponents gc;
    protected final AppendProcessor processor;
    protected final ReqInfoEx<C> reqInfo;

    protected boolean needRelease = true;

    public AbstractAppendFrame(String appendType, AppendProcessor processor, ReqInfoEx<C> reqInfo) {
        this.appendType = appendType;
        this.gc = reqInfo.raftGroup.groupComponents;
        this.processor = processor;
        this.reqInfo = reqInfo;
    }

    @Override
    protected FrameCallResult doFinally() {
        if (needRelease) {
            reqInfo.reqFrame.clean();
        }
        return Fiber.frameReturn();
    }

    protected abstract int getLeaderId();

    protected abstract int getRemoteTerm();

    protected abstract FrameCallResult process() throws Exception;

    @Override
    public FrameCallResult execute(Void input) throws Throwable {
        if (isGroupShouldStopPlain()) {
            // RaftSequenceProcessor checked, however the fiber may suspend to for wait write finish,
            // the stop flag may be changed, so we should re-check it
            log.warn("raft group is stopping. ignore {} request", appendType);
            return Fiber.frameReturn();
        }
        int remoteTerm = getRemoteTerm();
        int leaderId = getLeaderId();
        RaftStatusImpl raftStatus = gc.raftStatus;
        if (gc.memberManager.isValidCandidate(leaderId)) {
            int localTerm = raftStatus.currentTerm;
            if (remoteTerm == localTerm) {
                switch (raftStatus.getRole()) {
                    case leader:
                        BugLog.log("leader receive {} request. term={}, remote={}", appendType,
                                remoteTerm, reqInfo.reqContext.getDtChannel().getRemoteAddr());
                        return writeAppendResp(AppendProcessor.APPEND_REQ_ERROR, "leader receive raft install snapshot request");
                    case candidate:
                        String r = "candidate receive append request from leader";
                        gc.voteManager.cancelVote(r);
                        RaftUtil.resetElectTimer(raftStatus);
                        RaftUtil.changeToFollower(raftStatus, leaderId, r);
                        if (reqInfo.reqContext.getTimeout().isTimeout(raftStatus.ts)) {
                            log.info("request timeout, ignore. groupId={}", raftStatus.groupId);
                            // not generate response
                            return Fiber.frameReturn();
                        }
                        return process();
                    default:
                        gc.voteManager.cancelVote("receive append request from leader");
                        RaftUtil.resetElectTimer(raftStatus);
                        RaftUtil.updateLeader(raftStatus, leaderId);
                        raftStatus.copyShareStatus();
                        if (reqInfo.reqContext.getTimeout().isTimeout(raftStatus.ts)) {
                            log.info("request timeout, ignore. groupId={}", raftStatus.groupId);
                            // not generate response
                            return Fiber.frameReturn();
                        }
                        return process();
                }
            } else if (remoteTerm > localTerm) {
                gc.voteManager.cancelVote("receive append request with larger term");
                RaftUtil.incrTerm(remoteTerm, raftStatus, leaderId, "receive append request with larger term");
                RaftUtil.resetElectTimer(raftStatus);
                gc.statusManager.persistAsync();
                return gc.statusManager.waitUpdateFinish(this);
            } else {
                log.info("receive {} request with a smaller term, ignore, remoteTerm={}, localTerm={}",
                        appendType, remoteTerm, localTerm);
                return writeAppendResp(AppendProcessor.APPEND_REQ_ERROR, "small term");
            }
        } else {
            log.error("receive {} request from a non-member, ignore. remoteId={}, group={}, localMembers={}, localPreparedMembers={}",
                    appendType, leaderId, reqInfo.raftGroup.getGroupId(), raftStatus.nodeIdOfMembers, raftStatus.nodeIdOfPreparedMembers);
            String msg = "not member, members=" + raftStatus.nodeIdOfMembers + ", prepareMembers=" + raftStatus.nodeIdOfPreparedMembers;
            return writeAppendResp(AppendProcessor.APPEND_NOT_MEMBER_IN_GROUP, msg);
        }
    }

    protected FrameCallResult writeAppendResp(int code, int suggestTerm, long suggestIndex, String msg) {
        AppendResp resp = new AppendResp();
        resp.term = gc.raftStatus.currentTerm;
        if (code == AppendProcessor.APPEND_SUCCESS) {
            resp.success = true;
        } else {
            resp.success = false;
            resp.appendCode = code;
        }
        resp.suggestTerm = suggestTerm;
        resp.suggestIndex = suggestIndex;
        SimpleWritePacket p = new SimpleWritePacket(resp);
        p.respCode = CmdCodes.SUCCESS;
        p.msg = msg;
        reqInfo.reqContext.writeRespInBizThreads(p);
        return Fiber.frameReturn();
    }

    protected FrameCallResult writeAppendResp(int code, String msg) {
        return writeAppendResp(code, 0, 0, msg);
    }
}

class AppendFiberFrame extends AbstractAppendFrame<AppendReq> {

    private static final DtLog log = DtLogs.getLogger(AppendProcessor.class);

    public AppendFiberFrame(ReqInfoEx<AppendReq> reqInfo, AppendProcessor processor) {
        super("append", processor, reqInfo);
    }

    @Override
    protected FrameCallResult handle(Throwable ex) {
        gc.raftStatus.truncating = false;
        // to notify RaftUtil.waitWriteFinish() to resume
        gc.raftStatus.logForceFinishCondition.signalAll();

        log.error("append error", ex);
        writeAppendResp(AppendProcessor.APPEND_SERVER_ERROR, ex.toString());
        return Fiber.frameReturn();
    }

    @Override
    protected int getLeaderId() {
        return reqInfo.reqFrame.getBody().leaderId;
    }

    @Override
    protected int getRemoteTerm() {
        return reqInfo.reqFrame.getBody().term;
    }

    @Override
    protected FrameCallResult process() {
        AppendReq req = reqInfo.reqFrame.getBody();
        RaftStatusImpl raftStatus = gc.raftStatus;
        if (raftStatus.installSnapshot) {
            writeAppendResp(AppendProcessor.APPEND_INSTALL_SNAPSHOT, null);
            return Fiber.frameReturn();
        }
        if (req.logs == null || req.logs.isEmpty()) {
            updateLeaderCommit(req, raftStatus);
            writeAppendResp(AppendProcessor.APPEND_SUCCESS, null);
            return Fiber.frameReturn();
        } else if (req.prevLogIndex != raftStatus.lastLogIndex || req.prevLogTerm != raftStatus.lastLogTerm) {
            log.info("log not match. prevLogIndex={}, localLastLogIndex={}, prevLogTerm={}, localLastLogTerm={}, count={}, leaderId={}, groupId={}",
                    req.prevLogIndex, raftStatus.lastLogIndex, req.prevLogTerm,
                    raftStatus.lastLogTerm, req.logs.size(), req.leaderId, raftStatus.groupId);
            int currentTerm = raftStatus.currentTerm;
            Supplier<Boolean> cancelIndicator = () -> raftStatus.currentTerm != currentTerm;
            FiberFrame<Pair<Integer, Long>> replicatePosFrame = gc.raftLog.tryFindMatchPos(
                    req.prevLogTerm, req.prevLogIndex, cancelIndicator);
            return Fiber.call(replicatePosFrame, pos -> resumeWhenFindReplicatePosFinish(pos, currentTerm));
        } else {
            return doAppend(req);
        }
    }

    private FrameCallResult doAppend(AppendReq req) {
        RaftStatusImpl raftStatus = gc.raftStatus;
        if (req.prevLogIndex < raftStatus.commitIndex) {
            BugLog.log("leader append request prevLogIndex less than local commit index. leaderId={}, prevLogIndex={}, commitIndex={}, groupId={}",
                    req.leaderId, req.prevLogIndex, raftStatus.commitIndex, raftStatus.groupId);
            writeAppendResp(AppendProcessor.APPEND_PREV_LOG_INDEX_LESS_THAN_LOCAL_COMMIT, null);
            return Fiber.frameReturn();
        }

        updateLeaderCommit(req, raftStatus);

        long expectLogIndex = LinearTaskRunner.lastIndex(raftStatus);
        if (expectLogIndex != req.prevLogIndex) {
            log.error("bad request: log index not match. prevLogIndex={}, expectIndex={}, leaderId={}, groupId={}",
                    req.prevLogIndex, expectLogIndex, req.leaderId, raftStatus.groupId);
            writeAppendResp(AppendProcessor.APPEND_REQ_ERROR, "log index not match, prevLogIndex="
                    + req.prevLogIndex + ", expectIndex=" + expectLogIndex);
            return Fiber.frameReturn();
        }
        List<LogItem> logs = req.logs;
        for (int i = 0, len = logs.size(); i < len; i++) {
            LogItem li = logs.get(i);
            if (li.index != ++expectLogIndex) {
                log.error("bad request: log index not continuous at pos {}, expected={}, actual={}, leaderId={}",
                        i, expectLogIndex, li.index, req.leaderId);
                writeAppendResp(AppendProcessor.APPEND_REQ_ERROR, "log index " + i + " not continuous");
                return Fiber.frameReturn();
            }
        }

        // Here, we refreshed the ts. Next, the time t set on RaftTask is greater than the time when the raft
        // client constructs the request. Since there is a 1ms error in the ts refresh, the time when the raft
        // client constructs the request happens before (t + 1ms).
        raftStatus.ts.refresh(1);

        ArrayList<RaftTask> list = new ArrayList<>(logs.size());
        for (int i = 0, len = logs.size(); i < len; i++) {
            LogItem li = logs.get(i);
            RaftInput raftInput = new RaftInput(li.bizType, li.getHeader(), li.getBody(), null,
                    li.type == LogItem.TYPE_LOG_READ);
            RaftTask task = new RaftTask(li.type, raftInput, null);
            task.init(li, raftStatus.ts.nanoTime);
            list.add(task);

            if (li.index < raftStatus.groupReadyIndex && raftStatus.getRole() != RaftRole.none) {
                log.info("set groupReadyIndex to {}, groupId={}", li.index, raftStatus.groupId);
                raftStatus.groupReadyIndex = li.index;
            }
            if (i == len - 1) {
                registerRespWriter(raftStatus, li.index);
            }
        }
        gc.commitManager.updateCommitHistory(req.leaderCommit);
        needRelease = false;
        FiberFrame<Void> f = gc.linearTaskRunner.append(raftStatus, list);
        // success response write in CommitManager fiber
        return Fiber.call(f, this::justReturn);
    }

    private void updateLeaderCommit(AppendReq req, RaftStatusImpl raftStatus) {
        if (req.leaderCommit < raftStatus.commitIndex) {
            log.info("leader commitIndex less than local, maybe leader restart recently. leaderId={}," +
                            " leaderTerm={}, leaderCommitIndex={}, localCommitIndex={}, groupId={}",
                    req.leaderId, req.term, req.leaderCommit, raftStatus.commitIndex, raftStatus.groupId);
        }
        if (req.leaderCommit > raftStatus.leaderCommit) {
            raftStatus.leaderCommit = req.leaderCommit;
            gc.commitManager.followerTryCommit(raftStatus);
        }
    }

    private void registerRespWriter(RaftStatusImpl raftStatus, long index) {
        int term = raftStatus.currentTerm;
        // register write response callback
        gc.commitManager.registerRespWriter(lastPersistIndex -> {
            if (raftStatus.currentTerm == term) {
                if (lastPersistIndex >= index) {
                    writeAppendResp(AppendProcessor.APPEND_SUCCESS, null);
                    return true;
                } else {
                    return false;
                }
            } else {
                return true;
            }
        });
    }

    private FrameCallResult resumeWhenFindReplicatePosFinish(Pair<Integer, Long> pos, int oldTerm) {
        GroupComponents gc = reqInfo.raftGroup.groupComponents;
        RaftStatusImpl raftStatus = gc.raftStatus;
        if (oldTerm != raftStatus.currentTerm) {
            log.info("term changed when find replicate pos, ignore result. oldTerm={}, newTerm={}, groupId={}",
                    oldTerm, raftStatus.currentTerm, raftStatus.groupId);
            writeAppendResp(AppendProcessor.APPEND_SERVER_ERROR, "term changed");
            return Fiber.frameReturn();
        }
        if (reqInfo.reqContext.getTimeout().isTimeout(raftStatus.ts)) {
            // not generate response
            return Fiber.frameReturn();
        }
        AppendReq req = reqInfo.reqFrame.getBody();
        if (pos == null) {
            log.info("follower has no suggest index, will install snapshot. groupId={}", raftStatus.groupId);
            writeAppendResp(AppendProcessor.APPEND_LOG_NOT_MATCH, null);
            return Fiber.frameReturn();
        } else if (pos.getLeft() == req.prevLogTerm && pos.getRight() == req.prevLogIndex) {
            if (RaftUtil.writeNotFinished(raftStatus)) {
                // resume to this::execute to re-run all check
                return RaftUtil.waitWriteFinish(raftStatus, this);
            } else {
                gc.raftStatus.truncating = true;
                long truncateIndex = req.prevLogIndex + 1;

                log.info("local log truncate to {}(inclusive)", truncateIndex);

                TailCache tailCache = reqInfo.raftGroup.groupComponents.raftStatus.tailCache;
                tailCache.truncate(truncateIndex);
                return Fiber.call(gc.raftLog.truncateTail(truncateIndex),
                        v -> afterTruncate(req.prevLogIndex, req.prevLogTerm));
            }
        } else {
            log.info("follower suggest term={}, index={}, groupId={}", pos.getLeft(), pos.getRight(), raftStatus.groupId);
            writeAppendResp(AppendProcessor.APPEND_LOG_NOT_MATCH, pos.getLeft(), pos.getRight(), null);
            return Fiber.frameReturn();
        }
    }

    private FrameCallResult afterTruncate(long matchIndex, int matchTerm) {
        RaftStatusImpl raftStatus = gc.raftStatus;

        raftStatus.truncating = false;
        // to notify RaftUtil.waitWriteFinish() to resume
        gc.raftStatus.logForceFinishCondition.signalAll();

        raftStatus.lastWriteLogIndex = matchIndex;
        raftStatus.lastForceLogIndex = matchIndex;
        raftStatus.lastLogIndex = matchIndex;
        raftStatus.lastLogTerm = matchTerm;
        return doAppend(reqInfo.reqFrame.getBody());
    }

}// end of AppendFiberFrame

class InstallFiberFrame extends AbstractAppendFrame<InstallSnapshotReq> {
    private static final DtLog log = DtLogs.getLogger(InstallFiberFrame.class);
    private final int groupId = gc.raftStatus.groupId;
    private boolean markInstall = false;

    public InstallFiberFrame(ReqInfoEx<InstallSnapshotReq> reqInfo, AppendProcessor processor) {
        super("install snapshot", processor, reqInfo);
    }

    @Override
    protected FrameCallResult handle(Throwable ex) throws Throwable {
        log.error("install snapshot error", ex);
        return writeAppendResp(AppendProcessor.APPEND_SERVER_ERROR, ex.toString());
    }

    @Override
    protected int getLeaderId() {
        return reqInfo.reqFrame.getBody().leaderId;
    }

    protected int getRemoteTerm() {
        return reqInfo.reqFrame.getBody().term;
    }

    @Override
    protected FrameCallResult process() {
        RaftStatusImpl raftStatus = gc.raftStatus;
        InstallSnapshotReq req = reqInfo.reqFrame.getBody();
        needRelease = false;
        if (!req.members.isEmpty()) {
            return startInstall(raftStatus, req);
        } else {
            return doInstall(raftStatus, req);
        }
    }

    private FrameCallResult startInstall(RaftStatusImpl raftStatus, InstallSnapshotReq req) {
        if (!markInstall) {
            log.info("start install snapshot, groupId={}, lastIncludedIndex={}, lastIncludedTerm={}",
                    groupId, req.lastIncludedIndex, req.lastIncludedTerm);
            raftStatus.installSnapshot = true;
            gc.applyManager.wakeupApply(); // wakeup apply fiber to exit
            gc.statusManager.persistAsync();
            markInstall = true;
        }
        return gc.applyManager.getApplyFiber().join(this::afterApplyExit);
    }

    private FrameCallResult afterApplyExit(Void v) {
        // should we wait status machine to finish?
        return gc.statusManager.waitUpdateFinish(this::afterBeginStatusPersist);
    }

    private FrameCallResult afterBeginStatusPersist(Void v) throws Exception {
        return Fiber.call(gc.raftLog.beginInstall(), this::applyConfigChange);
    }

    private FrameCallResult applyConfigChange(Void unused) {
        gc.raftStatus.tailCache.cleanAll();

        MemberManager mm = reqInfo.raftGroup.groupComponents.memberManager;
        InstallSnapshotReq req = reqInfo.reqFrame.getBody();

        reqInfo.raftGroup.groupComponents.raftStatus.lastConfigChangeIndex = req.lastIncludedIndex;

        FiberFrame<Void> f = mm.applyConfigFrame("install snapshot config change",
                req.members, req.observers, req.preparedMembers, req.preparedObservers);
        return Fiber.call(f, v -> writeRespAndRelease(null));
    }

    private FrameCallResult doInstall(RaftStatusImpl raftStatus, InstallSnapshotReq req) {
        if (!raftStatus.installSnapshot) {
            log.error("not in install snapshot state, groupId={}", groupId);
            return writeRespAndRelease(new RaftException("not in install snapshot state"));
        }
        boolean done = req.done;
        ByteBuffer buf = req.data == null ? null : req.data.getBuffer();
        log.info("apply snapshot, groupId={}, offset={}, bytes={}, done={}", groupId,
                req.offset, buf == null ? 0 : buf.remaining(), done);
        FiberFuture<Void> f = gc.stateMachine.installSnapshot(req.lastIncludedIndex,
                req.lastIncludedTerm, req.offset, done, buf);
        if (done) {
            return f.await(v -> finishInstall(req, raftStatus));
        } else {
            f.registerCallback((v, ex) -> writeRespAndRelease(ex));
            return Fiber.frameReturn();
        }
    }

    private FrameCallResult finishInstall(InstallSnapshotReq req, RaftStatusImpl raftStatus) throws Exception {
        raftStatus.installSnapshot = false;

        raftStatus.setLastApplied(req.lastIncludedIndex);
        raftStatus.lastAppliedTerm = req.lastIncludedTerm;
        raftStatus.lastApplying = req.lastIncludedIndex;

        raftStatus.commitIndex = req.lastIncludedIndex;

        raftStatus.lastLogTerm = req.lastIncludedTerm;
        raftStatus.lastLogIndex = req.lastIncludedIndex;
        raftStatus.lastWriteLogIndex = req.lastIncludedIndex;
        raftStatus.lastForceLogIndex = req.lastIncludedIndex;

        raftStatus.copyShareStatus();

        log.info("apply snapshot to state machine finished, groupId={}, index={}, term={}",
                groupId, req.lastIncludedIndex, req.lastIncludedTerm);

        long nextIdx = req.lastIncludedIndex + 1;
        FiberFrame<Void> ff = gc.raftLog.finishInstall(nextIdx, req.nextWritePos);
        return Fiber.call(ff, v -> afterRaftLogFinishInstall(nextIdx));
    }

    private FrameCallResult afterRaftLogFinishInstall(long nextLogIndex) {
        log.info("raft log reset to nextLogIndex={}", nextLogIndex);
        gc.raftStatus.firstValidIndex = nextLogIndex;
        StatusManager sm = gc.statusManager;
        sm.persistAsync();
        return sm.waitUpdateFinish(this::afterFinishStatusSaved);
    }

    private FrameCallResult afterFinishStatusSaved(Void v) {
        gc.applyManager.signalStartApply();
        log.info("apply snapshot finish, groupId={}", groupId);

        // Have no logs before lastIncludedIndex, so save snapshot immediately.
        // Restart before snapshot is saved will cause install snapshot (since it can't recover state machine).
        // The save is async and FiberFuture returned by saveSnapshot() is not used.
        gc.snapshotManager.saveSnapshot();

        return writeRespAndRelease(null);
    }

    private FrameCallResult writeRespAndRelease(Throwable ex) {
        reqInfo.reqFrame.clean();
        if (ex == null) {
            return writeAppendResp(AppendProcessor.APPEND_SUCCESS, null);
        } else {
            return writeAppendResp(AppendProcessor.APPEND_SERVER_ERROR, ex.toString());
        }
    }
}






