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
package com.github.dtprj.dongting.raft.impl;

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.buf.RefBufferFactory;
import com.github.dtprj.dongting.codec.DecoderCallbackCreator;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.common.PerfCallback;
import com.github.dtprj.dongting.common.PerfConsts;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.NetCodeException;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.RpcCallback;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.rpc.AppendProcessor;
import com.github.dtprj.dongting.raft.rpc.AppendReqWritePacket;
import com.github.dtprj.dongting.raft.rpc.AppendResp;
import com.github.dtprj.dongting.raft.rpc.InstallSnapshotReq;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.sm.Snapshot;
import com.github.dtprj.dongting.raft.sm.SnapshotInfo;
import com.github.dtprj.dongting.raft.sm.StateMachine;
import com.github.dtprj.dongting.raft.store.RaftLog;
import com.github.dtprj.dongting.raft.store.StatusManager;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class ReplicateManager {

    final NioClient client;
    private final GroupComponents gc;
    final int groupId;
    private final RaftStatusImpl raftStatus;
    final RaftGroupConfigEx groupConfig;
    final RaftServerConfig serverConfig;

    RaftLog raftLog;
    StateMachine stateMachine;
    private CommitManager commitManager;
    private StatusManager statusManager;

    public ReplicateManager(NioClient client, GroupComponents gc) {
        this.client = client;
        this.gc = gc;
        this.groupConfig = gc.groupConfig;
        this.groupId = groupConfig.getGroupId();
        this.raftStatus = gc.raftStatus;
        this.serverConfig = gc.serverConfig;
    }

    public void postInit() {
        this.raftLog = gc.raftLog;
        this.stateMachine = gc.stateMachine;
        this.commitManager = gc.commitManager;
        this.statusManager = gc.statusManager;
    }

    public void tryStartReplicateFibers() {
        if (raftStatus.getRole() != RaftRole.leader) {
            return;
        }
        List<RaftMember> list = raftStatus.replicateList;
        for (int size = list.size(), i = 0; i < size; i++) {
            RaftMember m = list.get(i);
            if (m.getNode().isSelf()) {
                continue;
            }
            if (!m.isReady()) {
                continue;
            }
            if (gc.memberManager.inLegacyMember(m)) {
                continue;
            }
            if (m.getReplicateFiber() == null || m.getReplicateFiber().isFinished()) {
                Fiber f;
                if (m.isInstallSnapshot()) {
                    LeaderInstallFrame ff = new LeaderInstallFrame(this, m);
                    f = new Fiber("install-" + m.getNode().getNodeId() + "-" + m.getReplicateEpoch(),
                            groupConfig.getFiberGroup(), ff, true);
                } else {
                    LeaderRepFrame ff = new LeaderRepFrame(this, commitManager, m);
                    f = new Fiber("replicate-" + m.getNode().getNodeId() + "-" + m.getReplicateEpoch(),
                            groupConfig.getFiberGroup(), ff, true);
                }
                f.start();
                m.setReplicateFiber(f);
            }
        }
    }

    boolean checkTermFailed(int remoteTerm, boolean append) {
        if (remoteTerm > raftStatus.currentTerm) {
            String msg = (append ? "append" : "install") + " response term greater than local";
            RaftUtil.incrTerm(remoteTerm, raftStatus, -1, msg);
            statusManager.persistAsync(true);
            return true;
        }

        return false;
    }

}

abstract class AbstractLeaderRepFrame extends FiberFrame<Void> {
    private static final DtLog log = DtLogs.getLogger(AbstractLeaderRepFrame.class);
    private final int replicateEpoch;
    protected final RaftStatusImpl raftStatus;
    protected final int term;
    protected final int groupId;
    protected final RaftMember member;

    protected static final DecoderCallbackCreator<AppendResp> APPEND_RESP_DECODER_CALLBACK_CREATOR = ctx -> {
        AppendResp.Callback c = ((DecodeContextEx) ctx).appendRespCallback();
        return ctx.toDecoderCallback(c);
    };

    public AbstractLeaderRepFrame(ReplicateManager replicateManager, RaftMember member) {
        this.groupId = replicateManager.groupId;
        this.member = member;
        RaftGroupConfigEx groupConfig = replicateManager.groupConfig;
        this.raftStatus = (RaftStatusImpl) groupConfig.getRaftStatus();
        this.replicateEpoch = member.getReplicateEpoch();
        this.term = raftStatus.currentTerm;
    }

    protected boolean shouldStopReplicate() {
        if (isGroupShouldStopPlain()) {
            log.debug("group should stop, stop replicate fiber. group={}", groupId);
            return true;
        }
        if (raftStatus.getRole() != RaftRole.leader) {
            log.info("not leader, stop replicate fiber. group={}", groupId);
            return true;
        }
        RaftNodeEx node = member.getNode();
        if (epochChange()) {
            log.info("epoch changed, stop replicate fiber. group={}, node={}, newEpoch={}, oldEpoch={}",
                    groupId, node.getNodeId(), member.getReplicateEpoch(), replicateEpoch);
            return true;
        }
        NodeStatus ns = node.getStatus();
        if (!ns.isReady()) {
            incrementEpoch();
            log.info("node not ready, stop replicate fiber. group={}, node={}", groupId, node.getNodeId());
            return true;
        }
        if (ns.getEpoch() != member.getNodeEpoch()) {
            incrementEpoch();
            log.info("node epoch change, stop replicate fiber. group={}, node={}", groupId, node.getNodeId());
            return true;
        }
        return false;
    }

    protected boolean epochChange() {
        return member.getReplicateEpoch() != replicateEpoch;
    }

    protected void incrementEpoch() {
        member.incrementReplicateEpoch(replicateEpoch);
    }

}

class LeaderRepFrame extends AbstractLeaderRepFrame {
    private static final DtLog log = DtLogs.getLogger(LeaderRepFrame.class);
    private static final long WAIT_CONDITION_TIMEOUT = 1000;

    private final RaftGroupConfigEx groupConfig;
    private final RaftServerConfig serverConfig;
    private final NioClient client;
    private final ReplicateManager replicateManager;
    private final CommitManager commitManager;
    private final RaftLog raftLog;
    private final FiberCondition repDoneCondition;
    private final FiberCondition needRepCondition;

    private final int maxReplicateItems;
    private final int restItemsToStartReplicate;
    private final long maxReplicateBytes;

    int pendingItems;
    long pendingBytes;
    private boolean multiAppend;
    private boolean updateCommitIndex;

    private RaftLog.LogIterator replicateIterator;
    private final Timestamp ts;
    private final PerfCallback perfCallback;

    public LeaderRepFrame(ReplicateManager replicateManager, CommitManager commitManager, RaftMember member) {
        super(replicateManager, member);
        this.groupConfig = replicateManager.groupConfig;
        this.serverConfig = replicateManager.serverConfig;
        this.ts = groupConfig.getTs();
        this.perfCallback = groupConfig.getPerfCallback();
        this.repDoneCondition = member.getRepDoneCondition();
        this.needRepCondition = raftStatus.needRepCondition;

        this.raftLog = replicateManager.raftLog;
        this.client = replicateManager.client;
        this.replicateManager = replicateManager;
        this.commitManager = commitManager;

        this.maxReplicateItems = groupConfig.getMaxReplicateItems();
        this.maxReplicateBytes = groupConfig.getMaxReplicateBytes();
        this.restItemsToStartReplicate = (int) (maxReplicateItems * 0.1);
    }

    @Override
    protected FrameCallResult handle(Throwable ex) throws Throwable {
        if (ex instanceof RaftCancelException) {
            log.info("ReplicateManager load raft log cancelled");
        } else {
            log.error("replicate fiber fail, remoteId={}", member.getNode().getNodeId(), ex);
            if (raftStatus.getRole() == RaftRole.leader) {
                // if log is deleted, the next load will never success, so we need to reset nextIndex.
                // however, the exception may be caused by other reasons
                member.setNextIndex(raftStatus.lastLogIndex + 1);
            }
        }
        return Fiber.frameReturn();
    }

    @Override
    protected FrameCallResult doFinally() {
        closeIterator();
        return Fiber.frameReturn();
    }

    private FrameCallResult await() {
        return repDoneCondition.await(WAIT_CONDITION_TIMEOUT, needRepCondition, this);
    }

    @Override
    public FrameCallResult execute(Void input) {
        if (shouldStopReplicate()) {
            return Fiber.frameReturn();
        }
        if (pendingItems >= maxReplicateItems) {
            return await();
        }
        if (pendingBytes >= maxReplicateBytes) {
            return await();
        }

        long nextIndex = member.getNextIndex();
        long diff = raftStatus.lastLogIndex - nextIndex + 1;
        if (diff <= 0 && member.repCommitIndex >= raftStatus.commitIndex) {
            // no data to replicate and no need to update repCommitIndex
            return await();
        }

        if (multiAppend) {
            return doReplicate(member, diff, nextIndex);
        } else {
            if (diff <= 0) {
                // don't update repCommitIndex in this case
                return await();
            } else if (pendingItems <= 0) {
                return doReplicate(member, diff, nextIndex);
            } else {
                return await();
            }
        }
    }

    private FrameCallResult doReplicate(RaftMember member, long diff, long nextIndex) {
        // flow control
        int rest = maxReplicateItems - pendingItems;
        if (pendingItems > 0 && rest <= restItemsToStartReplicate) {
            // avoid silly window syndrome
            return await();
        }

        TailCache tailCache = raftStatus.tailCache;
        if (diff == 0) {
            if (updateCommitIndex) {
                return await();
            } else {
                updateCommitIndex = true;
                sendAppendRequest(member, Collections.emptyList(), 0);
                return Fiber.resume(null, this);
            }
        }

        int limit = multiAppend ? (int) Math.min(rest, diff) : 1;

        RaftTask first = tailCache.get(nextIndex);
        if (first != null) {
            closeIterator();
            long sizeLimit = groupConfig.getSingleReplicateLimit();
            ArrayList<LogItem> items = new ArrayList<>(limit);
            long size = 0;
            long leaseStartNanos = 0;
            for (int i = 0; i < limit; i++) {
                RaftTask rt = tailCache.get(nextIndex + i);
                LogItem li = rt.getItem();
                size += li.getActualBodySize();
                if (i > 0 && size > sizeLimit) {
                    break;
                }
                leaseStartNanos = rt.getCreateTimeNanos();
                li.retain();
                items.add(li);
            }
            sendAppendRequest(member, items, leaseStartNanos);
            return Fiber.resume(null, this);
        } else {
            if (replicateIterator == null) {
                replicateIterator = raftLog.openIterator(this::epochChange);
            }
            FiberFrame<List<LogItem>> nextFrame = replicateIterator.next(nextIndex, Math.min(limit, 1024),
                    groupConfig.getSingleReplicateLimit());
            return Fiber.call(nextFrame, this::resumeAfterLogLoad);
        }
    }

    private FrameCallResult resumeAfterLogLoad(List<LogItem> items) {
        if (shouldStopReplicate()) {
            RaftUtil.release(items);
            return Fiber.frameReturn();
        }
        if (items == null || items.isEmpty()) {
            log.warn("load raft log return empty, ignore load result");
            closeIterator();
            return Fiber.resume(null, this);
        }
        if (member.getNextIndex() != items.get(0).getIndex()) {
            log.error("the first load item index not match nextIndex, ignore load result");
            RaftUtil.release(items);
            closeIterator();
            return Fiber.resume(null, this);
        }

        // can't get real lease start time since it's not be persisted
        long leaseStartTime = ts.getNanoTime() - Duration.ofDays(1).toNanos();
        sendAppendRequest(member, items, leaseStartTime);
        return Fiber.resume(null, this);
    }

    private void sendAppendRequest(RaftMember member, List<LogItem> items, long leaseStartNanos) {
        AppendReqWritePacket req = new AppendReqWritePacket();
        req.setCommand(Commands.RAFT_APPEND_ENTRIES);
        req.groupId = groupId;
        req.term = raftStatus.currentTerm;
        req.leaderId = serverConfig.getNodeId();
        req.leaderCommit = raftStatus.commitIndex;

        if (!items.isEmpty()) {
            LogItem firstItem = items.get(0);
            long prevLogIndex = firstItem.getIndex() - 1;
            req.prevLogIndex = prevLogIndex;
            req.prevLogTerm = firstItem.getPrevLogTerm();
            req.logs = items;
            member.setNextIndex(prevLogIndex + 1 + items.size());
        } else {
            member.repCommitIndex = raftStatus.commitIndex;
        }


        DtTime timeout = new DtTime(ts.getNanoTime(), serverConfig.getRpcTimeout(), TimeUnit.MILLISECONDS);
        long perfStartTime = perfCallback.takeTime(PerfConsts.RAFT_D_REPLICATE_RPC);
        long bytes = 0;
        for (int size = items.size(), i = 0; i < size; i++) {
            LogItem item = items.get(i);
            bytes += item.getActualBodySize();
        }
        long finalBytes = bytes;
        Executor ge = groupConfig.getFiberGroup().getExecutor();
        RpcCallback<AppendResp> c = (result, ex) ->
                ge.execute(() -> afterAppendRpc(result, ex, req, leaseStartNanos, finalBytes, perfStartTime));
        // release in AppendReqWritePacket
        client.sendRequest(member.getNode().getPeer(), req, APPEND_RESP_DECODER_CALLBACK_CREATOR, timeout, c);
        pendingItems += items.size();
        pendingBytes += bytes;
    }

    void afterAppendRpc(ReadPacket<AppendResp> rf, Throwable ex, AppendReqWritePacket req,
                        long leaseStartNanos, long bytes, long perfStartTime) {
        int itemCount = req.logs == null ? 0 : req.logs.size();
        perfCallback.fireTime(PerfConsts.RAFT_D_REPLICATE_RPC, perfStartTime, itemCount, bytes);
        repDoneCondition.signalAll();
        if (epochChange()) {
            log.info("receive outdated append result, replicateEpoch not match. ignore.");
            return;
        }

        descPending(itemCount, bytes);

        if (ex == null) {
            processAppendResult(rf, req, leaseStartNanos, itemCount);
        } else {
            incrementEpoch();

            ex = DtUtil.rootCause(ex);
            boolean warn = false;
            if (ex instanceof NetCodeException) {
                int c = ((NetCodeException) ex).getCode();
                warn = c == CmdCodes.RAFT_GROUP_STOPPED || c == CmdCodes.RAFT_GROUP_NOT_INIT || c == CmdCodes.STOPPING;
            }
            if (warn) {
                log.warn("append fail. remoteId={}, groupId={}, localTerm={}, reqTerm={}, prevLogIndex={}. {}",
                        member.getNode().getNodeId(), groupId, raftStatus.currentTerm,
                        term, req.prevLogIndex, ex.toString());
            } else {
                log.error("append fail. remoteId={}, groupId={}, localTerm={}, reqTerm={}, prevLogIndex={}",
                        member.getNode().getNodeId(), groupId, raftStatus.currentTerm, term, req.prevLogIndex, ex);
            }
        }
    }

    private void processAppendResult(ReadPacket<AppendResp> resp, AppendReqWritePacket req,
                                     long leaseStartNanos, int itemCount) {
        long prevLogIndex = req.prevLogIndex;
        int prevLogTerm = req.prevLogTerm;
        long expectNewMatchIndex = prevLogIndex + itemCount;
        AppendResp body = resp.getBody();
        RaftStatusImpl raftStatus = this.raftStatus;
        int remoteTerm = body.term;
        if (replicateManager.checkTermFailed(remoteTerm, true)) {
            return;
        }
        if (member.isInstallSnapshot()) {
            BugLog.getLog().error("receive append result when install snapshot, ignore. prevLogIndex={}, prevLogTerm={}, remoteId={}, groupId={}",
                    prevLogIndex, prevLogTerm, member.getNode().getNodeId(), groupId);
            closeIterator();
            return;
        }
        if (body.success) {
            member.repCommitIndexAcked = Math.max(member.repCommitIndexAcked, req.leaderCommit);
            if (itemCount == 0) {
                updateCommitIndex = false;
            } else if (member.getMatchIndex() <= prevLogIndex) {
                updateLease(member, leaseStartNanos, raftStatus);
                member.setMatchIndex(expectNewMatchIndex);
                multiAppend = true;
                commitManager.leaderTryCommit(expectNewMatchIndex);
            } else {
                BugLog.getLog().error("append miss order. old matchIndex={}, append prevLogIndex={}," +
                                " expectNewMatchIndex={}, remoteId={}, groupId={}, localTerm={}, reqTerm={}, remoteTerm={}",
                        member.getMatchIndex(), prevLogIndex, expectNewMatchIndex, member.getNode().getNodeId(),
                        groupId, raftStatus.currentTerm, term, body.term);
                closeIterator();
                incrementEpoch();
            }
        } else {
            closeIterator();
            incrementEpoch();
            int appendCode = body.appendCode;
            if (appendCode == AppendProcessor.APPEND_LOG_NOT_MATCH) {
                updateLease(member, leaseStartNanos, raftStatus);
                processLogNotMatch(prevLogIndex, prevLogTerm, body, raftStatus);
            } else if (appendCode == AppendProcessor.APPEND_SERVER_ERROR) {
                updateLease(member, leaseStartNanos, raftStatus);
                log.error("append fail because of remote error. groupId={}, prevLogIndex={}, msg={}",
                        groupId, prevLogIndex, resp.getMsg());
            } else if (appendCode == AppendProcessor.APPEND_INSTALL_SNAPSHOT) {
                log.warn("append fail because of member is install snapshot. groupId={}, remoteId={}",
                        groupId, member.getNode().getNodeId());
                updateLease(member, leaseStartNanos, raftStatus);
                member.setInstallSnapshot(true);
            } else {
                BugLog.getLog().error("append fail. appendCode={}, old matchIndex={}, append prevLogIndex={}, " +
                                "expectNewMatchIndex={}, remoteId={}, groupId={}, localTerm={}, reqTerm={}, remoteTerm={}",
                        AppendProcessor.getAppendResultStr(appendCode), member.getMatchIndex(), prevLogIndex, expectNewMatchIndex,
                        member.getNode().getNodeId(), groupId, raftStatus.currentTerm, term, body.term);
            }
        }
    }

    private void processLogNotMatch(long prevLogIndex, int prevLogTerm, AppendResp body,
                                    RaftStatusImpl raftStatus) {
        log.info("log not match. remoteId={}, groupId={}, matchIndex={}, prevLogIndex={}, prevLogTerm={}, remoteLogTerm={}, remoteLogIndex={}, localTerm={}",
                member.getNode().getNodeId(), groupId, member.getMatchIndex(), prevLogIndex, prevLogTerm, body.suggestTerm,
                body.suggestIndex, raftStatus.currentTerm);
        if (body.suggestTerm == 0 && body.suggestIndex == 0) {
            log.info("remote has no suggest match index, begin install snapshot. remoteId={}, groupId={}",
                    member.getNode().getNodeId(), groupId);
            member.setInstallSnapshot(true);
            return;
        }
        FiberFrame<Void> ff = new LeaderFindMatchPosFrame(replicateManager, member,
                body.suggestTerm, body.suggestIndex);
        Fiber f = new Fiber("find-match-pos-" + member.getNode().getNodeId()
                + "-" + member.getReplicateEpoch(), groupConfig.getFiberGroup(), ff, true);
        member.setReplicateFiber(f);
        f.start();
    }

    private void updateLease(RaftMember member, long reqNanos, RaftStatusImpl raftStatus) {
        member.setLastConfirmReqNanos(reqNanos);
        RaftUtil.updateLease(raftStatus);
        // not call raftStatus.copyShareStatus(), invoke after apply
    }

    public void descPending(int itemCount, long bytes) {
        pendingItems -= itemCount;
        pendingBytes -= bytes;
    }

    public void closeIterator() {
        if (replicateIterator != null) {
            DtUtil.close(replicateIterator);
            replicateIterator = null;
        }
    }

    public int getTerm() {
        return term;
    }

}

class LeaderFindMatchPosFrame extends AbstractLeaderRepFrame {
    private static final DtLog log = DtLogs.getLogger(LeaderFindMatchPosFrame.class);
    private final ReplicateManager replicateManager;
    private final int suggestTerm;
    private final long suggestIndex;

    public LeaderFindMatchPosFrame(ReplicateManager replicateManager, RaftMember member, int suggestTerm, long suggestIndex) {
        super(replicateManager, member);
        this.replicateManager = replicateManager;
        this.suggestTerm = suggestTerm;
        this.suggestIndex = suggestIndex;
    }

    @Override
    public FrameCallResult execute(Void input) throws Throwable {
        int epoch = member.getReplicateEpoch();
        FiberFrame<Pair<Integer, Long>> f = replicateManager.raftLog.tryFindMatchPos(
                suggestTerm, suggestIndex, () -> member.getReplicateEpoch() != epoch);
        return Fiber.call(f, this::resumeAfterFindReplicatePos);
    }

    @Override
    protected FrameCallResult handle(Throwable ex) throws Throwable {
        log.error("tryFindMatchPos fail", ex);
        return Fiber.frameReturn();
    }

    private FrameCallResult resumeAfterFindReplicatePos(Pair<Integer, Long> result) {
        if (epochChange()) {
            log.info("epoch not match. ignore result of nextIndexToReplicate call");
            return Fiber.frameReturn();
        }
        if (result == null) {
            log.info("follower has no suggest match index, begin install snapshot. remoteId={}, groupId={}",
                    member.getNode().getNodeId(), groupId);
            member.setInstallSnapshot(true);
        } else {
            if (result.getLeft() == suggestTerm && result.getRight() == suggestIndex) {
                log.info("match success: remote={}, group={}, term={}, index={}",
                        member.getNode().getNodeId(), groupId, suggestTerm, suggestIndex);
            } else {
                log.info("leader suggest: term={}, index={}, remote={}, group={}",
                        result.getLeft(), result.getRight(), member.getNode().getNodeId(), groupId);
            }
            member.setNextIndex(result.getRight() + 1);
        }
        replicateManager.tryStartReplicateFibers();
        return Fiber.frameReturn();
    }
}

class LeaderInstallFrame extends AbstractLeaderRepFrame {
    private static final DtLog log = DtLogs.getLogger(LeaderInstallFrame.class);

    private final RaftLog raftLog;
    private final StateMachine stateMachine;
    private final RaftGroupConfigEx groupConfig;
    private final RaftServerConfig serverConfig;
    private final NioClient client;
    private final ReplicateManager replicateManager;
    private final RefBufferFactory heapPool;

    private Snapshot snapshot;
    private long nextPosAfterInstallFinish;
    private long snapshotOffset;

    public LeaderInstallFrame(ReplicateManager replicateManager, RaftMember member) {
        super(replicateManager, member);
        this.stateMachine = replicateManager.stateMachine;
        this.raftLog = replicateManager.raftLog;
        this.groupConfig = replicateManager.groupConfig;
        this.serverConfig = replicateManager.serverConfig;
        this.client = replicateManager.client;
        this.replicateManager = replicateManager;
        this.heapPool = groupConfig.getFiberGroup().getThread().getHeapPool();
    }

    @Override
    protected FrameCallResult handle(Throwable ex) throws Throwable {
        log.error("install snapshot error: group={}, remoteId={}", groupId, member.getNode().getNodeId(), ex);
        return Fiber.frameReturn();
    }

    @Override
    protected FrameCallResult doFinally() {
        if (snapshot != null) {
            snapshot.close();
            snapshot = null;
        }
        return Fiber.frameReturn();
    }

    @Override
    public FrameCallResult execute(Void input) throws Throwable {
        if (shouldStopReplicate()) {
            return Fiber.frameReturn();
        }
        this.snapshot = stateMachine.takeSnapshot(new SnapshotInfo(raftStatus));
        if (snapshot == null) {
            log.error("open recent snapshot fail, return null");
            return Fiber.frameReturn();
        }
        FiberFrame<Long> f = raftLog.loadNextItemPos(snapshot.getSnapshotInfo().getLastIncludedIndex());
        return Fiber.call(f, result -> afterLoadNextItemPos(result, snapshot));
    }

    private FrameCallResult afterLoadNextItemPos(Long nextPos, Snapshot snapshot) {
        if (shouldStopReplicate()) {
            return Fiber.frameReturn();
        }
        log.info("begin install snapshot for member: nodeId={}, groupId={}",
                member.getNode().getNodeId(), groupId);
        this.snapshot = snapshot;
        this.nextPosAfterInstallFinish = nextPos;
        // send the first request, no data
        FiberFuture<Void> f = sendInstallSnapshotReq(null, true, false);
        return f.await(this::afterFirstReqFinished);
    }

    private FrameCallResult afterFirstReqFinished(Void unused) {
        if (shouldStopReplicate()) {
            return Fiber.frameReturn();
        }
        Supplier<RefBuffer> bufferCreator = () -> heapPool.create(groupConfig.getReplicateSnapshotBufferSize());

        int readConcurrency = groupConfig.getSnapshotConcurrency();
        int writeConcurrency = groupConfig.getReplicateSnapshotConcurrency();
        SnapshotReader r = new SnapshotReader(snapshot, readConcurrency, writeConcurrency, this::readerCallback,
                this::shouldStopReplicate, bufferCreator);
        return Fiber.call(r, this::afterReaderFinish);
    }

    private FiberFuture<Void> readerCallback(RefBuffer buf, Integer readBytes) {
        buf.getBuffer().clear();
        buf.getBuffer().limit(readBytes);
        return sendInstallSnapshotReq(buf, false, false);
    }

    private FrameCallResult afterReaderFinish(Void unused) {
        if (shouldStopReplicate()) {
            return Fiber.frameReturn();
        }
        return sendInstallSnapshotReq(null, false, true)
                .await(this::afterInstallFinish);
    }

    private FrameCallResult afterInstallFinish(Void unused) {
        if (shouldStopReplicate()) {
            return Fiber.frameReturn();
        }
        replicateManager.tryStartReplicateFibers();
        return Fiber.frameReturn();
    }

    private FiberFuture<Void> sendInstallSnapshotReq(RefBuffer data, boolean start, boolean finish) {
        SnapshotInfo si = snapshot.getSnapshotInfo();
        InstallSnapshotReq req = new InstallSnapshotReq();
        req.groupId = groupId;
        req.term = raftStatus.currentTerm;
        req.leaderId = serverConfig.getNodeId();
        req.lastIncludedIndex = si.getLastIncludedIndex();
        req.lastIncludedTerm = si.getLastIncludedTerm();
        req.offset = snapshotOffset;

        if (start) {
            req.members.addAll(si.getMembers());
            req.observers.addAll(si.getObservers());
            req.preparedMembers.addAll(si.getPreparedMembers());
            req.preparedObservers.addAll(si.getPreparedObservers());
            req.lastConfigChangeIndex = si.getLastConfigChangeIndex();
        }
        if (finish) {
            req.done = true;
            req.nextWritePos = nextPosAfterInstallFinish;
        }
        req.data = data;

        // data buffer released in WritePacket
        InstallSnapshotReq.InstallReqWritePacket wf = new InstallSnapshotReq.InstallReqWritePacket(req);
        wf.setCommand(Commands.RAFT_INSTALL_SNAPSHOT);
        FiberGroup fg = groupConfig.getFiberGroup();
        FiberFuture<Void> f = fg.newFuture("install-" + groupId + "-" + req.offset);
        DtTime timeout = new DtTime(serverConfig.getRpcTimeout(), TimeUnit.MILLISECONDS);
        RpcCallback<AppendResp> callback = (resp, ex) ->
                fg.getExecutor().execute(() -> afterInstallRpc(resp, ex, req, f));
        client.sendRequest(member.getNode().getPeer(), wf, APPEND_RESP_DECODER_CALLBACK_CREATOR,
                timeout, callback);
        int bytes = data == null ? 0 : data.getBuffer().remaining();
        snapshotOffset += bytes;
        log.info("transfer snapshot data to member {}. groupId={}, offset={}, bytes={}, done={}",
                member.getNode().getNodeId(), groupId, req.offset, bytes, req.done);
        return f;
    }

    private void afterInstallRpc(ReadPacket<AppendResp> rf, Throwable ex,
                                 InstallSnapshotReq req, FiberFuture<Void> f) {
        if (epochChange()) {
            f.completeExceptionally(new RaftCancelException("epoch not match, ignore install snapshot response."));
            return;
        }
        if (ex != null) {
            incrementEpoch();
            f.completeExceptionally(ex);
            return;
        }
        AppendResp respBody = rf.getBody();
        if (!respBody.success) {
            incrementEpoch();
            f.completeExceptionally(new RaftException("install snapshot fail. remoteNode="
                    + member.getNode().getNodeId() + ", groupId=" + groupId));
            return;
        }
        if (replicateManager.checkTermFailed(respBody.term, false)) {
            incrementEpoch();
            f.completeExceptionally(new RaftException("remote node has larger term. remoteNode="
                    + member.getNode().getNodeId() + ", groupId=" + groupId));
            return;
        }
        if (req.done) {
            log.info("install snapshot for member finished success. nodeId={}, groupId={}",
                    member.getNode().getNodeId(), groupId);
            incrementEpoch();
            member.setInstallSnapshot(false);
            member.setMatchIndex(req.lastIncludedIndex);
            member.setNextIndex(req.lastIncludedIndex + 1);
        }
        f.complete(null);
    }

}
