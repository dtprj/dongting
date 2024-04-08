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
import com.github.dtprj.dongting.codec.PbNoCopyDecoder;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.raft.rpc.AppendProcessor;
import com.github.dtprj.dongting.raft.rpc.AppendReqWriteFrame;
import com.github.dtprj.dongting.raft.rpc.AppendRespCallback;
import com.github.dtprj.dongting.raft.rpc.InstallSnapshotReq;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.sm.Snapshot;
import com.github.dtprj.dongting.raft.sm.StateMachine;
import com.github.dtprj.dongting.raft.store.RaftLog;
import com.github.dtprj.dongting.raft.store.StatusManager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class ReplicateManager {
    private static final DtLog log = DtLogs.getLogger(ReplicateManager.class);

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
        this.groupConfig = gc.getGroupConfig();
        this.groupId = groupConfig.getGroupId();
        this.raftStatus = gc.getRaftStatus();
        this.serverConfig = gc.getServerConfig();
    }

    public void postInit() {
        this.raftLog = gc.getRaftLog();
        this.stateMachine = gc.getStateMachine();
        this.commitManager = gc.getCommitManager();
        this.statusManager = gc.getStatusManager();
    }

    public void tryStartReplicateFibers() {
        if (raftStatus.getRole() != RaftRole.leader) {
            return;
        }
        for (RaftMember m : raftStatus.getReplicateList()) {
            if (m.getNode().isSelf()) {
                continue;
            }
            if (!m.isReady()) {
                continue;
            }
            if (m.getReplicateFiber() == null || m.getReplicateFiber().isFinished()) {
                Fiber f;
                if (m.isInstallSnapshot()) {
                    InstallFrame ff = new InstallFrame(this, m);
                    f = new Fiber("install-" + m.getNode().getNodeId() + "-" + m.getReplicateEpoch(),
                            groupConfig.getFiberGroup(), ff, true);
                } else {
                    RepFrame ff = new RepFrame(this, commitManager, m);
                    f = new Fiber("replicate-" + m.getNode().getNodeId() + "-" + m.getReplicateEpoch(),
                            groupConfig.getFiberGroup(), ff, true);
                }
                f.start();
                m.setReplicateFiber(f);
            }
        }
    }

    boolean checkTermFailed(int remoteTerm) {
        if (remoteTerm > raftStatus.getCurrentTerm()) {
            log.info("find remote term greater than local term. remoteTerm={}, localTerm={}",
                    remoteTerm, raftStatus.getCurrentTerm());
            RaftUtil.incrTerm(remoteTerm, raftStatus, -1);
            statusManager.persistAsync(true);
            return true;
        }

        return false;
    }

}

abstract class AbstractRepFrame extends FiberFrame<Void> {
    private static final DtLog log = DtLogs.getLogger(AbstractRepFrame.class);
    private final int replicateEpoch;
    protected final RaftStatusImpl raftStatus;
    protected final FiberCondition repCondition;
    protected final int term;
    protected final int groupId;
    protected final RaftMember member;

    protected static final PbNoCopyDecoder<AppendRespCallback> APPEND_RESP_DECODER =
            new PbNoCopyDecoder<>(c -> new AppendRespCallback());

    public AbstractRepFrame(ReplicateManager replicateManager, RaftMember member) {
        this.groupId = replicateManager.groupId;
        this.member = member;
        RaftGroupConfigEx groupConfig = replicateManager.groupConfig;
        this.raftStatus = (RaftStatusImpl) groupConfig.getRaftStatus();
        this.replicateEpoch = member.getReplicateEpoch();
        this.term = raftStatus.getCurrentTerm();
        this.repCondition = member.getFinishCondition();
    }

    protected boolean shouldStopReplicate() {
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

    public boolean epochChange() {
        return member.getReplicateEpoch() != replicateEpoch;
    }

    public void incrementEpoch() {
        member.incrementReplicateEpoch(replicateEpoch);
    }

}

class RepFrame extends AbstractRepFrame {
    private static final DtLog log = DtLogs.getLogger(RepFrame.class);
    private static final long WAIT_CONDITION_TIMEOUT = 1000;

    private final RaftGroupConfigEx groupConfig;
    private final RaftServerConfig serverConfig;
    private final NioClient client;
    private final ReplicateManager replicateManager;
    private final CommitManager commitManager;
    private final RaftLog raftLog;

    private final int maxReplicateItems;
    private final int restItemsToStartReplicate;
    private final long maxReplicateBytes;

    int pendingItems;
    long pendingBytes;
    private boolean multiAppend;

    private RaftLog.LogIterator replicateIterator;

    public RepFrame(ReplicateManager replicateManager, CommitManager commitManager, RaftMember member) {
        super(replicateManager, member);
        this.groupConfig = replicateManager.groupConfig;
        this.serverConfig = replicateManager.serverConfig;

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
            log.error("load raft log failed", ex);
            if (raftStatus.getRole() == RaftRole.leader) {
                // if log is deleted, the next load will never success, so we need to reset nextIndex.
                // however, the exception may be caused by other reasons
                member.setNextIndex(raftStatus.getLastLogIndex() + 1);
            }
        }
        return Fiber.frameReturn();
    }

    @Override
    protected FrameCallResult doFinally() {
        closeIterator();
        return Fiber.frameReturn();
    }

    @Override
    public FrameCallResult execute(Void input) {
        if (shouldStopReplicate()) {
            return Fiber.frameReturn();
        }
        if (pendingBytes >= maxReplicateBytes) {
            return repCondition.await(WAIT_CONDITION_TIMEOUT, this);
        }

        if (multiAppend) {
            return doReplicate(member);
        } else {
            if (pendingItems == 0) {
                return doReplicate(member);
            } else {
                return repCondition.await(WAIT_CONDITION_TIMEOUT, this);
            }
        }
    }

    private FrameCallResult doReplicate(RaftMember member) {
        long nextIndex = member.getNextIndex();
        long diff = raftStatus.getLastLogIndex() - nextIndex + 1;
        if (diff <= 0) {
            // no data to replicate
            return raftStatus.getDataArrivedCondition().await(WAIT_CONDITION_TIMEOUT, this);
        }

        // flow control
        int rest = maxReplicateItems - pendingItems;
        if (rest <= restItemsToStartReplicate) {
            // avoid silly window syndrome
            return repCondition.await(WAIT_CONDITION_TIMEOUT, this);
        }

        int limit = multiAppend ? (int) Math.min(rest, diff) : 1;

        RaftTask first = raftStatus.getTailCache().get(nextIndex);
        if (first != null && !first.getInput().isReadOnly()) {
            closeIterator();
            long sizeLimit = groupConfig.getSingleReplicateLimit();
            while (limit > 0) {
                ArrayList<LogItem> items = new ArrayList<>(limit);
                long size = 0;
                for (int i = 0; i < limit; i++) {
                    LogItem li = raftStatus.getTailCache().get(nextIndex + i).getItem();
                    size += li.getActualBodySize();
                    if (size > sizeLimit && i != 0) {
                        break;
                    }
                    li.retain();
                    items.add(li);
                }
                limit -= items.size();
                sendAppendRequest(member, items);
            }
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

        sendAppendRequest(member, items);
        return Fiber.resume(null, this);
    }

    private void sendAppendRequest(RaftMember member, List<LogItem> items) {
        LogItem firstItem = items.get(0);
        long prevLogIndex = firstItem.getIndex() - 1;

        AppendReqWriteFrame req = new AppendReqWriteFrame();
        req.setCommand(Commands.RAFT_APPEND_ENTRIES);
        req.setGroupId(groupId);
        req.setTerm(raftStatus.getCurrentTerm());
        req.setLeaderId(serverConfig.getNodeId());
        req.setLeaderCommit(raftStatus.getCommitIndex());
        req.setPrevLogIndex(prevLogIndex);
        req.setPrevLogTerm(firstItem.getPrevLogTerm());
        req.setLogs(items);

        member.setNextIndex(prevLogIndex + 1 + items.size());

        DtTime timeout = new DtTime(serverConfig.getRpcTimeout(), TimeUnit.MILLISECONDS);
        // release in AppendReqWriteFrame
        CompletableFuture<ReadFrame<AppendRespCallback>> f = client.sendRequest(member.getNode().getPeer(),
                req, APPEND_RESP_DECODER, timeout);

        // the time refresh happens before this line
        long reqNanos = raftStatus.getTs().getNanoTime();

        long bytes = 0;
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < items.size(); i++) {
            LogItem item = items.get(i);
            bytes += item.getActualBodySize();
        }

        pendingItems += items.size();
        pendingBytes += bytes;

        long finalBytes = bytes;
        f.whenCompleteAsync((rf, ex) -> afterAppendRpc(rf, ex, prevLogIndex,
                        firstItem.getPrevLogTerm(), reqNanos, items.size(), finalBytes),
                getFiberGroup().getExecutor());
    }

    void afterAppendRpc(ReadFrame<AppendRespCallback> rf, Throwable ex,
                        long prevLogIndex, int prevLogTerm, long reqNanos, int itemCount, long bytes) {
        if (epochChange()) {
            log.info("receive outdated append result, replicateEpoch not match. ignore.");
            return;
        }

        descPending(itemCount, bytes);

        if (ex == null) {
            processAppendResult(rf, prevLogIndex, prevLogTerm, reqNanos, itemCount);
        } else {
            incrementEpoch();
            repCondition.signalAll();

            String msg = "append fail. remoteId={}, groupId={}, localTerm={}, reqTerm={}, prevLogIndex={}";
            log.error(msg, member.getNode().getNodeId(), groupId, raftStatus.getCurrentTerm(),
                    term, prevLogIndex, ex);
        }
    }

    private void processAppendResult(ReadFrame<AppendRespCallback> rf, long prevLogIndex,
                                     int prevLogTerm, long reqNanos, int count) {
        long expectNewMatchIndex = prevLogIndex + count;
        AppendRespCallback body = rf.getBody();
        RaftStatusImpl raftStatus = this.raftStatus;
        int remoteTerm = body.getTerm();
        if (replicateManager.checkTermFailed(remoteTerm)) {
            return;
        }
        if (member.isInstallSnapshot()) {
            BugLog.getLog().error("receive append result when install snapshot, ignore. prevLogIndex={}, prevLogTerm={}, remoteId={}, groupId={}",
                    prevLogIndex, prevLogTerm, member.getNode().getNodeId(), groupId);
            closeIterator();
            return;
        }
        if (body.isSuccess()) {
            if (member.getMatchIndex() <= prevLogIndex) {
                updateLease(member, reqNanos, raftStatus);
                member.setMatchIndex(expectNewMatchIndex);
                multiAppend = true;
                commitManager.tryCommit(expectNewMatchIndex);
                if (raftStatus.getLastLogIndex() >= member.getNextIndex()) {
                    repCondition.signalAll();
                }
            } else {
                BugLog.getLog().error("append miss order. old matchIndex={}, append prevLogIndex={}," +
                                " expectNewMatchIndex={}, remoteId={}, groupId={}, localTerm={}, reqTerm={}, remoteTerm={}",
                        member.getMatchIndex(), prevLogIndex, expectNewMatchIndex, member.getNode().getNodeId(),
                        groupId, raftStatus.getCurrentTerm(), term, body.getTerm());
                closeIterator();
                incrementEpoch();
            }
        } else {
            closeIterator();
            incrementEpoch();
            int appendCode = body.getAppendCode();
            if (appendCode == AppendProcessor.APPEND_LOG_NOT_MATCH) {
                updateLease(member, reqNanos, raftStatus);
                processLogNotMatch(prevLogIndex, prevLogTerm, body, raftStatus);
            } else if (appendCode == AppendProcessor.APPEND_SERVER_ERROR) {
                updateLease(member, reqNanos, raftStatus);
                log.error("append fail because of remote error. groupId={}, prevLogIndex={}, msg={}",
                        groupId, prevLogIndex, rf.getMsg());
            } else if (appendCode == AppendProcessor.APPEND_INSTALL_SNAPSHOT) {
                log.warn("append fail because of member is install snapshot. groupId={}, remoteId={}",
                        groupId, member.getNode().getNodeId());
                updateLease(member, reqNanos, raftStatus);
                member.setInstallSnapshot(true);
            } else {
                BugLog.getLog().error("append fail. appendCode={}, old matchIndex={}, append prevLogIndex={}, " +
                                "expectNewMatchIndex={}, remoteId={}, groupId={}, localTerm={}, reqTerm={}, remoteTerm={}",
                        AppendProcessor.getAppendResultStr(appendCode), member.getMatchIndex(), prevLogIndex, expectNewMatchIndex,
                        member.getNode().getNodeId(), groupId, raftStatus.getCurrentTerm(), term, body.getTerm());
            }
        }
    }

    private void processLogNotMatch(long prevLogIndex, int prevLogTerm, AppendRespCallback body,
                                    RaftStatusImpl raftStatus) {
        log.info("log not match. remoteId={}, groupId={}, matchIndex={}, prevLogIndex={}, prevLogTerm={}, remoteLogTerm={}, remoteLogIndex={}, localTerm={}",
                member.getNode().getNodeId(), groupId, member.getMatchIndex(), prevLogIndex, prevLogTerm, body.getSuggestTerm(),
                body.getSuggestIndex(), raftStatus.getCurrentTerm());
        if (body.getSuggestTerm() == 0 && body.getSuggestIndex() == 0) {
            log.info("remote has no suggest match index, begin install snapshot. remoteId={}, groupId={}",
                    member.getNode().getNodeId(), groupId);
            member.setInstallSnapshot(true);
            return;
        }
        FiberFrame<Void> ff = new FindMatchPosFrame(replicateManager,member,
                body.getSuggestTerm(), body.getSuggestIndex());
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

class FindMatchPosFrame extends AbstractRepFrame {
    private static final DtLog log = DtLogs.getLogger(FindMatchPosFrame.class);
    private final ReplicateManager replicateManager;
    private final int suggestTerm;
    private final long suggestIndex;

    public FindMatchPosFrame(ReplicateManager replicateManager, RaftMember member, int suggestTerm, long suggestIndex) {
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

class InstallFrame extends AbstractRepFrame {
    private static final DtLog log = DtLogs.getLogger(InstallFrame.class);

    private final RaftLog raftLog;
    private final StateMachine stateMachine;
    private final RaftGroupConfigEx groupConfig;
    private final RaftServerConfig serverConfig;
    private final NioClient client;
    private final ReplicateManager replicateManager;
    private final long restBytesThreshold;

    private boolean readFinish;
    private boolean installFinish;

    private Snapshot snapshot;
    private long nextPosAfterInstallFinish;
    private long snapshotOffset;

    long pendingBytes;

    public InstallFrame(ReplicateManager replicateManager, RaftMember member) {
        super(replicateManager, member);
        this.stateMachine = replicateManager.stateMachine;
        this.raftLog = replicateManager.raftLog;
        this.groupConfig = replicateManager.groupConfig;
        this.serverConfig = replicateManager.serverConfig;
        this.client = replicateManager.client;
        this.replicateManager = replicateManager;
        this.restBytesThreshold = (long) (groupConfig.getMaxReplicateBytes() * 0.1);
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
        return Fiber.call(stateMachine.takeSnapshot(raftStatus.getCurrentTerm()), this::afterTakeSnapshot);
    }

    private FrameCallResult afterTakeSnapshot(Snapshot snapshot) {
        this.snapshot = snapshot;
        if (shouldStopReplicate()) {
            return Fiber.frameReturn();
        }
        if (snapshot == null) {
            log.error("open recent snapshot fail, return null");
            return Fiber.frameReturn();
        }
        FiberFrame<Long> f = raftLog.loadNextItemPos(snapshot.getLastIncludedIndex());
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
        return installSnapshot();
    }

    private FrameCallResult installSnapshot() {
        if (shouldStopReplicate()) {
            return Fiber.frameReturn();
        }
        if (installFinish) {
            return Fiber.frameReturn();
        }
        if (readFinish || groupConfig.getMaxReplicateBytes() - pendingBytes <= restBytesThreshold) {
            // wait rpc finish
            return repCondition.await(1000, v -> installSnapshot());
        }
        FiberFuture<RefBuffer> ff = snapshot.readNext();
        // TODO optimise performance, fiber blocked here
        return ff.await(this::afterSnapshotRead);
    }

    private FrameCallResult afterSnapshotRead(RefBuffer rb) {
        if (shouldStopReplicate()) {
            rb.release();
            return Fiber.frameReturn();
        }
        // rb release in InstallReqWriteFrame.doClean()
        sendInstallSnapshotReq(member, rb);
        return installSnapshot();
    }

    private void sendInstallSnapshotReq(RaftMember member, RefBuffer data) {
        InstallSnapshotReq req = new InstallSnapshotReq();
        req.groupId = groupId;
        req.term = raftStatus.getCurrentTerm();
        req.leaderId = serverConfig.getNodeId();
        req.lastIncludedIndex = snapshot.getLastIncludedIndex();
        req.lastIncludedTerm = snapshot.getLastIncludedTerm();
        req.offset = snapshotOffset;
        req.nextWritePos = nextPosAfterInstallFinish;
        req.data = data;
        req.done = data == null || data.getBuffer() == null || !data.getBuffer().hasRemaining();

        if (req.done) {
            readFinish = true;
        }

        InstallSnapshotReq.InstallReqWriteFrame wf = new InstallSnapshotReq.InstallReqWriteFrame(req);
        wf.setCommand(Commands.RAFT_INSTALL_SNAPSHOT);
        DtTime timeout = new DtTime(serverConfig.getRpcTimeout(), TimeUnit.MILLISECONDS);
        CompletableFuture<ReadFrame<AppendRespCallback>> future = client.sendRequest(
                member.getNode().getPeer(), wf, APPEND_RESP_DECODER, timeout);
        int bytes = data == null ? 0 : data.getBuffer().remaining();
        snapshotOffset += bytes;
        future.whenCompleteAsync((rf, ex) -> afterInstallRpc(
                        rf, ex, req.offset, bytes, req.done, req.lastIncludedIndex),
                getFiberGroup().getExecutor());
    }

    void afterInstallRpc(ReadFrame<AppendRespCallback> rf, Throwable ex, long reqOffset,
                         int reqBytes, boolean reqDone, long reqLastIncludedIndex) {
        try {
            if (epochChange()) {
                log.info("epoch not match, ignore install snapshot response.");
                return;
            }
            descPending(reqBytes);
            if (ex != null) {
                log.error("install snapshot fail, group={},remoteId={}", groupId, member.getNode().getNodeId(), ex);
                incrementEpoch();
                return;
            }
            AppendRespCallback respBody = rf.getBody();
            if (!respBody.isSuccess()) {
                log.error("install snapshot fail. remoteNode={}, groupId={}",
                        member.getNode().getNodeId(), groupId);
                incrementEpoch();
                return;
            }
            if (replicateManager.checkTermFailed(respBody.getTerm())) {
                return;
            }
            log.info("transfer snapshot data to member. nodeId={}, groupId={}, offset={}", member.getNode().getNodeId(), groupId, reqOffset);
            if (reqDone) {
                log.info("install snapshot for member finished success. nodeId={}, groupId={}",
                        member.getNode().getNodeId(), groupId);
                incrementEpoch();
                setInstallFinish(true);
                member.setInstallSnapshot(false);
                member.setMatchIndex(reqLastIncludedIndex);
                member.setNextIndex(reqLastIncludedIndex + 1);
            }
        } finally {
            repCondition.signalAll();
        }
    }

    public void setInstallFinish(boolean installFinish) {
        this.installFinish = installFinish;
    }

    public void descPending(int reqBytes) {
        this.pendingBytes -= reqBytes;
    }
}
