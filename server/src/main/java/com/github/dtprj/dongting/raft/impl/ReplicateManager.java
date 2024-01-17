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

import com.github.dtprj.dongting.codec.PbNoCopyDecoder;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberFrame;
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
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.sm.StateMachine;
import com.github.dtprj.dongting.raft.store.RaftLog;
import com.github.dtprj.dongting.raft.store.StatusManager;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class ReplicateManager {
    private static final DtLog log = DtLogs.getLogger(ReplicateManager.class);

    private static final long FAIL_TIMEOUT = Duration.ofSeconds(1).toNanos();

    final int groupId;
    private final RaftGroupConfigEx groupConfig;
    private final GroupComponents gc;
    private final RaftStatusImpl raftStatus;
    private final RaftServerConfig config;
    private final StateMachine stateMachine;
    private final NioClient client;
    private final CommitManager commitManager;
    private final Timestamp ts;

    private final StatusManager statusManager;

    private long installSnapshotFailTime;

    public ReplicateManager(GroupComponents gc, NioClient client) {
        this.groupConfig = gc.getGroupConfig();
        this.gc = gc;
        this.groupId = groupConfig.getGroupId();
        this.raftStatus = (RaftStatusImpl) groupConfig.getRaftStatus();
        this.config = gc.getServerConfig();
        this.stateMachine = gc.getStateMachine();
        this.client = client;
        this.commitManager = gc.getCommitManager();
        this.ts = raftStatus.getTs();

        this.statusManager = gc.getStatusManager();

        this.installSnapshotFailTime = ts.getNanoTime() - TimeUnit.SECONDS.toNanos(10);
    }

    public void startReplicateFibers() {
        for (RaftMember m : raftStatus.getReplicateList()) {
            if (m.getNode().isSelf()) {
                continue;
            }
            RepFrame ff = new RepFrame(gc, client, this, m.getReplicateEpoch(), m, 0);
            Fiber f = new Fiber("replicate-" + m.getNode().getNodeId() + "-" + m.getReplicateEpoch(),
                    groupConfig.getFiberGroup(), ff, true);
            f.start();
        }
    }

    public void afterAppendRpc(ReadFrame<AppendRespCallback> rf, Throwable ex, RepFrame repFrame,
                               long prevLogIndex, int prevLogTerm, long reqNanos, int itemCount, long bytes) {
        if (repFrame.epochChange()) {
            log.info("receive outdated append result, replicateEpoch not match. ignore.");
            repFrame.closeIterator();
            return;
        }

        repFrame.descPending(itemCount, bytes);

        if (ex == null) {
            processAppendResult(repFrame, rf, prevLogIndex, prevLogTerm, reqNanos, itemCount);
        } else {
            repFrame.closeIterator();
            RaftMember member = repFrame.getMember();
            member.incrReplicateEpoch(repFrame.getEpoch());
            member.setLastFailNanos(raftStatus.getTs().getNanoTime());
            member.getReplicateCondition().signalAll();

            String msg = "append fail. remoteId={}, groupId={}, localTerm={}, reqTerm={}, prevLogIndex={}";
            log.error(msg, member.getNode().getNodeId(), groupId, raftStatus.getCurrentTerm(),
                    repFrame.getTerm(), prevLogIndex, ex);
        }
    }

    private void processAppendResult(RepFrame repFrame, ReadFrame<AppendRespCallback> rf, long prevLogIndex,
                                     int prevLogTerm, long reqNanos, int count) {
        long expectNewMatchIndex = prevLogIndex + count;
        AppendRespCallback body = rf.getBody();
        RaftStatusImpl raftStatus = this.raftStatus;
        int remoteTerm = body.getTerm();
        if (checkTermFailed(remoteTerm)) {
            repFrame.closeIterator();
            return;
        }
        RaftMember member = repFrame.getMember();
        if (member.isInstallSnapshot()) {
            BugLog.getLog().error("receive append result when install snapshot, ignore. prevLogIndex={}, prevLogTerm={}, remoteId={}, groupId={}",
                    prevLogIndex, prevLogTerm, member.getNode().getNodeId(), groupId);
            repFrame.closeIterator();
            return;
        }
        if (body.isSuccess()) {
            if (member.getMatchIndex() <= prevLogIndex) {
                updateLease(member, reqNanos, raftStatus);
                member.setMatchIndex(expectNewMatchIndex);
                repFrame.setMultiAppend(true);
                commitManager.tryCommit(expectNewMatchIndex);
                if (raftStatus.getLastLogIndex() >= member.getNextIndex()) {
                    raftStatus.getReplicateCondition(member.getNode().getNodeId()).signalAll();
                }
            } else {
                BugLog.getLog().error("append miss order. old matchIndex={}, append prevLogIndex={}," +
                                " expectNewMatchIndex={}, remoteId={}, groupId={}, localTerm={}, reqTerm={}, remoteTerm={}",
                        member.getMatchIndex(), prevLogIndex, expectNewMatchIndex, member.getNode().getNodeId(),
                        groupId, raftStatus.getCurrentTerm(), repFrame.getTerm(), body.getTerm());
                repFrame.closeIterator();
            }
        } else {
            repFrame.closeIterator();
            member.incrReplicateEpoch(repFrame.getEpoch());
            int appendCode = body.getAppendCode();
            if (appendCode == AppendProcessor.CODE_LOG_NOT_MATCH) {
                updateLease(member, reqNanos, raftStatus);
                // TODO processLogNotMatch(member, prevLogIndex, prevLogTerm, body, raftStatus);
            } else if (appendCode == AppendProcessor.CODE_SERVER_ERROR) {
                updateLease(member, reqNanos, raftStatus);
                member.setLastFailNanos(ts.getNanoTime());
                log.error("append fail because of remote error. groupId={}, prevLogIndex={}, msg={}",
                        groupId, prevLogIndex, rf.getMsg());
            } else if (appendCode == AppendProcessor.CODE_INSTALL_SNAPSHOT) {
                log.warn("append fail because of member is install snapshot. groupId={}, remoteId={}",
                        groupId, member.getNode().getNodeId());
                updateLease(member, reqNanos, raftStatus);
                // TODO beginInstallSnapshot(member);
            } else {
                member.setLastFailNanos(ts.getNanoTime());
                BugLog.getLog().error("append fail. appendCode={}, old matchIndex={}, append prevLogIndex={}, " +
                                "expectNewMatchIndex={}, remoteId={}, groupId={}, localTerm={}, reqTerm={}, remoteTerm={}",
                        AppendProcessor.getCodeStr(appendCode), member.getMatchIndex(), prevLogIndex, expectNewMatchIndex,
                        member.getNode().getNodeId(), groupId, raftStatus.getCurrentTerm(), repFrame.getTerm(), body.getTerm());
            }
        }
    }

    private boolean checkTermFailed(int remoteTerm) {
        if (remoteTerm > raftStatus.getCurrentTerm()) {
            log.info("find remote term greater than local term. remoteTerm={}, localTerm={}",
                    remoteTerm, raftStatus.getCurrentTerm());
            RaftUtil.incrTerm(remoteTerm, raftStatus, -1);
            statusManager.persistAsync(true);
            return true;
        }

        return false;
    }

    private void updateLease(RaftMember member, long reqNanos, RaftStatusImpl raftStatus) {
        member.setLastConfirmReqNanos(reqNanos);
        RaftUtil.updateLease(raftStatus);
    }

}

class RepFrame extends FiberFrame<Void> {
    private static final DtLog log = DtLogs.getLogger(RepFrame.class);
    private static final long WAIT_CONDITION_TIMEOUT = 1000;

    private final int groupId;
    private final RaftGroupConfigEx groupConfig;
    private final RaftServerConfig config;
    private final NioClient client;
    private final ReplicateManager replicateManager;
    private final RaftLog raftLog;
    private final StateMachine stateMachine;

    private final int epoch;
    private final int term;

    private final RaftMember member;
    private final long initDelayMillis;
    private final FiberCondition repCondition;

    private final RaftStatusImpl raftStatus;

    private final int maxReplicateItems;
    private final int restItemsToStartReplicate;
    private final long maxReplicateBytes;

    int pendingItems;
    long pendingBytes;
    private boolean multiAppend;

    private RaftLog.LogIterator replicateIterator;

    private static final PbNoCopyDecoder<AppendRespCallback> APPEND_RESP_DECODER =
            new PbNoCopyDecoder<>(c -> new AppendRespCallback());

    public RepFrame(GroupComponents gc, NioClient client, ReplicateManager replicateManager,
                    int epoch, RaftMember member, long initDelayMillis) {
        this.config = gc.getServerConfig();
        this.groupConfig = gc.getGroupConfig();
        this.raftLog = gc.getRaftLog();
        this.stateMachine = gc.getStateMachine();
        this.client = client;
        this.replicateManager = replicateManager;
        this.epoch = epoch;
        this.member = member;
        this.initDelayMillis = initDelayMillis;
        this.groupId = groupConfig.getGroupId();

        this.raftStatus = (RaftStatusImpl) groupConfig.getRaftStatus();
        this.term = raftStatus.getCurrentTerm();
        this.repCondition = raftStatus.getReplicateCondition(member.getNode().getNodeId());

        this.maxReplicateItems = config.getMaxReplicateItems();
        this.maxReplicateBytes = config.getMaxReplicateBytes();
        this.restItemsToStartReplicate = (int) (maxReplicateItems * 0.1);
    }

    @Override
    protected FrameCallResult handle(Throwable ex) throws Throwable {
        if (ex instanceof RaftCancelException) {
            log.info("ReplicateManager load raft log cancelled");
        } else {
            log.error("load raft log failed", ex);
            member.setLastFailNanos(raftStatus.getTs().getNanoTime());
            if (raftStatus.getRole() == RaftRole.leader) {
                // if log is deleted, the next load will never success, so we need to reset nextIndex.
                // however, the exception may be caused by other reasons
                member.setNextIndex(raftStatus.getLastLogIndex() + 1);
            }
        }
        closeIterator();
        return Fiber.frameReturn();
    }

    @Override
    public FrameCallResult execute(Void input) {
        if (shouldStopReplicate()) {
            return Fiber.frameReturn();
        }
        if (initDelayMillis > 0) {
            return Fiber.sleepUntilShouldStop(initDelayMillis, this::replicate);
        } else {
            return replicate(null);
        }
    }

    private boolean shouldStopReplicate() {
        if (epochChange()) {
            log.debug("epoch changed, stop replicate fiber. member={}, group={}, newEpoch={}, oldEpoch={}",
                    member.getNode().getNodeId(), groupId, member.getReplicateEpoch(), epoch);
            return true;
        }
        return member.isReady();
    }

    public boolean epochChange() {
        return member.getReplicateEpoch() != epoch;
    }

    private FrameCallResult replicate(Void unused) {
        if (shouldStopReplicate()) {
            return Fiber.frameReturn();
        }
        if (pendingBytes >= maxReplicateBytes) {
            return repCondition.await(WAIT_CONDITION_TIMEOUT, this::replicate);
        }

        if (member.isInstallSnapshot()) {
            // TODO installSnapshot(member);
            return null;
        } else if (multiAppend) {
            return doReplicate(member);
        } else {
            if (pendingItems == 0) {
                return doReplicate(member);
            } else {
                return repCondition.await(WAIT_CONDITION_TIMEOUT, this::replicate);
            }
        }
    }

    private FrameCallResult doReplicate(RaftMember member) {
        long nextIndex = member.getNextIndex();
        long diff = raftStatus.getLastLogIndex() - nextIndex + 1;
        if (diff <= 0) {
            // no data to replicate
            return raftStatus.getDataArrivedCondition().await(WAIT_CONDITION_TIMEOUT, this::replicate);
        }

        // flow control
        int rest = maxReplicateItems - pendingItems;
        if (rest <= restItemsToStartReplicate) {
            // avoid silly window syndrome
            return repCondition.await(WAIT_CONDITION_TIMEOUT, this::replicate);
        }

        int limit = multiAppend ? (int) Math.min(rest, diff) : 1;

        RaftTask first = raftStatus.getTailCache().get(nextIndex);
        if (first != null && !first.getInput().isReadOnly()) {
            closeIterator();
            long sizeLimit = config.getSingleReplicateLimit();
            while (limit > 0) {
                ArrayList<LogItem> items = new ArrayList<>(limit);
                long size = 0;
                for (int i = 0; i < limit; i++) {
                    LogItem li = raftStatus.getTailCache().get(nextIndex + i).getItem();
                    size += li.getActualBodySize();
                    if (size > sizeLimit && i != 0) {
                        break;
                    }
                    items.add(li);
                }
                limit -= items.size();
                sendAppendRequest(member, items);
            }
            return Fiber.resume(null, this::replicate);
        } else {
            RaftLog.LogIterator logIterator = member.getReplicateIterator();
            if (logIterator == null) {
                logIterator = raftLog.openIterator(() -> epochChange());
                member.setReplicateIterator(logIterator);
            }
            FiberFrame<List<LogItem>> nextFrame = logIterator.next(nextIndex, Math.min(limit, 1024),
                    config.getSingleReplicateLimit());
            return Fiber.call(nextFrame, this::resumeAfterLogLoad);
        }
    }

    private FrameCallResult resumeAfterLogLoad(List<LogItem> items) {
        if (shouldStopReplicate()) {
            closeIterator();
            return Fiber.frameReturn();
        }
        if (items == null || items.isEmpty()) {
            log.warn("load raft log return empty, ignore load result");
            closeIterator();
            return Fiber.resume(null, this::replicate);
        }
        if (member.getNextIndex() != items.get(0).getIndex()) {
            log.error("the first load item index not match nextIndex, ignore load result");
            closeIterator();
            return Fiber.resume(null, this::replicate);
        }

        sendAppendRequest(member, items);
        return Fiber.resume(null, this::replicate);
    }

    private void sendAppendRequest(RaftMember member, List<LogItem> items) {
        LogItem firstItem = items.get(0);
        long prevLogIndex = firstItem.getIndex() - 1;

        AppendReqWriteFrame req = new AppendReqWriteFrame(stateMachine);
        req.setCommand(Commands.RAFT_APPEND_ENTRIES);
        req.setGroupId(groupId);
        req.setTerm(raftStatus.getCurrentTerm());
        req.setLeaderId(config.getNodeId());
        req.setLeaderCommit(raftStatus.getCommitIndex());
        req.setPrevLogIndex(prevLogIndex);
        req.setPrevLogTerm(firstItem.getPrevLogTerm());
        req.setLogs(items);

        member.setNextIndex(prevLogIndex + 1 + items.size());

        DtTime timeout = new DtTime(config.getRpcTimeout(), TimeUnit.MILLISECONDS);
        retain(items);// release in AppendReqWriteFrame
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
        f.whenCompleteAsync((rf, ex) -> replicateManager.afterAppendRpc(rf, ex,this, prevLogIndex,
                        firstItem.getPrevLogTerm(), reqNanos, items.size(), finalBytes),
                getFiberGroup().getDispatcher().getExecutor());
    }

    private static void retain(List<LogItem> items) {
        if (items == null) {
            return;
        }
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < items.size(); i++) {
            items.get(i).retain();
        }
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

    public RaftMember getMember() {
        return member;
    }

    public int getTerm() {
        return term;
    }

    public int getEpoch() {
        return epoch;
    }

    public FiberCondition getRepCondition() {
        return repCondition;
    }

    public void setMultiAppend(boolean multiAppend) {
        this.multiAppend = multiAppend;
    }
}
