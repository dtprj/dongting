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
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.common.Timestamp;
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
import com.github.dtprj.dongting.raft.rpc.InstallSnapshotResp;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.sm.Snapshot;
import com.github.dtprj.dongting.raft.sm.StateMachine;
import com.github.dtprj.dongting.raft.store.RaftLog;
import com.github.dtprj.dongting.raft.store.StatusManager;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class ReplicateManager {

    private static final DtLog log = DtLogs.getLogger(ReplicateManager.class);
    private static final long FAIL_TIMEOUT = Duration.ofSeconds(1).toNanos();

    private final int groupId;
    private final RaftStatusImpl raftStatus;
    private final RaftServerConfig config;
    private final RaftLog raftLog;
    private final StateMachine stateMachine;
    private final NioClient client;
    private final RaftExecutor raftExecutor;
    private final CommitManager commitManager;
    private final Timestamp ts;

    private final int maxReplicateItems;
    private final int restItemsToStartReplicate;
    private final long maxReplicateBytes;
    private final StatusManager statusManager;

    private long installSnapshotFailTime;

    private static final PbNoCopyDecoder<AppendRespCallback> APPEND_RESP_DECODER = new PbNoCopyDecoder<>(c -> new AppendRespCallback());
    private static final PbNoCopyDecoder<InstallSnapshotResp> INSTALL_SNAPSHOT_RESP_DECODER = new PbNoCopyDecoder<>(c -> new InstallSnapshotResp.Callback());

    public ReplicateManager(RaftServerConfig config, RaftGroupConfig groupConfig, RaftStatusImpl raftStatus, RaftLog raftLog,
                            StateMachine stateMachine, NioClient client, RaftExecutor executor,
                            CommitManager commitManager, StatusManager statusManager) {
        this.groupId = groupConfig.getGroupId();
        this.raftStatus = raftStatus;
        this.config = config;
        this.raftLog = raftLog;
        this.stateMachine = stateMachine;
        this.client = client;
        this.raftExecutor = executor;
        this.commitManager = commitManager;
        this.ts = raftStatus.getTs();

        this.maxReplicateItems = config.getMaxReplicateItems();
        this.maxReplicateBytes = config.getMaxReplicateBytes();
        this.statusManager = statusManager;
        this.restItemsToStartReplicate = (int) (maxReplicateItems * 0.1);

        this.installSnapshotFailTime = ts.getNanoTime() - TimeUnit.SECONDS.toNanos(10);
    }

    private static void release(List<LogItem> items) {
        if (items == null) {
            return;
        }
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < items.size(); i++) {
            items.get(i).release();
        }
    }

    @SuppressWarnings("ForLoopReplaceableByForEach")
    public void replicateAfterRaftExec(RaftStatusImpl raftStatus) {
        List<RaftMember> list = raftStatus.getReplicateList();
        int len = list.size();
        for (int i = 0; i < len; i++) {
            RaftMember node = list.get(i);
            if (node.getNode().isSelf()) {
                continue;
            }
            replicate(node);
        }
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private void replicate(RaftMember member) {
        if (raftStatus.getRole() != RaftRole.leader) {
            return;
        }
        if (!member.isReady()) {
            return;
        }
        if (member.getReplicateFuture() != null) {
            return;
        }
        if (member.getPendingStat().getPendingBytesPlain() >= maxReplicateBytes) {
            return;
        }
        if (ts.getNanoTime() - member.getLastFailNanos() <= FAIL_TIMEOUT) {
            return;
        }
        if (member.isInstallSnapshot()) {
            installSnapshot(member);
        } else if (member.isMultiAppend()) {
            doReplicate(member);
        } else {
            if (member.getPendingStat().getPendingRequestsPlain() == 0) {
                doReplicate(member);
            } else {
                // waiting all pending request complete
            }
        }
    }

    private void doReplicate(RaftMember member) {
        long nextIndex = member.getNextIndex();
        long diff = raftStatus.getLastLogIndex() - nextIndex + 1;
        if (diff <= 0) {
            // no data to replicate
            return;
        }

        // flow control
        PendingStat ps = member.getPendingStat();
        int rest = maxReplicateItems - ps.getPendingRequestsPlain();
        if (rest <= restItemsToStartReplicate) {
            // avoid silly window syndrome
            return;
        }

        int limit = member.isMultiAppend() ? (int) Math.min(rest, diff) : 1;

        RaftTask first = raftStatus.getTailCache().get(nextIndex);
        if (first != null && !first.getInput().isReadOnly()) {
            RaftUtil.closeIterator(member);
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
                if (limit == 0 || ps.getPendingBytesPlain() >= maxReplicateBytes) {
                    return;
                }
            }
        } else {
            RaftLog.LogIterator logIterator = member.getReplicateIterator();
            if (logIterator == null) {
                int currentEpoch = member.getReplicateEpoch();
                logIterator = raftLog.openIterator(() -> member.getReplicateEpoch() != currentEpoch);
                member.setReplicateIterator(logIterator);
            }
            CompletableFuture<List<LogItem>> future = logIterator.next(nextIndex, Math.min(limit, 1024),
                    config.getSingleReplicateLimit());
            member.setReplicateFuture(future);
            future.whenCompleteAsync((items, ex) -> resumeAfterLogLoad(member.getReplicateEpoch(), member, items, ex),
                    raftStatus.getRaftExecutor());
        }
    }

    private void resumeAfterLogLoad(int repEpoch, RaftMember member, List<LogItem> items, Throwable ex) {
        member.setReplicateFuture(null);
        if (epochNotMatch(member, repEpoch)) {
            log.info("replicate epoch changed, ignore load result");
            release(items);
            RaftUtil.closeIterator(member);
            return;
        }
        if (ex != null) {
            if (ex instanceof CancellationException) {
                log.info("ReplicateManager load raft log cancelled");
            } else {
                // if log is deleted, the next load will never success, so we need to reset nextIndex.
                // however, the exception may be caused by other reasons
                member.setNextIndex(raftStatus.getLastLogIndex() + 1);
                member.setLastFailNanos(ts.getNanoTime());
                log.error("load raft log failed", ex);
            }
            RaftUtil.closeIterator(member);
            return;
        }
        if (!member.isReady()) {
            log.warn("member is not ready, ignore load result");
            release(items);
            RaftUtil.closeIterator(member);
            return;
        }
        if (member.isInstallSnapshot()) {
            log.warn("member is installing snapshot, ignore load result");
            release(items);
            RaftUtil.closeIterator(member);
            return;
        }
        if (items == null || items.isEmpty()) {
            log.warn("load raft log return empty, ignore load result");
            release(items);
            RaftUtil.closeIterator(member);
            return;
        }
        if (member.getNextIndex() != items.get(0).getIndex()) {
            log.warn("the first load item index not match nextIndex, ignore load result");
            release(items);
            RaftUtil.closeIterator(member);
            return;
        }

        if (member.isMultiAppend()) {
            // release in AppendReqWriteFrame
            sendAppendRequest(member, items);
            replicate(member);
        } else {
            if (member.getPendingStat().getPendingRequestsPlain() == 0) {
                // release in AppendReqWriteFrame
                sendAppendRequest(member, items);
            } else {
                BugLog.getLog().error("pending request not empty, ignore load result");
                release(items);
                RaftUtil.closeIterator(member);
            }
        }

    }

    private void sendAppendRequest(RaftMember member, List<LogItem> items) {
        LogItem firstItem = items.get(0);
        long bytes = 0;
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < items.size(); i++) {
            LogItem item = items.get(i);
            bytes += item.getActualBodySize();
        }
        sendAppendRequest(member, firstItem.getIndex() - 1, firstItem.getPrevLogTerm(), items, bytes);
    }

    private void sendAppendRequest(RaftMember member, long prevLogIndex, int prevLogTerm, List<LogItem> logs, long bytes) {
        AppendReqWriteFrame req = new AppendReqWriteFrame(stateMachine);
        req.setCommand(Commands.RAFT_APPEND_ENTRIES);
        req.setGroupId(groupId);
        req.setTerm(raftStatus.getCurrentTerm());
        req.setLeaderId(config.getNodeId());
        req.setLeaderCommit(raftStatus.getCommitIndex());
        req.setPrevLogIndex(prevLogIndex);
        req.setPrevLogTerm(prevLogTerm);
        req.setLogs(logs);

        member.setNextIndex(prevLogIndex + 1 + logs.size());

        DtTime timeout = new DtTime(config.getRpcTimeout(), TimeUnit.MILLISECONDS);
        CompletableFuture<ReadFrame<AppendRespCallback>> f = client.sendRequest(member.getNode().getPeer(),
                req, APPEND_RESP_DECODER, timeout);
        registerAppendResultCallback(member, prevLogIndex, prevLogTerm, f, logs, bytes);
    }

    private void registerAppendResultCallback(RaftMember member, long prevLogIndex, int prevLogTerm,
                                              CompletableFuture<ReadFrame<AppendRespCallback>> f,
                                              List<LogItem> logs, long bytes) {
        int reqTerm = raftStatus.getCurrentTerm();
        // the time refresh happens before this line
        long reqNanos = ts.getNanoTime();
        // if PendingStat is reset, we should not invoke decrAndGetPendingRequests() on new instance
        final int logSize = logs.size();
        member.getPendingStat().incrPlain(logSize, bytes);
        int repEpoch = member.getReplicateEpoch();
        for (int i = 0; i < logSize; i++) {
            LogItem item = logs.get(i);
            item.retain();
        }
        f.whenCompleteAsync((rf, ex) -> {
            for (int i = 0; i < logSize; i++) {
                LogItem item = logs.get(i);
                item.release();
            }
            // can't access logs after release

            if (reqTerm != raftStatus.getCurrentTerm()) {
                log.info("receive outdated append result, term not match. reqTerm={}, currentTerm={}",
                        reqTerm, raftStatus.getCurrentTerm());
                RaftUtil.closeIterator(member);
                return;
            }
            if (epochNotMatch(member, repEpoch)) {
                log.info("receive outdated append result, replicateEpoch not match. ignore.");
                RaftUtil.closeIterator(member);
                return;
            }
            member.getPendingStat().decrPlain(logSize, bytes);
            if (ex == null) {
                processAppendResult(member, rf, prevLogIndex, prevLogTerm, reqTerm, reqNanos, logSize, repEpoch);
            } else {
                RaftUtil.closeIterator(member);
                member.incrReplicateEpoch(repEpoch);
                member.setLastFailNanos(ts.getNanoTime());
                if (member.isMultiAppend()) {
                    member.setMultiAppend(false);
                    String msg = "append fail. remoteId={}, groupId={}, localTerm={}, reqTerm={}, prevLogIndex={}";
                    log.warn(msg, member.getNode().getNodeId(), groupId, raftStatus.getCurrentTerm(),
                            reqTerm, prevLogIndex, ex);
                } else {
                    String msg = "append fail. remoteId={}, groupId={}, localTerm={}, reqTerm={}, prevLogIndex={}, ex={}";
                    log.warn(msg, member.getNode().getNodeId(), groupId, raftStatus.getCurrentTerm(),
                            reqTerm, prevLogIndex, ex.toString());
                }
            }
        }, raftExecutor);
    }

    private boolean checkTermFailed(int remoteTerm) {
        if (remoteTerm > raftStatus.getCurrentTerm()) {
            log.info("find remote term greater than local term. remoteTerm={}, localTerm={}",
                    remoteTerm, raftStatus.getCurrentTerm());
            RaftUtil.incrTerm(remoteTerm, raftStatus, -1);
            statusManager.persistSync();
            return true;
        }

        return false;
    }

    // in raft thread
    private void processAppendResult(RaftMember member, ReadFrame<AppendRespCallback> rf, long prevLogIndex,
                                     int prevLogTerm, int reqTerm, long reqNanos, int count, int reqEpoch) {
        long expectNewMatchIndex = prevLogIndex + count;
        AppendRespCallback body = rf.getBody();
        RaftStatusImpl raftStatus = this.raftStatus;
        int remoteTerm = body.getTerm();
        if (checkTermFailed(remoteTerm)) {
            RaftUtil.closeIterator(member);
            return;
        }
        if (member.isInstallSnapshot()) {
            BugLog.getLog().error("receive append result when install snapshot, ignore. prevLogIndex={}, prevLogTerm={}, remoteId={}, groupId={}",
                    prevLogIndex, prevLogTerm, member.getNode().getNodeId(), groupId);
            RaftUtil.closeIterator(member);
            return;
        }
        if (body.isSuccess()) {
            if (member.getMatchIndex() <= prevLogIndex) {
                updateLease(member, reqNanos, raftStatus);
                member.setMatchIndex(expectNewMatchIndex);
                member.setMultiAppend(true);
                commitManager.tryCommit(expectNewMatchIndex);
                if (raftStatus.getLastLogIndex() >= member.getNextIndex()) {
                    replicate(member);
                }
            } else {
                BugLog.getLog().error("append miss order. old matchIndex={}, append prevLogIndex={}," +
                                " expectNewMatchIndex={}, remoteId={}, groupId={}, localTerm={}, reqTerm={}, remoteTerm={}",
                        member.getMatchIndex(), prevLogIndex, expectNewMatchIndex, member.getNode().getNodeId(),
                        groupId, raftStatus.getCurrentTerm(), reqTerm, body.getTerm());
                RaftUtil.closeIterator(member);
            }
        } else {
            RaftUtil.closeIterator(member);
            member.setMultiAppend(false);
            member.incrReplicateEpoch(reqEpoch);
            int appendCode = body.getAppendCode();
            if (appendCode == AppendProcessor.CODE_LOG_NOT_MATCH) {
                updateLease(member, reqNanos, raftStatus);
                processLogNotMatch(member, prevLogIndex, prevLogTerm, body, raftStatus);
            } else if (appendCode == AppendProcessor.CODE_SERVER_ERROR) {
                updateLease(member, reqNanos, raftStatus);
                member.setLastFailNanos(ts.getNanoTime());
                log.error("append fail because of remote error. groupId={}, prevLogIndex={}, msg={}",
                        groupId, prevLogIndex, rf.getMsg());
            } else if (appendCode == AppendProcessor.CODE_INSTALL_SNAPSHOT) {
                log.warn("append fail because of member is install snapshot. groupId={}, remoteId={}",
                        groupId, member.getNode().getNodeId());
                updateLease(member, reqNanos, raftStatus);
                beginInstallSnapshot(member);
            } else {
                member.setLastFailNanos(ts.getNanoTime());
                BugLog.getLog().error("append fail. appendCode={}, old matchIndex={}, append prevLogIndex={}, " +
                                "expectNewMatchIndex={}, remoteId={}, groupId={}, localTerm={}, reqTerm={}, remoteTerm={}",
                        AppendProcessor.getCodeStr(appendCode), member.getMatchIndex(), prevLogIndex, expectNewMatchIndex,
                        member.getNode().getNodeId(), groupId, raftStatus.getCurrentTerm(), reqTerm, body.getTerm());
            }
        }
    }

    private void updateLease(RaftMember member, long reqNanos, RaftStatusImpl raftStatus) {
        member.setLastConfirmReqNanos(reqNanos);
        RaftUtil.updateLease(raftStatus);
    }

    private void processLogNotMatch(RaftMember member, long prevLogIndex, int prevLogTerm, AppendRespCallback body, RaftStatusImpl raftStatus) {
        log.info("log not match. remoteId={}, groupId={}, matchIndex={}, prevLogIndex={}, prevLogTerm={}, remoteLogTerm={}, remoteLogIndex={}, localTerm={}",
                member.getNode().getNodeId(), groupId, member.getMatchIndex(), prevLogIndex, prevLogTerm, body.getSuggestTerm(),
                body.getSuggestIndex(), raftStatus.getCurrentTerm());
        int reqEpoch = member.getReplicateEpoch();
        if (body.getSuggestTerm() == 0 && body.getSuggestIndex() == 0) {
            log.info("remote has no suggest match index, begin install snapshot. remoteId={}, groupId={}",
                    member.getNode().getNodeId(), groupId);
            beginInstallSnapshot(member);
            return;
        }
        CompletableFuture<Pair<Integer, Long>> future = raftLog.tryFindMatchPos(
                body.getSuggestTerm(), body.getSuggestIndex(),
                () -> epochNotMatch(member, reqEpoch));
        member.setReplicateFuture(future);
        future.whenCompleteAsync((r, ex) -> resumeAfterFindReplicatePos(r, ex, member, reqEpoch,
                body.getSuggestTerm(), body.getSuggestIndex()), raftExecutor);
    }

    private void resumeAfterFindReplicatePos(Pair<Integer, Long> result, Throwable ex, RaftMember member,
                                               int reqEpoch, int suggestTerm, long suggestIndex) {
        member.setReplicateFuture(null);
        if (epochNotMatch(member, reqEpoch)) {
            log.info("epoch not match. ignore result of nextIndexToReplicate call");
            return;
        }
        if (ex == null) {
            if (result == null) {
                log.info("follower has no suggest match index, begin install snapshot. remoteId={}, groupId={}",
                        member.getNode().getNodeId(), groupId);
                beginInstallSnapshot(member);
            } else {
                if (result.getLeft() == suggestTerm && result.getRight() == suggestIndex) {
                    log.info("match success: remote={}, group={}, term={}, index={}",
                            member.getNode().getNodeId(), groupId, suggestTerm, suggestIndex);
                } else {
                    log.info("leader suggest: term={}, index={}, remote={}, group={}",
                            result.getLeft(), result.getRight(), member.getNode().getNodeId(), groupId);
                }
                member.setNextIndex(result.getRight() + 1);
                replicate(member);
            }
        } else {
            member.setLastFailNanos(ts.getNanoTime());
            log.error("nextIndexToReplicate fail", ex);
        }
    }

    private void beginInstallSnapshot(RaftMember member) {
        if (!member.isInstallSnapshot()) {
            member.setInstallSnapshot(true);
            member.setPendingStat(new PendingStat());
            installSnapshot(member);
        } else {
            BugLog.getLog().error("member is installing snapshot, ignore");
        }
    }

    private void installSnapshot(RaftMember member) {
        openSnapshotIterator(member);
        SnapshotInfo si = member.getSnapshotInfo();
        if (si == null) {
            return;
        }
        if (si.readFinished) {
            return;
        }
        int reqEpoch = member.getReplicateEpoch();
        try {
            CompletableFuture<RefBuffer> future = si.snapshot.readNext();
            member.setReplicateFuture(future);
            future.whenCompleteAsync((rb, ex) -> resumeAfterSnapshotLoad(rb, ex, member, si, reqEpoch), raftExecutor);
        } catch (Exception e) {
            processInstallSnapshotError(member, si, e, reqEpoch);
        }
    }

    private void resumeAfterSnapshotLoad(RefBuffer rb, Throwable ex,
                                         RaftMember member, SnapshotInfo si, int reqEpoch) {
        member.setReplicateFuture(null);
        if (epochNotMatch(member, reqEpoch)) {
            log.info("epoch not match, abort install snapshot.");
            closeSnapshotAndResetStatus(member, si);
            if (rb != null) {
                rb.release();
            }
            return;
        }
        if (ex != null) {
            processInstallSnapshotError(member, si, ex, reqEpoch);
            if (rb != null) {
                rb.release();
            }
            return;
        }
        if (!member.isReady()) {
            log.info("member is not ready, abort install snapshot.");
            closeSnapshotAndResetStatus(member, si);
            if (rb != null) {
                rb.release();
            }
            return;
        }
        // rb release in InstallReqWriteFrame.doClean()
        sendInstallSnapshotReq(member, si, rb);
        replicate(member);
    }

    private boolean epochNotMatch(RaftMember member, int reqEpoch) {
        return reqEpoch != member.getReplicateEpoch();
    }

    private void processInstallSnapshotError(RaftMember member, SnapshotInfo si, Throwable e, int reqEpoch) {
        installSnapshotFailTime = raftStatus.getTs().getNanoTime();
        member.incrReplicateEpoch(reqEpoch);
        member.setLastFailNanos(ts.getNanoTime());
        if (e != null) {
            log.error("install snapshot fail", e);
        }
        closeSnapshotAndResetStatus(member, si);
    }

    private void closeSnapshotAndResetStatus(RaftMember member, SnapshotInfo si) {
        try {
            if (si != null) {
                si.snapshot.close();
            }
        } catch (Throwable e) {
            log.error("close snapshot fail", e);
        }
        member.setSnapshotInfo(null);
        member.setPendingStat(new PendingStat());
    }

    private void openSnapshotIterator(RaftMember member) {
        SnapshotInfo si = member.getSnapshotInfo();
        if (si != null) {
            return;
        }
        long diff = raftStatus.getTs().getNanoTime() - installSnapshotFailTime;
        if (TimeUnit.NANOSECONDS.toMillis(diff) < 1000) {
            return;
        }
        try {
            Snapshot snapshot = stateMachine.takeSnapshot(raftStatus.getCurrentTerm());
            if (snapshot == null) {
                installSnapshotFailTime = raftStatus.getTs().getNanoTime();
                log.error("open recent snapshot fail, return null");
                return;
            }
            long nextPos = raftLog.syncLoadNextItemPos(snapshot.getLastIncludedIndex());
            si = new SnapshotInfo(snapshot, nextPos);
            log.info("begin install snapshot for member: nodeId={}, groupId={}", member.getNode().getNodeId(), groupId);
            si.offset = 0;
            member.setSnapshotInfo(si);
        } catch (Exception e) {
            installSnapshotFailTime = raftStatus.getTs().getNanoTime();
            log.error("open recent snapshot fail", e);
        }
    }

    private void sendInstallSnapshotReq(RaftMember member, SnapshotInfo si, RefBuffer data) {
        InstallSnapshotReq req = new InstallSnapshotReq();
        req.groupId = groupId;
        req.term = raftStatus.getCurrentTerm();
        req.leaderId = config.getNodeId();
        req.lastIncludedIndex = si.snapshot.getLastIncludedIndex();
        req.lastIncludedTerm = si.snapshot.getLastIncludedTerm();
        req.offset = si.offset;
        req.nextWritePos = si.nextWritePos;
        req.data = data;
        req.done = data == null || data.getBuffer() == null || !data.getBuffer().hasRemaining();

        if (req.done) {
            si.readFinished = true;
        }

        InstallSnapshotReq.InstallReqWriteFrame wf = new InstallSnapshotReq.InstallReqWriteFrame(req);
        wf.setCommand(Commands.RAFT_INSTALL_SNAPSHOT);
        DtTime timeout = new DtTime(config.getRpcTimeout(), TimeUnit.MILLISECONDS);
        CompletableFuture<ReadFrame<InstallSnapshotResp>> future = client.sendRequest(
                member.getNode().getPeer(), wf, INSTALL_SNAPSHOT_RESP_DECODER, timeout);
        int bytes = data == null ? 0 : data.getBuffer().remaining();
        si.offset += bytes;
        registerInstallSnapshotCallback(future, member, si, req.term, req.offset, bytes, req.done, req.lastIncludedIndex);
    }

    private void registerInstallSnapshotCallback(CompletableFuture<ReadFrame<InstallSnapshotResp>> future, RaftMember member,
                                                 SnapshotInfo si, int reqTerm, long reqOffset,
                                                 int reqBytes, boolean reqDone, long reqLastIncludedIndex) {
        PendingStat pd = member.getPendingStat();
        pd.incrPlain(1, reqBytes);
        int reqEpoch = member.getReplicateEpoch();
        future.whenCompleteAsync((rf, ex) -> {
            if (reqTerm != raftStatus.getCurrentTerm()) {
                log.info("receive outdated append result, term not match. reqTerm={}, currentTerm={}, remoteNode={}, groupId={}",
                        reqTerm, raftStatus.getCurrentTerm(), member.getNode().getNodeId(), groupId);
                return;
            }
            if (epochNotMatch(member, reqEpoch)) {
                log.info("epoch not match, ignore install snapshot response.");
                return;
            }
            pd.decrPlain(1, reqBytes);
            if (ex != null) {
                processInstallSnapshotError(member, si, ex, reqEpoch);
                return;
            }
            InstallSnapshotResp respBody = rf.getBody();
            if (!respBody.success) {
                log.error("send install snapshot fail. remoteNode={}, groupId={}",
                        member.getNode().getNodeId(), groupId);
                processInstallSnapshotError(member, si, null, reqEpoch);
                return;
            }
            if (checkTermFailed(respBody.term)) {
                return;
            }
            log.info("transfer snapshot data to member. nodeId={}, groupId={}, offset={}",
                    member.getNode().getNodeId(), groupId, reqOffset);
            if (reqDone) {
                log.info("install snapshot for member finished success. nodeId={}, groupId={}",
                        member.getNode().getNodeId(), groupId);
                closeSnapshotAndResetStatus(member, si);
                member.setInstallSnapshot(false);
                member.setMatchIndex(reqLastIncludedIndex);
                member.setNextIndex(reqLastIncludedIndex + 1);
            }
            replicate(member);

        }, raftExecutor);
    }

}
