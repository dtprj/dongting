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

import com.github.dtprj.dongting.buf.RefByteBuffer;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.PbZeroCopyDecoder;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.raft.rpc.AppendProcessor;
import com.github.dtprj.dongting.raft.rpc.AppendReqWriteFrame;
import com.github.dtprj.dongting.raft.rpc.AppendRespCallback;
import com.github.dtprj.dongting.raft.rpc.InstallSnapshotReq;
import com.github.dtprj.dongting.raft.rpc.InstallSnapshotResp;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftLog;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.server.Snapshot;
import com.github.dtprj.dongting.raft.server.StateMachine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * @author huangli
 */
public class ReplicateManager {

    private static final DtLog log = DtLogs.getLogger(ReplicateManager.class);

    private final int groupId;
    private final RaftStatus raftStatus;
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

    private long installSnapshotFailTime;

    private static final PbZeroCopyDecoder APPEND_RESP_DECODER = new PbZeroCopyDecoder(c -> new AppendRespCallback());
    private static final PbZeroCopyDecoder INSTALL_SNAPSHOT_RESP_DECODER = new PbZeroCopyDecoder(c -> new InstallSnapshotResp.Callback());

    public ReplicateManager(RaftServerConfig config, int groupId, RaftStatus raftStatus, RaftLog raftLog,
                            StateMachine stateMachine, NioClient client, RaftExecutor executor,
                            CommitManager commitManager) {
        this.groupId = groupId;
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
        this.restItemsToStartReplicate = (int) (maxReplicateItems * 0.1);

        this.installSnapshotFailTime = ts.getNanoTime() - TimeUnit.SECONDS.toNanos(10);
    }

    @SuppressWarnings("ForLoopReplaceableByForEach")
    public void replicateAfterRaftExec(RaftStatus raftStatus) {
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
        if (member.isInstallSnapshot()) {
            installSnapshot(member);
        } else if (member.isMultiAppend()) {
            doReplicate(member);
        } else {
            if (member.getPendingStat().getPendingRequests() == 0) {
                doReplicate(member);
            } else {
                // waiting all pending request complete
            }
        }
    }

    private void doReplicate(RaftMember member) {
        long nextIndex = member.getNextIndex();
        int diff = (int) (raftStatus.getLastLogIndex() - nextIndex + 1);
        if (diff <= 0) {
            // no data to replicate
            return;
        }

        // flow control
        PendingStat ps = member.getPendingStat();
        int rest = maxReplicateItems - ps.getPendingRequests();
        if (rest <= restItemsToStartReplicate) {
            // avoid silly window syndrome
            return;
        }
        if (ps.getPendingBytes() >= maxReplicateBytes) {
            return;
        }

        int limit = member.isMultiAppend() ? Math.min(rest, diff) : 1;

        RaftTask first = raftStatus.getPendingRequests().get(nextIndex);
        if (first != null && !first.input.isReadOnly()) {
            long sizeLimit = config.getSingleReplicateLimit();
            while (limit > 0) {
                ArrayList<LogItem> items = new ArrayList<>(limit);
                long size = 0;
                for (int i = 0; i < limit; i++) {
                    LogItem li = raftStatus.getPendingRequests().get(nextIndex + i).item;
                    size += li.getBuffer().getBuffer().remaining();
                    if (size > sizeLimit && i != 0) {
                        break;
                    }
                    items.add(li);
                }
                limit -= items.size();
                sendAppendRequest(member, items);
                if (limit == 0 || ps.getPendingBytes() >= maxReplicateBytes) {
                    return;
                }
            }
        } else {
            CompletableFuture<LogItem[]> future = raftLog.load(nextIndex, Math.min(limit, 1024),
                    config.getSingleReplicateLimit());
            member.setReplicateFuture(future);
            future.whenCompleteAsync((r, ex) -> resumeAfterLogLoad(member.getReplicateEpoch(), member, r, ex),
                    raftStatus.getRaftExecutor());
        }
    }

    private void resumeAfterLogLoad(int repEpoch, RaftMember member, LogItem[] items, Throwable ex) {
        try {
            member.setReplicateFuture(null);
            if (epochNotMatch(member, repEpoch)) {
                log.info("replicate epoch changed, ignore load result");
                return;
            }
            if (ex != null) {
                log.error("load raft log failed", ex);
                return;
            }
            if (!member.isReady()) {
                log.warn("member is not ready, ignore load result");
                return;
            }
            if (member.isInstallSnapshot()) {
                log.warn("member is installing snapshot, ignore load result");
                return;
            }
            if (items == null || items.length == 0) {
                log.warn("load raft log return empty, ignore load result");
                return;
            }
            if (member.getNextIndex() != items[0].getIndex()) {
                log.warn("the first load item index not match nextIndex, ignore load result");
                return;
            }
            if (member.isMultiAppend()) {
                sendAppendRequest(member, Arrays.asList(items));
                replicate(member);
            } else {
                //noinspection StatementWithEmptyBody
                if (member.getPendingStat().getPendingRequests() == 0) {
                    sendAppendRequest(member, Arrays.asList(items));
                } else {
                    // waiting all pending request complete
                }
            }
        } finally {
            // the data loaded will not put into pending map, so release it here
            if (items != null) {
                int len = items.length;
                //noinspection ForLoopReplaceableByForEach
                for (int i = 0; i < len; i++) {
                    RaftUtil.release(items[i]);
                }
            }
        }
    }

    private void sendAppendRequest(RaftMember member, List<LogItem> items) {
        LogItem firstItem = items.get(0);
        long bytes = 0;
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < items.size(); i++) {
            LogItem item = items.get(i);
            bytes += item.getBuffer() == null ? 0 : item.getBuffer().getBuffer().remaining();
        }
        sendAppendRequest(member, firstItem.getIndex() - 1, firstItem.getPrevLogTerm(), items, bytes);
    }

    private void sendAppendRequest(RaftMember member, long prevLogIndex, int prevLogTerm, List<LogItem> logs, long bytes) {
        AppendReqWriteFrame req = new AppendReqWriteFrame();
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
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < logs.size(); i++) {
            RaftUtil.retain(logs.get(i));
        }
        CompletableFuture<ReadFrame> f = client.sendRequest(member.getNode().getPeer(), req, APPEND_RESP_DECODER, timeout);
        registerAppendResultCallback(member, prevLogIndex, prevLogTerm, f, logs, bytes);
    }

    private void registerAppendResultCallback(RaftMember member, long prevLogIndex, int prevLogTerm,
                                              CompletableFuture<ReadFrame> f, List<LogItem> logs, long bytes) {
        int reqTerm = raftStatus.getCurrentTerm();
        // the time refresh happens before this line
        long reqNanos = ts.getNanoTime();
        // if PendingStat is reset, we should not invoke decrAndGetPendingRequests() on new instance
        PendingStat ps = member.getPendingStat();
        ps.incrAndGetPendingRequests(logs.size(), bytes);
        int repEpoch = member.getReplicateEpoch();
        f.whenCompleteAsync((rf, ex) -> {
            try {
                if (reqTerm != raftStatus.getCurrentTerm()) {
                    log.info("receive outdated append result, term not match. reqTerm={}, currentTerm={}",
                            reqTerm, raftStatus.getCurrentTerm());
                    return;
                }
                if (epochNotMatch(member, repEpoch)) {
                    log.info("receive outdated append result, replicateEpoch not match. ignore.");
                    return;
                }
                ps.decrAndGetPendingRequests(logs.size(), bytes);
                if (ex == null) {
                    processAppendResult(member, rf, prevLogIndex, prevLogTerm, reqTerm, reqNanos, logs.size(), repEpoch);
                } else {
                    member.incrReplicateEpoch(repEpoch);
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
            } finally {
                //noinspection ForLoopReplaceableByForEach
                for (int i = 0; i < logs.size(); i++) {
                    RaftUtil.release(logs.get(i));
                }
            }
        }, raftExecutor);
    }

    private boolean checkTermAndRoleFailed(int reqTerm, int remoteTerm) {
        if (remoteTerm > raftStatus.getCurrentTerm()) {
            log.info("find remote term greater than local term. remoteTerm={}, localTerm={}",
                    remoteTerm, raftStatus.getCurrentTerm());
            RaftUtil.incrTerm(remoteTerm, raftStatus, -1);
            return true;
        }

        if (raftStatus.getRole() != RaftRole.leader) {
            log.warn("receive response, not leader, ignore. reqTerm={}, currentTerm={}",
                    reqTerm, raftStatus.getCurrentTerm());
            return true;
        }
        return false;
    }

    // in raft thread
    private void processAppendResult(RaftMember member, ReadFrame rf, long prevLogIndex,
                                     int prevLogTerm, int reqTerm, long reqNanos, int count, int reqEpoch) {
        long expectNewMatchIndex = prevLogIndex + count;
        AppendRespCallback body = (AppendRespCallback) rf.getBody();
        RaftStatus raftStatus = this.raftStatus;
        int remoteTerm = body.getTerm();
        if (checkTermAndRoleFailed(reqTerm, remoteTerm)) {
            return;
        }
        if (member.isInstallSnapshot()) {
            BugLog.getLog().error("receive append result when install snapshot, ignore. prevLogIndex={}, prevLogTerm={}, remoteId={}, groupId={}",
                    prevLogIndex, prevLogTerm, member.getNode().getNodeId(), groupId);
            return;
        }
        if (body.isSuccess()) {
            if (member.getMatchIndex() <= prevLogIndex) {
                member.setLastConfirmReqNanos(reqNanos);
                RaftUtil.updateLease(raftStatus);
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
            }
        } else if (body.getAppendCode() == AppendProcessor.CODE_LOG_NOT_MATCH) {
            member.incrReplicateEpoch(reqEpoch);
            processLogNotMatch(member, prevLogIndex, prevLogTerm, reqTerm, reqNanos, body, raftStatus);
        } else if (body.getAppendCode() == AppendProcessor.CODE_INSTALL_SNAPSHOT) {
            log.error("remote member is installing snapshot, prevLogIndex={}", prevLogIndex);
        } else {
            BugLog.getLog().error("append fail. appendCode={}, old matchIndex={}, append prevLogIndex={}, " +
                            "expectNewMatchIndex={}, remoteId={}, groupId={}, localTerm={}, reqTerm={}, remoteTerm={}",
                    body.getAppendCode(), member.getMatchIndex(), prevLogIndex, expectNewMatchIndex,
                    member.getNode().getNodeId(), groupId, raftStatus.getCurrentTerm(), reqTerm, body.getTerm());
        }
    }

    private void processLogNotMatch(RaftMember member, long prevLogIndex, int prevLogTerm, int reqTerm,
                                    long reqNanos, AppendRespCallback body, RaftStatus raftStatus) {
        log.info("log not match. remoteId={}, groupId={}, matchIndex={}, prevLogIndex={}, prevLogTerm={}, remoteLogTerm={}, remoteLogIndex={}, localTerm={}, reqTerm={}, remoteTerm={}",
                member.getNode().getNodeId(), groupId, member.getMatchIndex(), prevLogIndex, prevLogTerm, body.getMaxLogTerm(),
                body.getMaxLogIndex(), raftStatus.getCurrentTerm(), reqTerm, body.getTerm());
        member.setLastConfirmReqNanos(reqNanos);
        member.setMultiAppend(false);
        RaftUtil.updateLease(raftStatus);
        if (body.getTerm() == raftStatus.getCurrentTerm() && reqTerm == raftStatus.getCurrentTerm()) {
            member.setNextIndex(body.getMaxLogIndex() + 1);
            replicate(member);
        } else {
            int reqEpoch = member.getReplicateEpoch();
            CompletableFuture<Long> future = raftLog.nextIndexToReplicate(body.getMaxLogTerm(), body.getMaxLogIndex());
            member.setReplicateFuture(future);
            future.whenCompleteAsync(resumeAfterFindIndex(member, reqEpoch, body.getMaxLogIndex()), raftExecutor);
        }
    }

    private BiConsumer<Long, Throwable> resumeAfterFindIndex(RaftMember member, int reqEpoch, long remoteMayIndex) {
        return (nextIndex, ex) -> {
            member.setReplicateFuture(null);
            if (epochNotMatch(member, reqEpoch)) {
                log.info("epoch not match. ignore result of nextIndexToReplicate call");
                return;
            }
            if (ex == null) {
                if (nextIndex > 0) {
                    member.setNextIndex(Math.min(nextIndex, remoteMayIndex));
                    replicate(member);
                } else {
                    beginInstallSnapshot(member);
                }
            } else {
                log.error("nextIndexToReplicate fail", ex);
            }
        };
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
        if (member.getPendingStat().getPendingBytes() >= maxReplicateBytes) {
            return;
        }
        int reqEpoch = member.getReplicateEpoch();
        try {
            CompletableFuture<RefByteBuffer> future = stateMachine.readNext(si.snapshot);
            member.setReplicateFuture(future);
            future.whenCompleteAsync(resumeAfterSnapshotLoad(member, si, reqEpoch), raftExecutor);
        } catch (Exception e) {
            processInstallSnapshotError(member, si, e, reqEpoch);
        }
    }

    private BiConsumer<RefByteBuffer, Throwable> resumeAfterSnapshotLoad(RaftMember member, SnapshotInfo si, int reqEpoch) {
        return (data, ex) -> {
            try {
                member.setReplicateFuture(null);
                if (ex != null) {
                    processInstallSnapshotError(member, si, ex, reqEpoch);
                    return;
                }
                if (epochNotMatch(member, reqEpoch)) {
                    log.info("epoch not match, abort install snapshot.");
                    closeSnapshotAndResetStatus(member, si);
                    return;
                }
                if (!member.isReady()) {
                    log.info("member is not ready, abort install snapshot.");
                    closeSnapshotAndResetStatus(member, si);
                    return;
                }
                sendInstallSnapshotReq(member, si, data);
            } finally {
                if (data != null) {
                    data.release();
                }
            }
            installSnapshot(member);
        };
    }

    private boolean epochNotMatch(RaftMember member, int reqEpoch) {
        return reqEpoch != member.getReplicateEpoch();
    }

    private void processInstallSnapshotError(RaftMember member, SnapshotInfo si, Throwable e, int reqEpoch) {
        installSnapshotFailTime = raftStatus.getTs().getNanoTime();
        member.incrReplicateEpoch(reqEpoch);
        log.error("install snapshot fail", e);
        closeSnapshotAndResetStatus(member, si);
    }

    private void closeSnapshotAndResetStatus(RaftMember member, SnapshotInfo si) {
        try {
            if (si != null) {
                stateMachine.closeSnapshot(si.snapshot);
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
            Snapshot snapshot = stateMachine.openLatestSnapshot();
            if (snapshot == null) {
                installSnapshotFailTime = raftStatus.getTs().getNanoTime();
                log.error("open recent snapshot fail, return null");
                return;
            }
            log.info("begin install snapshot for member: nodeId={}, groupId={}", member.getNode().getNodeId(), groupId);
            si = new SnapshotInfo();
            si.snapshot = snapshot;
            si.offset = 0;
            si.replicateEpoch = member.getReplicateEpoch();
            si.stateMachine = stateMachine;
            member.setSnapshotInfo(si);
        } catch (Exception e) {
            installSnapshotFailTime = raftStatus.getTs().getNanoTime();
            log.error("open recent snapshot fail", e);
        }
    }

    private void sendInstallSnapshotReq(RaftMember member, SnapshotInfo si, RefByteBuffer data) {
        InstallSnapshotReq req = new InstallSnapshotReq();
        req.groupId = groupId;
        req.term = raftStatus.getCurrentTerm();
        req.leaderId = config.getNodeId();
        req.lastIncludedIndex = si.snapshot.getLastIncludedIndex();
        req.lastIncludedTerm = si.snapshot.getLastIncludedTerm();
        req.offset = si.offset;
        req.data = data;
        req.done = data == null || !data.getBuffer().hasRemaining();

        if (req.done) {
            si.readFinished = true;
        }

        InstallSnapshotReq.WriteFrame wf = new InstallSnapshotReq.WriteFrame(req);
        wf.setCommand(Commands.RAFT_INSTALL_SNAPSHOT);
        DtTime timeout = new DtTime(config.getRpcTimeout(), TimeUnit.MILLISECONDS);
        CompletableFuture<ReadFrame> future = client.sendRequest(member.getNode().getPeer(), wf, INSTALL_SNAPSHOT_RESP_DECODER, timeout);
        future.whenComplete((rf, ex) -> {
            if (data != null) {
                data.release();
            }
        });
        int bytes = data == null ? 0 : data.getBuffer().remaining();
        si.offset += bytes;
        registerInstallSnapshotCallback(future, member, si, req.term, req.offset, bytes, req.done, req.lastIncludedIndex);
    }

    private void registerInstallSnapshotCallback(CompletableFuture<ReadFrame> future, RaftMember member,
                                                 SnapshotInfo si, int reqTerm, long reqOffset,
                                                 int reqBytes, boolean reqDone, long reqLastIncludedIndex) {
        PendingStat pd = member.getPendingStat();
        pd.incrAndGetPendingRequests(1, reqBytes);
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
            pd.decrAndGetPendingRequests(1, reqBytes);
            if (ex != null) {
                log.error("send install snapshot fail. remoteNode={}, groupId={}",
                        member.getNode().getNodeId(), groupId, ex);
                processInstallSnapshotError(member, si, ex, reqEpoch);
                return;
            }
            InstallSnapshotResp respBody = (InstallSnapshotResp) rf.getBody();
            if (checkTermAndRoleFailed(reqTerm, respBody.term)) {
                return;
            }
            log.info("transfer snapshot data to member. nodeId={}, groupId={}, offset={}",
                    member.getNode().getNodeId(), groupId, reqOffset);
            if (reqDone) {
                log.info("install snapshot for member finished success. nodeId={}, groupId={}",
                        member.getNode().getNodeId(), groupId);
                closeSnapshotAndResetStatus(member, si);
                member.setInstallSnapshot(false);
                member.setNextIndex(reqLastIncludedIndex + 1);
            }
            replicate(member);

        }, raftExecutor);
    }

}
