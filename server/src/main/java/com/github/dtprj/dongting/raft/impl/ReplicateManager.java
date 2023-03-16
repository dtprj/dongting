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

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.Pair;
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
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;
import com.github.dtprj.dongting.raft.server.RaftLog;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.server.Snapshot;
import com.github.dtprj.dongting.raft.server.StateMachine;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
class ReplicateManager {

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

    private long readSnapshotFailTime;

    private static final PbZeroCopyDecoder APPEND_RESP_DECODER = new PbZeroCopyDecoder(c -> new AppendRespCallback());
    private static final PbZeroCopyDecoder INSTALL_SNAPSHOT_RESP_DECODER = new PbZeroCopyDecoder(c -> new InstallSnapshotResp.Callback());

    ReplicateManager(RaftServerConfig config, RaftGroupConfig groupConfig, RaftStatus raftStatus,
                     RaftLog raftLog, StateMachine stateMachine, NioClient client, RaftExecutor executor,
                     CommitManager commitManager) {
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
        this.restItemsToStartReplicate = (int) (maxReplicateItems * 0.1);

        this.readSnapshotFailTime = ts.getNanoTime() - TimeUnit.SECONDS.toNanos(10);
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
        if (member.isInstallSnapshot()) {
            installSnapshot(member);
        } else if (member.isMultiAppend()) {
            doReplicate(member, false);
        } else {
            if (member.getPendingStat().getPendingRequests() == 0) {
                doReplicate(member, true);
            } else {
                // waiting all pending request complete
            }
        }
    }

    @SuppressWarnings("ForLoopReplaceableByForEach")
    private void doReplicate(RaftMember member, boolean tryMatch) {
        long nextIndex = member.getNextIndex();
        long lastLogIndex = raftStatus.getLastLogIndex();
        if (lastLogIndex < nextIndex) {
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

        int limit = tryMatch ? 1 : rest;

        RaftTask first = raftStatus.getPendingRequests().get(nextIndex);
        LogItem[] items;
        if (first != null && !first.input.isReadOnly()) {
            items = new LogItem[limit];
            for (int i = 0; i < limit; i++) {
                RaftTask t = raftStatus.getPendingRequests().get(nextIndex + i);
                items[i] = t.item;
            }
        } else {
            items = RaftUtil.load(raftLog, raftStatus, nextIndex, limit, config.getReplicateLoadBytesLimit());
        }

        LogItem firstItem = items[0];
        long bytes = 0;
        for (int i = 0; i < items.length; i++) {
            LogItem item = items[i];
            bytes += item.getBuffer() == null ? 0 : item.getBuffer().remaining();
        }
        sendAppendRequest(member, firstItem.getIndex() - 1, firstItem.getPrevLogTerm(), Arrays.asList(items), bytes);
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
        CompletableFuture<ReadFrame> f = client.sendRequest(member.getNode().getPeer(), req, APPEND_RESP_DECODER, timeout);
        registerAppendResultCallback(member, prevLogIndex, prevLogTerm, f, logs.size(), bytes);
    }

    private void registerAppendResultCallback(RaftMember member, long prevLogIndex, int prevLogTerm,
                                              CompletableFuture<ReadFrame> f, int count, long bytes) {
        int reqTerm = raftStatus.getCurrentTerm();
        // the time refresh happens before this line
        long reqNanos = ts.getNanoTime();
        // if PendingStat is reset, we should not invoke decrAndGetPendingRequests() on new instance
        PendingStat ps = member.getPendingStat();
        ps.incrAndGetPendingRequests(count, bytes);
        f.whenCompleteAsync((rf, ex) -> {
            if (reqTerm != raftStatus.getCurrentTerm()) {
                log.info("receive outdated append result, term not match. reqTerm={}, currentTerm={}",
                        reqTerm, raftStatus.getCurrentTerm());
                return;
            }
            ps.decrAndGetPendingRequests(count, bytes);
            if (ex == null) {
                processAppendResult(member, rf, prevLogIndex, prevLogTerm, reqTerm, reqNanos, count);
            } else {
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

    private boolean checkTermAndRoleFailed(int reqTerm, int remoteTerm) {
        if (remoteTerm > raftStatus.getCurrentTerm()) {
            log.info("find remote term greater than local term. remoteTerm={}, localTerm={}",
                    remoteTerm, raftStatus.getCurrentTerm());
            RaftUtil.incrTermAndConvertToFollower(remoteTerm, raftStatus, -1, true);
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
                                     int prevLogTerm, int reqTerm, long reqNanos, int count) {
        long expectNewMatchIndex = prevLogIndex + count;
        AppendRespCallback body = (AppendRespCallback) rf.getBody();
        RaftStatus raftStatus = this.raftStatus;
        int remoteTerm = body.getTerm();
        if (checkTermAndRoleFailed(reqTerm, remoteTerm)) {
            return;
        }
        if (member.isInstallSnapshot()) {
            log.info("receive append result when install snapshot, ignore. prevLogIndex={}, prevLogTerm={}, remoteId={}, groupId={}",
                    prevLogIndex, prevLogTerm, member.getNode().getNodeId(), groupId);
            return;
        }
        if (body.isSuccess()) {
            if (member.getMatchIndex() <= prevLogIndex) {
                member.setLastConfirm(true, reqNanos);
                RaftUtil.updateLease(reqNanos, raftStatus);
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
            processLogNotMatch(member, prevLogIndex, prevLogTerm, reqTerm, reqNanos, body, raftStatus);
        } else if (body.getAppendCode() == AppendProcessor.CODE_INSTALL_SNAPSHOT) {
            initInstallSnapshot(member);
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
        member.setLastConfirm(true, reqNanos);
        member.setMultiAppend(false);
        RaftUtil.updateLease(reqNanos, raftStatus);
        if (body.getTerm() == raftStatus.getCurrentTerm() && reqTerm == raftStatus.getCurrentTerm()) {
            member.setNextIndex(body.getMaxLogIndex() + 1);
            replicate(member);
        } else {
            long idx = findMaxIndexByTerm(body.getMaxLogTerm());
            if (idx > 0) {
                member.setNextIndex(Math.min(body.getMaxLogIndex(), idx) + 1);
                replicate(member);
            } else {
                int t = findLastTermLessThan(body.getMaxLogTerm());
                if (t > 0) {
                    idx = findMaxIndexByTerm(t);
                    if (idx > 0) {
                        member.setNextIndex(Math.min(body.getMaxLogIndex(), idx) + 1);
                        replicate(member);
                    } else {
                        log.error("can't find max index of term to replicate: {}. may truncated recently?", t);
                        initInstallSnapshot(member);
                    }
                } else {
                    log.warn("can't find local term to replicate. follower maxTerm={}", body.getMaxLogTerm());
                    initInstallSnapshot(member);
                }
            }
        }
    }

    private long findMaxIndexByTerm(int term) {
        return RaftUtil.doWithSyncRetry(() -> raftLog.findMaxIndexByTerm(term),
                raftStatus, 1000, "findMaxIndexByTerm fail");
    }

    private int findLastTermLessThan(int term) {
        return RaftUtil.doWithSyncRetry(() -> raftLog.findLastTermLessThan(term),
                raftStatus, 1000, "findLastTermLessThan fail");
    }

    private void initInstallSnapshot(RaftMember member) {
        member.setInstallSnapshot(true);
        member.setPendingStat(new PendingStat());
        installSnapshot(member);
    }

    private void installSnapshot(RaftMember member) {
        openSnapshotIterator(member);
        SnapshotInfo si = member.getSnapshotInfo();
        if (si == null) {
            return;
        }
        if (member.getPendingStat().getPendingBytes() >= maxReplicateBytes) {
            return;
        }
        ByteBuffer data;
        try {
            data = si.iterator.next();
        } catch (Exception e) {
            readSnapshotFailTime = raftStatus.getTs().getNanoTime();
            log.error("read snapshot fail", e);
            closeIteratorAndResetStatus(member, si);
            return;
        }
        sendInstallSnapshotReq(member, si, data);
    }

    private void closeIteratorAndResetStatus(RaftMember member, SnapshotInfo si) {
        try {
            stateMachine.closeIterator(si.iterator);
        } catch (Throwable e1) {
            log.error("close snapshot fail", e1);
        }
        member.setSnapshotInfo(null);
        member.setPendingStat(new PendingStat());
    }

    private void openSnapshotIterator(RaftMember member) {
        SnapshotInfo si = member.getSnapshotInfo();
        if (si != null) {
            return;
        }
        long diff = raftStatus.getTs().getNanoTime() - readSnapshotFailTime;
        if (TimeUnit.NANOSECONDS.toMillis(diff) < 1000) {
            return;
        }
        Pair<Snapshot, Iterator<ByteBuffer>> pair;
        try {
            pair = stateMachine.openLatestSnapshotIterator();
            if (pair == null || pair.getLeft() == null || pair.getRight() == null) {
                readSnapshotFailTime = raftStatus.getTs().getNanoTime();
                log.error("open recent snapshot fail, something is null");
                return;
            }
        } catch (Exception e) {
            readSnapshotFailTime = raftStatus.getTs().getNanoTime();
            log.error("open recent snapshot fail", e);
            return;
        }
        if (!pair.getRight().hasNext()) {
            BugLog.getLog().error("open recent snapshot fail, iterator has no data");
            try {
                stateMachine.closeIterator(pair.getRight());
            } catch (Throwable e) {
                log.error("close snapshot fail", e);
            }
            return;
        }
        log.info("begin install snapshot for member: nodeId={}, groupId={}", member.getNode().getNodeId(), groupId);
        si = new SnapshotInfo();
        si.snapshot = pair.getLeft();
        si.iterator = pair.getRight();
        si.offset = 0;
        member.setSnapshotInfo(si);
    }

    private void sendInstallSnapshotReq(RaftMember member, SnapshotInfo si, ByteBuffer data) {
        InstallSnapshotReq req = new InstallSnapshotReq();
        req.groupId = groupId;
        req.term = raftStatus.getCurrentTerm();
        req.leaderId = config.getNodeId();
        req.lastIncludedIndex = si.snapshot.getLastIncludedIndex();
        req.lastIncludedTerm = si.snapshot.getLastIncludedTerm();
        req.offset = si.offset;
        req.data = data;
        req.done = !si.iterator.hasNext();

        InstallSnapshotReq.WriteFrame wf = new InstallSnapshotReq.WriteFrame(req);
        wf.setCommand(Commands.RAFT_INSTALL_SNAPSHOT);
        DtTime timeout = new DtTime(config.getRpcTimeout(), TimeUnit.MILLISECONDS);
        CompletableFuture<ReadFrame> future = client.sendRequest(member.getNode().getPeer(), wf, INSTALL_SNAPSHOT_RESP_DECODER, timeout);
        int bytes = data.remaining();
        si.offset += bytes;
        registerInstallSnapshotCallback(future, member, si, req.term, req.offset, bytes, req.done, req.lastIncludedIndex);
    }

    private void registerInstallSnapshotCallback(CompletableFuture<ReadFrame> future, RaftMember member,
                                                 SnapshotInfo si, int reqTerm, long reqOffset,
                                                 int reqBytes, boolean reqDone, long reqLastIncludedIndex) {
        PendingStat pd = member.getPendingStat();
        pd.incrAndGetPendingRequests(1, reqBytes);
        future.whenCompleteAsync((rf, ex) -> {
            if (reqTerm != raftStatus.getCurrentTerm()) {
                log.info("receive outdated append result, term not match. reqTerm={}, currentTerm={}, remoteNode={}, groupId={}",
                        reqTerm, raftStatus.getCurrentTerm(), member.getNode().getNodeId(), groupId);
                return;
            }
            pd.decrAndGetPendingRequests(1, reqBytes);
            if (ex != null) {
                log.error("send install snapshot fail. remoteNode={}, groupId={}",
                        member.getNode().getNodeId(), groupId, ex);
                closeIteratorAndResetStatus(member, si);
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
                closeIteratorAndResetStatus(member, si);
                member.setInstallSnapshot(false);
                member.setNextIndex(reqLastIncludedIndex + 1);
            } else {
                submitReplicateTask(member);
            }
        }, raftExecutor);
    }

    private void submitReplicateTask(RaftMember member) {
        raftExecutor.execute(() -> replicate(member));
    }

}
