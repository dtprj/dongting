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
import com.github.dtprj.dongting.net.FrameType;
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

    ReplicateManager(RaftComponents raftComponents, CommitManager commitManager) {
        this.raftStatus = raftComponents.getRaftStatus();
        this.config = raftComponents.getConfig();
        this.raftLog = raftComponents.getRaftLog();
        this.stateMachine = raftComponents.getStateMachine();
        this.client = raftComponents.getClient();
        this.raftExecutor = raftComponents.getRaftExecutor();
        this.commitManager = commitManager;
        this.ts = raftStatus.getTs();

        this.maxReplicateItems = config.getMaxReplicateItems();
        this.maxReplicateBytes = config.getMaxReplicateBytes();
        this.restItemsToStartReplicate = (int) (maxReplicateItems * 0.1);

        this.readSnapshotFailTime = ts.getNanoTime() - TimeUnit.SECONDS.toNanos(10);
    }

    @SuppressWarnings("StatementWithEmptyBody")
    void replicate(RaftNode node) {
        if (raftStatus.getRole() != RaftRole.leader) {
            return;
        }
        if (!node.isReady()) {
            return;
        }
        if (node.isInstallSnapshot()) {
            installSnapshot(node);
        } else if (node.isMultiAppend()) {
            doReplicate(node, false);
        } else {
            if (node.getPendingStat().getPendingRequests() == 0) {
                doReplicate(node, true);
            } else {
                // waiting all pending request complete
            }
        }
    }

    private void doReplicate(RaftNode node, boolean tryMatch) {
        long nextIndex = node.getNextIndex();
        long lastLogIndex = raftStatus.getLastLogIndex();
        if (lastLogIndex < nextIndex) {
            // no data to replicate
            return;
        }

        // flow control
        PendingStat ps = node.getPendingStat();
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
        for (LogItem i : items) {
            bytes += i.getBuffer() == null ? 0 : i.getBuffer().remaining();

        }
        sendAppendRequest(node, firstItem.getIndex() - 1, firstItem.getPrevLogTerm(), Arrays.asList(items), bytes);
    }

    private void sendAppendRequest(RaftNode node, long prevLogIndex, int prevLogTerm, List<LogItem> logs, long bytes) {
        AppendReqWriteFrame req = new AppendReqWriteFrame();
        req.setFrameType(FrameType.TYPE_REQ);
        req.setCommand(Commands.RAFT_APPEND_ENTRIES);
        req.setTerm(raftStatus.getCurrentTerm());
        req.setLeaderId(config.getId());
        req.setLeaderCommit(raftStatus.getCommitIndex());
        req.setPrevLogIndex(prevLogIndex);
        req.setPrevLogTerm(prevLogTerm);
        req.setLogs(logs);

        node.setNextIndex(prevLogIndex + 1 + logs.size());

        DtTime timeout = new DtTime(config.getRpcTimeout(), TimeUnit.MILLISECONDS);
        CompletableFuture<ReadFrame> f = client.sendRequest(node.getPeer(), req, APPEND_RESP_DECODER, timeout);
        registerAppendResultCallback(node, prevLogIndex, prevLogTerm, f, logs.size(), bytes);
    }

    private void registerAppendResultCallback(RaftNode node, long prevLogIndex, int prevLogTerm,
                                              CompletableFuture<ReadFrame> f, int count, long bytes) {
        int reqTerm = raftStatus.getCurrentTerm();
        // the time refresh happens before this line
        long reqNanos = ts.getNanoTime();
        // if PendingStat is reset, we should not invoke decrAndGetPendingRequests() on new instance
        PendingStat ps = node.getPendingStat();
        ps.incrAndGetPendingRequests(count, bytes);
        f.whenCompleteAsync((rf, ex) -> {
            if (reqTerm != raftStatus.getCurrentTerm()) {
                log.info("receive outdated append result, term not match. reqTerm={}, currentTerm={}",
                        reqTerm, raftStatus.getCurrentTerm());
                return;
            }
            ps.decrAndGetPendingRequests(count, bytes);
            if (ex == null) {
                processAppendResult(node, rf, prevLogIndex, prevLogTerm, reqTerm, reqNanos, count);
            } else {
                if (node.isMultiAppend()) {
                    node.setMultiAppend(false);
                    String msg = "append fail. remoteId={}, localTerm={}, reqTerm={}, prevLogIndex={}";
                    log.warn(msg, node.getId(), raftStatus.getCurrentTerm(), reqTerm, prevLogIndex, ex);
                } else {
                    String msg = "append fail. remoteId={}, localTerm={}, reqTerm={}, prevLogIndex={}, ex={}";
                    log.warn(msg, node.getId(), raftStatus.getCurrentTerm(), reqTerm, prevLogIndex, ex.toString());
                }
            }
        }, raftExecutor);
    }

    private boolean checkTermAndRoleFailed(int reqTerm, int remoteTerm){
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
    private void processAppendResult(RaftNode node, ReadFrame rf, long prevLogIndex,
                                     int prevLogTerm, int reqTerm, long reqNanos, int count) {
        long expectNewMatchIndex = prevLogIndex + count;
        AppendRespCallback body = (AppendRespCallback) rf.getBody();
        RaftStatus raftStatus = this.raftStatus;
        int remoteTerm = body.getTerm();
        if (checkTermAndRoleFailed(reqTerm, remoteTerm)) {
            return;
        }
        if (node.isInstallSnapshot()) {
            log.info("receive append result when install snapshot, ignore. prevLogIndex={}, prevLogTerm={}, remoteId={}",
                    prevLogIndex, prevLogTerm, node.getId());
            return;
        }
        if (body.isSuccess()) {
            if (node.getMatchIndex() <= prevLogIndex) {
                node.setLastConfirm(true, reqNanos);
                RaftUtil.updateLease(reqNanos, raftStatus);
                node.setMatchIndex(expectNewMatchIndex);
                node.setMultiAppend(true);
                commitManager.tryCommit(expectNewMatchIndex);
                if (raftStatus.getLastLogIndex() >= node.getNextIndex()) {
                    replicate(node);
                }
            } else {
                BugLog.getLog().error("append miss order. old matchIndex={}, append prevLogIndex={}, expectNewMatchIndex={}, remoteId={}, localTerm={}, reqTerm={}, remoteTerm={}",
                        node.getMatchIndex(), prevLogIndex, expectNewMatchIndex, node.getId(), raftStatus.getCurrentTerm(), reqTerm, body.getTerm());
            }
        } else if (body.getAppendCode() == AppendProcessor.CODE_LOG_NOT_MATCH) {
            processLogNotMatch(node, prevLogIndex, prevLogTerm, reqTerm, reqNanos, body, raftStatus);
        } else if (body.getAppendCode() == AppendProcessor.CODE_INSTALL_SNAPSHOT) {
            initInstallSnapshot(node);
        } else {
            BugLog.getLog().error("append fail. appendCode={}, old matchIndex={}, append prevLogIndex={}, expectNewMatchIndex={}, remoteId={}, localTerm={}, reqTerm={}, remoteTerm={}",
                    body.getAppendCode(), node.getMatchIndex(), prevLogIndex, expectNewMatchIndex, node.getId(), raftStatus.getCurrentTerm(), reqTerm, body.getTerm());
        }
    }

    private void processLogNotMatch(RaftNode node, long prevLogIndex, int prevLogTerm, int reqTerm,
                                    long reqNanos, AppendRespCallback body, RaftStatus raftStatus) {
        log.info("log not match. remoteId={}, matchIndex={}, prevLogIndex={}, prevLogTerm={}, remoteLogTerm={}, remoteLogIndex={}, localTerm={}, reqTerm={}, remoteTerm={}",
                node.getId(), node.getMatchIndex(), prevLogIndex, prevLogTerm, body.getMaxLogTerm(),
                body.getMaxLogIndex(), raftStatus.getCurrentTerm(), reqTerm, body.getTerm());
        node.setLastConfirm(true, reqNanos);
        node.setMultiAppend(false);
        RaftUtil.updateLease(reqNanos, raftStatus);
        if (body.getTerm() == raftStatus.getCurrentTerm() && reqTerm == raftStatus.getCurrentTerm()) {
            node.setNextIndex(body.getMaxLogIndex() + 1);
            replicate(node);
        } else {
            long idx = findMaxIndexByTerm(body.getMaxLogTerm());
            if (idx > 0) {
                node.setNextIndex(Math.min(body.getMaxLogIndex(), idx) + 1);
                replicate(node);
            } else {
                int t = findLastTermLessThan(body.getMaxLogTerm());
                if (t > 0) {
                    idx = findMaxIndexByTerm(t);
                    if (idx > 0) {
                        node.setNextIndex(Math.min(body.getMaxLogIndex(), idx) + 1);
                        replicate(node);
                    } else {
                        log.error("can't find max index of term to replicate: {}. may truncated recently?", t);
                        initInstallSnapshot(node);
                    }
                } else {
                    log.warn("can't find local term to replicate. follower maxTerm={}", body.getMaxLogTerm());
                    initInstallSnapshot(node);
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

    private void initInstallSnapshot(RaftNode node) {
        node.setInstallSnapshot(true);
        node.setPendingStat(new PendingStat());
        installSnapshot(node);
    }

    private void installSnapshot(RaftNode node) {
        openSnapshotIterator(node);
        SnapshotInfo si = node.getSnapshotInfo();
        if (si == null) {
            return;
        }
        if (node.getPendingStat().getPendingBytes() >= maxReplicateBytes) {
            return;
        }
        ByteBuffer data;
        try {
            data = si.iterator.next();
        } catch (Exception e) {
            readSnapshotFailTime = raftStatus.getTs().getNanoTime();
            log.error("read snapshot fail", e);
            closeIteratorAndResetStatus(node, si);
            return;
        }
        sendInstallSnapshotReq(node, si, data);
    }

    private void closeIteratorAndResetStatus(RaftNode node, SnapshotInfo si) {
        try {
            stateMachine.closeIterator(si.iterator);
        } catch (Throwable e1) {
            log.error("close snapshot fail", e1);
        }
        node.setSnapshotInfo(null);
        node.setPendingStat(new PendingStat());
    }

    private void openSnapshotIterator(RaftNode node) {
        SnapshotInfo si = node.getSnapshotInfo();
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
        log.info("begin install snapshot for node: {}", node.getId());
        si = new SnapshotInfo();
        si.snapshot = pair.getLeft();
        si.iterator = pair.getRight();
        si.offset = 0;
        node.setSnapshotInfo(si);
    }

    private void sendInstallSnapshotReq(RaftNode node, SnapshotInfo si, ByteBuffer data) {
        InstallSnapshotReq req = new InstallSnapshotReq();
        req.term = raftStatus.getCurrentTerm();
        req.leaderId = config.getId();
        req.lastIncludedIndex = si.snapshot.getLastIncludedIndex();
        req.lastIncludedTerm = si.snapshot.getLastIncludedTerm();
        req.offset = si.offset;
        req.data = data;
        req.done = !si.iterator.hasNext();

        InstallSnapshotReq.WriteFrame wf = new InstallSnapshotReq.WriteFrame(req);
        wf.setCommand(Commands.RAFT_INSTALL_SNAPSHOT);
        wf.setFrameType(FrameType.TYPE_REQ);
        DtTime timeout = new DtTime(config.getRpcTimeout(), TimeUnit.MILLISECONDS);
        CompletableFuture<ReadFrame> future = client.sendRequest(node.getPeer(), wf, INSTALL_SNAPSHOT_RESP_DECODER, timeout);
        int bytes = data.remaining();
        si.offset += bytes;
        registerInstallSnapshotCallback(future, node, si, req, bytes);
    }

    private void registerInstallSnapshotCallback(CompletableFuture<ReadFrame> future, RaftNode node,
                                                 SnapshotInfo si, InstallSnapshotReq req, int bytes) {
        PendingStat pd = node.getPendingStat();
        pd.incrAndGetPendingRequests(1, bytes);
        future.whenCompleteAsync((rf, ex) -> {
            if (req.term != raftStatus.getCurrentTerm()) {
                log.info("receive outdated append result, term not match. reqTerm={}, currentTerm={}",
                        req.term, raftStatus.getCurrentTerm());
                return;
            }
            pd.decrAndGetPendingRequests(1, bytes);
            if (ex != null) {
                log.error("send install snapshot fail", ex);
                closeIteratorAndResetStatus(node, si);
                return;
            }
            InstallSnapshotResp respBody = (InstallSnapshotResp) rf.getBody();
            if (checkTermAndRoleFailed(req.term, respBody.term)) {
                return;
            }
            log.info("transfer snapshot data to node {}, offset={}", node.getId(), req.offset);
            if (req.done) {
                log.info("install snapshot for node {} finished success", node.getId());
                closeIteratorAndResetStatus(node, si);
                node.setInstallSnapshot(false);
                node.setNextIndex(req.lastIncludedIndex + 1);
            } else {
                raftExecutor.execute(new InstallSnapshotRunner(node));
            }
        }, raftExecutor);
    }

    class InstallSnapshotRunner implements Runnable {

        private final RaftNode node;

        InstallSnapshotRunner(RaftNode node) {
            this.node = node;
        }
        @Override
        public void run() {
            replicate(node);
        }
    }
}
