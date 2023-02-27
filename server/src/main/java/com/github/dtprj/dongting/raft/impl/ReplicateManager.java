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
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftLog;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;

import java.util.Arrays;
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
    private final NioClient client;
    private final RaftExecutor raftExecutor;
    private final CommitManager commitManager;
    private final Timestamp ts;

    private final int maxReplicateItems;
    private final int restItemsToStartReplicate;
    private final long maxReplicateBytes;

    private static final PbZeroCopyDecoder appendRespDecoder = new PbZeroCopyDecoder(c -> new AppendRespCallback());

    ReplicateManager(RaftContainer raftContainer, CommitManager commitManager) {
        this.raftStatus = raftContainer.getRaftStatus();
        this.config = raftContainer.getConfig();
        this.raftLog = raftContainer.getRaftLog();
        this.client = raftContainer.getClient();
        this.raftExecutor = raftContainer.getRaftExecutor();
        this.commitManager = commitManager;
        this.ts = raftStatus.getTs();

        this.maxReplicateItems = config.getMaxReplicateItems();
        this.maxReplicateBytes = config.getMaxReplicateBytes();
        this.restItemsToStartReplicate = (int) (maxReplicateItems * 0.1);
    }

    @SuppressWarnings("StatementWithEmptyBody")
    void replicate(RaftNode node) {
        if (raftStatus.getRole() != RaftRole.leader) {
            return;
        }
        if (!node.isReady()) {
            return;
        }
        if (node.isMultiAppend()) {
            doReplicate(node, false);
        } else {
            if (node.getPendingRequests() == 0) {
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
        int rest = maxReplicateItems - node.getPendingRequests();
        if (rest <= restItemsToStartReplicate) {
            // avoid silly window syndrome
            return;
        }
        if (node.getPendingBytes() >= maxReplicateBytes) {
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
        req.setCommand(Commands.RAFT_APPEND_ENTRIES);
        req.setTerm(raftStatus.getCurrentTerm());
        req.setLeaderId(config.getId());
        req.setLeaderCommit(raftStatus.getCommitIndex());
        req.setPrevLogIndex(prevLogIndex);
        req.setPrevLogTerm(prevLogTerm);
        req.setLogs(logs);

        node.incrAndGetPendingRequests(logs.size(), bytes);
        node.setNextIndex(prevLogIndex + 1 + logs.size());

        DtTime timeout = new DtTime(config.getRpcTimeout(), TimeUnit.MILLISECONDS);
        CompletableFuture<ReadFrame> f = client.sendRequest(node.getPeer(), req, appendRespDecoder, timeout);
        registerAppendResultCallback(node, prevLogIndex, prevLogTerm, f, logs.size(), bytes);
    }

    private void registerAppendResultCallback(RaftNode node, long prevLogIndex, int prevLogTerm,
                                              CompletableFuture<ReadFrame> f, int count, long bytes) {
        int reqTerm = raftStatus.getCurrentTerm();
        // the time refresh happens before this line
        long reqNanos = ts.getNanoTime();
        f.handleAsync((rf, ex) -> {
            node.decrAndGetPendingRequests(count, bytes);
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
            return null;
        }, raftExecutor);
    }

    // in raft thread
    private void processAppendResult(RaftNode node, ReadFrame rf, long prevLogIndex,
                                     int prevLogTerm, int reqTerm, long reqNanos, int count) {
        long expectNewMatchIndex = prevLogIndex + count;
        AppendRespCallback body = (AppendRespCallback) rf.getBody();
        RaftStatus raftStatus = this.raftStatus;
        int remoteTerm = body.getTerm();
        if (remoteTerm > raftStatus.getCurrentTerm()) {
            log.info("find remote term greater than local term. remoteTerm={}, localTerm={}",
                    body.getTerm(), raftStatus.getCurrentTerm());
            RaftUtil.incrTermAndConvertToFollower(remoteTerm, raftStatus, -1);
            return;
        }

        if (raftStatus.getRole() != RaftRole.leader) {
            log.info("receive append result, not leader, ignore. reqTerm={}, currentTerm={}",
                    reqTerm, raftStatus.getCurrentTerm());
            return;
        }
        if (reqTerm != raftStatus.getCurrentTerm()) {
            log.info("receive append result, term not match. reqTerm={}, currentTerm={}",
                    reqTerm, raftStatus.getCurrentTerm());
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
                            BugLog.getLog().error("can't find log to replicate. term={}", t);
                        }
                    } else {
                        BugLog.getLog().error("can't find log to replicate. follower maxTerm={}", body.getMaxLogTerm());
                    }
                }
            }
        } else {
            BugLog.getLog().error("append fail. appendCode={}, old matchIndex={}, append prevLogIndex={}, expectNewMatchIndex={}, remoteId={}, localTerm={}, reqTerm={}, remoteTerm={}",
                    body.getAppendCode(), node.getMatchIndex(), prevLogIndex, expectNewMatchIndex, node.getId(), raftStatus.getCurrentTerm(), reqTerm, body.getTerm());
        }
    }

    private long findMaxIndexByTerm(int term) {
        return RaftUtil.doWithRetry(() -> raftLog.findMaxIndexByTerm(term),
                raftStatus, 1000, "findMaxIndexByTerm fail");
    }

    private int findLastTermLessThan(int term) {
        return RaftUtil.doWithRetry(() -> raftLog.findLastTermLessThan(term),
                raftStatus, 1000, "findLastTermLessThan fail");
    }
}
