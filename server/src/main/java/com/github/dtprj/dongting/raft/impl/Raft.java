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
import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.PbZeroCopyDecoder;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.raft.rpc.AppendProcessor;
import com.github.dtprj.dongting.raft.rpc.AppendReqWriteFrame;
import com.github.dtprj.dongting.raft.rpc.AppendRespCallback;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.NotLeaderException;
import com.github.dtprj.dongting.raft.server.RaftExecTimeoutException;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftLog;
import com.github.dtprj.dongting.raft.server.RaftOutput;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.server.StateMachine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class Raft {

    private static final DtLog log = DtLogs.getLogger(Raft.class);

    private final RaftServerConfig config;
    private final RaftExecutor raftExecutor;
    private final RaftLog raftLog;
    private final RaftStatus raftStatus;
    private final NioClient client;
    private final StateMachine stateMachine;

    private final int maxReplicateItems;
    private final int restItemsToStartReplicate;
    private final long maxReplicateBytes;

    private RaftNode self;
    private final Timestamp ts;

    private static final PbZeroCopyDecoder appendRespDecoder = new PbZeroCopyDecoder(c -> new AppendRespCallback());

    public Raft(RaftContainer container) {
        this.config = container.getConfig();
        this.raftExecutor = container.getRaftExecutor();
        this.raftLog = container.getRaftLog();
        this.raftStatus = container.getRaftStatus();
        this.client = container.getClient();
        this.stateMachine = container.getStateMachine();

        this.maxReplicateItems = config.getMaxReplicateItems();
        this.maxReplicateBytes = config.getMaxReplicateBytes();
        this.restItemsToStartReplicate = (int) (maxReplicateItems * 0.1);
        this.ts = raftStatus.getTs();
    }

    private RaftNode getSelf() {
        if (self != null) {
            return self;
        }
        for (RaftNode node : raftStatus.getServers()) {
            if (node.isSelf()) {
                this.self = node;
                break;
            }
        }
        return self;
    }

    public void raftExec(List<RaftTask> inputs) {
        RaftStatus raftStatus = this.raftStatus;
        if (raftStatus.getRole() != RaftRole.leader) {
            HostPort leader = RaftUtil.getLeader(raftStatus.getCurrentLeader());
            for (RaftTask t : inputs) {
                if (t.future != null) {
                    t.future.completeExceptionally(new NotLeaderException(leader));
                }
            }
            return;
        }
        long oldIndex = raftStatus.getLastLogIndex();
        long newIndex = oldIndex;

        ArrayList<LogItem> logs = new ArrayList<>(inputs.size());
        int oldTerm = raftStatus.getLastLogTerm();
        int currentTerm = raftStatus.getCurrentTerm();
        PendingMap pending = raftStatus.getPendingRequests();
        for (RaftTask rt : inputs) {
            RaftInput input = rt.input;

            if (input.getDeadline().isTimeout(ts)) {
                rt.future.completeExceptionally(new RaftExecTimeoutException("timeout "
                        + input.getDeadline().getTimeout(TimeUnit.MILLISECONDS) + "ms"));
                continue;
            }

            if (!input.isReadOnly()) {
                newIndex++;
                LogItem item = new LogItem(rt.type, newIndex, currentTerm, oldTerm, input.getLogData());
                logs.add(item);

                rt.item = item;

                pending.put(newIndex, rt);
            } else {
                // read
                if (newIndex <= raftStatus.getLastApplied()) {
                    execInStateMachine0(newIndex, rt);
                } else {
                    RaftTask newTask = pending.get(newIndex);
                    if (newTask == null) {
                        pending.put(newIndex, rt);
                    } else {
                        newTask.addNext(rt);
                    }
                }
            }
        }

        if (logs.size() == 0) {
            return;
        }

        // TODO async append, error handle
        raftLog.append(oldIndex, oldTerm, logs);

        raftStatus.setLastLogTerm(currentTerm);
        raftStatus.setLastLogIndex(newIndex);

        RaftNode self = getSelf();
        self.setNextIndex(newIndex + 1);
        self.setMatchIndex(newIndex);
        self.setHasLastConfirmReqNanos(true);
        self.setLastConfirmReqNanos(ts.getNanoTime());

        // for single node mode
        if (raftStatus.getRwQuorum() == 1) {
            RaftUtil.updateLease(ts.getNanoTime(), raftStatus);
            tryCommit(newIndex);
        }


        for (RaftNode node : raftStatus.getServers()) {
            if (node.isSelf()) {
                continue;
            }
            replicate(node);
        }
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private void replicate(RaftNode node) {
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
            // TODO error handle
            items = raftLog.load(nextIndex, limit, maxReplicateBytes);
        }

        doReplicate(node, items);
    }

    private void doReplicate(RaftNode node, LogItem[] items) {
        ArrayList<LogItem> logs = new ArrayList<>();
        long bytes = 0;
        for (int i = 0; i < items.length; ) {
            LogItem item = items[i];
            int currentSize = item.getBuffer() == null ? 0 : item.getBuffer().remaining();
            if (bytes + currentSize > config.getMaxBodySize()) {
                if (logs.size() > 0) {
                    LogItem firstItem = logs.get(0);
                    sendAppendRequest(node, firstItem.getIndex() - 1, firstItem.getPrevLogTerm(), logs, bytes);

                    bytes = 0;
                    logs = new ArrayList<>();
                    continue;
                } else {
                    log.error("body too large: {}", currentSize);
                    return;
                }
            }
            bytes += currentSize;
            logs.add(item);
            i++;
        }

        if (logs.size() > 0) {
            LogItem firstItem = logs.get(0);
            sendAppendRequest(node, firstItem.getIndex() - 1, firstItem.getPrevLogTerm(), logs, bytes);
        }
    }

    public void sendHeartBeat() {
        DtTime deadline = new DtTime(ts, raftStatus.getElectTimeoutNanos(), TimeUnit.NANOSECONDS);
        RaftInput input = new RaftInput(null, null, deadline, false);
        RaftTask rt = new RaftTask(ts, LogItem.TYPE_HEARTBEAT, input, null);
        raftExec(Collections.singletonList(rt));
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
                node.setHasLastConfirmReqNanos(true);
                node.setLastConfirmReqNanos(reqNanos);
                RaftUtil.updateLease(reqNanos, raftStatus);
                node.setMatchIndex(expectNewMatchIndex);
                node.setMultiAppend(true);
                tryCommit(expectNewMatchIndex);
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
            node.setHasLastConfirmReqNanos(true);
            node.setLastConfirmReqNanos(reqNanos);
            node.setMultiAppend(false);
            RaftUtil.updateLease(reqNanos, raftStatus);
            if (body.getTerm() == raftStatus.getCurrentTerm() && reqTerm == raftStatus.getCurrentTerm()) {
                node.setNextIndex(body.getMaxLogIndex() + 1);
                replicate(node);
            } else {
                long idx = raftLog.findMaxIndexByTerm(body.getMaxLogTerm());
                if (idx > 0) {
                    node.setNextIndex(Math.min(body.getMaxLogIndex(), idx) + 1);
                    replicate(node);
                } else {
                    int t = raftLog.findLastTermLessThan(body.getMaxLogTerm());
                    if (t > 0) {
                        idx = raftLog.findMaxIndexByTerm(t);
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

    private void tryCommit(long recentMatchIndex) {
        RaftStatus raftStatus = this.raftStatus;

        boolean needCommit = RaftUtil.needCommit(raftStatus.getCommitIndex(), recentMatchIndex,
                raftStatus.getServers(), raftStatus.getRwQuorum());
        if (!needCommit) {
            return;
        }
        // leader can only commit log in current term, see raft paper 5.4.2
        boolean needNotify = false;
        if (raftStatus.getFirstCommitIndexOfCurrentTerm() <= 0) {
            int t = raftLog.getTermOf(recentMatchIndex);
            if (t != raftStatus.getCurrentTerm()) {
                return;
            } else {
                raftStatus.setFirstCommitIndexOfCurrentTerm(recentMatchIndex);
                needNotify = true;
            }
        }
        raftStatus.setCommitIndex(recentMatchIndex);

        for (long i = raftStatus.getLastApplied() + 1; i <= recentMatchIndex; i++) {
            RaftTask rt = raftStatus.getPendingRequests().get(i);
            if (rt == null) {
                // TODO error handle
                LogItem item = raftLog.load(i, 1, 0)[0];
                RaftInput input;
                if (item.getType() != LogItem.TYPE_HEARTBEAT) {
                    Object o = stateMachine.decode(item.getBuffer());
                    input = new RaftInput(item.getBuffer(), o, null, false);
                } else {
                    input = new RaftInput(item.getBuffer(), null, null, false);
                }
                rt = new RaftTask(ts, item.getType(), input, null);
            }
            execInStateMachine(i, rt);
        }

        raftStatus.setLastApplied(recentMatchIndex);
        if (needNotify) {
            raftStatus.getFirstCommitOfApplied().complete(null);
            raftStatus.setFirstCommitOfApplied(null);
        }
    }

    private void execInStateMachine(long index, RaftTask rt) {
        execInStateMachine0(index, rt);
        if (rt.nextReaders == null) {
            return;
        }
        for (RaftTask readerTask : rt.nextReaders) {
            execInStateMachine0(index, readerTask);
        }
    }

    private void execInStateMachine0(long index, RaftTask rt) {
        // TODO error handle
        if (rt.type == LogItem.TYPE_HEARTBEAT) {
            return;
        }
        RaftInput input = rt.input;
        CompletableFuture<RaftOutput> future = rt.future;
        if (input.isReadOnly() && input.getDeadline().isTimeout(ts)) {
            if (future != null) {
                future.completeExceptionally(new RaftExecTimeoutException("timeout "
                        + input.getDeadline().getTimeout(TimeUnit.MILLISECONDS) + "ms"));
            }
            return;
        }
        Object result = stateMachine.exec(input);
        if (future != null) {
            future.complete(new RaftOutput(index, result));
        }
    }
}
