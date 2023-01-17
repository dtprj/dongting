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
import com.github.dtprj.dongting.common.LongObjMap;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.Decoder;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.PbZeroCopyDecoder;
import com.github.dtprj.dongting.net.ProcessContext;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.pb.PbCallback;
import com.github.dtprj.dongting.raft.rpc.AppendProcessor;
import com.github.dtprj.dongting.raft.rpc.AppendReqWriteFrame;
import com.github.dtprj.dongting.raft.rpc.AppendRespCallback;
import com.github.dtprj.dongting.raft.rpc.AppendRespDecoder;
import com.github.dtprj.dongting.raft.rpc.VoteReq;
import com.github.dtprj.dongting.raft.rpc.VoteResp;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftLog;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.server.StateMachine;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

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
    private final Function<ByteBuffer, Object> logDecoder;

    private final int maxReplicateItems;
    private final int maxReplicateBytes;


    static class RaftTask {
        CompletableFuture<Object> future;
        Object decodedInput;
    }

    public Raft(RaftServerConfig config, RaftExecutor raftExecutor, RaftLog raftLog,
                RaftStatus raftStatus, NioClient client, Function<ByteBuffer, Object> logDecoder, StateMachine stateMachine) {
        this.config = config;
        this.raftExecutor = raftExecutor;
        this.raftLog = raftLog;
        this.raftStatus = raftStatus;
        this.client = client;
        this.logDecoder = logDecoder;
        this.stateMachine = stateMachine;
        this.maxReplicateItems = config.getMaxReplicateItems();
        this.maxReplicateBytes = config.getMaxReplicateBytes();
    }

    public static void updateTermAndConvertToFollower(int remoteTerm, RaftStatus raftStatus) {
        log.info("update term from {} to {}, change from {} to follower",
                raftStatus.getCurrentTerm(), remoteTerm, raftStatus.getRole());
        raftStatus.setCurrentTerm(remoteTerm);
        raftStatus.setVoteFor(0);
        raftStatus.setRole(RaftRole.follower);
        raftStatus.getCurrentVotes().clear();
        long t = System.nanoTime();
        raftStatus.setLastLeaderActiveTime(t);
        raftStatus.setHeartbeatTime(t);
        raftStatus.setLastElectTime(t);
        raftStatus.setPendingRequests(new LongObjMap<>());
        processOtherRaftNodes(raftStatus, node -> {
            node.setMatchIndex(0);
            node.setNextIndex(0);
        });
    }

    public static void processOtherRaftNodes(RaftStatus raftStatus, Consumer<RaftNode> consumer) {
        for (RaftNode node : raftStatus.getServers()) {
            if (node.isSelf()) {
                continue;
            }
            consumer.accept(node);
        }
    }

    public CompletableFuture<Object> raftExec(ByteBuffer log, Object decodedInput) {
        RaftStatus raftStatus = this.raftStatus;
        long oldIndex = raftStatus.getLastLogIndex();
        long newIndex = oldIndex + 1;
        int oldTerm = raftStatus.getLastLogTerm();
        int currentTerm = raftStatus.getCurrentTerm();
        // TODO async append, error handle
        raftLog.append(newIndex, oldTerm, currentTerm, log);
        CompletableFuture<Object> f = new CompletableFuture<>();
        RaftTask rt = new RaftTask();
        rt.future = f;
        rt.decodedInput = decodedInput;
        raftStatus.getPendingRequests().put(newIndex, rt);
        raftStatus.setLastLogTerm(currentTerm);
        raftStatus.setLastLogIndex(newIndex);
        processOtherRaftNodes(raftStatus, this::replicate);
        return f;
    }

    private void replicate(RaftNode node) {
        if (raftStatus.getRole() != RaftRole.leader) {
            return;
        }
        if (!node.isReady()) {
            return;
        }
        if (node.getEpoch() == node.getLastEpoch()) {
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

        long matchIndex = node.getMatchIndex();
        // flow control
        if (nextIndex - matchIndex > maxReplicateItems) {
            return;
        }
        // TODO flow control by bytes

        for (long index = nextIndex; index <= lastLogIndex && index - matchIndex <= maxReplicateItems; index++) {
            LogItem item = raftLog.load(index);
            // TODO batch
            node.incrAndGetPendingRequests();
            sendAppendRequest(node, index - 1, item.getPrevLogTerm(), item.getBuffer());
            if (tryMatch) {
                break;
            }
        }
    }

    public void sendVoteRequest(RaftNode node) {
        VoteReq req = new VoteReq();
        req.setCandidateId(config.getId());
        req.setTerm(raftStatus.getCurrentTerm());
        // TODO log fields
        VoteReq.WriteFrame wf = new VoteReq.WriteFrame(req);
        wf.setCommand(Commands.RAFT_REQUEST_VOTE);
        DtTime timeout = new DtTime(config.getRpcTimeout(), TimeUnit.MILLISECONDS);
        Decoder decoder = new PbZeroCopyDecoder() {
            @Override
            protected PbCallback createCallback(ProcessContext context) {
                return new VoteResp.Callback();
            }
        };
        CompletableFuture<ReadFrame> f = client.sendRequest(node.getPeer(), wf, decoder, timeout);
        log.info("send vote request to {}, term={}, votes={}", node.getPeer().getEndPoint(),
                raftStatus.getCurrentTerm(), raftStatus.getCurrentVotes());
        f.handleAsync((rf, ex) -> processVoteResp(rf, ex, node, req), raftExecutor);
    }

    private Object processVoteResp(ReadFrame rf, Throwable ex, RaftNode remoteNode, VoteReq voteReq) {
        if (ex == null) {
            processVoteResp(rf, remoteNode, voteReq);
        } else {
            log.warn("request vote rpc fail.term={}, remote={}, error={}", voteReq.getTerm(),
                    remoteNode.getPeer().getEndPoint(), ex.toString());
            if (voteReq.getTerm() == raftStatus.getCurrentTerm() && raftStatus.getRole() == RaftRole.candidate) {
                sendVoteRequest(remoteNode);
            }
        }
        return null;
    }

    private void processVoteResp(ReadFrame rf, RaftNode remoteNode, VoteReq voteReq) {
        VoteResp voteResp = (VoteResp) rf.getBody();
        int remoteTerm = voteResp.getTerm();
        if (remoteTerm < raftStatus.getCurrentTerm()) {
            log.warn("receive outdated vote resp, ignore, remoteTerm={}, reqTerm={}, remote={}",
                    voteResp.getTerm(), voteReq.getTerm(), remoteNode.getPeer().getEndPoint());
        } else if (remoteTerm == raftStatus.getCurrentTerm()) {
            if (raftStatus.getRole() == RaftRole.follower) {
                log.warn("follower receive vote resp, ignore. remoteTerm={}, reqTerm={}, remote={}",
                        voteResp.getTerm(), voteReq.getTerm(), remoteNode.getPeer().getEndPoint());
            } else {
                HashSet<Integer> votes = raftStatus.getCurrentVotes();
                int oldCount = votes.size();
                log.info("receive vote resp, granted={}, remoteTerm={}, reqTerm={}, oldVotes={}, remote={}",
                        voteResp.isVoteGranted(), voteResp.getTerm(),
                        voteReq.getTerm(), oldCount, remoteNode.getPeer().getEndPoint());
                if (voteResp.isVoteGranted()) {
                    votes.add(remoteNode.getId());
                    int newCount = votes.size();
                    if (newCount > oldCount && newCount == raftStatus.getElectQuorum()) {
                        changeToLeader();
                    }
                }
            }
        } else {
            Raft.updateTermAndConvertToFollower(remoteTerm, raftStatus);
        }
    }

    private void changeToLeader() {
        raftStatus.setRole(RaftRole.leader);
        Raft.processOtherRaftNodes(raftStatus, n -> {
            n.setMatchIndex(0);
            n.setNextIndex(raftStatus.getLastLogIndex() + 1);
            n.setPendingRequests(0);
            n.incrEpoch();
            raftStatus.setPendingRequests(new LongObjMap<>());
        });
        log.info("change to leader. term={}", raftStatus.getCurrentTerm());
    }

    public void sendAppendRequest(RaftNode node, long prevLogIndex, int prevLogTerm, ByteBuffer log) {
        AppendReqWriteFrame req = new AppendReqWriteFrame();
        req.setCommand(Commands.RAFT_APPEND_ENTRIES);
        req.setTerm(raftStatus.getCurrentTerm());
        req.setLeaderId(config.getId());
        req.setLeaderCommit(raftStatus.getCommitIndex());
        req.setPrevLogIndex(prevLogIndex);
        req.setPrevLogTerm(prevLogTerm);
        req.setLog(log);

        DtTime timeout = new DtTime(config.getRpcTimeout(), TimeUnit.MILLISECONDS);
        CompletableFuture<ReadFrame> f = client.sendRequest(node.getPeer(), req, AppendRespDecoder.INSTANCE, timeout);
        registerAppendResultCallback(node, prevLogIndex, prevLogTerm, f, raftStatus.getCurrentTerm());
    }

    private void registerAppendResultCallback(RaftNode node, long prevLogIndex, int prevLogTerm,
                                              CompletableFuture<ReadFrame> f, int reqTerm) {
        f.handleAsync((rf, ex) -> {
            if (ex == null) {
                processAppendResult(node, rf, prevLogIndex, prevLogTerm, reqTerm);
            } else {
                String msg = "append fail. remoteId={}, localTerm={}, reqTerm={}, prevLogIndex={}";
                if (node.incrEpoch()) {
                    log.warn(msg, node.getId(), raftStatus.getCurrentTerm(), reqTerm, prevLogIndex, ex);
                } else {
                    log.warn(msg, node.getId(), raftStatus.getCurrentTerm(), reqTerm, prevLogIndex);
                }
            }
            return null;
        }, raftExecutor);
    }

    // in raft thread
    private void processAppendResult(RaftNode node, ReadFrame rf, long prevLogIndex, int prevLogTerm, int reqTerm) {
        long expectNewMatchIndex = prevLogIndex + 1;
        AppendRespCallback body = (AppendRespCallback) rf.getBody();
        RaftStatus raftStatus = this.raftStatus;
        int remoteTerm = body.getTerm();
        if (remoteTerm > raftStatus.getCurrentTerm()) {
            log.info("find remote term greater than local term. remoteTerm={}, localTerm={}",
                    body.getTerm(), raftStatus.getCurrentTerm());
            Raft.updateTermAndConvertToFollower(remoteTerm, raftStatus);
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
        node.decrAndGetPendingRequests();
        if (body.isSuccess()) {
            if (node.getMatchIndex() <= prevLogIndex) {
                node.setMatchIndex(expectNewMatchIndex);
                // update last epoch
                node.setLastEpoch(node.getEpoch());
                tryCommit();
                if (raftStatus.getLastLogIndex() >= node.getNextIndex()) {
                    replicate(node);
                }
            } else {
                BugLog.getLog().error("append miss order. old matchIndex={}, append prevLogIndex={}, expectNewMatchIndex={}, remoteId={}, localTerm={}, reqTerm={}, remoteTerm={}",
                        node.getMatchIndex(), prevLogIndex, expectNewMatchIndex, node.getId(), raftStatus.getCurrentTerm(), reqTerm, body.getTerm());
            }
        } else if (body.getAppendCode() == AppendProcessor.CODE_LOG_NOT_MATCH) {
            log.info("log not match. remoteId={}, matchIndex={}, prevLogIndex={}, expectNewMatchIndex={}, maxLogTerm={}, maxLogIndex={}, localTerm={}, reqTerm={}, remoteTerm={}",
                    node.getId(), node.getMatchIndex(), prevLogIndex, expectNewMatchIndex, body.getMaxLogTerm(),
                    body.getMaxLogIndex(), raftStatus.getCurrentTerm(), reqTerm, body.getTerm());
            if (body.getTerm() == raftStatus.getCurrentTerm() && reqTerm == raftStatus.getCurrentTerm()) {
                node.setNextIndex(body.getMaxLogIndex() + 1);
            } else {
                long idx = raftLog.findLastLogItemIndexByTerm(body.getMaxLogTerm());
                if (idx > 0) {
                    node.setNextIndex(Math.min(body.getMaxLogIndex(), idx) + 1);
                } else {
                    int t = raftLog.findLastTermLessThan(body.getMaxLogTerm());
                    if (t > 0) {
                        idx = raftLog.findLastLogItemIndexByTerm(t);
                        if (idx > 0) {
                            node.setNextIndex(Math.min(body.getMaxLogIndex(), idx) + 1);
                        } else {
                            BugLog.getLog().error("can't find log to replicate. term={}", t);
                        }
                    } else {
                        BugLog.getLog().error("can't find log to replicate. follower maxTerm={}", body.getMaxLogTerm());
                    }
                }
            }
            replicate(node);
        } else {
            BugLog.getLog().error("append fail. appendCode={}, old matchIndex={}, append prevLogIndex={}, expectNewMatchIndex={}, remoteId={}, localTerm={}, reqTerm={}, remoteTerm={}",
                    body.getAppendCode(), node.getMatchIndex(), prevLogIndex, expectNewMatchIndex, node.getId(), raftStatus.getCurrentTerm(), reqTerm, body.getTerm());
        }
    }

    private void tryCommit() {
        RaftStatus raftStatus = this.raftStatus;
        List<RaftNode> servers = raftStatus.getServers();
        int rwQuorum = raftStatus.getRwQuorum() - 1; // exclude self
        long commitIndex = raftStatus.getCommitIndex();
        while (true) {
            int match = 0;
            boolean needCommit = false;
            for (RaftNode node : servers) {
                if (node.getMatchIndex() > commitIndex) {
                    if (++match >= rwQuorum) {
                        needCommit = true;
                        break;
                    }
                }
            }
            if (needCommit) {
                commitIndex++;
                // TODO error handle
                RaftTask rt = raftStatus.getPendingRequests().remove(commitIndex);
                Object input;
                if (rt != null) {
                    input = rt.decodedInput;
                } else {
                    LogItem item = raftLog.load(commitIndex);
                    input = logDecoder.apply(item.getBuffer());
                }
                Object result = stateMachine.apply(input);
                rt.future.complete(result);
            } else {
                break;
            }
        }
        raftStatus.setCommitIndex(commitIndex);
        raftStatus.setLastApplied(commitIndex);
    }
}
