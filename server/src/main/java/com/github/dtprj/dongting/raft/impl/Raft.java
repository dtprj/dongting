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

import com.github.dtprj.dongting.buf.RefCountByteBuffer;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.Decoder;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.PbZeroCopyDecoder;
import com.github.dtprj.dongting.net.ProcessContext;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.pb.PbCallback;
import com.github.dtprj.dongting.raft.rpc.AppendReqWriteFrame;
import com.github.dtprj.dongting.raft.rpc.AppendRespCallback;
import com.github.dtprj.dongting.raft.rpc.VoteReq;
import com.github.dtprj.dongting.raft.rpc.VoteResp;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftLog;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;

import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

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

    private final int maxReplicateItems;
    private final int maxReplicateBytes;

    public Raft(RaftServerConfig config, RaftExecutor raftExecutor, RaftLog raftLog,
                RaftStatus raftStatus, NioClient client) {
        this.config = config;
        this.raftExecutor = raftExecutor;
        this.raftLog = raftLog;
        this.raftStatus = raftStatus;
        this.client = client;
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

    public void raftExec(RefCountByteBuffer log) {
        RaftStatus raftStatus = this.raftStatus;
        long oldIndex = raftStatus.getLastLogIndex();
        long newIndex = oldIndex + 1;
        int oldTerm = raftStatus.getLastLogTerm();
        int currentTerm = raftStatus.getCurrentTerm();
        // TODO async append
        raftLog.append(newIndex, oldTerm, currentTerm, log);
        raftStatus.setLastLogTerm(currentTerm);
        raftStatus.setLastLogIndex(newIndex);
        processOtherRaftNodes(raftStatus, this::replicate);
    }

    private void replicate(RaftNode node) {
        if (raftStatus.getRole() != RaftRole.leader) {
            return;
        }
        if (!node.isReady()) {
            return;
        }
        if (node.getConnectionId() == node.getLastConnectionId()) {
            doReplicate(node);
        } else {
            if (node.getPendingRequests() == 0) {
                node.setLastConnectionId(node.getConnectionId());
                doReplicate(node);
            } else {
                // reconnected, waiting all pending request complete
            }
        }
    }

    private void doReplicate(RaftNode node) {
        long nextIndex = node.getNextIndex();
        long lastLogIndex = raftStatus.getLastLogIndex();
        if (lastLogIndex < nextIndex) {
            // no data to replicate
            return;
        }

        long matchIndex = node.getMatchIndex();
        int pending = node.getPendingRequests();
        if (matchIndex == 0 && pending > 0) {
            // try matching, don't send more
            return;
        }

        // flow control
        if (nextIndex - matchIndex > maxReplicateItems) {
            return;
        }
        // TODO flow control by bytes

        for (long index = nextIndex; index <= lastLogIndex && index - matchIndex <= maxReplicateItems; index++) {
            LogItem item = raftLog.load(index);
            // TODO batch
            node.setPendingRequests(node.getPendingRequests() + 1);
            sendAppendRequest(node, index - 1, item.getPrevLogTerm(), item.getBuffer());
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
                        raftStatus.setRole(RaftRole.leader);
                        Raft.processOtherRaftNodes(raftStatus, n -> {
                            n.setMatchIndex(0);
                            n.setNextIndex(raftLog.lastIndex() + 1);
                            n.setPendingRequests(0);
                        });
                        log.info("change to leader. term={}", raftStatus.getCurrentTerm());
                    }
                }
            }
        } else {
            Raft.updateTermAndConvertToFollower(remoteTerm, raftStatus);
        }
    }

    public void sendAppendRequest(RaftNode node, long prevLogIndex, int prevLogTerm, RefCountByteBuffer log) {
        AppendReqWriteFrame req = new AppendReqWriteFrame();
        req.setCommand(Commands.RAFT_APPEND_ENTRIES);
        req.setTerm(raftStatus.getCurrentTerm());
        req.setLeaderId(config.getId());
        req.setLeaderCommit(raftStatus.getCommitIndex());
        req.setPrevLogIndex(prevLogIndex);
        req.setPrevLogTerm(prevLogTerm);
        req.setLog(log);

        DtTime timeout = new DtTime(config.getRpcTimeout(), TimeUnit.MILLISECONDS);
        Decoder decoder = new PbZeroCopyDecoder() {
            @Override
            protected PbCallback createCallback(ProcessContext context) {
                return new AppendRespCallback();
            }
        };
        CompletableFuture<ReadFrame> f = client.sendRequest(node.getPeer(), req, decoder, timeout);
        f.handleAsync((rf, ex) -> processAppendResultInRaftThread(rf, ex), raftExecutor);
    }

    private Object processAppendResultInRaftThread(ReadFrame rf, Throwable ex) {
        if (ex == null) {
            AppendRespCallback resp = (AppendRespCallback) rf.getBody();
            int remoteTerm = resp.getTerm();
            if (remoteTerm > raftStatus.getCurrentTerm()) {
                Raft.updateTermAndConvertToFollower(remoteTerm, raftStatus);
            } else {

            }
        } else {
            // TODO
        }
        return null;
    }

}
