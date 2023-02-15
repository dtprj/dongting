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
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.Decoder;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.PbZeroCopyDecoder;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.raft.rpc.VoteReq;
import com.github.dtprj.dongting.raft.rpc.VoteResp;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;

import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class VoteManager {

    private static final DtLog log = DtLogs.getLogger(VoteManager.class);
    private final Raft raft;
    private final NioClient client;
    private final RaftStatus raftStatus;
    private final RaftServerConfig config;
    private final RaftExecutor raftExecutor;

    public VoteManager(RaftContainer container, Raft raft) {
        this.raft = raft;
        this.client = container.getClient();
        this.raftStatus = container.getRaftStatus();
        this.config = container.getConfig();
        this.raftExecutor = container.getRaftExecutor();
    }

    public void sendVoteRequest(RaftNode node) {
        VoteReq req = new VoteReq();
        req.setCandidateId(config.getId());
        req.setTerm(raftStatus.getCurrentTerm());
        req.setLastLogIndex(raftStatus.getLastLogIndex());
        req.setLastLogTerm(raftStatus.getLastLogTerm());
        VoteReq.WriteFrame wf = new VoteReq.WriteFrame(req);
        wf.setCommand(Commands.RAFT_REQUEST_VOTE);
        DtTime timeout = new DtTime(config.getRpcTimeout(), TimeUnit.MILLISECONDS);
        Decoder decoder = new PbZeroCopyDecoder(c -> new VoteResp.Callback());
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
            // don't send more request for simplification
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
                        RaftUtil.changeToLeader(raftStatus);
                        raft.sendHeartBeat();
                    }
                }
            }
        } else {
            RaftUtil.incrTermAndConvertToFollower(remoteTerm, raftStatus);
        }
    }
}
