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

import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.raft.rpc.AppendReqCallback;
import com.github.dtprj.dongting.raft.rpc.AppendRespCallback;
import com.github.dtprj.dongting.raft.rpc.AppendRespWriteFrame;
import com.github.dtprj.dongting.raft.rpc.VoteReq;
import com.github.dtprj.dongting.raft.rpc.VoteResp;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;

import java.time.Duration;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class RaftThread extends Thread {
    private static final DtLog log = DtLogs.getLogger(RaftThread.class);

    private final RaftServerConfig config;
    private final RaftStatus raftStatus;
    private final int pollTimeout = new Random().nextInt(100) + 100;
    private final RaftRpc raftRpc;
    private final GroupConManager groupConManager;

    private long lastLeaderActiveTime = System.nanoTime();
    private final long timeoutNanos;

    // TODO optimise blocking queue
    private final LinkedBlockingQueue<RaftTask> queue;

    public RaftThread(RaftServerConfig config, LinkedBlockingQueue<RaftTask> queue, RaftStatus raftStatus,
                      RaftRpc raftRpc, GroupConManager groupConManager) {
        this.config = config;
        this.raftStatus = raftStatus;
        this.queue = queue;
        this.raftRpc = raftRpc;
        this.groupConManager = groupConManager;
        timeoutNanos = Duration.ofMillis(config.getLeaderTimeout()).toNanos();
    }

    @Override
    public void run() {
        while (true) {
            RaftTask t;
            try {
                t = queue.poll(pollTimeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                log.error("", e);
                return;
            }
            if (t == null) {
                idle();
                continue;
            }
            switch (t.getType()) {
                case RaftTask.TYPE_SHUTDOWN:
                    return;
                case RaftTask.TYPE_APPEND_ENTRIES_REQ:
                    processAppendEntriesReq(t);
                    break;
                case RaftTask.TYPE_APPEND_ENTRIES_RESP:
                    processAppendEntriesResp(t);
                    break;
                case RaftTask.TYPE_REQUEST_VOTE_REQ:
                    processVoteReq(t);
                    break;
                case RaftTask.TYPE_REQUEST_VOTE_RESP:
                    processVoteResp(t);
                    break;
            }
        }
    }

    private boolean timeout() {
        return System.nanoTime() - lastLeaderActiveTime > timeoutNanos;
    }

    private void refresh() {
        this.lastLeaderActiveTime = System.nanoTime();
    }

    private void processAppendEntriesReq(RaftTask t) {
        AppendRespWriteFrame resp = new AppendRespWriteFrame();
        ReadFrame rf = (ReadFrame) t.getData();
        AppendReqCallback req = (AppendReqCallback) rf.getBody();
        if (req.getTerm() >= raftStatus.getCurrentTerm()) {
            checkTerm(req.getTerm());
            refresh();
            resp.setTerm(raftStatus.getCurrentTerm());
            resp.setSuccess(true);
        } else {
            resp.setTerm(raftStatus.getCurrentTerm());
            resp.setSuccess(false);
        }
        resp.setRespCode(CmdCodes.SUCCESS);
        t.getRespWriter().writeRespInBizThreads(rf, resp);
    }

    private void checkTerm(int remoteTerm) {
        if (remoteTerm > raftStatus.getCurrentTerm()) {
            log.info("update term from {} to {}, change from {} to follower",
                    raftStatus.getCurrentTerm(), remoteTerm, raftStatus.getRole());
            raftStatus.setCurrentTerm(remoteTerm);
            raftStatus.setVoteFor(0);
            raftStatus.setRole(RaftRole.follower);
            raftStatus.getCurrentVotes().clear();
        }
    }

    private void processAppendEntriesResp(RaftTask t) {
        AppendRespCallback resp = (AppendRespCallback) t.getData();
        checkTerm(resp.getTerm());
    }

    private void processVoteReq(RaftTask t) {
        ReadFrame rf = (ReadFrame) t.getData();
        VoteReq voteReq = (VoteReq) rf.getBody();
        VoteResp resp = new VoteResp();
        int oldTerm = raftStatus.getCurrentTerm();
        if (voteReq.getTerm() > raftStatus.getCurrentTerm()) {
            checkTerm(voteReq.getTerm());
            raftStatus.setVoteFor(voteReq.getCandidateId());
            resp.setVoteGranted(true);
        } else if (voteReq.getTerm() == raftStatus.getCurrentTerm()) {
            resp.setVoteGranted(raftStatus.getVoteFor() == voteReq.getCandidateId());
        } else {
            resp.setVoteGranted(false);
        }
        log.info("receive vote request. granted={}. remoteTerm={}, localTerm={}",
                resp.isVoteGranted(), voteReq.getTerm(), oldTerm);
        resp.setTerm(raftStatus.getCurrentTerm());
        VoteResp.WriteFrame wf = new VoteResp.WriteFrame(resp);
        t.getRespWriter().writeRespInBizThreads(rf, wf);
    }

    private void processVoteResp(RaftTask t) {
        ReadFrame rf = (ReadFrame) t.getData();
        VoteResp voteResp = (VoteResp) rf.getBody();
        if (voteResp.getTerm() < raftStatus.getCurrentTerm()) {
            log.warn("receive vote resp, ignore, remoteTerm={}, localTerm={}",
                    voteResp.getTerm(), raftStatus.getCurrentTerm());
        } else if (voteResp.getTerm() == raftStatus.getCurrentTerm()) {
            if (raftStatus.getRole() == RaftRole.follower) {
                log.warn("follower receive vote resp, ignore. remoteTerm={}, localTerm={}",
                        voteResp.getTerm(), raftStatus.getCurrentTerm());
            } else {
                HashSet<Integer> votes = raftStatus.getCurrentVotes();
                int oldCount = votes.size();
                log.info("receive vote resp, granted={}, remote={}, remoteTerm={}, localTerm={}, currentVotes={}",
                        voteResp.isVoteGranted(), voteResp.getTerm(), raftStatus.getCurrentTerm(), oldCount);
                if (voteResp.isVoteGranted()) {
                    votes.add(t.getRemoteNode().getId());
                    int newCount = votes.size();
                    if (newCount > oldCount && newCount == raftStatus.getElectQuorum()) {
                        raftStatus.setRole(RaftRole.leader);
                        log.info("change to leader. term={}", raftStatus.getCurrentTerm());
                    }
                }
            }
        } else {
            checkTerm(voteResp.getTerm());
        }
    }

    public void requestShutdown() {
        RaftTask t = new RaftTask();
        t.setType(RaftTask.TYPE_SHUTDOWN);
        queue.offer(t);
    }

    private void idle() {
        switch (raftStatus.getRole()) {
            case follower:
            case candidate:
                if (timeout()) {
                    startElect();
                }
                break;
            case leader:
                sendHeartBeat();
                break;
        }
    }

    private void sendHeartBeat() {
        for (RaftNode node : groupConManager.getServers()) {
            if (node.isSelf()) {
                continue;
            }
            raftRpc.sendHeartBeat(node);
        }
    }

    private void startElect() {
        raftStatus.setCurrentTerm(raftStatus.getCurrentTerm() + 1);
        raftStatus.setVoteFor(config.getId());
        raftStatus.setRole(RaftRole.candidate);
        raftStatus.getCurrentVotes().clear();
        raftStatus.getCurrentVotes().add(config.getId());
        refresh();
        for (RaftNode node : groupConManager.getServers()) {
            if (node.isSelf()) {
                continue;
            }
            raftRpc.sendVoteRequest(node, this::timeout);
        }
    }


}
