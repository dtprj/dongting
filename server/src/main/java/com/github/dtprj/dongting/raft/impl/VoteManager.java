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
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.RpcCallback;
import com.github.dtprj.dongting.net.SimpleWritePacket;
import com.github.dtprj.dongting.raft.rpc.VoteReq;
import com.github.dtprj.dongting.raft.rpc.VoteResp;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.store.StatusManager;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class VoteManager {

    private static final DtLog log = DtLogs.getLogger(VoteManager.class);

    private final GroupComponents gc;

    private final RaftGroupConfigEx groupConfig;
    private final NioClient client;
    private final RaftStatusImpl raftStatus;
    private final RaftServerConfig config;
    private final int groupId;

    private LinearTaskRunner linearTaskRunner;
    private StatusManager statusManager;

    private boolean voting;
    private HashSet<Integer> votes;
    private int currentVoteId;

    int firstDelayMin = 1;
    int firstDelayMax = 30;
    int checkIntervalMin = 10;
    int checkIntervalMax = 700;

    public VoteManager(NioClient client, GroupComponents gc) {
        this.gc = gc;
        this.client = client;
        this.groupConfig = gc.groupConfig;
        this.raftStatus = gc.raftStatus;
        this.config = gc.serverConfig;
        this.groupId = groupConfig.groupId;
    }

    public void postInit() {
        this.linearTaskRunner = gc.linearTaskRunner;
        this.statusManager = gc.statusManager;
    }

    public void startVoteFiber() {
        VoteFiberFrame ff = new VoteFiberFrame();
        Fiber f = new Fiber("vote-" + groupId, groupConfig.fiberGroup, ff, true);
        f.start();
    }

    public void cancelVote(String reason) {
        if (voting) {
            log.info("cancel current voting. groupId={}, voteId={}, reason: {}", groupId, currentVoteId, reason);
            voting = false;
            votes = null;
            currentVoteId++;
        }
    }

    private void initStatusForVoting() {
        voting = true;
        currentVoteId++;
        votes = new HashSet<>();
    }

    private int readyCount(List<RaftMember> list) {
        int count = 0;
        for (int s = list.size(), i = 0; i < s; i++) {
            RaftMember member = list.get(i);
            if (member.ready) {
                // include self
                count++;
            }
        }
        return count;
    }

    private boolean readyNodesNotEnough(boolean preVote) {
        if (readyNodesNotEnough(raftStatus.members, preVote, false)) {
            return true;
        }
        return readyNodesNotEnough(raftStatus.preparedMembers, preVote, true);
    }

    private boolean readyNodesNotEnough(List<RaftMember> list, boolean preVote, boolean jointConsensus) {
        if (list.isEmpty()) {
            // for joint consensus
            return false;
        }
        int count = readyCount(list);
        if (count < RaftUtil.getElectQuorum(list.size())) {
            log.warn("{} only {} node is ready, can't start {}. groupId={}, term={}",
                    jointConsensus ? "[joint consensus]" : "", count,
                    preVote ? "pre-vote" : "vote", groupId, raftStatus.currentTerm);
            return true;
        }
        return false;
    }

    private void tryStartPreVote() {
        if (!MemberManager.validCandidate(raftStatus, config.nodeId)) {
            log.info("not valid candidate, can't start pre vote. groupId={}, term={}",
                    groupId, raftStatus.currentTerm);
            return;
        }

        if (readyNodesNotEnough(true)) {
            return;
        }

        RaftUtil.resetElectTimer(raftStatus);
        raftStatus.self.lastConfirmReqNanos = raftStatus.ts.nanoTime;

        Set<RaftMember> voter = RaftUtil.union(raftStatus.members, raftStatus.preparedMembers);
        initStatusForVoting();

        log.info("node ready, start pre vote. groupId={}, term={}, voteId={}, lastLogTerm={}, lastLogIndex={}",
                groupId, raftStatus.currentTerm, currentVoteId, raftStatus.lastLogTerm,
                raftStatus.lastLogIndex);
        startPreVote(voter);
    }

    private void startPreVote(Set<RaftMember> voter) {
        for (RaftMember member : voter) {
            if (member.ready) {
                sendRequest(member, true);
            }
        }
    }

    private void sendRequest(RaftMember member, boolean preVote) {
        VoteReq req = new VoteReq();
        int currentTerm = raftStatus.currentTerm;
        req.groupId = groupId;
        req.term = currentTerm;
        req.candidateId = config.nodeId;
        req.lastLogIndex = raftStatus.lastLogIndex;
        req.lastLogTerm = raftStatus.lastLogTerm;
        req.preVote = preVote;
        SimpleWritePacket wf = new SimpleWritePacket(req);
        wf.command = Commands.RAFT_REQUEST_VOTE;
        DtTime timeout = new DtTime(config.rpcTimeout, TimeUnit.MILLISECONDS);

        final int voteIdOfRequest = this.currentVoteId;

        long reqStartNanos = raftStatus.ts.nanoTime;
        if (member.node.self) {
            VoteResp resp = new VoteResp();
            resp.voteGranted = true;
            resp.term = currentTerm;
            fireRespProcessFiber(req, resp, null, member, voteIdOfRequest, reqStartNanos);
        } else {
            try {
                RpcCallback<VoteResp> c = (rf, ex) ->
                        fireRespProcessFiber(req, rf == null ? null : rf.getBody(), ex, member, voteIdOfRequest, reqStartNanos);
                client.sendRequest(member.node.peer, wf, ctx -> ctx.toDecoderCallback(new VoteResp.Callback()),
                        timeout, c);
                log.info("send {} request. remoteNode={}, groupId={}, term={}, lastLogIndex={}, lastLogTerm={}",
                        preVote ? "pre-vote" : "vote", member.node.nodeId, groupId,
                        currentTerm, req.lastLogIndex, req.lastLogTerm);
            } catch (Exception e) {
                fireRespProcessFiber(req, null, e, member, voteIdOfRequest, reqStartNanos);
            }
        }
    }

    private void fireRespProcessFiber(VoteReq req, VoteResp resp, Throwable ex, RaftMember member, int voteIdOfRequest, long reqStartNanos) {
        String fiberName = "vote-resp-processor(" + voteIdOfRequest + "," + member.node.nodeId + ")";
        RespProcessFiberFrame initFrame = new RespProcessFiberFrame(resp, ex, member, req, voteIdOfRequest, reqStartNanos);
        groupConfig.fiberGroup.fireFiber(fiberName, initFrame);
    }

    private static int getVoteCount(Set<Integer> members, Set<Integer> votes) {
        int count = 0;
        for (int nodeId : votes) {
            if (members.contains(nodeId)) {
                count++;
            }
        }
        return count;
    }

    private boolean isElectedAfterVote(int nodeId, boolean preVote) {
        if (!votes.add(nodeId)) {
            return false;
        }
        int quorum = raftStatus.electQuorum;
        int voteCount = getVoteCount(raftStatus.nodeIdOfMembers, votes);
        String voteType = preVote ? "pre-vote" : "vote";
        if (raftStatus.preparedMembers.isEmpty()) {
            log.info("[{}] get {} valid votes of {}, term={}, current votes: {}",
                    voteType, voteCount, raftStatus.nodeIdOfMembers.size(), raftStatus.currentTerm, votes);
            return voteCount >= quorum;
        } else {
            int jointQuorum = RaftUtil.getElectQuorum(raftStatus.preparedMembers.size());
            int jointVoteCount = getVoteCount(raftStatus.nodeIdOfPreparedMembers, votes);
            log.info("[{}] get {} valid votes of {}, joint consensus get {} valid votes of {}, term={}, current votes: {}",
                    voteType, voteCount, raftStatus.nodeIdOfMembers.size(), jointVoteCount,
                    raftStatus.nodeIdOfPreparedMembers.size(), raftStatus.currentTerm, votes);
            return voteCount >= quorum && jointVoteCount >= jointQuorum;
        }
    }

    private class VoteFiberFrame extends FiberFrame<Void> {

        private final Random r = new Random();

        @Override
        protected FrameCallResult handle(Throwable ex) {
            log.error("vote fiber error", ex);
            if (!isGroupShouldStopPlain()) {
                log.info("restart vote fiber. groupId={}", groupId);
                startVoteFiber();
            }
            return Fiber.frameReturn();
        }

        public VoteFiberFrame() {
        }

        @Override
        public FrameCallResult execute(Void input) {
            // sleep a random time to avoid multi nodes in same JVM start pre vote at almost same time (in tests)
            return randomSleep(raftStatus.ts.nanoTime, firstDelayMin, firstDelayMax);
        }

        private FrameCallResult loop(Void input) {
            if (isGroupShouldStopPlain()) {
                return Fiber.frameReturn();
            }
            if (raftStatus.installSnapshot) {
                return sleepAwhile();
            }
            RaftStatusImpl raftStatus = VoteManager.this.raftStatus;
            //if (raftStatus.getRole() == RaftRole.leader && raftStatus.getLeaseStartNanos()
            //        + raftStatus.getElectTimeoutNanos() - raftStatus.ts.nanoTime < 0) {
            //    RaftUtil.changeToFollower(raftStatus, -1, "leader lease timeout");
            //}
            boolean timeout = raftStatus.ts.nanoTime - raftStatus.lastElectTime > raftStatus.getElectTimeoutNanos();
            if (voting) {
                if (timeout) {
                    cancelVote("vote timeout");
                } else {
                    return sleepToNextElectTime();
                }
            }
            if (timeout) {
                if (raftStatus.getCurrentLeader() != null) {
                    raftStatus.setCurrentLeader(null);
                    raftStatus.copyShareStatus();
                }
                if (raftStatus.getRole() == RaftRole.observer || raftStatus.getRole() == RaftRole.none) {
                    return sleepToNextElectTime();
                } else if (raftStatus.getRole() == RaftRole.leader) {
                    RaftUtil.changeToFollower(raftStatus, -1, "leader timeout");
                }
                if (RaftUtil.writeNotFinished(raftStatus)) {
                    log.info("elect timer timeout and write not finished, groupId={}, term={}", groupId, raftStatus.currentTerm);
                    return sleepAwhile();
                }
                if (raftStatus.getLastApplied() < raftStatus.commitIndex) {
                    log.info("elect timer timeout and apply not finished, groupId={}, term={}, applied={}, commit={}",
                            groupId, raftStatus.currentTerm, raftStatus.getLastApplied(), raftStatus.commitIndex);
                    return sleepAwhile();
                }
                log.info("elect timer timeout, groupId={}, term={}, lastLogTerm={}, lastLogIndex={}",
                        groupId, raftStatus.currentTerm, raftStatus.lastLogTerm, raftStatus.lastLogIndex);
                tryStartPreVote();
            }
            return sleepToNextElectTime();
        }

        private FrameCallResult randomSleep(long baseNanos, int min, int max) {
            long base = baseNanos - raftStatus.ts.nanoTime;
            if (base < 0) {
                base = 0;
            } else {
                // convert to millis
                base = base / 1000000;
            }
            long t = base + r.nextInt(max - min + 1) + min;
            if (t <= 0) {
                return Fiber.resume(null, this::loop);
            }
            return Fiber.sleep(t, this::loop);
        }

        private FrameCallResult sleepAwhile() {
            return randomSleep(raftStatus.ts.nanoTime, checkIntervalMin, checkIntervalMax);
        }

        private FrameCallResult sleepToNextElectTime() {
            long base = raftStatus.lastElectTime + raftStatus.getElectTimeoutNanos();
            return randomSleep(base, checkIntervalMin, checkIntervalMax);
        }
    }

    private class RespProcessFiberFrame extends FiberFrame<Void> {

        private final VoteResp resp;
        private final Throwable ex;
        private final RaftMember remoteMember;
        private final VoteReq req;
        private final int voteIdOfRequest;
        private final long reqStartNanos;

        private RespProcessFiberFrame(VoteResp resp, Throwable ex, RaftMember remoteMember, VoteReq req,
                                      int voteIdOfRequest, long reqStartNanos) {
            this.resp = resp;
            this.ex = ex;
            this.remoteMember = remoteMember;
            this.req = req;
            this.voteIdOfRequest = voteIdOfRequest;
            this.reqStartNanos = reqStartNanos;
        }

        @Override
        public FrameCallResult execute(Void input) {
            if (isGroupShouldStopPlain()) {
                cancelVote("stop");
                return Fiber.frameReturn();
            }
            String voteType = req.preVote ? "pre-vote" : "vote";
            int remoteId = remoteMember.node.nodeId;
            if (ex != null) {
                log.warn("{} rpc fail. groupId={}, term={}, remote={}, error={}", voteType,
                        groupId, req.term, remoteId, ex.toString());
                // don't send more request for simplification
                return Fiber.frameReturn();
            }
            if (voteCheckFail(voteIdOfRequest)) {
                return Fiber.frameReturn();
            }

            int remoteTerm = resp.term;
            if (remoteTerm < raftStatus.currentTerm) {
                log.warn("receive outdated {} resp, ignore, remoteTerm={}, reqTerm={}, remoteId={}, groupId={}",
                        voteType, resp.term, req.term, remoteId, groupId);
                return Fiber.frameReturn();
            }
            if (remoteTerm > raftStatus.currentTerm) {
                RaftUtil.incrTerm(remoteTerm, raftStatus, -1, "remote term in vote resp greater than local");
                statusManager.persistAsync(true);
                // no rest action, so not call statusManager.waitSync
                return Fiber.frameReturn();
            }
            RaftRole r = raftStatus.getRole();
            if ((req.preVote && r != RaftRole.follower && r != RaftRole.candidate) ||
                    (!req.preVote) && r != RaftRole.candidate) {
                log.warn("{} receive {} resp, ignore. remoteTerm={}, reqTerm={}, remoteId={}, groupId={}",
                        r, voteType, resp.term, req.term, remoteId, groupId);
                return Fiber.frameReturn();
            }
            if (remoteId != config.nodeId) {
                log.info("receive vote resp, granted={}, remoteTerm={}, reqTerm={}, remoteId={}, groupId={}",
                        resp.voteGranted, resp.term, req.term, remoteId, groupId);
            }
            if (resp.voteGranted) {
                if (!req.preVote) {
                    remoteMember.lastConfirmReqNanos = reqStartNanos;
                }
                if (isElectedAfterVote(remoteId, req.preVote)) {
                    if (req.preVote) {
                        log.info("pre-vote success. groupId={}, term={}, lastLogTerm={}, lastLogIndex={}", groupId,
                                raftStatus.currentTerm, raftStatus.lastLogTerm, raftStatus.lastLogIndex);
                        return startVote();
                    } else {
                        log.info("successfully elected, change to leader. groupId={}, term={}, lastLogTerm={}, lastLogIndex={}",
                                groupId, raftStatus.currentTerm, raftStatus.lastLogTerm, raftStatus.lastLogIndex);
                        RaftUtil.changeToLeader(raftStatus);
                        cancelVote("successfully elected");
                        linearTaskRunner.issueHeartBeat();
                        return Fiber.frameReturn();
                    }
                }
            }
            return Fiber.frameReturn();
        }

        private boolean voteCheckFail(long oldVoteId) {
            if (oldVoteId != currentVoteId) {
                log.info("vote id changed, ignore {} response. remoteNode={}, grant={}",
                        req.preVote ? "preVote" : "vote", remoteMember.node.nodeId,
                        resp.voteGranted);
                return true;
            }
            if (MemberManager.validCandidate(raftStatus, config.nodeId)) {
                return false;
            } else {
                log.error("not valid candidate, cancel vote. groupId={}, term={}",
                        groupId, raftStatus.currentTerm);
                cancelVote("not valid candidate");
                return true;
            }
        }

        private FrameCallResult startVote() {
            if (isGroupShouldStopPlain()) {
                cancelVote("stop");
                return Fiber.frameReturn();
            }
            if (readyNodesNotEnough(false)) {
                cancelVote("ready nodes not enough");
                return Fiber.frameReturn();
            }

            Set<RaftMember> voter = RaftUtil.union(raftStatus.members, raftStatus.preparedMembers);
            // add vote id, so rest pre-vote response is ignored
            initStatusForVoting();


            RaftUtil.resetStatus(raftStatus);
            if (raftStatus.getRole() != RaftRole.candidate) {
                log.info("change to candidate. groupId={}, oldTerm={}, lastLogTerm={}, lastLogIndex={}",
                        groupId, raftStatus.currentTerm, raftStatus.lastLogTerm, raftStatus.lastLogIndex);
                raftStatus.setRole(RaftRole.candidate);
            }

            RaftUtil.resetElectTimer(raftStatus);
            raftStatus.self.lastConfirmReqNanos = raftStatus.ts.nanoTime;

            raftStatus.currentTerm = raftStatus.currentTerm + 1;
            raftStatus.votedFor = config.nodeId;
            raftStatus.copyShareStatus();
            log.info("set currentTerm to {}, groupId={}", raftStatus.currentTerm, groupId);

            statusManager.persistAsync(true);
            int voteIdBeforePersist = currentVoteId;
            return statusManager.waitUpdateFinish(v -> afterStartVotePersist(voter, voteIdBeforePersist));
        }

        private FrameCallResult afterStartVotePersist(Set<RaftMember> voter, int oldVoteId) {
            if (isGroupShouldStopPlain()) {
                cancelVote("stop");
                return Fiber.frameReturn();
            }
            if (voteCheckFail(oldVoteId)) {
                return Fiber.frameReturn();
            }
            if (readyNodesNotEnough(false)) {
                cancelVote("ready nodes not enough after vote status persist");
                return Fiber.frameReturn();
            }
            log.info("start vote. groupId={}, newTerm={}, voteId={}, lastIndex={}", groupId,
                    raftStatus.currentTerm, currentVoteId, raftStatus.lastLogIndex);

            for (RaftMember member : voter) {
                sendRequest(member, false);
            }
            return Fiber.frameReturn();
        }

    }

}
