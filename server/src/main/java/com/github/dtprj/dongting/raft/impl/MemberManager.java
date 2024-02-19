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

import com.github.dtprj.dongting.codec.PbNoCopyDecoder;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.IntObjMap;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.PbIntWriteFrame;
import com.github.dtprj.dongting.net.PeerStatus;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.rpc.QueryStatusResp;
import com.github.dtprj.dongting.raft.rpc.RaftPingFrameCallback;
import com.github.dtprj.dongting.raft.rpc.RaftPingProcessor;
import com.github.dtprj.dongting.raft.rpc.RaftPingWriteFrame;
import com.github.dtprj.dongting.raft.rpc.TransferLeaderReq;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.NotLeaderException;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftOutput;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

/**
 * @author huangli
 */
public class MemberManager {
    private static final DtLog log = DtLogs.getLogger(MemberManager.class);
    private final GroupComponents gc;
    private final NioClient client;

    private final RaftServerConfig serverConfig;
    private final RaftStatusImpl raftStatus;
    private final int groupId;
    private final RaftGroupConfigEx groupConfig;

    private ReplicateManager replicateManager;
    private NodeManager nodeManager;

    private final CompletableFuture<Void> startReadyFuture;
    private final int startReadyQuorum;

    public MemberManager(NioClient client, GroupComponents gc) {
        this.client = client;
        this.gc = gc;
        this.serverConfig = gc.getServerConfig();
        this.groupConfig = gc.getGroupConfig();
        this.raftStatus = gc.getRaftStatus();
        this.groupId = raftStatus.getGroupId();

        if (raftStatus.getNodeIdOfMembers().isEmpty()) {
            this.startReadyFuture = CompletableFuture.completedFuture(null);
            this.startReadyQuorum = 0;
        } else {
            this.startReadyQuorum = RaftUtil.getElectQuorum(raftStatus.getNodeIdOfMembers().size());
            this.startReadyFuture = new CompletableFuture<>();
        }
    }

    public void postInit() {
        this.replicateManager = gc.getReplicateManager();
        this.nodeManager = gc.getNodeManager();
    }

    /**
     * invoke by RaftServer init thread or schedule thread
     */
    public void init(IntObjMap<RaftNodeEx> allNodes) {
        raftStatus.setMembers(new ArrayList<>());
        for (int nodeId : raftStatus.getNodeIdOfMembers()) {
            RaftNodeEx node = allNodes.get(nodeId);
            RaftMember m = new RaftMember(node, groupConfig.getFiberGroup());
            if (node.isSelf()) {
                initSelf(m, RaftRole.follower);
            }
            raftStatus.getMembers().add(m);
        }
        if (!raftStatus.getNodeIdOfObservers().isEmpty()) {
            List<RaftMember> observers = new ArrayList<>();
            for (int nodeId : raftStatus.getNodeIdOfObservers()) {
                RaftNodeEx node = allNodes.get(nodeId);
                RaftMember m = new RaftMember(node, groupConfig.getFiberGroup());
                if (node.isSelf()) {
                    initSelf(m, RaftRole.observer);
                }
                observers.add(m);
            }
            raftStatus.setObservers(observers);
        } else {
            raftStatus.setObservers(emptyList());
        }
        raftStatus.setPreparedMembers(emptyList());
        computeDuplicatedData(raftStatus);
        if (!raftStatus.getNodeIdOfMembers().contains(serverConfig.getNodeId())
                && !raftStatus.getNodeIdOfObservers().contains(serverConfig.getNodeId())) {
            raftStatus.setSelf(null);
            raftStatus.setRole(RaftRole.observer);
        }
    }

    private void initSelf(RaftMember m, RaftRole role) {
        m.setReady(true);
        raftStatus.setSelf(m);
        raftStatus.setRole(role);
        raftStatus.copyShareStatus();
    }

    static void computeDuplicatedData(RaftStatusImpl raftStatus) {
        ArrayList<RaftMember> replicateList = new ArrayList<>();
        Set<Integer> memberIds = new HashSet<>();
        Set<Integer> observerIds = new HashSet<>();
        Set<Integer> jointMemberIds = new HashSet<>();
        Set<Integer> jointObserverIds = new HashSet<>();
        for (RaftMember m : raftStatus.getMembers()) {
            replicateList.add(m);
            memberIds.add(m.getNode().getNodeId());
        }
        for (RaftMember m : raftStatus.getObservers()) {
            replicateList.add(m);
            observerIds.add(m.getNode().getNodeId());
        }
        for (RaftMember m : raftStatus.getPreparedMembers()) {
            replicateList.add(m);
            jointMemberIds.add(m.getNode().getNodeId());
        }
        for (RaftMember m : raftStatus.getPreparedObservers()) {
            jointObserverIds.add(m.getNode().getNodeId());
        }
        raftStatus.setReplicateList(replicateList.isEmpty() ? emptyList() : replicateList);
        raftStatus.setNodeIdOfMembers(memberIds.isEmpty() ? emptySet() : memberIds);
        raftStatus.setNodeIdOfObservers(observerIds.isEmpty() ? emptySet() : observerIds);
        raftStatus.setNodeIdOfPreparedMembers(jointMemberIds.isEmpty() ? emptySet() : jointMemberIds);
        raftStatus.setNodeIdOfPreparedObservers(jointObserverIds.isEmpty() ? emptySet() : jointObserverIds);

        raftStatus.setElectQuorum(RaftUtil.getElectQuorum(raftStatus.getMembers().size()));
        raftStatus.setRwQuorum(RaftUtil.getRwQuorum(raftStatus.getMembers().size()));
    }

    public Fiber createRaftPingFiber() {
        FiberFrame<Void> fiberFrame = new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                ensureRaftMemberStatus();
                replicateManager.tryStartReplicateFibers();
                return Fiber.sleep(1000, this);
            }
        };
        // daemon fiber
        return new Fiber("raftPing", groupConfig.getFiberGroup(), fiberFrame, true);
    }

    public void ensureRaftMemberStatus() {
        List<RaftMember> replicateList = raftStatus.getReplicateList();
        for (RaftMember member : replicateList) {
            check(member);
        }
    }

    private void check(RaftMember member) {
        RaftNodeEx node = member.getNode();
        NodeStatus nodeStatus = node.getStatus();
        if (!nodeStatus.isReady()) {
            setReady(member, false);
        } else if (nodeStatus.getEpoch() != member.getNodeEpoch()) {
            setReady(member, false);
            if (!member.isPinging()) {
                raftPing(node, member, nodeStatus.getEpoch());
            }
        }
    }

    private void raftPing(RaftNodeEx raftNodeEx, RaftMember member, int nodeEpochWhenStartPing) {
        if (raftNodeEx.getPeer().getStatus() != PeerStatus.connected) {
            setReady(member, false);
            return;
        }

        member.setPinging(true);
        DtTime timeout = new DtTime(serverConfig.getRpcTimeout(), TimeUnit.MILLISECONDS);
        RaftPingWriteFrame f = new RaftPingWriteFrame(groupId, serverConfig.getNodeId(),
                raftStatus.getNodeIdOfMembers(), raftStatus.getNodeIdOfObservers());
        client.sendRequest(raftNodeEx.getPeer(), f, RaftPingProcessor.DECODER, timeout)
                .whenCompleteAsync((rf, ex) -> processPingResult(raftNodeEx, member, rf, ex, nodeEpochWhenStartPing),
                        groupConfig.getFiberGroup().getExecutor());
    }

    private void processPingResult(RaftNodeEx raftNodeEx, RaftMember member,
                                   ReadFrame<RaftPingFrameCallback> rf, Throwable ex, int nodeEpochWhenStartPing) {
        member.setPinging(false);
        RaftPingFrameCallback callback = rf.getBody();
        if (ex != null) {
            log.warn("raft ping fail, remote={}", raftNodeEx.getHostPort(), ex);
            setReady(member, false);
        } else {
            if (callback.nodeId != raftNodeEx.getNodeId() || callback.groupId != groupId) {
                log.error("raft ping error, groupId or nodeId not found, groupId={}, remote={}",
                        groupId, raftNodeEx.getHostPort());
                setReady(member, false);
            } else if (checkRemoteConfig(callback)) {
                NodeStatus currentNodeStatus = member.getNode().getStatus();
                if (currentNodeStatus.isReady() && nodeEpochWhenStartPing == currentNodeStatus.getEpoch()) {
                    log.info("raft ping success, id={}, remote={}", callback.nodeId, raftNodeEx.getHostPort());
                    setReady(member, true);
                    member.setNodeEpoch(nodeEpochWhenStartPing);
                    replicateManager.tryStartReplicateFibers();
                } else {
                    log.warn("raft ping success but current node status not match. "
                                    + "id={}, remoteHost={}, nodeReady={}, nodeEpoch={}, pingEpoch={}",
                            callback.nodeId, raftNodeEx.getHostPort(), currentNodeStatus.isReady(),
                            currentNodeStatus.getEpoch(), nodeEpochWhenStartPing);
                    setReady(member, false);
                }
            } else {
                log.error("raft ping error, group ids not match: remote={}, localIds={}, remoteIds={}, localObservers={}, remoteObservers={}",
                        raftNodeEx, raftStatus.getNodeIdOfMembers(), callback.nodeIdOfMembers, raftStatus.getNodeIdOfObservers(), callback.nodeIdOfObservers);
                setReady(member, false);
            }
        }
    }

    private boolean checkRemoteConfig(RaftPingFrameCallback callback) {
        if (serverConfig.isStaticConfig()) {
            return raftStatus.getNodeIdOfMembers().equals(callback.nodeIdOfMembers)
                    && raftStatus.getNodeIdOfObservers().equals(callback.nodeIdOfObservers);
        }
        return true;
    }

    public void setReady(RaftMember member, boolean ready) {
        if (ready && !startReadyFuture.isDone()) {
            int readyCount = getReadyCount(raftStatus.getMembers());
            if (readyCount >= startReadyQuorum) {
                log.info("member manager is ready: groupId={}", groupId);
                startReadyFuture.complete(null);
            }
        }
        member.setReady(ready);
    }

    private int getReadyCount(List<RaftMember> list) {
        int count = 0;
        for (RaftMember m : list) {
            if (m.isReady()) {
                count++;
            }
        }
        return count;
    }

    public void leaderPrepareJointConsensus(Set<Integer> newMemberNodes, Set<Integer> newObserverNodes,
                                            CompletableFuture<Long> f) {
        leaderConfigChange(LogItem.TYPE_PREPARE_CONFIG_CHANGE, getInputData(newMemberNodes, newObserverNodes))
                .whenComplete((output, ex) -> {
                    if (ex != null) {
                        f.completeExceptionally(ex);
                    } else {
                        f.complete(output.getLogIndex());
                    }
                });
    }

    public void leaderAbortJointConsensus(CompletableFuture<Void> f) {
        leaderConfigChange(LogItem.TYPE_DROP_CONFIG_CHANGE, null).whenComplete((output, ex) -> {
            if (ex != null) {
                f.completeExceptionally(ex);
            } else {
                f.complete(null);
            }
        });
    }

    public void leaderCommitJointConsensus(CompletableFuture<Void> finalFuture, long prepareIndex) {
        final HashMap<Integer, CompletableFuture<Boolean>> resultMap = new HashMap<>();

        for (RaftMember m : raftStatus.getMembers()) {
            queryPrepareStatus(prepareIndex, resultMap, m);
        }
        for (RaftMember m : raftStatus.getPreparedMembers()) {
            if (resultMap.containsKey(m.getNode().getNodeId())) {
                continue;
            }
            queryPrepareStatus(prepareIndex, resultMap, m);
        }

        List<CompletableFuture<Boolean>> list = new ArrayList<>(resultMap.values());
        for (CompletableFuture<Boolean> resultFuture : list) {
            resultFuture.thenRun(() -> checkPrepareStatus(resultMap, prepareIndex, finalFuture));
        }
    }

    private void queryPrepareStatus(long prepareIndex, HashMap<Integer, CompletableFuture<Boolean>> resultMap, RaftMember m) {
        RaftNodeEx n = m.getNode();
        if (n.isSelf()) {
            log.info("self prepare status, groupId={}, lastApplied={}, prepareIndex={}",
                    groupId, raftStatus.getLastApplied(), prepareIndex);
            boolean result = raftStatus.getLastApplied() >= prepareIndex;
            resultMap.put(n.getNodeId(), CompletableFuture.completedFuture(result));
        } else {
            final PbNoCopyDecoder<QueryStatusResp> decoder = new PbNoCopyDecoder<>(
                    c -> new QueryStatusResp.QueryStatusRespCallback());
            CompletableFuture<Boolean> queryFuture = client.sendRequest(n.getPeer(), new PbIntWriteFrame(groupId),
                            decoder, new DtTime(3, TimeUnit.SECONDS))
                    .handle((resp, ex) -> {
                        if (ex != null) {
                            log.warn("query prepare status failed, groupId={}, remoteId={}", n.getNodeId(), groupId, ex);
                            return Boolean.FALSE;
                        } else {
                            QueryStatusResp body = resp.getBody();
                            log.info("query prepare status success, groupId={}, remoteId={}, lastApplied={}, prepareIndex={}",
                                    groupId, n.getNodeId(), body.getLastApplied(), prepareIndex);
                            return body.getLastApplied() >= prepareIndex;
                        }
                    });
            resultMap.put(n.getNodeId(), queryFuture);
        }
    }

    private void checkPrepareStatus(HashMap<Integer, CompletableFuture<Boolean>> resultMap, long prepareIndex,
                                    CompletableFuture<Void> finalFuture) {
        if (resultMap.isEmpty()) {
            // prevent duplicate change
            return;
        }
        if (prepareIndex != raftStatus.getLastConfigChangeIndex()) {
            return;
        }
        int memberReadyCount = 0;
        for (RaftMember m : raftStatus.getMembers()) {
            CompletableFuture<Boolean> queryResult = resultMap.get(m.getNode().getNodeId());
            if (queryResult != null && queryResult.getNow(false)) {
                memberReadyCount++;
            }
        }
        int preparedMemberReadyCount = 0;
        for (RaftMember m : raftStatus.getPreparedMembers()) {
            CompletableFuture<Boolean> queryResult = resultMap.get(m.getNode().getNodeId());
            if (queryResult != null && queryResult.getNow(false)) {
                preparedMemberReadyCount++;
            }
        }
        if (memberReadyCount >= raftStatus.getElectQuorum()
                && preparedMemberReadyCount >= RaftUtil.getElectQuorum(raftStatus.getPreparedMembers().size())) {
            log.info("members prepare status check success, groupId={}, memberReadyCount={}, preparedMemberReadyCount={}",
                    groupId, memberReadyCount, preparedMemberReadyCount);

            // prevent duplicate change
            resultMap.clear();

            leaderConfigChange(LogItem.TYPE_COMMIT_CONFIG_CHANGE, null).whenComplete((output, ex) -> {
                if (ex != null) {
                    finalFuture.completeExceptionally(ex);
                } else {
                    finalFuture.complete(null);
                }
            });
        }
    }

    private ByteBuffer getInputData(Set<Integer> newMemberNodes, Set<Integer> newObserverNodes) {
        StringBuilder sb = new StringBuilder(64);
        appendSet(sb, raftStatus.getNodeIdOfMembers());
        appendSet(sb, raftStatus.getNodeIdOfObservers());
        appendSet(sb, newMemberNodes);
        appendSet(sb, newObserverNodes);
        sb.deleteCharAt(sb.length() - 1);
        return ByteBuffer.wrap(sb.toString().getBytes());
    }

    private void appendSet(StringBuilder sb, Set<Integer> set) {
        if (!set.isEmpty()) {
            for (int nodeId : set) {
                sb.append(nodeId).append(',');
            }
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append(';');
    }

    private CompletableFuture<RaftOutput> leaderConfigChange(int type, ByteBuffer data) {
        if (raftStatus.getRole() != RaftRole.leader) {
            String stageStr;
            switch (type) {
                case LogItem.TYPE_PREPARE_CONFIG_CHANGE:
                    stageStr = "prepare";
                    break;
                case LogItem.TYPE_COMMIT_CONFIG_CHANGE:
                    stageStr = "commit";
                    break;
                case LogItem.TYPE_DROP_CONFIG_CHANGE:
                    stageStr = "abort";
                    break;
                default:
                    throw new IllegalArgumentException(String.valueOf(type));
            }
            log.error("leader config change {}, not leader, role={}, groupId={}",
                    stageStr, raftStatus.getRole(), groupId);
            return CompletableFuture.failedFuture(new NotLeaderException(raftStatus.getCurrentLeaderNode()));
        }
        CompletableFuture<RaftOutput> outputFuture = new CompletableFuture<>();
        RaftInput input = new RaftInput(0, null, data, null, 0);
        RaftTask rt = new RaftTask(raftStatus.getTs(), type, input, outputFuture);

        gc.getLinearTaskRunner().raftExec(Collections.singletonList(rt));

        return outputFuture;
    }

    public void doPrepare(byte[] data) {
        String dataStr = new String(data);
        String[] fields = dataStr.split(";");
        Set<Integer> oldMemberIds = parseSet(fields[0]);
        Set<Integer> oldObserverIds = parseSet(fields[1]);
        Set<Integer> newMemberIds = parseSet(fields[2]);
        Set<Integer> newObserverIds = parseSet(fields[3]);
        if (!oldMemberIds.equals(raftStatus.getNodeIdOfMembers())) {
            log.error("oldMemberIds not match, oldMemberIds={}, currentMembers={}, groupId={}",
                    oldMemberIds, raftStatus.getNodeIdOfMembers(), raftStatus.getGroupId());
        }
        if (!oldObserverIds.equals(raftStatus.getNodeIdOfObservers())) {
            log.error("oldObserverIds not match, oldObserverIds={}, currentObservers={}, groupId={}",
                    oldObserverIds, raftStatus.getNodeIdOfObservers(), raftStatus.getGroupId());
        }

        Pair<List<RaftNodeEx>, List<RaftNodeEx>> pair = nodeManager.doPrepare(raftStatus.getNodeIdOfPreparedMembers(),
                raftStatus.getNodeIdOfPreparedObservers(), newMemberIds, newObserverIds);
        List<RaftNodeEx> newMemberNodes = pair.getLeft();
        List<RaftNodeEx> newObserverNodes = pair.getRight();

        IntObjMap<RaftMember> currentNodes = new IntObjMap<>();
        for (RaftMember m : raftStatus.getMembers()) {
            currentNodes.put(m.getNode().getNodeId(), m);
        }
        for (RaftMember m : raftStatus.getObservers()) {
            currentNodes.put(m.getNode().getNodeId(), m);
        }

        List<RaftMember> newMembers = new ArrayList<>();
        List<RaftMember> newObservers = new ArrayList<>();
        for (RaftNodeEx node : newMemberNodes) {
            RaftMember m = currentNodes.get(node.getNodeId());
            if (m == null) {
                m = new RaftMember(node, FiberGroup.currentGroup());
                if (node.getNodeId() == serverConfig.getNodeId()) {
                    initSelf(m, RaftRole.follower);
                }
            } else {
                if (node.getNodeId() == serverConfig.getNodeId() && raftStatus.getRole() == RaftRole.observer) {
                    if (raftStatus.getCurrentLeader() == null) {
                        RaftUtil.changeToFollower(raftStatus, -1);
                    } else {
                        RaftUtil.changeToFollower(raftStatus, raftStatus.getCurrentLeader().getNode().getNodeId());
                    }
                }
            }
            newMembers.add(m);
        }
        for (RaftNodeEx node : newObserverNodes) {
            RaftMember m = currentNodes.get(node.getNodeId());
            if (m == null) {
                m = new RaftMember(node, FiberGroup.currentGroup());
                if (node.getNodeId() == serverConfig.getNodeId()) {
                    initSelf(m, RaftRole.observer);
                }
            }
            newObservers.add(m);
        }
        raftStatus.setPreparedMembers(newMembers);
        raftStatus.setPreparedObservers(newObservers);

        computeDuplicatedData(raftStatus);

        gc.getVoteManager().cancelVote();
    }

    public Set<Integer> parseSet(String s) {
        if (s.isEmpty()) {
            return emptySet();
        }
        String[] fields = s.split(",");
        Set<Integer> set = new HashSet<>();
        for (String f : fields) {
            set.add(Integer.parseInt(f));
        }
        return set;
    }

    public void doAbort() {
        HashSet<Integer> ids = new HashSet<>(raftStatus.getNodeIdOfPreparedMembers());
        for (RaftMember m : raftStatus.getPreparedObservers()) {
            ids.add(m.getNode().getNodeId());
        }
        if (ids.isEmpty()) {
            return;
        }

        raftStatus.setPreparedMembers(emptyList());
        raftStatus.setPreparedObservers(emptyList());
        MemberManager.computeDuplicatedData(raftStatus);

        if (!raftStatus.getNodeIdOfMembers().contains(serverConfig.getNodeId())) {
            if (raftStatus.getRole() != RaftRole.observer) {
                RaftUtil.changeToObserver(raftStatus, -1);
            }
        }
        nodeManager.doAbort(ids);
    }

    public void doCommit() {
        HashSet<Integer> ids = new HashSet<>(raftStatus.getNodeIdOfMembers());
        ids.addAll(raftStatus.getNodeIdOfObservers());

        raftStatus.setMembers(raftStatus.getPreparedMembers());
        raftStatus.setObservers(raftStatus.getPreparedObservers());

        raftStatus.setPreparedMembers(emptyList());
        raftStatus.setPreparedObservers(emptyList());
        MemberManager.computeDuplicatedData(raftStatus);

        if (raftStatus.getNodeIdOfMembers().contains(serverConfig.getNodeId())) {
            if (raftStatus.getRole() != RaftRole.observer) {
                RaftUtil.changeToObserver(raftStatus, -1);
            }
        }

        nodeManager.doCommit(ids);
    }

    public boolean checkLeader(int nodeId) {
        RaftMember leader = raftStatus.getCurrentLeader();
        if (leader != null && leader.getNode().getNodeId() == nodeId) {
            return true;
        }
        return validCandidate(raftStatus, nodeId);
    }

    public static boolean validCandidate(RaftStatusImpl raftStatus, int nodeId) {
        return raftStatus.getNodeIdOfMembers().contains(nodeId)
                || raftStatus.getNodeIdOfPreparedMembers().contains(nodeId);
    }

    public void transferLeadership(int nodeId, CompletableFuture<Void> f, DtTime deadline) {
        groupConfig.getFiberGroup().fireFiber("transfer-leader",
                new TranferLeaderFiberFrame(nodeId, f, deadline));
    }

    private class TranferLeaderFiberFrame extends FiberFrame<Void> {

        private final int nodeId;
        private final CompletableFuture<Void> f;
        private final DtTime deadline;

        TranferLeaderFiberFrame(int nodeId, CompletableFuture<Void> f, DtTime deadline){
            this.nodeId = nodeId;
            this.f = f;
            this.deadline = deadline;
        }

        @Override
        public FrameCallResult execute(Void input) {
            if (raftStatus.getTransferLeaderCondition() != null) {
                f.completeExceptionally(new RaftException("transfer leader in progress"));
                return Fiber.frameReturn();
            }
            raftStatus.setTransferLeaderCondition(groupConfig.getFiberGroup().newCondition("transferLeader"));
            return checkBeforeTransferLeader(null);
        }

        private FrameCallResult checkBeforeTransferLeader(Void v) {
            if (raftStatus.getRole() != RaftRole.leader) {
                f.completeExceptionally(new NotLeaderException(raftStatus.getCurrentLeaderNode()));
                RaftUtil.clearTransferLeaderCondition(raftStatus);
                return Fiber.frameReturn();
            }
            RaftMember newLeader = null;
            for (RaftMember m : raftStatus.getMembers()) {
                if (m.getNode().getNodeId() == nodeId) {
                    newLeader = m;
                    break;
                }
            }
            if (newLeader == null) {
                for (RaftMember m : raftStatus.getPreparedMembers()) {
                    if (m.getNode().getNodeId() == nodeId) {
                        newLeader = m;
                        break;
                    }
                }
            }
            if (newLeader == null) {
                f.completeExceptionally(new RaftException("nodeId not found: " + nodeId));
                RaftUtil.clearTransferLeaderCondition(raftStatus);
                return Fiber.frameReturn();
            }

            if (deadline.isTimeout()) {
                f.completeExceptionally(new RaftException("transfer leader timeout"));
                RaftUtil.clearTransferLeaderCondition(raftStatus);
                return Fiber.frameReturn();
            }
            if (f.isCancelled()) {
                RaftUtil.clearTransferLeaderCondition(raftStatus);
                return Fiber.frameReturn();
            }

            boolean lastLogCommit = raftStatus.getCommitIndex() == raftStatus.getLastLogIndex();
            boolean newLeaderHasLastLog = newLeader.getMatchIndex() == raftStatus.getLastLogIndex();

            if (newLeader.isReady() && lastLogCommit && newLeaderHasLastLog) {
                RaftUtil.clearTransferLeaderCondition(raftStatus);
                RaftUtil.changeToFollower(raftStatus, newLeader.getNode().getNodeId());
                TransferLeaderReq req = new TransferLeaderReq();
                req.term = raftStatus.getCurrentTerm();
                req.logIndex = raftStatus.getLastLogIndex();
                req.oldLeaderId = serverConfig.getNodeId();
                req.groupId = groupId;
                TransferLeaderReq.TransferLeaderReqWriteFrame frame = new TransferLeaderReq.TransferLeaderReqWriteFrame(req);
                client.sendRequest(newLeader.getNode().getPeer(), frame,
                                null, new DtTime(5, TimeUnit.SECONDS))
                        .whenComplete((rf, ex) -> {
                            if (ex != null) {
                                log.error("transfer leader failed, groupId={}", groupId, ex);
                                f.completeExceptionally(ex);
                            } else {
                                log.info("transfer leader success, groupId={}", groupId);
                                f.complete(null);
                            }
                        });
                return Fiber.frameReturn();
            } else {
                return Fiber.sleep(1, this::checkBeforeTransferLeader);
            }
        }
    }

    public CompletableFuture<Void> getStartReadyFuture() {
        return startReadyFuture;
    }
}
