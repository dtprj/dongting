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

import com.github.dtprj.dongting.common.IntObjMap;
import com.github.dtprj.dongting.fiber.FiberChannel;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.sm.SnapshotManager;
import com.github.dtprj.dongting.raft.sm.StateMachine;
import com.github.dtprj.dongting.raft.store.RaftLog;
import com.github.dtprj.dongting.raft.store.StatusManager;

/**
 * @author huangli
 */
public class GroupComponents {
    private RaftServerConfig serverConfig;
    private RaftGroupConfigEx groupConfig;
    private RaftStatusImpl raftStatus;
    private MemberManager memberManager;
    private VoteManager voteManager;
    private LinearTaskRunner linearTaskRunner;
    private CommitManager commitManager;
    private ApplyManager applyManager;
    private SnapshotManager snapshotManager;
    private StatusManager statusManager;
    private ReplicateManager replicateManager;

    private NodeManager nodeManager;
    private PendingStat serverStat;

    private RaftLog raftLog;
    private StateMachine stateMachine;

    private FiberGroup fiberGroup;

    private final IntObjMap<FiberChannel<Object>> processorChannels = new IntObjMap<>();

    public RaftServerConfig getServerConfig() {
        return serverConfig;
    }

    public void setServerConfig(RaftServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }

    public RaftGroupConfigEx getGroupConfig() {
        return groupConfig;
    }

    public void setGroupConfig(RaftGroupConfigEx groupConfig) {
        this.groupConfig = groupConfig;
    }

    public RaftStatusImpl getRaftStatus() {
        return raftStatus;
    }

    public void setRaftStatus(RaftStatusImpl raftStatus) {
        this.raftStatus = raftStatus;
    }

    public MemberManager getMemberManager() {
        return memberManager;
    }

    public void setMemberManager(MemberManager memberManager) {
        this.memberManager = memberManager;
    }

    public VoteManager getVoteManager() {
        return voteManager;
    }

    public void setVoteManager(VoteManager voteManager) {
        this.voteManager = voteManager;
    }

    public LinearTaskRunner getLinearTaskRunner() {
        return linearTaskRunner;
    }

    public void setLinearTaskRunner(LinearTaskRunner linearTaskRunner) {
        this.linearTaskRunner = linearTaskRunner;
    }

    public CommitManager getCommitManager() {
        return commitManager;
    }

    public void setCommitManager(CommitManager commitManager) {
        this.commitManager = commitManager;
    }

    public ApplyManager getApplyManager() {
        return applyManager;
    }

    public void setApplyManager(ApplyManager applyManager) {
        this.applyManager = applyManager;
    }

    public SnapshotManager getSnapshotManager() {
        return snapshotManager;
    }

    public void setSnapshotManager(SnapshotManager snapshotManager) {
        this.snapshotManager = snapshotManager;
    }

    public StatusManager getStatusManager() {
        return statusManager;
    }

    public void setStatusManager(StatusManager statusManager) {
        this.statusManager = statusManager;
    }

    public NodeManager getNodeManager() {
        return nodeManager;
    }

    public void setNodeManager(NodeManager nodeManager) {
        this.nodeManager = nodeManager;
    }

    public PendingStat getServerStat() {
        return serverStat;
    }

    public void setServerStat(PendingStat serverStat) {
        this.serverStat = serverStat;
    }

    public RaftLog getRaftLog() {
        return raftLog;
    }

    public void setRaftLog(RaftLog raftLog) {
        this.raftLog = raftLog;
    }

    public StateMachine getStateMachine() {
        return stateMachine;
    }

    public void setStateMachine(StateMachine stateMachine) {
        this.stateMachine = stateMachine;
    }

    public FiberGroup getFiberGroup() {
        return fiberGroup;
    }

    public void setFiberGroup(FiberGroup fiberGroup) {
        this.fiberGroup = fiberGroup;
    }

    public ReplicateManager getReplicateManager() {
        return replicateManager;
    }

    public void setReplicateManager(ReplicateManager replicateManager) {
        this.replicateManager = replicateManager;
    }

    public IntObjMap<FiberChannel<Object>> getProcessorChannels() {
        return processorChannels;
    }
}
