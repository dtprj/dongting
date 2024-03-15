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

import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.raft.server.RaftNode;
import com.github.dtprj.dongting.raft.server.RaftStatus;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
public class RaftStatusImpl extends RaftStatus {

    private volatile ShareStatus shareStatus;

    private boolean installSnapshot;
    private int stateMachineEpoch;

    private RaftRole role; // shared
    private RaftMember currentLeader; // shared
    private final Timestamp ts;
    private int electQuorum;
    private int rwQuorum;

    private RaftMember self;
    private List<RaftMember> members;
    private List<RaftMember> observers;
    private List<RaftMember> preparedMembers;
    private List<RaftMember> preparedObservers;

    private Set<Integer> nodeIdOfMembers;
    private Set<Integer> nodeIdOfObservers;
    private Set<Integer> nodeIdOfPreparedMembers;
    private Set<Integer> nodeIdOfPreparedObservers;
    private long lastConfigChangeIndex;

    private List<RaftMember> replicateList;

    private FiberCondition dataArrivedCondition;
    private TailCache tailCache;
    private long firstIndexOfCurrentTerm;
    private CompletableFuture<Void> firstCommitOfApplied; // shared

    private boolean shareStatusUpdated;
    private long electTimeoutNanos; // shared

    private long leaseStartNanos; // shared
    private long[] leaseComputeArray = new long[0];

    private long lastElectTime;

    private long lastLogIndex;
    private int lastLogTerm;
    private long lastForceLogIndex;
    private long lastWriteLogIndex;

    private long leaderCommit;

    private FiberGroup fiberGroup;

    private FiberCondition logForceFinishCondition;
    private FiberCondition logWriteFinishCondition;
    private FiberCondition transferLeaderCondition;

    private final CompletableFuture<Void> initFuture = new CompletableFuture<>();
    private final CompletableFuture<Void> startFuture = new CompletableFuture<>();

    public RaftStatusImpl(Timestamp ts) {
        this.ts = ts;
        lastElectTime = ts.getNanoTime() - Duration.ofDays(1).toNanos();
    }

    public void copyShareStatus() {
        if (shareStatusUpdated) {
            ShareStatus ss = new ShareStatus();
            ss.role = role;
            ss.lastApplied = lastApplied;
            ss.leaseEndNanos = leaseStartNanos + electTimeoutNanos;
            ss.currentLeader = currentLeader;
            ss.firstCommitOfApplied = firstCommitOfApplied;

            this.shareStatusUpdated = false;
            this.shareStatus = ss;
        }
    }

    public RaftNode getCurrentLeaderNode() {
        return currentLeader == null ? null : currentLeader.getNode();
    }

    public void setLastApplied(long lastApplied) {
        this.lastApplied = lastApplied;
        this.shareStatusUpdated = true;
    }

    public void setRole(RaftRole role) {
        this.role = role;
        this.shareStatusUpdated = true;
    }

    public void setLeaseStartNanos(long leaseStartNanos) {
        this.leaseStartNanos = leaseStartNanos;
        this.shareStatusUpdated = true;
    }

    public void setCurrentLeader(RaftMember currentLeader) {
        this.currentLeader = currentLeader;
        this.shareStatusUpdated = true;
    }

    public void setElectTimeoutNanos(long electTimeoutNanos) {
        this.electTimeoutNanos = electTimeoutNanos;
        this.shareStatusUpdated = true;
    }

    public void setFirstCommitOfApplied(CompletableFuture<Void> firstCommitOfApplied) {
        this.firstCommitOfApplied = firstCommitOfApplied;
        this.shareStatusUpdated = true;
    }

    //------------------------- simple getters and setters--------------------------------

    public RaftRole getRole() {
        return role;
    }

    public int getElectQuorum() {
        return electQuorum;
    }

    public int getRwQuorum() {
        return rwQuorum;
    }

    public void setElectQuorum(int electQuorum) {
        this.electQuorum = electQuorum;
    }

    public void setRwQuorum(int rwQuorum) {
        this.rwQuorum = rwQuorum;
    }

    public long getLastElectTime() {
        return lastElectTime;
    }

    public void setLastElectTime(long lastElectTime) {
        this.lastElectTime = lastElectTime;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public void setLastLogIndex(long lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
    }

    public int getLastLogTerm() {
        return lastLogTerm;
    }

    public void setLastLogTerm(int lastLogTerm) {
        this.lastLogTerm = lastLogTerm;
    }

    public List<RaftMember> getMembers() {
        return members;
    }

    public void setMembers(List<RaftMember> members) {
        this.members = members;
    }

    public List<RaftMember> getObservers() {
        return observers;
    }

    public void setObservers(List<RaftMember> observers) {
        this.observers = observers;
    }

    public List<RaftMember> getPreparedMembers() {
        return preparedMembers;
    }

    public void setPreparedMembers(List<RaftMember> preparedMembers) {
        this.preparedMembers = preparedMembers;
    }

    public Set<Integer> getNodeIdOfMembers() {
        return nodeIdOfMembers;
    }

    public void setNodeIdOfMembers(Set<Integer> nodeIdOfMembers) {
        this.nodeIdOfMembers = nodeIdOfMembers;
    }

    public Set<Integer> getNodeIdOfObservers() {
        return nodeIdOfObservers;
    }

    public void setNodeIdOfObservers(Set<Integer> nodeIdOfObservers) {
        this.nodeIdOfObservers = nodeIdOfObservers;
    }

    public Set<Integer> getNodeIdOfPreparedMembers() {
        return nodeIdOfPreparedMembers;
    }

    public void setNodeIdOfPreparedMembers(Set<Integer> nodeIdOfPreparedMembers) {
        this.nodeIdOfPreparedMembers = nodeIdOfPreparedMembers;
    }

    public List<RaftMember> getReplicateList() {
        return replicateList;
    }

    public void setReplicateList(List<RaftMember> replicateList) {
        this.replicateList = replicateList;
    }

    public TailCache getTailCache() {
        return tailCache;
    }

    public void setTailCache(TailCache tailCache) {
        this.tailCache = tailCache;
    }

    public long getFirstIndexOfCurrentTerm() {
        return firstIndexOfCurrentTerm;
    }

    public void setFirstIndexOfCurrentTerm(long firstIndexOfCurrentTerm) {
        this.firstIndexOfCurrentTerm = firstIndexOfCurrentTerm;
    }

    public Timestamp getTs() {
        return ts;
    }

    public ShareStatus getShareStatus() {
        return shareStatus;
    }

    public RaftMember getCurrentLeader() {
        return currentLeader;
    }

    public long getElectTimeoutNanos() {
        return electTimeoutNanos;
    }

    public CompletableFuture<Void> getFirstCommitOfApplied() {
        return firstCommitOfApplied;
    }

    public boolean isInstallSnapshot() {
        return installSnapshot;
    }

    public void setInstallSnapshot(boolean installSnapshot) {
        this.installSnapshot = installSnapshot;
    }

    public long[] getLeaseComputeArray() {
        return leaseComputeArray;
    }

    public void setLeaseComputeArray(long[] leaseComputeArray) {
        this.leaseComputeArray = leaseComputeArray;
    }

    public long getLeaseStartNanos() {
        return leaseStartNanos;
    }

    public RaftMember getSelf() {
        return self;
    }

    public void setSelf(RaftMember self) {
        this.self = self;
    }

    public void setGroupId(int groupId) {
        this.groupId = groupId;
    }

    public Set<Integer> getNodeIdOfPreparedObservers() {
        return nodeIdOfPreparedObservers;
    }

    public void setNodeIdOfPreparedObservers(Set<Integer> nodeIdOfPreparedObservers) {
        this.nodeIdOfPreparedObservers = nodeIdOfPreparedObservers;
    }

    public List<RaftMember> getPreparedObservers() {
        return preparedObservers;
    }

    public void setPreparedObservers(List<RaftMember> preparedObservers) {
        this.preparedObservers = preparedObservers;
    }

    public long getLastConfigChangeIndex() {
        return lastConfigChangeIndex;
    }

    public void setLastConfigChangeIndex(long lastConfigChangeIndex) {
        this.lastConfigChangeIndex = lastConfigChangeIndex;
    }

    public long getLastForceLogIndex() {
        return lastForceLogIndex;
    }

    public void setLastForceLogIndex(long lastForceLogIndex) {
        this.lastForceLogIndex = lastForceLogIndex;
    }

    public long getLeaderCommit() {
        return leaderCommit;
    }

    public void setLeaderCommit(long leaderCommit) {
        this.leaderCommit = leaderCommit;
    }

    public int getStateMachineEpoch() {
        return stateMachineEpoch;
    }

    public void setStateMachineEpoch(int stateMachineEpoch) {
        this.stateMachineEpoch = stateMachineEpoch;
    }

    public FiberCondition getDataArrivedCondition() {
        return dataArrivedCondition;
    }

    public void setDataArrivedCondition(FiberCondition dataArrivedCondition) {
        this.dataArrivedCondition = dataArrivedCondition;
    }

    public FiberGroup getFiberGroup() {
        return fiberGroup;
    }

    public void setFiberGroup(FiberGroup fiberGroup) {
        this.fiberGroup = fiberGroup;
    }

    public FiberCondition getLogWriteFinishCondition() {
        return logWriteFinishCondition;
    }

    public void setLogWriteFinishCondition(FiberCondition logWriteFinishCondition) {
        this.logWriteFinishCondition = logWriteFinishCondition;
    }

    public FiberCondition getLogForceFinishCondition() {
        return logForceFinishCondition;
    }

    public void setLogForceFinishCondition(FiberCondition logForceFinishCondition) {
        this.logForceFinishCondition = logForceFinishCondition;
    }

    public long getLastWriteLogIndex() {
        return lastWriteLogIndex;
    }

    public void setLastWriteLogIndex(long lastWriteLogIndex) {
        this.lastWriteLogIndex = lastWriteLogIndex;
    }

    public FiberCondition getTransferLeaderCondition() {
        return transferLeaderCondition;
    }

    public void setTransferLeaderCondition(FiberCondition transferLeaderCondition) {
        this.transferLeaderCondition = transferLeaderCondition;
    }

    public CompletableFuture<Void> getInitFuture() {
        return initFuture;
    }

    public CompletableFuture<Void> getStartFuture() {
        return startFuture;
    }
}
