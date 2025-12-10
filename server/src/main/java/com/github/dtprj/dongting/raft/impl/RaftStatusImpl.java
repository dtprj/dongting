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

import com.github.dtprj.dongting.common.IndexedQueue;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.net.NioServer;
import com.github.dtprj.dongting.raft.RaftNode;
import com.github.dtprj.dongting.raft.server.RaftStatus;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * @author huangli
 */
public final class RaftStatusImpl extends RaftStatus {

    public NioServer serviceNioServer;
    public final Timestamp ts;
    public int electQuorum;
    public int rwQuorum;

    public BiConsumer<RaftRole, RaftRole> roleChangeListener;

    public RaftMember self;
    public List<RaftMember> members;
    public List<RaftMember> observers;
    public List<RaftMember> preparedMembers;
    public List<RaftMember> preparedObservers;

    public Set<Integer> nodeIdOfMembers;
    public Set<Integer> nodeIdOfObservers;
    public Set<Integer> nodeIdOfPreparedMembers;
    public Set<Integer> nodeIdOfPreparedObservers;
    public long lastConfigChangeIndex;

    public List<RaftMember> replicateList;

    public TailCache tailCache;
    public FiberCondition needRepCondition;

    public boolean installSnapshot;

    public int currentTerm; // raft paper persistent state of all servers
    public int votedFor; // raft paper persistent state of all servers

    public long commitIndex; // raft paper volatile state on all servers
    private long lastApplied; // raft paper volatile state on all servers

    private RaftRole role; // shared
    private RaftMember currentLeader; // shared

    // for leader, groupReadyIndex is the firstIndex of currentTerm.
    // for follower, groupReadyIndex is the first log item index of a valid AppendEntries request.
    // reset to Long.MAX_VALUE in RaftUtil.resetStatus(), called when change to follower/observer/candidate or increase term.
    public long groupReadyIndex = Long.MAX_VALUE;
    private boolean groupReady; // shared

    private boolean shareStatusUpdated;
    private long electTimeoutNanos; // shared
    private long leaseDelta;

    private long leaseStartNanos; // shared
    long[] leaseComputeArray = new long[0];

    long lastElectTime;

    public int lastAppliedTerm;

    // lastApplied <= lastApplying <= commitIndex (<= lastForceLogIndex <=) lastWriteLogIndex <= lastLogIndex
    // IdxFiles.nextPersistIndex may less than lastApplied, since it's update asynchronously
    public long lastLogIndex;
    public int lastLogTerm;
    public long lastForceLogIndex;
    public long lastWriteLogIndex;
    public long lastApplying;

    public IndexedQueue<long[]> commitHistory = new IndexedQueue<>(16);
    public long applyLagNanos;
    public long lastApplyNanos;

    // update after install snapshot by leader, so current node has no raft logs before the index
    public long firstValidIndex = 1;

    public long lastSavedSnapshotIndex = 0;

    public boolean truncating;

    public long leaderCommit;

    public FiberGroup fiberGroup;

    public FiberCondition logForceFinishCondition;
    public FiberCondition logWriteFinishCondition;
    public FiberCondition transferLeaderCondition;

    public final CompletableFuture<Void> initFuture = new CompletableFuture<>();
    boolean initFinished;
    private boolean initFailed;

    public RaftStatusImpl(int groupId, Timestamp ts) {
        super(groupId);
        this.ts = ts;
        lastElectTime = ts.nanoTime - Duration.ofDays(1).toNanos();
    }

    @Override
    public void copy(boolean volatileMode) {
        RaftShareStatus ss = new RaftShareStatus();
        ss.shouldStop = shouldStop;
        ss.role = role;
        // ss.lastApplied = lastApplied;
        if (role == RaftRole.leader) {
            ss.leaseEndNanos = leaseStartNanos + electTimeoutNanos - leaseDelta;
        }
        ss.currentLeader = currentLeader;
        ss.groupReady = groupReady;

        ss.initFinished = initFinished;
        ss.initFailed = initFailed;

        this.shareStatusUpdated = false;

        if (volatileMode) {
            shareStatus = ss;
        } else {
            SHARE_STATUS.setRelease(this, ss);
        }
    }

    public void copyShareStatus() {
        if (shareStatusUpdated) {
            copy(true);
        }
    }

    public void markInit(boolean initFailed) {
        this.initFinished = true;
        this.initFailed = initFailed;
        this.shareStatusUpdated = true;
    }

    public RaftNode getCurrentLeaderNode() {
        return currentLeader == null ? null : currentLeader.node;
    }

    public void setLastApplied(long lastApplied) {
        if (lastApplied != this.lastApplied) {
            this.lastApplied = lastApplied;
            this.lastApplyNanos = ts.nanoTime;
            // this.shareStatusUpdated = true;
        }
    }

    public void setRole(RaftRole role) {
        RaftRole oldRole = this.role;
        if (role != oldRole) {
            this.role = role;
            this.shareStatusUpdated = true;
            if (roleChangeListener != null) {
                roleChangeListener.accept(oldRole, role);
            }
        }
    }

    public void setLeaseStartNanos(long leaseStartNanos) {
        if (leaseStartNanos != this.leaseStartNanos) {
            this.leaseStartNanos = leaseStartNanos;
            this.shareStatusUpdated = true;
        }
    }

    public void setCurrentLeader(RaftMember currentLeader) {
        if (currentLeader != this.currentLeader) {
            this.currentLeader = currentLeader;
            this.shareStatusUpdated = true;
        }
    }

    public void setElectTimeoutNanos(long electTimeoutNanos) {
        if (electTimeoutNanos != this.electTimeoutNanos) {
            this.electTimeoutNanos = electTimeoutNanos;
            this.leaseDelta = electTimeoutNanos / 10;
            // keep electTimeoutDelta from 3 to 500ms
            this.leaseDelta = Math.min(Duration.ofMillis(500).toNanos(), leaseDelta);
            this.leaseDelta = Math.max(Duration.ofMillis(3).toNanos(), leaseDelta);
            this.shareStatusUpdated = true;
        }
    }

    public void setGroupReady(boolean groupReady) {
        if (groupReady != this.groupReady) {
            this.groupReady = groupReady;
            this.shareStatusUpdated = true;
        }
    }

    public RaftShareStatus getShareStatus() {
        return (RaftShareStatus) shareStatus;
    }

    //------------------------- simple getters and setters--------------------------------

    public RaftRole getRole() {
        return role;
    }

    public RaftMember getCurrentLeader() {
        return currentLeader;
    }

    public long getElectTimeoutNanos() {
        return electTimeoutNanos;
    }

    public boolean isGroupReady() {
        return groupReady;
    }

    public long getLeaseStartNanos() {
        return leaseStartNanos;
    }

    public long getLastApplied() {
        return lastApplied;
    }
}
