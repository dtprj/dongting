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

import com.github.dtprj.dongting.common.LongObjMap;
import com.github.dtprj.dongting.common.Timestamp;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
public class RaftStatus {

    // persistent state on all servers
    private int currentTerm;
    private int voteFor;

    // volatile state on all servers
    private long commitIndex;
    private long lastApplied; // shared

    private RaftRole role; // shared
    private RaftNode currentLeader; // shared
    private final Timestamp ts = new Timestamp();
    private final int electQuorum;
    private final int rwQuorum;
    private final List<RaftNode> servers = new ArrayList<>();

    private LongObjMap<RaftTask> pendingRequests = new LongObjMap<>();
    private long firstCommitIndexOfCurrentTerm;
    private CompletableFuture<Void> firstCommitOfApplied; // shared

    private boolean shareStatusUpdated;
    private volatile ShareStatus shareStatus;
    private long electTimeoutNanos; // shared

    private long leaseStartNanos; // shared
    private boolean hasLeaseStartNanos; // shared

    private long lastElectTime;
    private long heartbeatTime;

    private long lastLogIndex;
    private int lastLogTerm;

    public RaftStatus(int electQuorum, int rwQuorum) {
        this.electQuorum = electQuorum;
        this.rwQuorum = rwQuorum;
        lastElectTime = ts.getNanoTime();
        heartbeatTime = ts.getNanoTime();
    }

    public void copyShareStatus() {
        if (shareStatusUpdated) {
            ShareStatus ss = new ShareStatus();
            ss.role = role;
            ss.lastApplied = lastApplied;
            ss.hasLease = hasLeaseStartNanos;
            ss.leaseEndNanos = leaseStartNanos + electTimeoutNanos;
            ss.currentLeader = currentLeader;
            ss.firstCommitOfApplied = firstCommitOfApplied;

            this.shareStatusUpdated = false;
            this.shareStatus = ss;
        }
    }

    public void setLastApplied(long lastApplied) {
        this.lastApplied = lastApplied;
        this.shareStatusUpdated = true;
    }

    public void setRole(RaftRole role) {
        this.role = role;
        this.shareStatusUpdated = true;
    }

    public void setHasLeaseStartNanos(boolean hasLeaseStartNanos) {
        this.hasLeaseStartNanos = hasLeaseStartNanos;
        this.shareStatusUpdated = true;
    }

    public void setLeaseStartNanos(long leaseStartNanos) {
        this.leaseStartNanos = leaseStartNanos;
        this.shareStatusUpdated = true;
    }

    public void setCurrentLeader(RaftNode currentLeader) {
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

    public int getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(int currentTerm) {
        this.currentTerm = currentTerm;
    }

    public int getVoteFor() {
        return voteFor;
    }

    public void setVoteFor(int voteFor) {
        this.voteFor = voteFor;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public long getLastApplied() {
        return lastApplied;
    }

    public RaftRole getRole() {
        return role;
    }

    public int getElectQuorum() {
        return electQuorum;
    }

    public int getRwQuorum() {
        return rwQuorum;
    }

    public long getHeartbeatTime() {
        return heartbeatTime;
    }

    public void setHeartbeatTime(long heartbeatTime) {
        this.heartbeatTime = heartbeatTime;
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

    public List<RaftNode> getServers() {
        return servers;
    }

    public LongObjMap<RaftTask> getPendingRequests() {
        return pendingRequests;
    }

    public void setPendingRequests(LongObjMap<RaftTask> pendingRequests) {
        this.pendingRequests = pendingRequests;
    }

    public long getFirstCommitIndexOfCurrentTerm() {
        return firstCommitIndexOfCurrentTerm;
    }

    public void setFirstCommitIndexOfCurrentTerm(long firstCommitIndexOfCurrentTerm) {
        this.firstCommitIndexOfCurrentTerm = firstCommitIndexOfCurrentTerm;
    }

    public Timestamp getTs() {
        return ts;
    }

    public long getLeaseStartNanos() {
        return leaseStartNanos;
    }

    public boolean isHasLeaseStartNanos() {
        return hasLeaseStartNanos;
    }

    public ShareStatus getShareStatus() {
        return shareStatus;
    }

    public RaftNode getCurrentLeader() {
        return currentLeader;
    }

    public long getElectTimeoutNanos() {
        return electTimeoutNanos;
    }

    public CompletableFuture<Void> getFirstCommitOfApplied() {
        return firstCommitOfApplied;
    }
}
