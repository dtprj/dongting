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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * @author huangli
 */
public class RaftStatus {

    // persistent state on all servers
    private int currentTerm;
    private int voteFor;

    // volatile state on all servers
    private long commitIndex;
    private long lastApplied;

    // dongting fields
    private RaftRole role;

    private final int electQuorum;
    private final int rwQuorum;
    private final HashSet<Integer> currentVotes = new HashSet<>();

    private long lastLeaderActiveTime;

    private long lastElectTime;

    private long heartbeatTime;

    private long lastLogIndex;
    private int lastLogTerm;

    private List<RaftNode> servers = new ArrayList<>();

    private LongObjMap<RaftTask> pendingRequests = new LongObjMap<>();

    private long firstCommitIndexOfCurrentTerm;

    public RaftStatus(int electQuorum, int rwQuorum) {
        this.electQuorum = electQuorum;
        this.rwQuorum = rwQuorum;
        long t = System.nanoTime();
        lastLeaderActiveTime = t;
        lastElectTime = t;
        heartbeatTime = t;
    }

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

    public void setLastApplied(long lastApplied) {
        this.lastApplied = lastApplied;
    }

    public RaftRole getRole() {
        return role;
    }

    public void setRole(RaftRole role) {
        this.role = role;
    }

    public int getElectQuorum() {
        return electQuorum;
    }

    public HashSet<Integer> getCurrentVotes() {
        return currentVotes;
    }

    public int getRwQuorum() {
        return rwQuorum;
    }

    public long getLastLeaderActiveTime() {
        return lastLeaderActiveTime;
    }

    public void setLastLeaderActiveTime(long lastLeaderActiveTime) {
        this.lastLeaderActiveTime = lastLeaderActiveTime;
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

    public void setServers(List<RaftNode> servers) {
        this.servers = servers;
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
}
