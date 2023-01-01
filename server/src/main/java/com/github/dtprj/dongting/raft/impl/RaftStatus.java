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

import java.util.HashSet;

/**
 * @author huangli
 */
public class RaftStatus {

    private RaftRole role;

    private int currentTerm;
    private int voteFor;

    private final int electQuorum;
    private final int rwQuorum;
    private final HashSet<Integer> currentVotes = new HashSet<>();

    private long lastLeaderActiveTime;

    private long lastElectTime;

    private long heartbeatTime;

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
}
