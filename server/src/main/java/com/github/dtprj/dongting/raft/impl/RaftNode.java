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

import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.net.Peer;

import java.util.Set;

/**
 * @author huangli
 */
public class RaftNode {
    private int id;
    private final Peer peer;
    private Set<HostPort> servers;
    private boolean self;
    private boolean ready;
    private boolean pinging;
    private long epoch;
    private long lastEpoch;
    private int pendingRequests;//only include append requests

    // volatile state on leaders
    private long nextIndex;
    private long matchIndex;

    public RaftNode(Peer peer) {
        this.peer = peer;
    }

    public int incrAndGetPendingRequests() {
        return ++pendingRequests;
    }

    public int decrAndGetPendingRequests() {
        return --pendingRequests;
    }

    public boolean incrEpoch() {
        if (epoch == lastEpoch) {
            epoch++;
            return true;
        } else {
            return false;
        }
    }

    //-------------------------getter and setters-------------------------------

    public int getId() {
        return id;
    }

    public Peer getPeer() {
        return peer;
    }

    public Set<HostPort> getServers() {
        return servers;
    }

    public boolean isSelf() {
        return self;
    }

    public boolean isReady() {
        return ready;
    }

    public void setReady(boolean ready) {
        this.ready = ready;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setServers(Set<HostPort> servers) {
        this.servers = servers;
    }

    public void setSelf(boolean self) {
        this.self = self;
    }

    public long getNextIndex() {
        return nextIndex;
    }

    public void setNextIndex(long nextIndex) {
        this.nextIndex = nextIndex;
    }

    public long getMatchIndex() {
        return matchIndex;
    }

    public void setMatchIndex(long matchIndex) {
        this.matchIndex = matchIndex;
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public long getLastEpoch() {
        return lastEpoch;
    }

    public void setLastEpoch(long lastEpoch) {
        this.lastEpoch = lastEpoch;
    }

    public int getPendingRequests() {
        return pendingRequests;
    }

    public void setPendingRequests(int pendingRequests) {
        this.pendingRequests = pendingRequests;
    }

    public boolean isPinging() {
        return pinging;
    }

    public void setPinging(boolean pinging) {
        this.pinging = pinging;
    }
}
