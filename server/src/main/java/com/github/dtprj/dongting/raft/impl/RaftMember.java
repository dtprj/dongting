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

import java.util.Set;

/**
 * @author huangli
 */
public class RaftMember {
    private final RaftNodeEx node;

    private int id;
    private Set<HostPort> servers;
    private boolean self;
    private boolean ready;
    private boolean pinging;
    private boolean multiAppend;

    private int epoch;

    private PendingStat pendingStat;

    private long lastConfirmReqNanos;
    private boolean hasLastConfirmReqNanos;

    private boolean installSnapshot;
    private SnapshotInfo snapshotInfo;

    // volatile state on leaders
    private long nextIndex;
    private long matchIndex;

    public RaftMember(RaftNodeEx node) {
        this.node = node;
    }

    public void setLastConfirm(boolean hasLastConfirmReqNanos, long lastConfirmReqNanos) {
        this.hasLastConfirmReqNanos = hasLastConfirmReqNanos;
        this.lastConfirmReqNanos = lastConfirmReqNanos;
    }

    //-------------------------getter and setters-------------------------------

    public int getId() {
        return id;
    }

    public RaftNodeEx getNode() {
        return node;
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

    public boolean isMultiAppend() {
        return multiAppend;
    }

    public void setMultiAppend(boolean multiAppend) {
        this.multiAppend = multiAppend;
    }

    public boolean isPinging() {
        return pinging;
    }

    public void setPinging(boolean pinging) {
        this.pinging = pinging;
    }

    public long getLastConfirmReqNanos() {
        return lastConfirmReqNanos;
    }

    public boolean isHasLastConfirmReqNanos() {
        return hasLastConfirmReqNanos;
    }

    public boolean isInstallSnapshot() {
        return installSnapshot;
    }

    public void setInstallSnapshot(boolean installSnapshot) {
        this.installSnapshot = installSnapshot;
    }

    public SnapshotInfo getSnapshotInfo() {
        return snapshotInfo;
    }

    public void setSnapshotInfo(SnapshotInfo snapshotInfo) {
        this.snapshotInfo = snapshotInfo;
    }

    public PendingStat getPendingStat() {
        return pendingStat;
    }

    public void setPendingStat(PendingStat pendingStat) {
        this.pendingStat = pendingStat;
    }

    public int getEpoch() {
        return epoch;
    }

    public void setEpoch(int epoch) {
        this.epoch = epoch;
    }
}
