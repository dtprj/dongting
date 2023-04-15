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

import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
public class RaftMember {
    private final RaftNodeEx node;
    private boolean ready;
    private boolean pinging;
    private boolean multiAppend;

    private int nodeEpoch;
    private int replicateEpoch;

    private PendingStat pendingStat;

    private long lastConfirmReqNanos;

    private boolean installSnapshot;
    private SnapshotInfo snapshotInfo;

    // volatile state on leaders
    private long nextIndex;
    private long matchIndex;

    private CompletableFuture<?> replicateFuture;

    public RaftMember(RaftNodeEx node) {
        this.node = node;
    }

    public void incrReplicateEpoch(int reqEpoch) {
        if (reqEpoch == replicateEpoch) {
            replicateEpoch++;
        }
    }

    public void setLastConfirmReqNanos(long lastConfirmReqNanos) {
        this.lastConfirmReqNanos = lastConfirmReqNanos;
    }

    public RaftNodeEx getNode() {
        return node;
    }

    public boolean isReady() {
        return ready;
    }

    public void setReady(boolean ready) {
        this.ready = ready;
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

    public int getNodeEpoch() {
        return nodeEpoch;
    }

    public void setNodeEpoch(int nodeEpoch) {
        this.nodeEpoch = nodeEpoch;
    }

    public CompletableFuture<?> getReplicateFuture() {
        return replicateFuture;
    }

    public void setReplicateFuture(CompletableFuture<?> replicateFuture) {
        this.replicateFuture = replicateFuture;
    }

    public int getReplicateEpoch() {
        return replicateEpoch;
    }

}
