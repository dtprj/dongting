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

import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.raft.store.RaftLog;

import java.time.Duration;

/**
 * @author huangli
 */
public class RaftMember {
    private final RaftNodeEx node;
    private boolean ready;
    private boolean pinging;

    private int nodeEpoch;

    // may check whether epoch change in io thread, so mark it volatile
    private volatile int replicateEpoch;

    private long lastConfirmReqNanos;
    private long lastFailNanos = System.nanoTime() - Duration.ofSeconds(30).toNanos();

    private boolean installSnapshot;
    private SnapshotInfo snapshotInfo;

    // in raft paper: volatile state on leaders
    private long nextIndex;
    private long matchIndex;

    private FiberFuture<?> replicateFuture;
    private RaftLog.LogIterator replicateIterator;

    private FiberCondition replicateCondition;

    public RaftMember(RaftNodeEx node) {
        this.node = node;
    }

    public void incrReplicateEpoch(int reqEpoch) {
        if (reqEpoch == replicateEpoch) {
            //noinspection NonAtomicOperationOnVolatileField
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

    public int getNodeEpoch() {
        return nodeEpoch;
    }

    public void setNodeEpoch(int nodeEpoch) {
        this.nodeEpoch = nodeEpoch;
    }

    public FiberFuture<?> getReplicateFuture() {
        return replicateFuture;
    }

    public void setReplicateFuture(FiberFuture<?> replicateFuture) {
        this.replicateFuture = replicateFuture;
    }

    public int getReplicateEpoch() {
        return replicateEpoch;
    }

    public RaftLog.LogIterator getReplicateIterator() {
        return replicateIterator;
    }

    public void setReplicateIterator(RaftLog.LogIterator replicateIterator) {
        this.replicateIterator = replicateIterator;
    }

    public long getLastFailNanos() {
        return lastFailNanos;
    }

    public void setLastFailNanos(long lastFailNanos) {
        this.lastFailNanos = lastFailNanos;
    }

    public FiberCondition getReplicateCondition() {
        return replicateCondition;
    }

    public void setReplicateCondition(FiberCondition replicateCondition) {
        this.replicateCondition = replicateCondition;
    }
}
