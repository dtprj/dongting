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

import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberGroup;

/**
 * @author huangli
 */
public class RaftMember {
    private final RaftNodeEx node;
    private final FiberCondition repCondition;
    private boolean ready;
    private boolean pinging;

    private long lastConfirmReqNanos;

    // in raft paper: volatile state on leaders
    private long nextIndex;
    private long matchIndex;

    private int replicateEpoch;
    private int nodeEpoch;
    private Fiber replicateFiber;
    private boolean installSnapshot;

    public RaftMember(RaftNodeEx node, FiberGroup fg) {
        this.node = node;
        this.repCondition = fg.newCondition("rep-" + node.getNodeId());
    }

    public void incrementReplicateEpoch(int oldEpoch) {
        if (replicateEpoch == oldEpoch) {
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

    public int getReplicateEpoch() {
        return replicateEpoch;
    }

    public FiberCondition getRepCondition() {
        return repCondition;
    }

    public Fiber getReplicateFiber() {
        return replicateFiber;
    }

    public void setReplicateFiber(Fiber replicateFiber) {
        this.replicateFiber = replicateFiber;
    }

    public boolean isInstallSnapshot() {
        return installSnapshot;
    }

    public void setInstallSnapshot(boolean installSnapshot) {
        this.installSnapshot = installSnapshot;
    }

    public int getNodeEpoch() {
        return nodeEpoch;
    }

    public void setNodeEpoch(int nodeEpoch) {
        this.nodeEpoch = nodeEpoch;
    }

}
