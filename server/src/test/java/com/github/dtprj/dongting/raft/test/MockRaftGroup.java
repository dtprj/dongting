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
package com.github.dtprj.dongting.raft.test;

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.raft.sm.StateMachine;
import com.github.dtprj.dongting.raft.server.RaftGroup;
import com.github.dtprj.dongting.raft.server.RaftInput;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class MockRaftGroup extends RaftGroup {

    private final int groupId;

    public MockRaftGroup(int groupId) {
        this.groupId = groupId;
    }

    @Override
    public int getGroupId() {
        return groupId;
    }

    @Override
    public StateMachine getStateMachine() {
        return null;
    }

    @Override
    public void submitLinearTask(RaftInput input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isLeaseReadValid(Timestamp ts, DtTime deadline) {
        return false;
    }

    @Override
    public CompletableFuture<Void> addGroupReadyListener(DtTime deadline) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void markTruncateByIndex(long index, long delayMillis) {
    }

    @Override
    public void markTruncateByTimestamp(long timestampMillis, long delayMillis) {
    }

    @Override
    public CompletableFuture<Long> fireSaveSnapshot() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> transferLeadership(int nodeId, long timeoutMillis) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Long> leaderPrepareJointConsensus(Set<Integer> members, Set<Integer> observers,
            Set<Integer> prepareMembers, Set<Integer> prepareObservers) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Long> leaderAbortJointConsensus() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Long> leaderCommitJointConsensus(long prepareIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isLeader() {
        return false;
    }
}
