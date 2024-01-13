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

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.IntObjMap;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.fiber.FiberChannel;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.server.RaftGroup;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftOutput;
import com.github.dtprj.dongting.raft.sm.StateMachine;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
public class RaftGroupImpl extends RaftGroup {
    private static final DtLog log = DtLogs.getLogger(RaftGroupImpl.class);
    private final Timestamp readTimestamp = new Timestamp();
    private final GroupComponents gc;
    private final IntObjMap<FiberChannel<Object>> processorChannels = new IntObjMap<>();

    public RaftGroupImpl(GroupComponents gc) {
        this.gc = gc;
    }

    public IntObjMap<FiberChannel<Object>> getProcessorChannels() {
        return processorChannels;
    }

    @Override
    public int getGroupId() {
        return 0;
    }

    @Override
    public StateMachine getStateMachine() {
        return null;
    }

    @Override
    public CompletableFuture<RaftOutput> submitLinearTask(RaftInput input) {
        return null;
    }

    @Override
    public CompletableFuture<Long> getLogIndexForRead(DtTime deadline) {
        return null;
    }

    @Override
    public void markTruncateByIndex(long index, long delayMillis) {

    }

    @Override
    public void markTruncateByTimestamp(long timestampMillis, long delayMillis) {

    }

    @Override
    public CompletableFuture<Long> saveSnapshot() {
        return null;
    }

    @Override
    public CompletableFuture<Void> transferLeadership(int nodeId, long timeoutMillis) {
        return null;
    }

    @Override
    public CompletableFuture<Long> leaderPrepareJointConsensus(Set<Integer> members, Set<Integer> observers) {
        return null;
    }

    @Override
    public CompletableFuture<Void> leaderAbortJointConsensus() {
        return null;
    }

    @Override
    public CompletableFuture<Void> leaderCommitJointConsensus(long prepareIndex) {
        return null;
    }

    public GroupComponents getGroupComponents() {
        return gc;
    }
}
