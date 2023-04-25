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
package com.github.dtprj.dongting.raft.server;

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.raft.client.RaftException;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public abstract class RaftGroup {
    private final Supplier<Boolean> isServerRunning;

    public RaftGroup(Supplier<Boolean> isServerRunning) {
        this.isServerRunning = isServerRunning;
    }

    protected void checkStatus() {
        if (!isServerRunning.get()) {
            throw new RaftException("raft server is not running");
        }
    }

    public abstract int getGroupId();

    @SuppressWarnings("unused")
    public abstract CompletableFuture<RaftOutput> submitLinearTask(RaftInput input) throws RaftException;

    @SuppressWarnings("unused")
    public abstract long getLogIndexForRead(DtTime deadline)
            throws RaftException, InterruptedException, TimeoutException;

    /**
     * ADMIN API.
     */
    @SuppressWarnings("unused")
    public abstract CompletableFuture<Void> transferLeadership(int nodeId, long timeoutMillis);

    /**
     * ADMIN API. This method is idempotent.
     */
    @SuppressWarnings("unused")
    public abstract CompletableFuture<Void> leaderPrepareJointConsensus(Set<Integer> members, Set<Integer> observers);

    /**
     * ADMIN API. This method is idempotent.
     */
    @SuppressWarnings("unused")
    public abstract CompletableFuture<Void> leaderAbortJointConsensus();

    /**
     * ADMIN API. This method is idempotent.
     */
    @SuppressWarnings("unused")
    public abstract CompletableFuture<Void> leaderCommitJointConsensus();
}
