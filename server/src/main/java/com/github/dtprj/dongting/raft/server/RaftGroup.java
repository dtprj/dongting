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
import com.github.dtprj.dongting.raft.sm.StateMachine;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
public abstract class RaftGroup {

    public abstract int getGroupId();

    public abstract StateMachine getStateMachine();

    public abstract void submitLinearTask(RaftInput input, RaftCallback callback);

    /**
     * Get raft lease read index, use this index to read data from the state machine.
     * Generally, the future returned by this method should complete immediately,
     * however, it may be blocked in some conditions.
     *
     * <p>NOTE: Lease read is also linearizable.
     *
     * <li>If current node is not leader, or lease timeout(indicates something wrong),
     * the future will complete with a NotLeaderException. </li>
     * <li>If can't get the index before deadline, the future will complete with a RaftExecTimeoutException. </li>
     */
    public abstract CompletableFuture<Long> getLeaseReadIndex(DtTime deadline);


    /**
     * ADMIN API.
     * try to delete logs before the index(exclude).
     * This method should be called on each member, respectively.
     * @param index the index of the last log to be deleted
     * @param delayMillis delay millis to delete the logs, to wait read complete
     */
    @SuppressWarnings("unused")
    public abstract void markTruncateByIndex(long index, long delayMillis);

    /**
     * ADMIN API.
     * try to delete logs before the timestamp(may include).
     * This method should be called on each member, respectively.
     * @param timestampMillis the timestamp of the log
     * @param delayMillis delay millis to delete the logs, to wait read complete
     */
    @SuppressWarnings("unused")
    public abstract void markTruncateByTimestamp(long timestampMillis, long delayMillis);

    /**
     * ADMIN API.
     * This method should be called on each member, respectively.
     * @return Future indicating the latest log index of the snapshot. If there is a saving action running, return -1.
     */
    public abstract CompletableFuture<Long> fireSaveSnapshot();

    /**
     * ADMIN API. This method should be called on the leader; otherwise, it will throw a NotLeaderException.
     */
    @SuppressWarnings("unused")
    public abstract CompletableFuture<Void> transferLeadership(int nodeId, long timeoutMillis);

    /**
     * ADMIN API. This method is idempotent. This method should be called on the leader; otherwise, it will throw a NotLeaderException.
     */
    @SuppressWarnings("unused")
    public abstract CompletableFuture<Long> leaderPrepareJointConsensus(Set<Integer> members, Set<Integer> observers);

    /**
     * ADMIN API. This method is idempotent. This method should be called on the leader; otherwise, it will throw a NotLeaderException.
     */
    @SuppressWarnings("unused")
    public abstract CompletableFuture<Void> leaderAbortJointConsensus();

    /**
     * ADMIN API. This method is idempotent. This method should be called on the leader; otherwise, it will throw a NotLeaderException.
     */
    @SuppressWarnings("unused")
    public abstract CompletableFuture<Void> leaderCommitJointConsensus(long prepareIndex);

    public abstract boolean isLeader();

}
