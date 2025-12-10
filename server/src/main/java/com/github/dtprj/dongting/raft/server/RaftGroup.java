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
import com.github.dtprj.dongting.common.FutureCallback;
import com.github.dtprj.dongting.common.Timestamp;
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
     * Generally, the callback should be called immediately in current thread,
     * however, if group not ready it may be called in raft thread after some time.
     *
     * <p>NOTE1: Lease read is also linearizable.</p>
     *
     * <p>NOTE2: The DtKV does not use the read index (always read the latest snapshot),
     * so current implementation always returns 0 as the read index to improve performance.</p>
     *
     *
     * <li>If current node is not leader, or lease timeout(indicates something wrong),
     * callback will fail with a NotLeaderException. </li>
     * <li>If it can't get the index before deadline, callback will fail with a RaftExecTimeoutException. </li>
     */
    public abstract void leaseRead(Timestamp ts, DtTime deadline, FutureCallback<Long> callback);


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
    public abstract CompletableFuture<Void> transferLeadership(int nodeId, long timeoutMillis);

    /**
     * ADMIN API. This method is idempotent. This method should be called on the leader; otherwise, it will throw a NotLeaderException.
     */
    public abstract CompletableFuture<Long> leaderPrepareJointConsensus(Set<Integer> members, Set<Integer> observers,
            Set<Integer> prepareMembers, Set<Integer> prepareObservers);

    /**
     * ADMIN API. This method is idempotent. This method should be called on the leader; otherwise, it will throw a NotLeaderException.
     */
    public abstract CompletableFuture<Long> leaderAbortJointConsensus();

    /**
     * ADMIN API. This method is idempotent. This method should be called on the leader; otherwise, it will throw a NotLeaderException.
     */
    public abstract CompletableFuture<Long> leaderCommitJointConsensus(long prepareIndex);

    public abstract boolean isLeader();

}
