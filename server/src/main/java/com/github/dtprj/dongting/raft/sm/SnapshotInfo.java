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
package com.github.dtprj.dongting.raft.sm;

import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.RaftStatus;

import java.util.Set;

/**
 * @author huangli
 */
public class SnapshotInfo {
    public final long lastIncludedIndex;
    public final int lastIncludedTerm;
    public final Set<Integer> members;
    public final Set<Integer> observers;
    public final Set<Integer> preparedMembers;
    public final Set<Integer> preparedObservers;
    public final long lastConfigChangeIndex;

    public SnapshotInfo(RaftStatus rs) {
        RaftStatusImpl raftStatus = (RaftStatusImpl) rs;
        this.lastIncludedIndex = raftStatus.getLastApplied();
        this.lastIncludedTerm = raftStatus.lastAppliedTerm;
        this.members = raftStatus.nodeIdOfMembers;
        this.observers = raftStatus.nodeIdOfObservers;
        this.preparedMembers = raftStatus.nodeIdOfPreparedMembers;
        this.preparedObservers = raftStatus.nodeIdOfPreparedObservers;
        this.lastConfigChangeIndex = raftStatus.lastConfigChangeIndex;
    }

    public SnapshotInfo(long lastIncludedIndex, int lastIncludedTerm, Set<Integer> members, Set<Integer> observers,
                        Set<Integer> preparedMembers, Set<Integer> preparedObservers, long lastConfigChangeIndex) {
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedTerm = lastIncludedTerm;
        this.members = members;
        this.observers = observers;
        this.preparedMembers = preparedMembers;
        this.preparedObservers = preparedObservers;
        this.lastConfigChangeIndex = lastConfigChangeIndex;
    }

}
