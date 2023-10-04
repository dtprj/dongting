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

import com.github.dtprj.dongting.raft.store.RaftLog;

/**
 * @author huangli
 */
public class AppendCallback implements RaftLog.AppendCallback {

    private final RaftStatusImpl raftStatus;
    private final CommitManager commitManager;

    public AppendCallback(RaftStatusImpl raftStatus, CommitManager commitManager) {
        this.raftStatus = raftStatus;
        this.commitManager = commitManager;
    }

    @Override
    public void finish(int lastPersistTerm, long lastPersistIndex) {
        RaftStatusImpl raftStatus = this.raftStatus;
        if (lastPersistIndex > raftStatus.getLastLogIndex()) {
            RaftUtil.fail("lastPersistIndex > lastLogIndex. lastPersistIndex="
                    + lastPersistIndex + ", lastLogIndex=" + raftStatus.getLastLogIndex());
        }
        if (lastPersistTerm > raftStatus.getLastLogTerm()) {
            RaftUtil.fail("lastPersistTerm > lastLogTerm. lastPersistTerm="
                    + lastPersistTerm + ", lastLogTerm=" + raftStatus.getLastLogTerm());
        }
        raftStatus.setLastPersistLogIndex(lastPersistIndex);
        raftStatus.setLastPersistLogTerm(lastPersistTerm);
        if (raftStatus.getRole() == RaftRole.leader) {
            RaftMember self = raftStatus.getSelf();
            if (self != null) {
                self.setNextIndex(lastPersistIndex + 1);
                self.setMatchIndex(lastPersistIndex);
                self.setLastConfirmReqNanos(raftStatus.getTs().getNanoTime());
            }

            // for single node mode
            if (raftStatus.getRwQuorum() == 1) {
                RaftUtil.updateLease(raftStatus);
            }
            commitManager.tryCommit(lastPersistIndex);
        } else {
            if (raftStatus.getLeaderCommit() > raftStatus.getCommitIndex()) {
                long newCommitIndex = Math.min(lastPersistIndex, raftStatus.getLeaderCommit());
                if (newCommitIndex > raftStatus.getCommitIndex()) {
                    raftStatus.setCommitIndex(newCommitIndex);
                }
            }
        }

        if (lastPersistIndex == raftStatus.getLastLogIndex()) {
            raftStatus.getWriteCompleteCondition().signal();
        }
    }
}
