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

import com.github.dtprj.dongting.raft.server.RaftLog;

/**
 * @author huangli
 */
public class CommitManager {

    private final RaftStatus raftStatus;
    private final RaftLog raftLog;
    private final ApplyManager applyManager;

    public CommitManager(RaftStatus raftStatus, RaftLog raftLog, ApplyManager applyManager) {
        this.raftStatus = raftStatus;
        this.raftLog = raftLog;
        this.applyManager = applyManager;
    }

    void tryCommit(long recentMatchIndex) {
        RaftStatus raftStatus = this.raftStatus;

        if (!needCommit(recentMatchIndex, raftStatus)) {
            return;
        }
        // leader can only commit log in current term, see raft paper 5.4.2
        if (raftStatus.getFirstCommitIndexOfCurrentTerm() <= 0) {
            int t = RaftUtil.doWithSyncRetry(() -> raftLog.getTermOf(recentMatchIndex),
                    raftStatus, 1000, "RaftLog.getTermOf fail");
            if (t != raftStatus.getCurrentTerm()) {
                return;
            } else {
                raftStatus.setFirstCommitIndexOfCurrentTerm(recentMatchIndex);
            }
        }
        raftStatus.setCommitIndex(recentMatchIndex);
        StatusUtil.tryPersist(raftStatus);
        applyManager.apply(raftStatus);
        if (raftStatus.getFirstCommitOfApplied() != null) {
            raftStatus.getFirstCommitOfApplied().complete(null);
            raftStatus.setFirstCommitOfApplied(null);
        }
    }

    private static boolean needCommit(long recentMatchIndex, RaftStatus raftStatus) {
        boolean needCommit = RaftUtil.needCommit(raftStatus.getCommitIndex(), recentMatchIndex,
                raftStatus.getMembers(), raftStatus.getRwQuorum());
        if (needCommit && raftStatus.getPreparedMembers().size() > 0) {
            needCommit = RaftUtil.needCommit(raftStatus.getCommitIndex(), recentMatchIndex,
                    raftStatus.getPreparedMembers(), raftStatus.getRwQuorum());
        }
        return needCommit;
    }
}
