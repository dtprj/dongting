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

import com.github.dtprj.dongting.raft.client.RaftException;
import com.github.dtprj.dongting.raft.server.RaftLog;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

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

    public void tryCommit(long recentMatchIndex) {
        RaftStatus raftStatus = this.raftStatus;

        if (!needCommit(recentMatchIndex, raftStatus)) {
            return;
        }
        // leader can only commit log in current term, see raft paper 5.4.2
        if (raftStatus.getFirstCommitIndexOfCurrentTerm() <= 0) {
            Supplier<Integer> callback = () -> {
                try {
                    return raftLog.getTermOf(recentMatchIndex);
                } catch (IOException e) {
                    throw new RaftException(e);
                }
            };
            int t = RaftUtil.doWithSyncRetry(callback, raftStatus, 1000, "RaftLog.getTermOf fail");
            if (t != raftStatus.getCurrentTerm()) {
                return;
            } else {
                raftStatus.setFirstCommitIndexOfCurrentTerm(recentMatchIndex);
            }
        }
        raftStatus.setCommitIndex(recentMatchIndex);
        applyManager.apply(raftStatus);
    }

    private static boolean needCommit(long recentMatchIndex, RaftStatus raftStatus) {
        boolean needCommit = needCommit(raftStatus.getCommitIndex(), recentMatchIndex,
                raftStatus.getMembers(), raftStatus.getRwQuorum());
        if (needCommit && raftStatus.getPreparedMembers().size() > 0) {
            needCommit = needCommit(raftStatus.getCommitIndex(), recentMatchIndex,
                    raftStatus.getPreparedMembers(), raftStatus.getRwQuorum());
        }
        return needCommit;
    }


    @SuppressWarnings("ForLoopReplaceableByForEach")
    public static boolean needCommit(long currentCommitIndex, long recentMatchIndex,
                                     List<RaftMember> servers, int rwQuorum) {
        if (recentMatchIndex < currentCommitIndex) {
            return false;
        }
        int count = 0;
        for (int i = 0; i < servers.size(); i++) {
            RaftMember member = servers.get(i);
            if (member.getNode().isSelf()) {
                if (recentMatchIndex > member.getMatchIndex()) {
                    return false;
                }
            }
            if (member.getMatchIndex() >= recentMatchIndex) {
                count++;
            }
        }
        return count >= rwQuorum;
    }
}
