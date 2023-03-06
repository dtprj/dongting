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

import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftLog;
import com.github.dtprj.dongting.raft.server.StateMachine;

/**
 * @author huangli
 */
class CommitManager {

    private final RaftStatus raftStatus;
    private final RaftLog raftLog;
    private final StateMachine stateMachine;
    private final ApplyManager applyManager;
    private final Timestamp ts;

    CommitManager(RaftStatus raftStatus, RaftLog raftLog, StateMachine stateMachine, ApplyManager applyManager) {
        this.raftStatus = raftStatus;
        this.raftLog = raftLog;
        this.stateMachine = stateMachine;
        this.applyManager = applyManager;
        this.ts = raftStatus.getTs();
    }

    void tryCommit(long recentMatchIndex) {
        RaftStatus raftStatus = this.raftStatus;

        boolean needCommit = RaftUtil.needCommit(raftStatus.getCommitIndex(), recentMatchIndex,
                raftStatus.getAllMembers(), raftStatus.getRwQuorum());
        if (!needCommit) {
            return;
        }
        // leader can only commit log in current term, see raft paper 5.4.2
        boolean needNotify = false;
        if (raftStatus.getFirstCommitIndexOfCurrentTerm() <= 0) {
            int t = RaftUtil.doWithSyncRetry(() -> raftLog.getTermOf(recentMatchIndex),
                    raftStatus, 1000, "RaftLog.getTermOf fail");
            if (t != raftStatus.getCurrentTerm()) {
                return;
            } else {
                raftStatus.setFirstCommitIndexOfCurrentTerm(recentMatchIndex);
                needNotify = true;
            }
        }
        raftStatus.setCommitIndex(recentMatchIndex);

        for (long i = raftStatus.getLastApplied() + 1; i <= recentMatchIndex; i++) {
            RaftTask rt = raftStatus.getPendingRequests().get(i);
            if (rt == null) {
                LogItem item = RaftUtil.load(raftLog, raftStatus, i, 1, 0)[0];

                RaftInput input;
                if (item.getType() != LogItem.TYPE_HEARTBEAT) {
                    Object o = stateMachine.decode(item.getBuffer());
                    input = new RaftInput(item.getBuffer(), o, null, false);
                } else {
                    input = new RaftInput(item.getBuffer(), null, null, false);
                }
                rt = new RaftTask(ts, item.getType(), input, null);
            }
            applyManager.execChain(i, rt);
        }

        raftStatus.setLastApplied(recentMatchIndex);
        if (needNotify) {
            raftStatus.getFirstCommitOfApplied().complete(null);
            raftStatus.setFirstCommitOfApplied(null);
        }
    }
}
