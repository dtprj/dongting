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

import com.github.dtprj.dongting.common.IndexedQueue;
import com.github.dtprj.dongting.raft.store.RaftLog;

import java.util.List;

/**
 * @author huangli
 */
public class CommitManager implements RaftLog.AppendCallback {

    private final RaftStatusImpl raftStatus;
    private final ApplyManager applyManager;
    private final IndexedQueue<AppendRespWriter> respQueue = new IndexedQueue<>(128);

    public CommitManager(RaftStatusImpl raftStatus, ApplyManager applyManager) {
        this.raftStatus = raftStatus;
        this.applyManager = applyManager;
    }

    public void tryCommit(long recentMatchIndex) {
        RaftStatusImpl raftStatus = this.raftStatus;

        if (!needCommit(recentMatchIndex, raftStatus)) {
            return;
        }
        // leader can only commit log in current term, see raft paper 5.4.2
        if (recentMatchIndex < raftStatus.getFirstIndexOfCurrentTerm()) {
            return;
        }
        raftStatus.setCommitIndex(recentMatchIndex);
        applyManager.apply(raftStatus);
    }

    private static boolean needCommit(long recentMatchIndex, RaftStatusImpl raftStatus) {
        boolean needCommit = needCommit(raftStatus.getCommitIndex(), recentMatchIndex,
                raftStatus.getMembers(), raftStatus.getRwQuorum());
        if (needCommit && !raftStatus.getPreparedMembers().isEmpty()) {
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
            tryCommit(lastPersistIndex);
        } else {
            while (respQueue.size() > 0) {
                AppendRespWriter writer = respQueue.get(0);
                if (writer.writeResp(lastPersistIndex)) {
                    respQueue.removeFirst();
                } else {
                    break;
                }
            }
            if (raftStatus.getLeaderCommit() > raftStatus.getCommitIndex()) {
                long newCommitIndex = Math.min(lastPersistIndex, raftStatus.getLeaderCommit());
                if (newCommitIndex > raftStatus.getCommitIndex()) {
                    raftStatus.setCommitIndex(newCommitIndex);
                    applyManager.apply(raftStatus);
                }
            }
        }
    }

    public void registerRespWriter(AppendRespWriter writer) {
        respQueue.addLast(writer);
    }

    public interface AppendRespWriter {
        boolean writeResp(long index);
    }

}
