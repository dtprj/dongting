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
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.raft.RaftException;

import java.util.List;

/**
 * @author huangli
 */
public class CommitManager {

    private final GroupComponents gc;
    private final RaftStatusImpl raftStatus;
    private ApplyManager applyManager;
    private final IndexedQueue<AppendRespWriter> respQueue = new IndexedQueue<>(128);
    private final boolean syncForce;

    public CommitManager(GroupComponents gc) {
        this.gc = gc;
        this.raftStatus = gc.getRaftStatus();
        this.syncForce = gc.getGroupConfig().isSyncForce();
    }

    public void postInit() {
        this.applyManager = gc.getApplyManager();
    }

    public void startCommitFiber() {
        Fiber fiber = new Fiber("commit" + raftStatus.getGroupId(), FiberGroup.currentGroup(),
                new CommitFiberFrame(), true);
        fiber.start();
    }

    private class CommitFiberFrame extends FiberFrame<Void> {

        @Override
        protected FrameCallResult handle(Throwable ex) {
            BugLog.getLog().error("commit fiber error", ex);
            if (!isGroupShouldStopPlain()) {
                startCommitFiber();
            }
            return Fiber.frameReturn();
        }

        @Override
        public FrameCallResult execute(Void input) {
            RaftStatusImpl raftStatus = CommitManager.this.raftStatus;
            long idx = syncForce ? raftStatus.getLastForceLogIndex() : raftStatus.getLastWriteLogIndex();
            if (idx > raftStatus.getCommitIndex()) {
                CommitManager.this.finish(idx);
            }
            FiberCondition c = syncForce ? raftStatus.getLogForceFinishCondition()
                    : raftStatus.getLogWriteFinishCondition();
            return c.await(1000, this);
        }
    }

    public void finish(long lastPersistIndex) {
        RaftStatusImpl raftStatus = this.raftStatus;
        if (lastPersistIndex > raftStatus.getLastLogIndex()) {
            throw Fiber.fatal(new RaftException("lastPersistIndex > lastLogIndex. lastPersistIndex="
                    + lastPersistIndex + ", lastLogIndex=" + raftStatus.getLastLogIndex()));
        }
        if (raftStatus.getRole() == RaftRole.leader) {
            RaftMember self = raftStatus.getSelf();
            if (self != null) {
                self.setNextIndex(lastPersistIndex + 1);
                self.setMatchIndex(lastPersistIndex);
                self.setLastConfirmReqNanos(raftStatus.getTs().getNanoTime());
            }

            RaftUtil.updateLease(raftStatus);
            // not call raftStatus.copyShareStatus(), invoke after apply

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
                    applyManager.apply();
                }
            }
        }
    }

    public void tryCommit(long recentMatchIndex) {
        RaftStatusImpl raftStatus = this.raftStatus;

        if (!needCommit(recentMatchIndex, raftStatus)) {
            return;
        }
        // leader can only commit log in current term, see raft paper 5.4.2
        if (recentMatchIndex < raftStatus.getGroupReadyIndex()) {
            return;
        }
        raftStatus.setCommitIndex(recentMatchIndex);
        applyManager.apply();
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

    public void registerRespWriter(AppendRespWriter writer) {
        respQueue.addLast(writer);
    }

    public interface AppendRespWriter {
        boolean writeResp(long lastPersistIndex);
    }

}
