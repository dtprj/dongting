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

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.NotLeaderException;
import com.github.dtprj.dongting.raft.server.RaftExecTimeoutException;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftLog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class Raft {

    private final ReplicateManager replicateManager;
    private final ApplyManager applyManager;
    private final CommitManager commitManager;

    private final RaftLog raftLog;
    private final RaftStatus raftStatus;

    private RaftMember self;
    private final Timestamp ts;

    public Raft(RaftComponents container) {
        this.raftLog = container.getRaftLog();
        this.raftStatus = container.getRaftStatus();
        this.ts = raftStatus.getTs();

        this.applyManager = new ApplyManager(container.getStateMachine(), ts);
        this.commitManager = new CommitManager(raftStatus, raftLog, container.getStateMachine(), applyManager);
        this.replicateManager = new ReplicateManager(container, commitManager);
   }

    private RaftMember getSelf() {
        if (self != null) {
            return self;
        }
        for (RaftMember node : raftStatus.getServers()) {
            if (node.isSelf()) {
                this.self = node;
                break;
            }
        }
        return self;
    }

    public void raftExec(List<RaftTask> inputs) {
        RaftStatus raftStatus = this.raftStatus;
        if (raftStatus.getRole() != RaftRole.leader) {
            HostPort leader = RaftUtil.getLeader(raftStatus.getCurrentLeader());
            for (RaftTask t : inputs) {
                if (t.future != null) {
                    t.future.completeExceptionally(new NotLeaderException(leader));
                }
            }
            return;
        }
        long oldIndex = raftStatus.getLastLogIndex();
        long newIndex = oldIndex;

        ArrayList<LogItem> logs = new ArrayList<>(inputs.size());
        int oldTerm = raftStatus.getLastLogTerm();
        int currentTerm = raftStatus.getCurrentTerm();
        PendingMap pending = raftStatus.getPendingRequests();
        for (RaftTask rt : inputs) {
            RaftInput input = rt.input;

            if (input.getDeadline().isTimeout(ts)) {
                rt.future.completeExceptionally(new RaftExecTimeoutException("timeout "
                        + input.getDeadline().getTimeout(TimeUnit.MILLISECONDS) + "ms"));
                continue;
            }

            if (!input.isReadOnly()) {
                newIndex++;
                LogItem item = new LogItem(rt.type, newIndex, currentTerm, oldTerm, input.getLogData());
                logs.add(item);

                rt.item = item;

                pending.put(newIndex, rt);
            } else {
                // read
                if (newIndex <= raftStatus.getLastApplied()) {
                    applyManager.exec(newIndex, rt);
                } else {
                    RaftTask newTask = pending.get(newIndex);
                    if (newTask == null) {
                        pending.put(newIndex, rt);
                    } else {
                        newTask.addNext(rt);
                    }
                }
            }
        }

        if (logs.size() == 0) {
            return;
        }

        RaftUtil.append(raftLog, raftStatus, oldIndex, oldTerm, logs);

        raftStatus.setLastLogTerm(currentTerm);
        raftStatus.setLastLogIndex(newIndex);

        RaftMember self = getSelf();
        self.setNextIndex(newIndex + 1);
        self.setMatchIndex(newIndex);
        self.setLastConfirm(true, ts.getNanoTime());

        // for single node mode
        if (raftStatus.getRwQuorum() == 1) {
            RaftUtil.updateLease(ts.getNanoTime(), raftStatus);
            commitManager.tryCommit(newIndex);
        }


        for (RaftMember node : raftStatus.getServers()) {
            if (node.isSelf()) {
                continue;
            }
            replicateManager.replicate(node);
        }
    }

    public void sendHeartBeat() {
        DtTime deadline = new DtTime(ts, raftStatus.getElectTimeoutNanos(), TimeUnit.NANOSECONDS);
        RaftInput input = new RaftInput(null, null, deadline, false);
        RaftTask rt = new RaftTask(ts, LogItem.TYPE_HEARTBEAT, input, null);
        raftExec(Collections.singletonList(rt));
    }

}
