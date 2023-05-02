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
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.NotLeaderException;
import com.github.dtprj.dongting.raft.server.RaftExecTimeoutException;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftLog;
import com.github.dtprj.dongting.raft.server.RaftNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * @author huangli
 */
public class Raft implements BiConsumer<EventType, Object> {

    private final ReplicateManager replicateManager;
    private final ApplyManager applyManager;
    private final CommitManager commitManager;

    private final RaftLog raftLog;
    private final RaftStatusImpl raftStatus;

    private final Timestamp ts;

    public Raft(RaftStatusImpl raftStatus, RaftLog raftLog, ApplyManager applyManager,
                CommitManager commitManager, ReplicateManager replicateManager) {
        this.raftStatus = raftStatus;
        this.raftLog = raftLog;
        this.ts = raftStatus.getTs();

        this.applyManager = applyManager;
        this.commitManager = commitManager;
        this.replicateManager = replicateManager;
        this.encoder = encoder;
    }

    @Override
    public void accept(EventType eventType, Object o) {
        if (eventType == EventType.raftExec) {
            //noinspection unchecked
            raftExec((List<RaftTask>) o);
        }
    }

    @SuppressWarnings("ForLoopReplaceableByForEach")
    public void raftExec(List<RaftTask> inputs) {
        RaftStatusImpl raftStatus = this.raftStatus;
        if (raftStatus.getRole() != RaftRole.leader) {
            RaftNode leader = RaftUtil.getLeader(raftStatus.getCurrentLeader());
            for (RaftTask t : inputs) {
                if (t.future != null) {
                    t.future.completeExceptionally(new NotLeaderException(leader));
                }
            }
            return;
        }
        long newIndex = raftStatus.getLastLogIndex();

        ArrayList<LogItem> logs = new ArrayList<>(inputs.size());
        int oldTerm = raftStatus.getLastLogTerm();
        int currentTerm = raftStatus.getCurrentTerm();
        PendingMap pending = raftStatus.getPendingRequests();
        for (int i = 0; i < inputs.size(); i++) {
            RaftTask rt = inputs.get(i);
            RaftInput input = rt.input;

            if (input.getDeadline() != null && input.getDeadline().isTimeout(ts)) {
                rt.future.completeExceptionally(new RaftExecTimeoutException("timeout "
                        + input.getDeadline().getTimeout(TimeUnit.MILLISECONDS) + "ms"));
                continue;
            }

            if (!input.isReadOnly()) {
                newIndex++;
                LogItem item = new LogItem();
                item.setType(rt.type);
                item.setTerm(currentTerm);
                item.setIndex(newIndex);
                item.setPrevLogTerm(oldTerm);
                item.setTimestamp(ts.getWallClockMillis());
                item.setData(input.getInput());
                item.setDataSize(input.size());
                logs.add(item);

                rt.item = item;

                pending.put(newIndex, rt);
            } else {
                // read
                if (newIndex <= raftStatus.getLastApplied()) {
                    applyManager.execRead(newIndex, rt);
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

        RaftUtil.append(raftLog, raftStatus, logs);

        raftStatus.setLastLogTerm(currentTerm);
        raftStatus.setLastLogIndex(newIndex);

        RaftMember self = raftStatus.getSelf();
        self.setNextIndex(newIndex + 1);
        self.setMatchIndex(newIndex);
        self.setLastConfirmReqNanos(ts.getNanoTime());

        // for single node mode
        if (raftStatus.getRwQuorum() == 1) {
            RaftUtil.updateLease(raftStatus);
            commitManager.tryCommit(newIndex);
        }

        replicateManager.replicateAfterRaftExec(raftStatus);
    }

    public void sendHeartBeat() {
        DtTime deadline = new DtTime(ts, raftStatus.getElectTimeoutNanos(), TimeUnit.NANOSECONDS);
        RaftInput input = new RaftInput(null, deadline, false, 0);
        RaftTask rt = new RaftTask(ts, LogItem.TYPE_HEARTBEAT, input, null);
        raftExec(Collections.singletonList(rt));
    }

}
