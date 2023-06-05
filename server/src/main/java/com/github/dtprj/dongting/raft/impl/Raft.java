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

import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.Encoder;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.NotLeaderException;
import com.github.dtprj.dongting.raft.server.RaftExecTimeoutException;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftLog;
import com.github.dtprj.dongting.raft.server.RaftNode;
import com.github.dtprj.dongting.raft.sm.StateMachine;

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
    private final StateMachine stateMachine;
    private final ApplyManager applyManager;
    private final CommitManager commitManager;

    private final RaftLog raftLog;
    private final RaftStatusImpl raftStatus;

    private final Timestamp ts;

    private final EncodeContext encodeContext;

    public Raft(RaftStatusImpl raftStatus, RaftLog raftLog, ApplyManager applyManager, CommitManager commitManager,
                ReplicateManager replicateManager, StateMachine stateMachine, RaftGroupConfigEx groupConfig) {
        this.raftStatus = raftStatus;
        this.raftLog = raftLog;
        this.ts = raftStatus.getTs();

        this.applyManager = applyManager;
        this.commitManager = commitManager;
        this.replicateManager = replicateManager;
        this.stateMachine = stateMachine;

        this.encodeContext = groupConfig.getEncodeContext();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void accept(EventType eventType, Object o) {
        if (eventType == EventType.raftExec) {
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
                item.setBizType(input.getBizType());
                item.setTerm(currentTerm);
                item.setIndex(newIndex);
                item.setPrevLogTerm(oldTerm);
                item.setTimestamp(ts.getWallClockMillis());

                Object header = input.getHeader();
                item.setHeader(header);
                if (header != null) {
                    // TODO create encoder for each item
                    Encoder<Object> encoder = stateMachine.createEncoder(item.getBizType(), true);
                    item.setActualHeaderSize(encoder.actualSize(encodeContext, header));
                }

                Object body = input.getBody();
                item.setBody(body);
                if (body != null) {
                    Encoder<Object> encoder = stateMachine.createEncoder(item.getBizType(), false);
                    item.setActualBodySize(encoder.actualSize(encodeContext, body));
                }

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
        RaftInput input = new RaftInput(0, null, null, deadline, 0);
        RaftTask rt = new RaftTask(ts, LogItem.TYPE_HEARTBEAT, input, null);
        raftExec(Collections.singletonList(rt));
    }

}
