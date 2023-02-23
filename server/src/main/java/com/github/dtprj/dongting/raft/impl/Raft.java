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
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.NotLeaderException;
import com.github.dtprj.dongting.raft.server.RaftExecTimeoutException;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftLog;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class Raft {

    private static final DtLog log = DtLogs.getLogger(Raft.class);

    private final LeaderAppendManager leaderAppendManager;
    private final ApplyManager applyManager;
    private final CommitManager commitManager;

    private final RaftServerConfig config;
    private final RaftLog raftLog;
    private final RaftStatus raftStatus;

    private final int maxReplicateItems;
    private final int restItemsToStartReplicate;
    private final long maxReplicateBytes;

    private RaftNode self;
    private final Timestamp ts;

    public Raft(RaftContainer container) {
        this.config = container.getConfig();
        this.raftLog = container.getRaftLog();
        this.raftStatus = container.getRaftStatus();
        this.ts = raftStatus.getTs();

        this.applyManager = new ApplyManager(container.getStateMachine(), ts);
        this.commitManager = new CommitManager(raftStatus, raftLog, container.getStateMachine(), applyManager);
        this.leaderAppendManager = new LeaderAppendManager(container, this, commitManager);

        this.maxReplicateItems = config.getMaxReplicateItems();
        this.maxReplicateBytes = config.getMaxReplicateBytes();
        this.restItemsToStartReplicate = (int) (maxReplicateItems * 0.1);
    }

    private RaftNode getSelf() {
        if (self != null) {
            return self;
        }
        for (RaftNode node : raftStatus.getServers()) {
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

        RaftNode self = getSelf();
        self.setNextIndex(newIndex + 1);
        self.setMatchIndex(newIndex);
        self.setLastConfirm(true, ts.getNanoTime());

        // for single node mode
        if (raftStatus.getRwQuorum() == 1) {
            RaftUtil.updateLease(ts.getNanoTime(), raftStatus);
            commitManager.tryCommit(newIndex);
        }


        for (RaftNode node : raftStatus.getServers()) {
            if (node.isSelf()) {
                continue;
            }
            replicate(node);
        }
    }

    @SuppressWarnings("StatementWithEmptyBody")
    void replicate(RaftNode node) {
        if (raftStatus.getRole() != RaftRole.leader) {
            return;
        }
        if (!node.isReady()) {
            return;
        }
        if (node.isMultiAppend()) {
            doReplicate(node, false);
        } else {
            if (node.getPendingRequests() == 0) {
                doReplicate(node, true);
            } else {
                // waiting all pending request complete
            }
        }
    }

    private void doReplicate(RaftNode node, boolean tryMatch) {
        long nextIndex = node.getNextIndex();
        long lastLogIndex = raftStatus.getLastLogIndex();
        if (lastLogIndex < nextIndex) {
            // no data to replicate
            return;
        }

        // flow control
        int rest = maxReplicateItems - node.getPendingRequests();
        if (rest <= restItemsToStartReplicate) {
            // avoid silly window syndrome
            return;
        }
        if (node.getPendingBytes() >= maxReplicateBytes) {
            return;
        }

        int limit = tryMatch ? 1 : rest;

        RaftTask first = raftStatus.getPendingRequests().get(nextIndex);
        LogItem[] items;
        if (first != null && !first.input.isReadOnly()) {
            items = new LogItem[limit];
            for (int i = 0; i < limit; i++) {
                RaftTask t = raftStatus.getPendingRequests().get(nextIndex + i);
                items[i] = t.item;
            }
        } else {
            items = RaftUtil.load(raftLog, raftStatus, nextIndex, limit, maxReplicateBytes);
        }

        doReplicate(node, items);
    }

    private void doReplicate(RaftNode node, LogItem[] items) {
        ArrayList<LogItem> logs = new ArrayList<>();
        long bytes = 0;
        for (int i = 0; i < items.length; ) {
            LogItem item = items[i];
            int currentSize = item.getBuffer() == null ? 0 : item.getBuffer().remaining();
            if (bytes + currentSize > config.getMaxBodySize()) {
                if (logs.size() > 0) {
                    LogItem firstItem = logs.get(0);
                    leaderAppendManager.sendAppendRequest(node, firstItem.getIndex() - 1, firstItem.getPrevLogTerm(), logs, bytes);

                    bytes = 0;
                    logs = new ArrayList<>();
                    continue;
                } else {
                    log.error("body too large: {}", currentSize);
                    return;
                }
            }
            bytes += currentSize;
            logs.add(item);
            i++;
        }

        if (logs.size() > 0) {
            LogItem firstItem = logs.get(0);
            leaderAppendManager.sendAppendRequest(node, firstItem.getIndex() - 1, firstItem.getPrevLogTerm(), logs, bytes);
        }
    }

    public void sendHeartBeat() {
        DtTime deadline = new DtTime(ts, raftStatus.getElectTimeoutNanos(), TimeUnit.NANOSECONDS);
        RaftInput input = new RaftInput(null, null, deadline, false);
        RaftTask rt = new RaftTask(ts, LogItem.TYPE_HEARTBEAT, input, null);
        raftExec(Collections.singletonList(rt));
    }

}
