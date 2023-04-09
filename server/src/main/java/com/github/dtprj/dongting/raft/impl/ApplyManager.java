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
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftExecTimeoutException;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftLog;
import com.github.dtprj.dongting.raft.server.RaftOutput;
import com.github.dtprj.dongting.raft.server.StateMachine;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

/**
 * @author huangli
 */
public class ApplyManager {
    private static final DtLog log = DtLogs.getLogger(ApplyManager.class);

    private final int selfNodeId;
    private final RaftLog raftLog;
    private final StateMachine stateMachine;
    private final Timestamp ts;
    private final EventBus eventBus;
    private final RaftStatus raftStatus;
    private boolean configChanging = false;

    private boolean waiting;
    private long appliedIndex;

    public ApplyManager(int selfNodeId, RaftLog raftLog, StateMachine stateMachine, RaftStatus raftStatus, EventBus eventBus) {
        this.selfNodeId = selfNodeId;
        this.raftLog = raftLog;
        this.stateMachine = stateMachine;
        this.ts = raftStatus.getTs();
        this.raftStatus = raftStatus;
        this.eventBus = eventBus;
    }

    public void apply(RaftStatus raftStatus) {
        if (waiting) {
            return;
        }
        if (appliedIndex < raftStatus.getLastApplied()) {
            appliedIndex = raftStatus.getLastApplied();
        }
        long diff = raftStatus.getCommitIndex() - appliedIndex;
        while (diff > 0) {
            long index = appliedIndex + 1;
            RaftTask rt = raftStatus.getPendingRequests().get(index);
            if (rt == null) {
                waiting = true;
                int limit = (int) Math.min(diff, 100L);
                Supplier<CompletableFuture<LogItem[]>> s = () -> raftLog.load(index, limit, 16 * 1024 * 1024);
                AsyncRetryTask<LogItem[]> task = new AsyncRetryTask<>(s, raftStatus, "load raft log failed");
                task.exec();
                task.getFinalResult().thenAcceptAsync(this::resumeAfterLoad, raftStatus.getRaftExecutor());
                return;
            } else {
                execChain(index, rt);
                appliedIndex++;
                diff--;
            }
        }
    }

    private void resumeAfterLoad(LogItem[] items) {
        waiting = false;
        int readCount = items.length;
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < readCount; i++) {
            LogItem item = items[i];
            RaftTask rt = buildRaftTask(item);
            execChain(item.getIndex(), rt);
        }
        appliedIndex += readCount;
        apply(raftStatus);
    }

    private RaftTask buildRaftTask(LogItem item) {
        RaftInput input;
        if (item.getType() == LogItem.TYPE_NORMAL) {
            Object o = stateMachine.decode(item.getBuffer());
            input = new RaftInput(item.getBuffer(), o, null, false);
        } else {
            input = new RaftInput(item.getBuffer(), null, null, false);
        }
        return new RaftTask(ts, item.getType(), input, null);
    }

    @SuppressWarnings({"ForLoopReplaceableByForEach", "BooleanMethodIsAlwaysInverted"})
    private void execChain(long index, RaftTask rt) {
        switch (rt.type) {
            case LogItem.TYPE_NORMAL:
                execWrite(index, rt);
                return;
            case LogItem.TYPE_PREPARE_CONFIG_CHANGE:
                doPrepare(index, rt);
                return;
            case LogItem.TYPE_DROP_CONFIG_CHANGE:
                doAbort();
                notifyConfigChange(index, rt);
                break;
            case LogItem.TYPE_COMMIT_CONFIG_CHANGE:
                doCommit();
                notifyConfigChange(index, rt);
                break;
            default:
                // heartbeat etc.
                break;
        }
        raftStatus.setLastApplied(index);
        execReaders(index, rt);
    }

    private void resumeAfterPrepare(long index, RaftTask rt) {
        waiting = false;
        notifyConfigChange(index, rt);
        raftStatus.setLastApplied(index);
        execReaders(index, rt);
        apply(raftStatus);
    }

    private void notifyConfigChange(long index, RaftTask rt) {
        if (rt.future != null) {
            rt.future.complete(new RaftOutput(index, null));
        }
    }

    private void execReaders(long index, RaftTask rt) {
        ArrayList<RaftTask> nextReaders = rt.nextReaders;
        if (nextReaders == null) {
            return;
        }
        for (int i = 0; i < nextReaders.size(); i++) {
            RaftTask readerTask = nextReaders.get(i);
            execRead(index, readerTask);
        }
    }

    private void execWrite(long index, RaftTask rt) {
        RaftInput input = rt.input;
        CompletableFuture<RaftOutput> future = rt.future;
        stateMachine.exec(index, input).whenCompleteAsync((r, e) -> {
            if (e != null) {
                log.warn("exec write failed. {}", e.toString());
                future.completeExceptionally(e);
            } else {
                future.complete(new RaftOutput(index, r));
            }
            if (raftStatus.getFirstCommitOfApplied() != null) {
                raftStatus.getFirstCommitOfApplied().complete(null);
                raftStatus.setFirstCommitOfApplied(null);
            }
            raftStatus.setLastApplied(index);
        }, raftStatus.getRaftExecutor());
        execReaders(index, rt);
    }

    public void execRead(long index, RaftTask rt) {
        RaftInput input = rt.input;
        CompletableFuture<RaftOutput> future = rt.future;
        if (input.getDeadline() != null && input.getDeadline().isTimeout(ts)) {
            future.completeExceptionally(new RaftExecTimeoutException("timeout "
                    + input.getDeadline().getTimeout(TimeUnit.MILLISECONDS) + "ms"));
        }
        try {
            // no need run in raft thread
            stateMachine.exec(index, input).whenComplete((r, e) -> {
                if (e != null) {
                    future.completeExceptionally(e);
                } else {
                    future.complete(new RaftOutput(index, r));
                }
            });
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
    }

    private void doPrepare(long index, RaftTask rt) {
        configChanging = true;

        ByteBuffer logData = rt.input.getLogData();
        byte[] data = new byte[logData.remaining()];
        logData.get(data);
        String dataStr = new String(data);
        String[] fields = dataStr.split(";");
        Set<Integer> oldMembers = parseSet(fields[0]);
        Set<Integer> oldObservers = parseSet(fields[1]);
        Set<Integer> newMembers = parseSet(fields[2]);
        Set<Integer> newObservers = parseSet(fields[3]);
        if (!oldMembers.equals(raftStatus.getNodeIdOfMembers())) {
            log.error("oldMembers not match, oldMembers={}, currentMembers={}, groupId={}",
                    oldMembers, raftStatus.getNodeIdOfMembers(), raftStatus.getGroupId());
        }
        if (!oldObservers.equals(raftStatus.getNodeIdOfObservers())) {
            log.error("oldObservers not match, oldObservers={}, currentObservers={}, groupId={}",
                    oldObservers, raftStatus.getNodeIdOfObservers(), raftStatus.getGroupId());
        }

        Runnable callback = () -> resumeAfterPrepare(index, rt);
        Object[] args = new Object[]{raftStatus.getGroupId(), raftStatus.getNodeIdOfPreparedMembers(),
                raftStatus.getNodeIdOfPreparedObservers(), newMembers, newObservers, callback};
        waiting = true;
        eventBus.fire(EventType.prepareConfChange, args);
    }


    public Set<Integer> parseSet(String s) {
        if (s.length() == 0) {
            return emptySet();
        }
        String[] fields = s.split(",");
        Set<Integer> set = new HashSet<>();
        for (String f : fields) {
            set.add(Integer.parseInt(f));
        }
        return set;
    }

    private void doAbort() {
        HashSet<Integer> ids = new HashSet<>(raftStatus.getNodeIdOfPreparedMembers());
        for (RaftMember m : raftStatus.getPreparedObservers()) {
            ids.add(m.getNode().getNodeId());
        }
        if (ids.size() == 0) {
            configChanging = false;
            return;
        }

        raftStatus.setPreparedMembers(emptyList());
        raftStatus.setPreparedObservers(emptyList());
        MemberManager.computeDuplicatedData(raftStatus);

        if (!raftStatus.getNodeIdOfMembers().contains(selfNodeId)) {
            if (raftStatus.getRole() != RaftRole.observer) {
                RaftUtil.changeToObserver(raftStatus, -1);
            }
        }
        eventBus.fire(EventType.abortConfChange, ids);
        configChanging = false;
    }

    private void doCommit() {
        if (!configChanging) {
            log.warn("no prepared config change, ignore commit, groupId={}", raftStatus.getGroupId());
            return;
        }
        HashSet<Integer> ids = new HashSet<>(raftStatus.getNodeIdOfMembers());
        ids.addAll(raftStatus.getNodeIdOfObservers());

        raftStatus.setMembers(raftStatus.getPreparedMembers());
        raftStatus.setObservers(raftStatus.getPreparedObservers());

        raftStatus.setPreparedMembers(emptyList());
        raftStatus.setPreparedObservers(emptyList());
        MemberManager.computeDuplicatedData(raftStatus);

        if (raftStatus.getNodeIdOfMembers().contains(selfNodeId)) {
            if (raftStatus.getRole() != RaftRole.observer) {
                RaftUtil.changeToObserver(raftStatus, -1);
            }
        }

        eventBus.fire(EventType.commitConfChange, ids);
        configChanging = false;
    }
}
