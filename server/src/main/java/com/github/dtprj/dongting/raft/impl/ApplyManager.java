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

import com.github.dtprj.dongting.buf.RefBufferFactory;
import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.Decoder;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftExecTimeoutException;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftOutput;
import com.github.dtprj.dongting.raft.sm.StateMachine;
import com.github.dtprj.dongting.raft.store.RaftLog;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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
    private final FutureEventSource futureEventSource;
    private final RaftStatusImpl raftStatus;

    private final DecodeContext decodeContext;

    private boolean configChanging = false;

    private boolean waiting;

    private RaftLog.LogIterator logIterator;

    public ApplyManager(int selfNodeId, RaftLog raftLog, StateMachine stateMachine,
                        RaftStatusImpl raftStatus, EventBus eventBus, FutureEventSource futureEventSource,
                        RefBufferFactory heapPool) {
        this.selfNodeId = selfNodeId;
        this.raftLog = raftLog;
        this.stateMachine = stateMachine;
        this.ts = raftStatus.getTs();
        this.raftStatus = raftStatus;
        this.eventBus = eventBus;
        this.futureEventSource = futureEventSource;
        this.decodeContext = new DecodeContext();
        this.decodeContext.setHeapPool(heapPool);
    }

    public void apply(RaftStatusImpl raftStatus) {
        if (waiting) {
            return;
        }
        long appliedIndex = raftStatus.getLastApplied();
        long diff = raftStatus.getCommitIndex() - appliedIndex;
        PendingMap pendingMap = raftStatus.getPendingRequests();
        while (diff > 0) {
            long index = appliedIndex + 1;
            RaftTask rt = pendingMap.get(index);
            if (rt == null || rt.getInput().isReadOnly()) {
                waiting = true;
                int limit = (int) Math.min(diff, 1024L);
                if (logIterator == null) {
                    logIterator = raftLog.openIterator(raftStatus::isStop);
                }
                logIterator.next(index, limit, 16 * 1024 * 1024)
                        .whenCompleteAsync((items, ex) -> resumeAfterLoad(items, ex, index),
                                raftStatus.getRaftExecutor());
                return;
            } else {
                if (logIterator != null) {
                    DtUtil.close(logIterator);
                    this.logIterator = null;
                }
                execChain(index, rt);
                appliedIndex++;
                diff--;
            }
        }
    }

    private void resumeAfterLoad(List<LogItem> items, Throwable ex, long loadStartIndex) {
        waiting = false;
        if (ex != null) {
            if (ex instanceof CancellationException) {
                log.info("ApplyManager load raft log cancelled");
            } else {
                log.error("load log failed", ex);
            }
            return;
        }
        if (items == null || items.size() == 0) {
            log.error("load log failed, items is null");
            return;
        }
        if (items.get(0).getIndex() != loadStartIndex) {
            // previous load failed, ignore
            log.warn("first index of load result not match appliedIndex, ignore.");
            return;
        }
        int readCount = items.size();
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < readCount; i++) {
            LogItem item = items.get(i);
            RaftTask rt = buildRaftTask(item);
            execChain(item.getIndex(), rt);
        }

        apply(raftStatus);
    }

    @SuppressWarnings("rawtypes")
    private RaftTask buildRaftTask(LogItem item) {
        try {
            ByteBuffer headerRbb = item.getHeaderBuffer();
            if (headerRbb != null) {
                if (item.getType() == LogItem.TYPE_NORMAL) {
                    Decoder decoder = stateMachine.createHeaderDecoder(item.getBizType());
                    Object o = decoder.decode(decodeContext, headerRbb, headerRbb.remaining(), 0);
                    item.setHeader(o);
                } else {
                    item.setHeader(RaftUtil.copy(headerRbb));
                }
            }
            ByteBuffer bodyRbb = item.getBodyBuffer();
            if (bodyRbb != null) {
                if (item.getType() == LogItem.TYPE_NORMAL) {
                    Decoder decoder = stateMachine.createBodyDecoder(item.getBizType());
                    Object o = decoder.decode(decodeContext, bodyRbb, bodyRbb.remaining(), 0);
                    item.setBody(o);
                } else {
                    item.setBody(RaftUtil.copy(bodyRbb));
                }
            }
            RaftInput input = new RaftInput(item.getBizType(), item.getHeader(), item.getBody(),
                    null, item.getActualBodySize());
            RaftTask result = new RaftTask(ts, item.getType(), input, null);
            result.setItem(item);
            RaftTask reader = raftStatus.getPendingRequests().get(item.getIndex());
            if (reader != null) {
                if (reader.getInput().isReadOnly()) {
                    result.setNextReader(reader);
                } else {
                    BugLog.getLog().error("not read only");
                }
            }
            return result;
        } finally {
            decodeContext.setStatus(null);
        }
    }

    private void execChain(long index, RaftTask rt) {
        switch (rt.getType()) {
            case LogItem.TYPE_NORMAL:
                execWrite(index, rt);
                afterExec(index, rt);
                break;
            case LogItem.TYPE_PREPARE_CONFIG_CHANGE:
                doPrepare(index, rt);
                break;
            case LogItem.TYPE_DROP_CONFIG_CHANGE:
                doAbort();
                notifyConfigChange(index, rt);
                afterExec(index, rt);
                break;
            case LogItem.TYPE_COMMIT_CONFIG_CHANGE:
                doCommit();
                notifyConfigChange(index, rt);
                afterExec(index, rt);
                break;
            default:
                afterExec(index, rt);
                // heartbeat etc.
                break;
        }
    }

    private void afterExec(long index, RaftTask rt) {
        raftStatus.setLastApplied(index);
        rt.getItem().release();
        execReaders(index, rt);

        // release reader memory
        rt.setNextReader(null);
        futureEventSource.fireInExecutorThread();
    }

    private void resumeAfterPrepare(long index, RaftTask rt) {
        waiting = false;
        notifyConfigChange(index, rt);
        afterExec(index, rt);
        apply(raftStatus);
    }

    private void notifyConfigChange(long index, RaftTask rt) {
        if (rt.getFuture() != null) {
            rt.getFuture().complete(new RaftOutput(index, null));
        }
    }

    private void execReaders(long index, RaftTask rt) {
        RaftTask nextReader = rt.getNextReader();
        while (nextReader != null) {
            execRead(index, nextReader);
            nextReader = nextReader.getNextReader();
        }
    }

    private void execWrite(long index, RaftTask rt) {
        RaftInput input = rt.getInput();
        CompletableFuture<RaftOutput> future = rt.getFuture();
        try {
            Object r = stateMachine.exec(index, input);
            future.complete(new RaftOutput(index, r));
        } catch (Exception ex) {
            log.warn("exec write failed. {}", ex);
            future.completeExceptionally(ex);
        } finally {
            rt.getItem().release();
            RaftStatusImpl raftStatus = this.raftStatus;
            if (raftStatus.getFirstCommitOfApplied() != null && index >= raftStatus.getFirstIndexOfCurrentTerm()) {
                raftStatus.getFirstCommitOfApplied().complete(null);
                raftStatus.setFirstCommitOfApplied(null);
            }
        }
    }

    public void execRead(long index, RaftTask rt) {
        RaftInput input = rt.getInput();
        CompletableFuture<RaftOutput> future = rt.getFuture();
        if (input.getDeadline() != null && input.getDeadline().isTimeout(ts)) {
            future.completeExceptionally(new RaftExecTimeoutException("timeout "
                    + input.getDeadline().getTimeout(TimeUnit.MILLISECONDS) + "ms"));
        }
        try {
            Object r = stateMachine.exec(index, input);
            future.complete(new RaftOutput(index, r));
        } catch (Throwable e) {
            log.error("exec read failed. {}", e);
            future.completeExceptionally(e);
        }
    }

    private void doPrepare(long index, RaftTask rt) {
        configChanging = true;

        ByteBuffer logData = (ByteBuffer) rt.getInput().getBody();
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

    public CompletableFuture<Void> initReadyFuture() {
        long initCommitIndex = raftStatus.getCommitIndex();
        return futureEventSource.registerInOtherThreads(() -> raftStatus.getLastApplied() >= initCommitIndex);
    }
}
