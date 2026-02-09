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

import com.github.dtprj.dongting.common.ByteArray;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.IndexedQueue;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.common.PerfCallback;
import com.github.dtprj.dongting.common.PerfConsts;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCall;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.RaftTimeoutException;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.sm.Snapshot;
import com.github.dtprj.dongting.raft.sm.SnapshotInfo;
import com.github.dtprj.dongting.raft.sm.StateMachine;
import com.github.dtprj.dongting.raft.store.RaftLog;
import com.github.dtprj.dongting.raft.store.StatusManager;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptySet;

/**
 * @author huangli
 */
public class ApplyManager implements Comparator<Pair<DtTime, CompletableFuture<Long>>> {
    private static final DtLog log = DtLogs.getLogger(ApplyManager.class);

    private final GroupComponents gc;
    private final Timestamp ts;
    private final FiberGroup fiberGroup;
    private final RaftStatusImpl raftStatus;

    private RaftLog raftLog;
    private StateMachine stateMachine;

    private final FiberCondition needApplyCond;
    private boolean waitApply;
    final FiberCondition applyFinishCond;
    private final FiberCondition applyMonitorCond;
    private final LinkedList<RaftTask> heartBeatQueue = new LinkedList<>();

    private long initCommitIndex;

    private final PriorityQueue<Pair<DtTime, CompletableFuture<Long>>> waitReadyQueue;
    private final LinkedList<FiberFuture<Snapshot>> takeSnapshotRequests = new LinkedList<>();

    private int execCount = 0;

    private final PerfCallback perfCallback;

    private Fiber applyFiber;

    private boolean shutdown;

    public ApplyManager(GroupComponents gc) {
        this.ts = gc.raftStatus.ts;
        this.raftStatus = gc.raftStatus;
        this.gc = gc;
        this.fiberGroup = gc.fiberGroup;
        this.perfCallback = gc.groupConfig.perfCallback;
        this.waitReadyQueue = new PriorityQueue<>(this);
        this.needApplyCond = fiberGroup.newCondition("needApply");
        this.applyFinishCond = fiberGroup.newCondition("applyFinish");
        this.applyMonitorCond = fiberGroup.newCondition("applyMonitor");
    }

    @Override
    public int compare(Pair<DtTime, CompletableFuture<Long>> o1, Pair<DtTime, CompletableFuture<Long>> o2) {
        long now = ts.nanoTime;
        long diff = (o1.getLeft().deadlineNanos - now) - (o2.getLeft().deadlineNanos - now);
        return diff < 0 ? -1 : diff > 0 ? 1 : 0;
    }

    public void postInit() {
        this.raftLog = gc.raftLog;
        this.stateMachine = gc.stateMachine;
    }

    public void init(FiberGroup fiberGroup) {
        this.initCommitIndex = raftStatus.commitIndex;
        startApplyFiber(fiberGroup);
        new Fiber("applyFiberMonitor", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                if (applyFiber.isFinished() && !shouldStopApply()) {
                    startApplyFiber(fiberGroup);
                }
                if (raftStatus.isGroupReady()) {
                    return applyMonitorCond.await(1000, this);
                } else {
                    processWaitGroupReadyQueue(true, false, 0);
                    return applyMonitorCond.await(100, this);
                }
            }
        }, true).start();
        if (raftStatus.getLastApplied() >= raftStatus.commitIndex) {
            log.info("apply manager init complete");
            raftStatus.markInit(false);
            raftStatus.copyShareStatus();
            raftStatus.initFuture.complete(null);
        } else if (raftStatus.installSnapshot) {
            log.info("install snapshot, apply manager init complete");
            raftStatus.markInit(false);
            raftStatus.copyShareStatus();
            raftStatus.initFuture.complete(null);
        }
    }

    private void startApplyFiber(FiberGroup fiberGroup) {
        applyFiber = new Fiber("apply", fiberGroup, new ApplyFrame(), true, 50);
        applyFiber.start();
    }

    public void wakeupApply() {
        needApplyCond.signalAll();
    }

    public void shutdown(DtTime timeout) {
        this.shutdown = true;
        wakeupApply();
        try {
            // start in InitFiberFrame
            stateMachine.stop(timeout);
        } catch (Throwable e) {
            log.error("state machine stop failed", e);
        }
        processWaitGroupReadyQueue(false, true, 0);
    }

    private FrameCallResult exec(RaftTask rt, long index, FrameCall<Void> resumePoint) {
        raftStatus.lastApplying = index;
        switch (rt.type) {
            case LogItem.TYPE_PREPARE_CONFIG_CHANGE:
            case LogItem.TYPE_DROP_CONFIG_CHANGE:
            case LogItem.TYPE_COMMIT_CONFIG_CHANGE:
                // block apply fiber util config change complete
                return Fiber.call(new ConfigChangeFrame(rt), unused -> {
                    raftStatus.lastConfigChangeIndex = index;
                    afterExec(index, rt, null, null);
                    return Fiber.resume(null, resumePoint);
                });
            case LogItem.TYPE_NORMAL:
            case LogItem.TYPE_LOG_READ: {
                RaftInput input = rt.input;
                if (input.readOnly && rt.callback == null) {
                    // no need to execute read only task if no one wait for result
                    afterExec(index, rt, null, null);
                } else {
                    long t = perfCallback.takeTimeAndRefresh(PerfConsts.RAFT_D_STATE_MACHINE_EXEC, ts);
                    FiberFuture<Object> f = null;
                    Throwable execEx = null;
                    try {
                        f = stateMachine.exec(index, rt.item.timestamp, rt.localCreateNanos, input);
                        execCount++;
                    } catch (Throwable e) {
                        execEx = e;
                    }
                    if (execEx != null) {
                        afterExec(index, rt, null, execEx);
                    } else if (f != null) {
                        f.registerCallback((result, ex) -> {
                            // the callback may not run in raft thread, so not access ts
                            perfCallback.fireTime(PerfConsts.RAFT_D_STATE_MACHINE_EXEC, t, 1, 0);
                            afterExec(index, rt, result, ex);
                        });
                    } else {
                        throw Fiber.fatal(new RaftException("statemachine exec return null future"));
                    }
                }
                return Fiber.resume(null, resumePoint);
            }
            default:
                // heart beat, no need to exec
                heartBeatQueue.addLast(rt);
                tryApplyHeartBeat(raftStatus.getLastApplied());
                return Fiber.resume(null, resumePoint);
        }
    }

    // if processAll and group should stop, use null as leaseReadIndex
    private void processWaitGroupReadyQueue(boolean processTimeout, boolean processStop, long leaseReadIndex) {
        if (waitReadyQueue.isEmpty()) {
            return;
        }
        Iterator<Pair<DtTime, CompletableFuture<Long>>> it = waitReadyQueue.iterator();
        while (it.hasNext()) {
            Pair<DtTime, CompletableFuture<Long>> p = it.next();
            DtTime deadline = p.getLeft();
            CompletableFuture<Long> f = p.getRight();
            if (processTimeout) {
                if (deadline.isTimeout(ts)) {
                    it.remove();
                    RaftTimeoutException e = new RaftTimeoutException("wait group ready timeout: "
                            + deadline.getTimeout(TimeUnit.MILLISECONDS) + "ms");
                    completeWaitReadyFuture(f, null, e);
                } else {
                    break;
                }
            } else {
                it.remove();
                if (processStop) {
                    completeWaitReadyFuture(f, null, new RaftException("group should stop"));
                } else {
                    completeWaitReadyFuture(f, leaseReadIndex, null);
                }
            }
        }
    }

    private void completeWaitReadyFuture(CompletableFuture<Long> f, Long index, Throwable ex) {
        try {
            if (ex != null) {
                f.completeExceptionally(ex);
            } else {
                f.complete(index);
            }
        } catch (Exception e) {
            log.error("lease read callback failed", e);
        }
    }

    public CompletableFuture<Long> addToWaitReadyQueue(DtTime t) {
        CompletableFuture<Long> f = new CompletableFuture<>();
        boolean b = fiberGroup.fireFiber("addToWaitReadyQueue", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                if (t.isTimeout(raftStatus.ts)) {
                    RaftTimeoutException e = new RaftTimeoutException("wait group ready timeout: "
                            + t.getTimeout(TimeUnit.MILLISECONDS) + "ms");
                    completeWaitReadyFuture(f, null, e);
                } else if (isGroupShouldStopPlain()) {
                    completeWaitReadyFuture(f, null, new RaftException("group should stop"));
                } else if (raftStatus.isGroupReady()) {
                    completeWaitReadyFuture(f, raftStatus.getLastApplied(), null);
                } else {
                    waitReadyQueue.add(new Pair<>(t, f));
                }
                return Fiber.frameReturn();
            }
        });
        if (!b) {
            f.completeExceptionally(new RaftException("group should stop"));
        }
        return f;
    }

    private void afterExec(long index, RaftTask rt, Object execResult, Throwable execEx) {
        if (execEx != null && !rt.input.readOnly) {
            throw Fiber.fatal(execEx);
        }
        RaftStatusImpl raftStatus = ApplyManager.this.raftStatus;

        raftStatus.setLastApplied(index);
        raftStatus.lastAppliedTerm = rt.item.term;

        updateApplyLagNanos(index, raftStatus);

        boolean fireInitFuture = false;
        if (!raftStatus.isInitFinished() && index >= initCommitIndex) {
            log.info("apply manager init complete, initCommitIndex={}", initCommitIndex);
            raftStatus.markInit(false);
            fireInitFuture = true;
        }

        boolean processWaitGroupReadyQueue = false;
        if (!raftStatus.isGroupReady() && raftStatus.getRole() != RaftRole.none
                && index >= raftStatus.groupReadyIndex) {
            raftStatus.setGroupReady(true);
            processWaitGroupReadyQueue = true;
            log.info("{} mark group ready: groupId={}, groupReadyIndex={}",
                    raftStatus.getRole(), raftStatus.groupId, raftStatus.groupReadyIndex);
        }

        // copy share status should happen before group ready notifications
        raftStatus.copyShareStatus();
        if (execEx == null) {
            rt.callSuccess(execResult);
        } else {
            // assert read only
            rt.callFail(execEx);
        }
        raftStatus.tailCache.removePending(rt);

        if (fireInitFuture) {
            raftStatus.initFuture.complete(null);
        }
        if (processWaitGroupReadyQueue) {
            processWaitGroupReadyQueue(false, false, index);
        }

        if (waitApply) {
            applyFinishCond.signal();
        }
        tryApplyHeartBeat(index);
    }

    private void updateApplyLagNanos(long index, RaftStatusImpl raftStatus) {
        IndexedQueue<long[]> his = raftStatus.commitHistory;
        long[] a = his.getFirst();
        if (a != null && index >= a[0]) {
            if (index == a[0]) {
                raftStatus.applyLagNanos = ts.nanoTime - a[1];
            }
            while (index >= a[0] && his.size() > 1) {
                his.removeFirst();
                a = his.getFirst();
            }
        }
    }

    private void tryApplyHeartBeat(long appliedIndex) {
        RaftTask t = heartBeatQueue.peekFirst();
        if (t != null && t.item.index == appliedIndex + 1) {
            heartBeatQueue.pollFirst();
            afterExec(appliedIndex + 1, t, null, null);
        }
    }

    private boolean shouldStopApply() {
        return raftStatus.installSnapshot || shutdown;
    }

    public Fiber getApplyFiber() {
        return applyFiber;
    }

    public void signalStartApply() {
        applyMonitorCond.signal();
    }

    public FiberFuture<Snapshot> requestTakeSnapshot() {
        FiberFuture<Snapshot> future = fiberGroup.newFuture("take-snapshot");
        takeSnapshotRequests.add(future);
        wakeupApply();
        return future;
    }

    private class ApplyFrame extends FiberFrame<Void> {

        private RaftLog.LogIterator logIterator;

        private final TailCache tailCache = raftStatus.tailCache;

        @Override
        protected FrameCallResult handle(Throwable ex) {
            log.error("apply failed, lastCommit={}, lastApplying={}, lastApplied={}",
                    raftStatus.commitIndex, raftStatus.lastApplying, raftStatus.getLastApplied(), ex);
            throw Fiber.fatal(ex);
        }

        @Override
        protected FrameCallResult doFinally() {
            log.info("apply fiber exit: groupId={}", raftStatus.groupId);
            closeIterator();
            return Fiber.frameReturn();
        }

        @Override
        public FrameCallResult execute(Void input) {
            if (shouldStopApply()) {
                return Fiber.frameReturn();
            }
            execCount = 1;
            return execLoop(null);
        }

        private FrameCallResult execLoop(Void v) {
            if (execCount >= 100) {
                return Fiber.yield(this);
            }
            RaftStatusImpl raftStatus = ApplyManager.this.raftStatus;
            long diff = raftStatus.commitIndex - raftStatus.lastApplying;
            if (!takeSnapshotRequests.isEmpty()) {
                return Fiber.call(new TakeSnapshotFrame(), this);
            } else if (diff == 0) {
                return needApplyCond.await(this);
            }
            long index = raftStatus.lastApplying + 1;
            RaftTask rt = tailCache.get(index);
            if (rt == null) {
                int limit = (int) Math.min(diff, 1024L);
                if (log.isDebugEnabled()) {
                    log.debug("load from {}, diff={}, limit={}, cacheSize={}, cacheFirstIndex={},commitIndex={},lastApplying={}",
                            index, diff, limit, tailCache.size(), tailCache.getFirstIndex(),
                            raftStatus.commitIndex, raftStatus.lastApplying);
                }
                if (logIterator == null) {
                    logIterator = raftLog.openIterator(null);
                }
                FiberFrame<List<LogItem>> ff = logIterator.next(index, limit, 16 * 1024 * 1024);
                return Fiber.call(ff, this::afterLoad);
            } else {
                closeIterator();
                return exec(rt, index, this::execLoop);
            }
        }

        private FrameCallResult afterLoad(List<LogItem> items) {
            ExecLoadResultFrame ff = new ExecLoadResultFrame(items);
            return Fiber.call(ff, this::execLoop);
        }

        public void closeIterator() {
            if (logIterator != null) {
                DtUtil.close(logIterator);
                logIterator = null;
            }
        }
    }

    private class ExecLoadResultFrame extends FiberFrame<Void> {

        private final List<LogItem> items;
        private int listIndex;

        public ExecLoadResultFrame(List<LogItem> items) {
            // Here, we refreshed the ts. Next, the time t set on RaftTask is greater than the time when the raft
            // client constructs the request. Since there is a 1ms error in the ts refresh, the time when the raft
            // client constructs the request happens before (t + 1ms).
            raftStatus.ts.refresh(1);
            this.items = items;
        }

        @Override
        protected FrameCallResult doFinally() {
            RaftUtil.release(items);
            return Fiber.frameReturn();
        }

        @Override
        public FrameCallResult execute(Void v) {
            if (shouldStopApply()) {
                return Fiber.frameReturn();
            }
            if (items == null || items.isEmpty()) {
                log.error("load log failed, items is null");
                return Fiber.frameReturn();
            }
            if (listIndex >= items.size()) {
                return Fiber.frameReturn();
            }
            LogItem item = items.get(listIndex++);

            RaftInput input = new RaftInput(item.bizType, item.getHeader(), item.getBody(), null,
                    item.type == LogItem.TYPE_LOG_READ);
            RaftTask rt = new RaftTask(item.type, input, null);

            // nanos can't persist, use wallClockMillis, so has week dependence on system clock.
            // this method only used to load logs that not apply after restart.
            long costTimeMillis = ts.wallClockMillis - item.timestamp;
            if (costTimeMillis < 0) {
                costTimeMillis = 0;
            }
            long localCreateNanos = ts.nanoTime - costTimeMillis * 1_000_000L;
            rt.init(item, localCreateNanos);

            return exec(rt, item.index, this);
        }

    }

    private class WaitApplyFrame extends FiberFrame<Void> {

        private final long targetIndex;
        private boolean logged;

        WaitApplyFrame(long targetIndex) {
            this.targetIndex = targetIndex;
        }

        @Override
        public FrameCallResult execute(Void input) {
            if (raftStatus.getLastApplied() < targetIndex) {
                waitApply = true;
                if (!logged) {
                    log.info("wait apply, targetIndex={}, lastApplied={}, lastApplying={}", targetIndex,
                            raftStatus.getLastApplied(), raftStatus.lastApplying);
                    logged = true;
                }
                return applyFinishCond.await(this);
            }
            return afterPreviousApplyFinish();
        }

        private FrameCallResult afterPreviousApplyFinish() {
            waitApply = false;
            log.info("previous apply finished, targetIndex={}, lastApplied={}, lastApplying={}", targetIndex,
                    raftStatus.getLastApplied(), raftStatus.lastApplying);
            return Fiber.frameReturn();
        }
    }

    private class ConfigChangeFrame extends FiberFrame<Void> {

        private final RaftTask rt;

        private ConfigChangeFrame(RaftTask rt) {
            this.rt = rt;
        }

        // no error handler, use ApplyFrame process ex

        @Override
        public FrameCallResult execute(Void v) {
            return Fiber.call(new WaitApplyFrame(rt.item.index - 1), this::afterSync);
        }

        private FrameCallResult afterSync(Void v) {
            StatusManager statusManager = gc.statusManager;
            statusManager.persistAsync();
            switch (rt.type) {
                case LogItem.TYPE_PREPARE_CONFIG_CHANGE:
                    return doPrepare(rt);
                case LogItem.TYPE_DROP_CONFIG_CHANGE:
                    return gc.memberManager.doAbort(rt.item.index);
                case LogItem.TYPE_COMMIT_CONFIG_CHANGE:
                    return gc.memberManager.doCommit(rt.item.index);
                default:
                    throw Fiber.fatal(new RaftException("unknown config change type"));
            }
        }

        private FrameCallResult doPrepare(RaftTask rt) {
            byte[] data = ((ByteArray) rt.input.body).getData();
            String dataStr = new String(data);
            String[] fields = dataStr.split(";", -1);
            Set<Integer> oldMemberIds = parseSet(fields[0]);
            Set<Integer> oldObserverIds = parseSet(fields[1]);
            Set<Integer> newMemberIds = parseSet(fields[2]);
            Set<Integer> newObserverIds = parseSet(fields[3]);
            if (!oldMemberIds.equals(raftStatus.nodeIdOfMembers)) {
                log.error("oldMemberIds not match, oldMemberIds={}, currentMembers={}, groupId={}",
                        oldMemberIds, raftStatus.nodeIdOfMembers, raftStatus.groupId);
            }
            if (!oldObserverIds.equals(raftStatus.nodeIdOfObservers)) {
                log.error("oldObserverIds not match, oldObserverIds={}, currentObservers={}, groupId={}",
                        oldObserverIds, raftStatus.nodeIdOfObservers, raftStatus.groupId);
            }
            return gc.memberManager.doPrepare(rt.item.index, newMemberIds, newObserverIds);
        }

        private Set<Integer> parseSet(String s) {
            if (s.isEmpty()) {
                return emptySet();
            }
            String[] fields = s.split(",");
            Set<Integer> set = new HashSet<>();
            for (String f : fields) {
                set.add(Integer.parseInt(f));
            }
            return set;
        }
    }

    private class TakeSnapshotFrame extends FiberFrame<Void> {
        private final FiberFuture<Snapshot> snapshotFuture;

        TakeSnapshotFrame() {
            this.snapshotFuture = takeSnapshotRequests.pollFirst();
        }

        @Override
        public FrameCallResult execute(Void v) {
            if (raftStatus.installSnapshot) {
                snapshotFuture.completeExceptionally(new RaftException("install snapshot"));
                return Fiber.frameReturn();
            }
            return Fiber.call(new WaitApplyFrame(raftStatus.lastApplying), this::afterSync);
        }

        private FrameCallResult afterSync(Void v) {
            if (raftStatus.installSnapshot) {
                snapshotFuture.completeExceptionally(new RaftException("install snapshot"));
                return Fiber.frameReturn();
            }
            SnapshotInfo si = new SnapshotInfo(raftStatus);
            FiberFuture<Snapshot> f = stateMachine.takeSnapshot(si);
            return f.await(this::afterTake);
        }

        private FrameCallResult afterTake(Snapshot snapshot) {
            snapshotFuture.complete(snapshot);
            return Fiber.frameReturn();
        }
    }
}
