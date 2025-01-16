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
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftExecTimeoutException;
import com.github.dtprj.dongting.raft.server.RaftInput;
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

    private FiberCondition needApplyCond;
    private boolean waitApply;
    private FiberCondition applyFinishCond;
    private FiberCondition applyMonitorCond;
    private final LinkedList<RaftTask> heartBeatQueue = new LinkedList<>();

    private long initCommitIndex;
    private boolean initFutureComplete = false;

    private final PriorityQueue<Pair<DtTime, CompletableFuture<Long>>> waitReadyQueue;

    private int execCount = 0;

    private final PerfCallback perfCallback;

    private Fiber applyFiber;

    public ApplyManager(GroupComponents gc) {
        this.ts = gc.getRaftStatus().getTs();
        this.raftStatus = gc.getRaftStatus();
        this.gc = gc;
        this.fiberGroup = gc.getFiberGroup();
        this.perfCallback = gc.getGroupConfig().getPerfCallback();
        this.waitReadyQueue = new PriorityQueue<>(this);
    }

    @Override
    public int compare(Pair<DtTime, CompletableFuture<Long>> o1, Pair<DtTime, CompletableFuture<Long>> o2) {
        long diff = o1.getLeft().rest(TimeUnit.NANOSECONDS, ts) - o2.getLeft().rest(TimeUnit.NANOSECONDS, ts);
        return diff < 0 ? -1 : diff > 0 ? 1 : 0;
    }

    public void postInit() {
        this.raftLog = gc.getRaftLog();
        this.stateMachine = gc.getStateMachine();
    }

    public void init(FiberGroup fiberGroup) {
        this.needApplyCond = fiberGroup.newCondition("needApply");
        this.applyFinishCond = fiberGroup.newCondition("applyFinish");
        this.applyMonitorCond = fiberGroup.newCondition("applyMonitor");
        this.initCommitIndex = raftStatus.getCommitIndex();
        startApplyFiber(fiberGroup);
        new Fiber("waitGroupReadyTimeout", fiberGroup, new WaitGroupReadyTimeoutFrame(), true).start();
        new Fiber("applyFiberMonitor", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                if (applyFiber.isFinished() && !shouldStopApply()) {
                    startApplyFiber(fiberGroup);
                }
                return applyMonitorCond.await(1000, this);
            }
        }, true).start();
        if (raftStatus.getLastApplied() >= raftStatus.getCommitIndex()) {
            log.info("apply manager init complete");
            raftStatus.getInitFuture().complete(null);
            this.initFutureComplete = true;
        } else if (raftStatus.isInstallSnapshot()) {
            log.info("install snapshot, apply manager init complete");
            raftStatus.getInitFuture().complete(null);
            this.initFutureComplete = true;
        }
    }

    private void startApplyFiber(FiberGroup fiberGroup) {
        applyFiber = new Fiber("apply", fiberGroup, new ApplyFrame(), true, 50);
        applyFiber.start();
    }

    public void wakeupApply() {
        needApplyCond.signal();
    }

    public void shutdown(DtTime timeout) {
        needApplyCond.signal();
        try {
            // start in InitFiberFrame
            stateMachine.stop(timeout);
        } catch (Throwable e) {
            log.error("state machine stop failed", e);
        }
        processWaitGroupReadyQueue(true, null);
    }

    private FrameCallResult exec(RaftTask rt, long index, FrameCall<Void> resumePoint) {
        raftStatus.setLastApplying(index);
        switch (rt.getType()) {
            case LogItem.TYPE_PREPARE_CONFIG_CHANGE:
            case LogItem.TYPE_DROP_CONFIG_CHANGE:
            case LogItem.TYPE_COMMIT_CONFIG_CHANGE:
                // block apply fiber util config change complete
                return Fiber.call(new ConfigChangeFrame(rt), unused -> {
                    raftStatus.setLastConfigChangeIndex(index);
                    afterExec(index, rt, null, null);
                    return Fiber.resume(null, resumePoint);
                });
            case LogItem.TYPE_NORMAL:
            case LogItem.TYPE_LOG_READ: {
                RaftInput input = rt.getInput();
                if (input.isReadOnly() && rt.getCallback() == null) {
                    // no need to execute read only task if no one wait for result
                    afterExec(index, rt, null, null);
                } else {
                    long t = perfCallback.takeTime(PerfConsts.RAFT_D_STATE_MACHINE_EXEC);
                    FiberFuture<Object> f = null;
                    Throwable execEx = null;
                    try {
                        f = stateMachine.exec(index, input);
                        execCount++;
                    } catch (Throwable e) {
                        execEx = e;
                    } finally {
                        if (input.isReadOnly() && rt.getItem() != null) {
                            // release log read resource as soon as possible
                            rt.getItem().release();
                        }
                    }
                    if (execEx != null) {
                        afterExec(index, rt, null, execEx);
                    } else if (f != null) {
                        f.registerCallback((result, ex) -> {
                            perfCallback.fireTime(PerfConsts.RAFT_D_STATE_MACHINE_EXEC, t);
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

    // if processItemsNotTimeout==true and group should stop, use null as leaseReadIndex
    private void processWaitGroupReadyQueue(boolean processItemsNotTimeout, Long leaseReadIndex) {
        if (waitReadyQueue.isEmpty()) {
            return;
        }
        Iterator<Pair<DtTime, CompletableFuture<Long>>> it = waitReadyQueue.iterator();
        while (it.hasNext()) {
            Pair<DtTime, CompletableFuture<Long>> p = it.next();
            DtTime deadline = p.getLeft();
            CompletableFuture<Long> f = p.getRight();
            if (deadline.isTimeout(ts)) {
                it.remove();
                RaftExecTimeoutException e = new RaftExecTimeoutException("wait group ready timeout: "
                        + deadline.getTimeout(TimeUnit.MILLISECONDS) + "ms");
                completeWaitReadyFuture(f, null, e);
            } else if (processItemsNotTimeout) {
                it.remove();
                if (leaseReadIndex != null) {
                    completeWaitReadyFuture(f, leaseReadIndex, null);
                } else {
                    completeWaitReadyFuture(f, null, new RaftException("group should stop"));
                }
            } else {
                break;
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
                if (t.isTimeout(raftStatus.getTs())) {
                    RaftExecTimeoutException e = new RaftExecTimeoutException("wait group ready timeout: "
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
        if (execEx != null && !rt.getInput().isReadOnly()) {
            throw Fiber.fatal(execEx);
        }
        RaftStatusImpl raftStatus = ApplyManager.this.raftStatus;

        raftStatus.setLastApplied(index);
        raftStatus.setLastAppliedTerm(rt.getItem().getTerm());

        if (!raftStatus.isGroupReady() && index >= raftStatus.getGroupReadyIndex()) {
            raftStatus.setGroupReady(true);
            // copy share status should happen before group ready notifications
            raftStatus.copyShareStatus();
            log.info("{} mark group ready: groupId={}, groupReadyIndex={}",
                    raftStatus.getRole(), raftStatus.getGroupId(), raftStatus.getGroupReadyIndex());
            processWaitGroupReadyQueue(true, index);
        } else {
            raftStatus.copyShareStatus();
        }

        if (!initFutureComplete && index >= initCommitIndex) {
            log.info("apply manager init complete, initCommitIndex={}", initCommitIndex);
            initFutureComplete = true;
            raftStatus.getInitFuture().complete(null);
        }
        if (execEx == null) {
            rt.callSuccess(execResult);
        } else {
            // assert read only
            rt.callFail(execEx);
        }
        if (waitApply) {
            applyFinishCond.signal();
        }
        tryApplyHeartBeat(index);
    }

    private void tryApplyHeartBeat(long appliedIndex) {
        RaftTask t = heartBeatQueue.peekFirst();
        if (t != null && t.getItem().getIndex() == appliedIndex + 1) {
            heartBeatQueue.pollFirst();
            afterExec(appliedIndex + 1, t, null, null);
        }
    }

    private boolean shouldStopApply() {
        return raftStatus.isInstallSnapshot() || fiberGroup.isShouldStop();
    }

    public Fiber getApplyFiber() {
        return applyFiber;
    }

    public void signalStartApply() {
        applyMonitorCond.signal();
    }

    private class ApplyFrame extends FiberFrame<Void> {

        private RaftLog.LogIterator logIterator;

        private final TailCache tailCache = raftStatus.getTailCache();

        @Override
        protected FrameCallResult handle(Throwable ex) {
            log.error("apply failed", ex);
            return Fiber.frameReturn();
        }

        @Override
        protected FrameCallResult doFinally() {
            log.info("apply fiber exit: groupId={}", raftStatus.getGroupId());
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
            long diff = raftStatus.getCommitIndex() - raftStatus.getLastApplying();
            if (diff == 0) {
                return needApplyCond.await(this);
            }
            long index = raftStatus.getLastApplying() + 1;
            RaftTask rt = tailCache.get(index);
            if (rt == null) {
                int limit = (int) Math.min(diff, 1024L);
                if (log.isDebugEnabled()) {
                    log.debug("load from {}, diff={}, limit={}, cacheSize={}, cacheFirstIndex={},commitIndex={},lastApplying={}",
                            index, diff, limit, tailCache.size(), tailCache.getFirstIndex(),
                            raftStatus.getCommitIndex(), raftStatus.getLastApplying());
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
            this.items = items;
        }

        @Override
        protected FrameCallResult doFinally() {
            RaftUtil.release(items);
            return Fiber.frameReturn();
        }

        @Override
        public FrameCallResult execute(Void input) {
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
            RaftTask rt = buildRaftTask(item);
            return exec(rt, item.getIndex(), this);
        }

        private RaftTask buildRaftTask(LogItem item) {
            RaftInput input = new RaftInput(item.getBizType(), item.getHeader(), item.getBody(), null,
                    item.getType() == LogItem.TYPE_LOG_READ);
            RaftTask result = new RaftTask(ts, item.getType(), input, null);
            result.setItem(item);
            return result;
        }
    }

    private class ConfigChangeFrame extends FiberFrame<Void> {

        private final RaftTask rt;

        private ConfigChangeFrame(RaftTask rt) {
            this.rt = rt;
        }

        // no error handler, use ApplyFrame process ex and retry

        @Override
        public FrameCallResult execute(Void input) {
            if (raftStatus.getLastApplied() != rt.getItem().getIndex() - 1) {
                waitApply = true;
                return applyFinishCond.await(this::afterApplyFinish);
            }
            return afterApplyFinish(null);
        }

        private FrameCallResult afterApplyFinish(Void unused) {
            waitApply = false;
            StatusManager statusManager = gc.getStatusManager();
            statusManager.persistAsync(true);
            switch (rt.getType()) {
                case LogItem.TYPE_PREPARE_CONFIG_CHANGE:
                    return doPrepare(rt);
                case LogItem.TYPE_DROP_CONFIG_CHANGE:
                    return gc.getMemberManager().doAbort();
                case LogItem.TYPE_COMMIT_CONFIG_CHANGE:
                    return gc.getMemberManager().doCommit();
                default:
                    throw Fiber.fatal(new RaftException("unknown config change type"));
            }
        }

        private FrameCallResult doPrepare(RaftTask rt) {
            byte[] data = ((ByteArray) rt.getInput().getBody()).getData();
            String dataStr = new String(data);
            String[] fields = dataStr.split(";", -1);
            Set<Integer> oldMemberIds = parseSet(fields[0]);
            Set<Integer> oldObserverIds = parseSet(fields[1]);
            Set<Integer> newMemberIds = parseSet(fields[2]);
            Set<Integer> newObserverIds = parseSet(fields[3]);
            if (!oldMemberIds.equals(raftStatus.getNodeIdOfMembers())) {
                log.error("oldMemberIds not match, oldMemberIds={}, currentMembers={}, groupId={}",
                        oldMemberIds, raftStatus.getNodeIdOfMembers(), raftStatus.getGroupId());
            }
            if (!oldObserverIds.equals(raftStatus.getNodeIdOfObservers())) {
                log.error("oldObserverIds not match, oldObserverIds={}, currentObservers={}, groupId={}",
                        oldObserverIds, raftStatus.getNodeIdOfObservers(), raftStatus.getGroupId());
            }
            return gc.getMemberManager().doPrepare(newMemberIds, newObserverIds);
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

    private class WaitGroupReadyTimeoutFrame extends FiberFrame<Void> {
        @Override
        public FrameCallResult execute(Void input) {
            processWaitGroupReadyQueue(false, null);
            return Fiber.sleep(100, this);
        }
    }
}
