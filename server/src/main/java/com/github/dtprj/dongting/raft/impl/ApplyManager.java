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

import com.github.dtprj.dongting.codec.ByteArrayEncoder;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.PerfCallback;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCall;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.PerfConsts;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftExecTimeoutException;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftOutput;
import com.github.dtprj.dongting.raft.sm.StateMachine;
import com.github.dtprj.dongting.raft.store.RaftLog;
import com.github.dtprj.dongting.raft.store.StatusManager;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptySet;

/**
 * @author huangli
 */
public class ApplyManager {
    private static final DtLog log = DtLogs.getLogger(ApplyManager.class);

    private final GroupComponents gc;
    private final Timestamp ts;
    private final RaftStatusImpl raftStatus;

    private RaftLog raftLog;
    private StateMachine stateMachine;

    private FiberCondition condition;

    private long initCommitIndex;
    private boolean initFutureComplete = false;

    private int execCount = 0;

    private final PerfCallback perfCallback;

    public ApplyManager(GroupComponents gc) {
        this.ts = gc.getRaftStatus().getTs();
        this.raftStatus = gc.getRaftStatus();
        this.gc = gc;
        this.perfCallback = gc.getGroupConfig().getPerfCallback();
    }

    public void postInit() {
        this.raftLog = gc.getRaftLog();
        this.stateMachine = gc.getStateMachine();
    }

    public void init(FiberGroup fiberGroup) {
        this.condition = fiberGroup.newCondition("needApply");
        this.initCommitIndex = raftStatus.getCommitIndex();
        startApplyFiber(fiberGroup);
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
        Fiber f = new Fiber("apply", fiberGroup, new ApplyFrame(), false, 50);
        f.start();
    }

    public void execRead(long index, RaftTask rt) {
        RaftInput input = rt.getInput();
        CompletableFuture<RaftOutput> future = rt.getFuture();
        try {
            if (input.getDeadline() != null && input.getDeadline().isTimeout(ts)) {
                future.completeExceptionally(new RaftExecTimeoutException("timeout "
                        + input.getDeadline().getTimeout(TimeUnit.MILLISECONDS) + "ms"));
            }
            long t = perfCallback.takeTime(PerfConsts.RAFT_D_STATE_MACHINE_EXEC);
            Object r = stateMachine.exec(index, rt.getItem().getTerm(), input);
            execCount++;
            perfCallback.fireTime(PerfConsts.RAFT_D_STATE_MACHINE_EXEC, t);
            future.complete(new RaftOutput(index, r));
        } catch (Throwable e) {
            log.error("exec read failed. {}", e);
            future.completeExceptionally(e);
        } finally {
            // for read task, no LogItem generated
            RaftUtil.release(input);
        }
    }

    private Object execNormalWrite(long index, RaftTask rt) {
        try {
            RaftInput input = rt.getInput();
            long t = perfCallback.takeTime(PerfConsts.RAFT_D_STATE_MACHINE_EXEC);
            Object r = stateMachine.exec(index, rt.getItem().getTerm(), input);
            execCount++;
            perfCallback.fireTime(PerfConsts.RAFT_D_STATE_MACHINE_EXEC, t);
            return r;
        } catch (Throwable ex) {
            throw Fiber.fatal(new RaftException("exec write failed.", ex));
        }
    }

    private void execReaders(long index, RaftTask rt) {
        RaftTask nextReader = rt.getNextReader();
        while (nextReader != null) {
            execRead(index, nextReader);
            nextReader = nextReader.getNextReader();
        }
    }

    public void apply() {
        condition.signal();
    }

    public void shutdown(DtTime timeout) {
        condition.signal();
        try {
            stateMachine.stop(timeout);
        } catch (Throwable e) {
            log.error("state machine stop failed", e);
        }
    }

    private FrameCallResult exec(RaftTask rt, long index, FrameCall<Void> resumePoint) {
        return switch (rt.getType()) {
            case LogItem.TYPE_PREPARE_CONFIG_CHANGE,
                 LogItem.TYPE_DROP_CONFIG_CHANGE,
                 LogItem.TYPE_COMMIT_CONFIG_CHANGE ->
                    Fiber.call(new ConfigChangeFrame(rt), unused -> afterExec(
                            true, index, rt, null, resumePoint));
            case LogItem.TYPE_NORMAL -> {
                Object r = execNormalWrite(index, rt);
                yield this.afterExec(false, index, rt, r, resumePoint);
            }
            // heart beat, no need to exec
            default -> this.afterExec(false, index, rt, null, resumePoint);
        };
    }

    private FrameCallResult afterExec(boolean configChange, long index, RaftTask rt, Object execResult,
                                      FrameCall<Void> resumePoint) {
        RaftStatusImpl raftStatus = ApplyManager.this.raftStatus;

        raftStatus.setLastApplied(index);
        raftStatus.setLastAppliedTerm(rt.getItem().getTerm());

        if (configChange) {
            raftStatus.setLastConfigChangeIndex(index);
        }

        // copy share status should happen before group ready future and raft task future complete
        if (raftStatus.getGroupReadyFuture() != null && index >= raftStatus.getGroupReadyIndex()) {
            CompletableFuture<Void> f = raftStatus.getGroupReadyFuture();
            raftStatus.setGroupReadyFuture(null);
            raftStatus.copyShareStatus();
            f.complete(null);
            log.info("{} mark group ready future complete: groupId={}, groupReadyIndex={}",
                    raftStatus.getRole(), raftStatus.getGroupId(), raftStatus.getGroupReadyIndex());
        } else {
            raftStatus.copyShareStatus();
        }

        if (!initFutureComplete && index >= initCommitIndex) {
            log.info("apply manager init complete, initCommitIndex={}", initCommitIndex);
            initFutureComplete = true;
            raftStatus.getInitFuture().complete(null);
        }
        if (rt.getFuture() != null) {
            rt.getFuture().complete(new RaftOutput(index, execResult));
        }

        execReaders(index, rt);

        // release reader memory
        rt.setNextReader(null);

        // resume loop
        return Fiber.resume(null, resumePoint);
    }

    private class ApplyFrame extends FiberFrame<Void> {

        private RaftLog.LogIterator logIterator;

        private final TailCache tailCache = raftStatus.getTailCache();

        @Override
        protected FrameCallResult handle(Throwable ex) {
            log.error("apply failed", ex);
            if (!isGroupShouldStopPlain()) {
                return Fiber.sleepUntilShouldStop(1000, this::restartApplyFiber);
            } else {
                return Fiber.frameReturn();
            }
        }

        private FrameCallResult restartApplyFiber(Void unused) {
            if (!isGroupShouldStopPlain()) {
                startApplyFiber(getFiberGroup());
            }
            return Fiber.frameReturn();
        }

        @Override
        protected FrameCallResult doFinally() {
            closeIterator();
            return Fiber.frameReturn();
        }

        @Override
        public FrameCallResult execute(Void input) {
            if (isGroupShouldStopPlain()) {
                return Fiber.frameReturn();
            }
            if (raftStatus.isInstallSnapshot()) {
                return Fiber.sleepUntilShouldStop(10, this);
            }
            execCount = 1;
            return execLoop(null);
        }

        private FrameCallResult execLoop(Void v) {
            if (execCount >= 100) {
                return Fiber.yield(this);
            }
            RaftStatusImpl raftStatus = ApplyManager.this.raftStatus;
            long diff = raftStatus.getCommitIndex() - raftStatus.getLastApplied();
            if (diff == 0) {
                return condition.await(this);
            }
            long index = raftStatus.getLastApplied() + 1;
            RaftTask rt = tailCache.get(index);
            if (rt == null || rt.getInput().isReadOnly()) {
                int limit = (int) Math.min(diff, 1024L);
                if (log.isDebugEnabled()) {
                    log.debug("load from {}, diff={}, limit={}, cacheSize={}, cacheFirstIndex={},commitIndex={},applyIndex={}",
                            index, diff, limit, tailCache.size(), tailCache.getFirstIndex(),
                            raftStatus.getCommitIndex(), raftStatus.getLastApplied());
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
            if (isGroupShouldStopPlain()) {
                return Fiber.frameReturn();
            }
            if (raftStatus.isInstallSnapshot()) {
                log.warn("install snapshot, ignore load result");
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
            RaftInput input = new RaftInput(item.getBizType(), item.getHeader(), item.getBody(), null);
            RaftTask result = new RaftTask(ts, item.getType(), input, null);
            result.setItem(item);
            RaftTask reader = raftStatus.getTailCache().get(item.getIndex());
            if (reader != null) {
                if (reader.getInput().isReadOnly()) {
                    result.setNextReader(reader);
                } else {
                    BugLog.getLog().error("not read only");
                }
            }
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
            StatusManager statusManager = gc.getStatusManager();
            statusManager.persistAsync(true);
            return statusManager.waitUpdateFinish(this::afterPersist);
        }

        private FrameCallResult afterPersist(Void unused) {
            return switch (rt.getType()) {
                case LogItem.TYPE_PREPARE_CONFIG_CHANGE -> doPrepare(rt);
                case LogItem.TYPE_DROP_CONFIG_CHANGE -> gc.getMemberManager().doAbort();
                case LogItem.TYPE_COMMIT_CONFIG_CHANGE -> gc.getMemberManager().doCommit();
                default -> throw Fiber.fatal(new RaftException("unknown config change type"));
            };
        }

        private FrameCallResult doPrepare(RaftTask rt) {
            byte[] data = ((ByteArrayEncoder) rt.getInput().getBody()).getData();
            String dataStr = new String(data);
            String[] fields = dataStr.split(";");
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
}
