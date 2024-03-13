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

import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.Decoder;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftExecTimeoutException;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftOutput;
import com.github.dtprj.dongting.raft.sm.StateMachine;
import com.github.dtprj.dongting.raft.store.RaftLog;
import com.github.dtprj.dongting.raft.store.StatusManager;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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

    private final DecodeContext decodeContext;

    private boolean configChanging = false;

    private FiberCondition condition;

    private long initCommitIndex;
    private boolean initCallbackRun = false;
    private Runnable initCallback;

    public ApplyManager(GroupComponents gc) {
        this.ts = gc.getRaftStatus().getTs();
        this.raftStatus = gc.getRaftStatus();
        this.gc = gc;
        this.decodeContext = new DecodeContext();
        this.decodeContext.setHeapPool(gc.getGroupConfig().getHeapPool());
    }

    public void postInit() {
        this.raftLog = gc.getRaftLog();
        this.stateMachine = gc.getStateMachine();
    }

    public void init(FiberGroup fiberGroup, Runnable initCallback) {
        this.condition = fiberGroup.newCondition("needApply");
        this.initCallback = initCallback;
        this.initCommitIndex = raftStatus.getCommitIndex();
        startApplyFiber(fiberGroup);
        if (raftStatus.getLastApplied() >= raftStatus.getCommitIndex()) {
            initCallback.run();
            initCallbackRun = true;
        }
    }

    private void startApplyFiber(FiberGroup fiberGroup) {
        Fiber f = new Fiber("apply", fiberGroup, new ApplyFrame());
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
            Object r = stateMachine.exec(index, input);
            future.complete(new RaftOutput(index, r));
        } catch (Throwable e) {
            log.error("exec read failed. {}", e);
            future.completeExceptionally(e);
        } finally {
            // for read task, no LogItem generated
            RaftUtil.release(input);
        }
    }

    private void execNormalWrite(long index, RaftTask rt) {
        try {
            RaftInput input = rt.getInput();
            Object r = stateMachine.exec(index, input);
            rt.getFuture().complete(new RaftOutput(index, r));
        } catch (Throwable ex) {
            if (rt.getFuture() != null) {
                rt.getFuture().completeExceptionally(ex);
            }
            throw Fiber.fatal(new RaftException("exec write failed.", ex));
        } finally {
            rt.getItem().release();
        }
    }

    private void execReaders(long index, RaftTask rt) {
        RaftTask nextReader = rt.getNextReader();
        while (nextReader != null) {
            execRead(index, nextReader);
            nextReader = nextReader.getNextReader();
        }
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
            RaftTask reader = raftStatus.getTailCache().get(item.getIndex());
            if (reader != null) {
                if (reader.getInput().isReadOnly()) {
                    result.setNextReader(reader);
                } else {
                    BugLog.getLog().error("not read only");
                }
            }
            return result;
        } finally {
            decodeContext.reset();
        }
    }

    public void apply() {
        condition.signal();
    }

    private FrameCallResult doPrepare(RaftTask rt) {
        configChanging = true;

        ByteBuffer logData = (ByteBuffer) rt.getInput().getBody();
        byte[] data = new byte[logData.remaining()];
        logData.get(data);
        return gc.getMemberManager().doPrepare1(data);
    }

    private FrameCallResult doAbort() {
        try {
            return gc.getMemberManager().doAbort();
        } finally {
            configChanging = false;
        }
    }

    private FrameCallResult doCommit() {
        try {
            if (!configChanging) {
                log.warn("no prepared config change, ignore commit, groupId={}", raftStatus.getGroupId());
                return Fiber.frameReturn();
            }

            return gc.getMemberManager().doCommit();
        } finally {
            configChanging = false;
        }
    }

    private void postConfigChange(long index, RaftTask rt) {
        raftStatus.setLastConfigChangeIndex(index);
        if (rt.getFuture() != null) {
            rt.getFuture().complete(new RaftOutput(index, null));
        }
    }

    private class ApplyFrame extends FiberFrame<Void> {

        private RaftLog.LogIterator logIterator;

        private int taskIndex;
        private final ArrayList<RaftTask> taskList = new ArrayList<>();

        @Override
        protected FrameCallResult handle(Throwable ex) {
            log.error("apply failed", ex);
            if (!isGroupShouldStopPlain()) {
                startApplyFiber(getFiberGroup());
            }
            return Fiber.frameReturn();
        }

        @Override
        public FrameCallResult execute(Void input) {
            long appliedIndex = raftStatus.getLastApplied();
            long diff = raftStatus.getCommitIndex() - appliedIndex;
            TailCache tailCache = raftStatus.getTailCache();
            taskList.clear();
            taskIndex = 0;
            while (diff > 0) {
                long index = appliedIndex + 1;
                RaftTask rt = tailCache.get(index);
                if (rt == null || rt.getInput().isReadOnly()) {
                    int limit = (int) Math.min(diff, 1024L);
                    if (logIterator == null) {
                        logIterator = raftLog.openIterator(null);
                    }
                    int stateMachineEpoch = raftStatus.getStateMachineEpoch();
                    FiberFrame<List<LogItem>> ff = logIterator.next(index, limit, 16 * 1024 * 1024);
                    return Fiber.call(ff, items -> afterLoad(items, stateMachineEpoch));
                } else {
                    closeIterator();
                    rt.getItem().retain();
                    taskList.add(rt);
                    appliedIndex++;
                    diff--;
                }
            }
            return exec(null);
        }

        private FrameCallResult afterLoad(List<LogItem> items, int stateMachineEpoch) {
            if (stateMachineEpoch != raftStatus.getStateMachineEpoch()) {
                log.warn("stateMachineEpoch changed, ignore load result");
                RaftUtil.release(items);
                return Fiber.frameReturn();
            }
            if (items == null || items.isEmpty()) {
                log.error("load log failed, items is null");
                return Fiber.frameReturn();
            }
            //noinspection ForLoopReplaceableByForEach
            for (int i = 0, readCount = items.size(); i < readCount; i++) {
                LogItem item = items.get(i);
                RaftTask rt = buildRaftTask(item);
                taskList.add(rt);
            }
            return exec(null);
        }

        private FrameCallResult exec(Void v) {
            if (taskIndex >= taskList.size()) {
                // loop execute
                return Fiber.resume(null, this);
            }
            RaftTask rt = taskList.get(taskIndex);
            long logIndex = rt.getItem().getIndex();
            switch (rt.getType()) {
                case LogItem.TYPE_PREPARE_CONFIG_CHANGE:
                case LogItem.TYPE_DROP_CONFIG_CHANGE:
                case LogItem.TYPE_COMMIT_CONFIG_CHANGE:
                    return Fiber.call(new ConfigChangeFrame(rt), unused -> afterExec(true, logIndex, rt));
                case LogItem.TYPE_NORMAL:
                    execNormalWrite(logIndex, rt);
                    // not break here
                case LogItem.TYPE_HEARTBEAT:
                default:
                    return this.afterExec(false, logIndex, rt);
            }
        }

        private FrameCallResult afterExec(boolean configChange, long index, RaftTask rt) {
            RaftStatusImpl raftStatus = ApplyManager.this.raftStatus;

            raftStatus.setLastApplied(index);

            if (raftStatus.getFirstCommitOfApplied() != null && index >= raftStatus.getFirstIndexOfCurrentTerm()) {
                raftStatus.getFirstCommitOfApplied().complete(null);
                raftStatus.setFirstCommitOfApplied(null);
            }

            if (configChange) {
                postConfigChange(index, rt);
            }

            if (!initCallbackRun && index >= initCommitIndex) {
                initCallbackRun = true;
                initCallback.run();
            }
            raftStatus.copyShareStatus();

            execReaders(index, rt);

            // release reader memory
            rt.setNextReader(null);

            // resume loop
            return Fiber.resume(null, this::exec);
        }

        public void closeIterator() {
            if (logIterator != null) {
                DtUtil.close(logIterator);
                logIterator = null;
            }
        }
    }

    private class ConfigChangeFrame extends FiberFrame<Void> {

        private final RaftTask rt;

        private ConfigChangeFrame(RaftTask rt) {
            this.rt = rt;
        }

        @Override
        protected FrameCallResult handle(Throwable ex) {
            if (rt.getFuture() != null) {
                rt.getFuture().completeExceptionally(ex);
            }
            throw Fiber.fatal(new RaftException("exec write failed.", ex));
        }

        @Override
        public FrameCallResult execute(Void input) {
            StatusManager statusManager = gc.getStatusManager();
            statusManager.persistAsync(true);
            return statusManager.waitForce(this::afterPersist);
        }

        private FrameCallResult afterPersist(Void unused) {
            switch (rt.getType()) {
                case LogItem.TYPE_PREPARE_CONFIG_CHANGE:
                    return doPrepare(rt);
                case LogItem.TYPE_DROP_CONFIG_CHANGE:
                    return doAbort();
                case LogItem.TYPE_COMMIT_CONFIG_CHANGE:
                    return doCommit();
                default:
                    throw Fiber.fatal(new RaftException("unknown config change type"));
            }
        }
    }
}
