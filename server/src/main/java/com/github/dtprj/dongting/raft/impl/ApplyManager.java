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
import com.github.dtprj.dongting.common.RefCount;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftExecTimeoutException;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftOutput;
import com.github.dtprj.dongting.raft.sm.StateMachine;
import com.github.dtprj.dongting.raft.store.RaftLog;
import com.github.dtprj.dongting.raft.store.StatusManager;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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
    private final RaftStatusImpl raftStatus;
    private final StatusManager statusManager;

    private final DecodeContext decodeContext;

    private boolean configChanging = false;

    private FiberCondition condition;

    public ApplyManager(int selfNodeId, RaftLog raftLog, StateMachine stateMachine,
                        RaftStatusImpl raftStatus, EventBus eventBus,
                        RefBufferFactory heapPool, StatusManager statusManager) {
        this.selfNodeId = selfNodeId;
        this.raftLog = raftLog;
        this.stateMachine = stateMachine;
        this.ts = raftStatus.getTs();
        this.raftStatus = raftStatus;
        this.eventBus = eventBus;
        this.decodeContext = new DecodeContext();
        this.decodeContext.setHeapPool(heapPool);
        this.statusManager = statusManager;
    }

    public void init(FiberGroup fiberGroup) {
        condition = fiberGroup.newCondition("needApply");
        startApplyFiber(fiberGroup);
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
            if (input.getHeader() instanceof RefCount) {
                ((RefCount) input.getHeader()).release();
            }
            if (input.getBody() instanceof RefCount) {
                ((RefCount) input.getBody()).release();
            }
        }
    }

    public void apply() {
        condition.signal();
    }

    private class ApplyFrame extends FiberFrame<Void> {

        private RaftLog.LogIterator logIterator;

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
                    execChain(index, rt);
                    appliedIndex++;
                    diff--;
                }
            }
            return Fiber.frameReturn();
        }

        private FrameCallResult afterLoad(List<LogItem> items, int stateMachineEpoch) {
            try {
                if (stateMachineEpoch != raftStatus.getStateMachineEpoch()) {
                    log.warn("stateMachineEpoch changed, ignore load result");
                    return Fiber.frameReturn();
                }
                if (items == null || items.isEmpty()) {
                    log.error("load log failed, items is null");
                    return Fiber.frameReturn();
                }
                int readCount = items.size();
                //noinspection ForLoopReplaceableByForEach
                for (int i = 0; i < readCount; i++) {
                    LogItem item = items.get(i);
                    RaftTask rt = buildRaftTask(item);
                    execChain(item.getIndex(), rt);
                }
                return Fiber.frameReturn();
            } finally {
                RaftUtil.release(items);
            }
        }

        public void closeIterator() {
            if (logIterator != null) {
                DtUtil.close(logIterator);
                logIterator = null;
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

        private void execChain(long index, RaftTask rt) {
            // TODO
        }
    }

}
