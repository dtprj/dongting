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
import com.github.dtprj.dongting.common.FlowControlException;
import com.github.dtprj.dongting.common.PerfCallback;
import com.github.dtprj.dongting.common.PerfConsts;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberChannel;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.fiber.HandlerFrame;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.RaftTimeoutException;
import com.github.dtprj.dongting.raft.server.NotLeaderException;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftReqData;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.store.LogHeader;
import com.github.dtprj.dongting.raft.store.RaftLog;

import java.util.ArrayList;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
public class LinearTaskRunner {

    private static final DtLog log = DtLogs.getLogger(LinearTaskRunner.class);

    private final RaftServerConfig serverConfig;
    private final RaftGroupConfigEx groupConfig;
    private final RaftStatusImpl raftStatus;
    private final GroupComponents gc;
    private RaftLog raftLog;

    private final Timestamp ts;

    private FiberChannel<RaftTask> taskChannel;

    private final PerfCallback perfCallback;

    private final CRC32C crc32c = new CRC32C();
    private final ByteBuffer crcBuf = ByteBuffer.allocate(LogHeader.ITEM_HEADER_SIZE - 4);

    public LinearTaskRunner(GroupComponents gc) {
        this.gc = gc;
        this.serverConfig = gc.serverConfig;
        this.groupConfig = gc.groupConfig;
        this.raftStatus = gc.raftStatus;
        this.ts = raftStatus.ts;
        this.perfCallback = gc.groupConfig.perfCallback;
    }

    public void postInit() {
        this.raftLog = gc.raftLog;
    }

    public void init(FiberChannel<RaftTask> taskChannel) {
        this.taskChannel = taskChannel;
        Fiber f = new Fiber("linearTaskRunner", groupConfig.fiberGroup,
                new RunnerFrame(), false, 50);
        f.start();
    }

    private class RunnerFrame extends FiberFrame<Void> {
        private final ArrayList<RaftTask> list = new ArrayList<>();

        @Override
        protected FrameCallResult handle(Throwable ex) {
            log.error("error in linear task runner", ex);
            throw Fiber.fatal(ex);
        }

        @Override
        public FrameCallResult execute(Void input) {
            list.clear();
            return taskChannel.takeAll(list, serverConfig.heartbeatInterval,
                    true, this::afterTakeAll);
        }

        private FrameCallResult afterTakeAll(Void unused) {
            if (isGroupShouldStopPlain()) {
                for (RaftTask rt : list) {
                    rt.reqData.release();
                    rt.callFail(new RaftException("raft group is stopping"));
                }
                taskChannel.markShutdown();
                // fiber exit
                return Fiber.frameReturn();
            }
            if (raftStatus.transferLeaderCondition != null) {
                FiberFrame<Void> f = new FiberFrame<>() {
                    @Override
                    public FrameCallResult execute(Void input) {
                        return raftStatus.transferLeaderCondition.await(this::justReturn);
                    }
                };
                // transfer leader future may complete exceptionally.
                // use HandlerFrame to catch it, but no need to handle it
                return Fiber.call(new HandlerFrame<>(f), p -> afterTakeAll(null));
            }
            if (!list.isEmpty()) {
                return Fiber.call(raftExec(list), this);
            } else if (raftStatus.getRole() == RaftRole.leader) {
                return Fiber.call(raftExec(Collections.singletonList(createHeartBeatInput())), this);
            }
            // loop
            return Fiber.resume(null, this);
        }
    }

    private static void onDispatchFail(RaftTask rt) {
        rt.reqData.release();
        rt.callFail(new RaftException("submit raft task failed, the fiber group is not running"));
    }

    public void submitRaftTaskInBizThread(RaftTask task) {
        task.perfTime = perfCallback.takeTime(PerfConsts.RAFT_D_LEADER_RUNNER_FIBER_LATENCY);
        taskChannel.fireOffer(task, LinearTaskRunner::onDispatchFail);
    }

    public static long lastIndex(RaftStatusImpl raftStatus) {
        TailCache tailCache = raftStatus.tailCache;
        if (tailCache.size() == 0) {
            log.info("tail cache is empty, use last log index {}", raftStatus.lastLogIndex);
            return raftStatus.lastLogIndex;
        } else {
            return tailCache.getLastIndex();
        }
    }

    public FiberFrame<Void> raftExec(List<RaftTask> inputs) {
        RaftStatusImpl raftStatus = this.raftStatus;
        if (raftStatus.getRole() != RaftRole.leader) {
            for (RaftTask t : inputs) {
                t.reqData.release();
                t.callFail(new NotLeaderException(raftStatus.getCurrentLeaderNode()));
            }
            return FiberFrame.voidCompletedFrame();
        }
        long newIndex = lastIndex(raftStatus);

        // Here, we refreshed the ts. Next, the time t set on RaftTask is greater than the time when the raft
        // client constructs the request. Since there is a 1ms error in the ts refresh, the time when the raft
        // client constructs the request happens before (t + 1ms).
        ts.refresh(1);

        int prevTerm = raftStatus.lastLogTerm;
        int currentTerm = raftStatus.currentTerm;
        ArrayList<RaftTask> filterInputs = null;
        for (int len = inputs.size(), i = 0; i < len; i++) {
            RaftTask rt = inputs.get(i);
            if (rt.perfTime != 0) {
                perfCallback.fireTime(PerfConsts.RAFT_D_LEADER_RUNNER_FIBER_LATENCY, rt.perfTime);
            }

            Throwable ex = checkTask(rt, raftStatus);
            if (ex != null) {
                rt.reqData.release();
                rt.callFail(ex);
                if (filterInputs == null) {
                    filterInputs = new ArrayList<>(inputs.size());
                    for (int j = 0; j < i; j++) {
                        filterInputs.add(inputs.get(j));
                    }
                }
                continue;
            }

            newIndex++;

            LogHeader lh = rt.logHeader;
            lh.term = currentTerm;
            lh.prevLogTerm = prevTerm;
            lh.index = newIndex;
            lh.timestamp = ts.wallClockMillis;
            if (lh.type == LogHeader.TYPE_LOG_READ) {
                lh.setLens(0, 0);
            } else {
                lh.setLens(rt.reqData.bizHeaderSize, rt.reqData.bizBodySize);
            }
            lh.computeCrc(crc32c, crcBuf);
            rt.init(ts.nanoTime);
            prevTerm = currentTerm;

            if (filterInputs != null) {
                filterInputs.add(rt);
            }
        }

        return append(raftStatus, filterInputs == null ? inputs : filterInputs);
    }

    private Throwable checkTask(RaftTask rt, RaftStatusImpl raftStatus) {
        if (rt.deadline != null && rt.deadline.isTimeout(ts)) {
            return new RaftTimeoutException("timeout " + rt.deadline.getTimeout(TimeUnit.MILLISECONDS) + "ms");
        }
        if (rt.logHeader.type == LogHeader.TYPE_NORMAL || rt.logHeader.type == LogHeader.TYPE_LOG_READ) {
            if (raftStatus.tailCache.pendingCount >= groupConfig.maxPendingTasks) {
                log.warn("reject task, pendingRequests={}, maxPendingRaftTasks={}",
                        raftStatus.tailCache.pendingCount, groupConfig.maxPendingTasks);
                return new FlowControlException("max pending tasks reached: " + groupConfig.maxPendingTasks);
            }
            if (raftStatus.tailCache.pendingBytes >= groupConfig.maxPendingTaskBytes) {
                log.warn("reject task, pendingBytes={}, maxPendingTaskBytes={}",
                        raftStatus.tailCache.pendingBytes, groupConfig.maxPendingTaskBytes);
                return new FlowControlException("max pending bytes reached: " + groupConfig.maxPendingTaskBytes);
            }
        }
        return null;
    }

    public FiberFrame<Void> append(RaftStatusImpl raftStatus, List<RaftTask> inputs) {
        TailCache tailCache = raftStatus.tailCache;
        for (int len = inputs.size(), i = 0; i < len; i++) {
            RaftTask rt = inputs.get(i);
            long index = rt.logHeader.index;

            // successful change owner to TailCache and release in TailCache.release(RaftTask)
            tailCache.put(index, rt);

            if (i == len - 1) {
                raftStatus.lastLogIndex = index;
                raftStatus.lastLogTerm = rt.logHeader.term;
            }
        }
        raftStatus.needRepCondition.signalAll();
        return raftLog.append(inputs);
    }

    private RaftTask createHeartBeatInput() {
        DtTime deadline = new DtTime(ts, raftStatus.getElectTimeoutNanos(), TimeUnit.NANOSECONDS);
        RaftReqData reqData = new RaftReqData(null, 0, null, 0);
        return new RaftTask(LogHeader.TYPE_HEARTBEAT, 0, reqData,
                null, null, deadline, false, null);
    }

    public void issueHeartBeat() {
        submitRaftTaskInBizThread(createHeartBeatInput());
    }

}
