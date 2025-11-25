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
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.NotLeaderException;
import com.github.dtprj.dongting.raft.server.RaftCallback;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.store.RaftLog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
                    RaftUtil.release(rt.input);
                    rt.callFail(new RaftException("raft group is stopping"));
                }
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
                RaftInput input = createHeartBeatInput();
                RaftTask task = new RaftTask(LogItem.TYPE_HEARTBEAT, input, null);
                return Fiber.call(raftExec(Collections.singletonList(task)), this);
            }
            // loop
            return Fiber.resume(null, this);
        }
    }

    public void submitRaftTaskInBizThread(int raftLogType, RaftInput input, RaftCallback callback) {
        RaftTask t = new RaftTask(raftLogType, input, callback);
        input.setPerfTime(perfCallback.takeTime(PerfConsts.RAFT_D_LEADER_RUNNER_FIBER_LATENCY));
        if (!taskChannel.fireOffer(t, true)) {
            RaftUtil.release(input);
            t.callFail(new RaftException("submit raft task failed"));
        }
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
                RaftUtil.release(t.input);
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
        for (int len = inputs.size(), i = 0; i < len; i++) {
            RaftTask rt = inputs.get(i);
            RaftInput input = rt.input;
            if (input.getPerfTime() != 0) {
                perfCallback.fireTime(PerfConsts.RAFT_D_LEADER_RUNNER_FIBER_LATENCY, input.getPerfTime());
            }

            Throwable ex = checkTask(rt, raftStatus);
            if (ex != null) {
                RaftUtil.release(input);
                rt.callFail(ex);
                // not removed from list, filter in append()
                continue;
            }

            newIndex++;
            LogItem item = new LogItem();
            item.setType(rt.type);
            item.setBizType(input.getBizType());
            item.setTerm(currentTerm);
            item.setIndex(newIndex);
            item.setPrevLogTerm(prevTerm);
            prevTerm = currentTerm;
            item.setTimestamp(ts.wallClockMillis);

            item.setHeader(input.getHeader(), input.isHeadReleasable());
            item.setBody(input.getBody(), input.isBodyReleasable());

            rt.init(item, ts.nanoTime);

            // decrease in ApplyManager
            raftStatus.pendingRequests++;
            raftStatus.pendingBytes += input.getFlowControlSize();
        }

        return append(raftStatus, inputs);
    }

    private Throwable checkTask(RaftTask rt, RaftStatusImpl raftStatus) {
        RaftInput input = rt.input;
        if (input.getDeadline() != null && input.getDeadline().isTimeout(ts)) {
            return new RaftTimeoutException("timeout " + input.getDeadline().getTimeout(TimeUnit.MILLISECONDS) + "ms");
        }
        if (rt.type == LogItem.TYPE_NORMAL || rt.type == LogItem.TYPE_LOG_READ) {
            if (raftStatus.pendingRequests >= groupConfig.maxPendingTasks) {
                log.warn("reject task, pendingRequests={}, maxPendingRaftTasks={}",
                        raftStatus.pendingRequests, groupConfig.maxPendingTasks);
                return new FlowControlException("max pending tasks reached: " + groupConfig.maxPendingTasks);
            }
            if (raftStatus.pendingBytes >= groupConfig.maxPendingTaskBytes) {
                log.warn("reject task, pendingBytes={}, maxPendingTaskBytes={}",
                        raftStatus.pendingBytes, groupConfig.maxPendingTaskBytes);
                return new FlowControlException("max pending bytes reached: " + groupConfig.maxPendingTaskBytes);
            }
        }
        return null;
    }

    public FiberFrame<Void> append(RaftStatusImpl raftStatus, List<RaftTask> inputs) {
        TailCache tailCache = raftStatus.tailCache;
        ArrayList<LogItem> logItems = new ArrayList<>(inputs.size());
        for (int len = inputs.size(), i = 0; i < len; i++) {
            RaftTask rt = inputs.get(i);
            LogItem li = rt.item;
            if (li == null) {
                // filer timeout items, released, see raftExec()
                continue;
            }
            long index = li.getIndex();

            // successful change owner to TailCache and release in TailCache.release(RaftTask)
            tailCache.put(index, rt);

            logItems.add(li);

            if (i == len - 1) {
                raftStatus.lastLogIndex = index;
                raftStatus.lastLogTerm = li.getTerm();
            }
        }
        raftStatus.needRepCondition.signalAll();
        return raftLog.append(logItems);
    }

    private RaftInput createHeartBeatInput() {
        DtTime deadline = new DtTime(ts, raftStatus.getElectTimeoutNanos(), TimeUnit.NANOSECONDS);
        return new RaftInput(0, null, null, deadline, false);
    }

    public void issueHeartBeat() {
        RaftInput input = createHeartBeatInput();
        submitRaftTaskInBizThread(LogItem.TYPE_HEARTBEAT, input, null);
    }

}
