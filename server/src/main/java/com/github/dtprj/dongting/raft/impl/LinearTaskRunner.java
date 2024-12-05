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
import com.github.dtprj.dongting.common.PerfCallback;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberChannel;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.PerfConsts;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.NotLeaderException;
import com.github.dtprj.dongting.raft.server.RaftCallback;
import com.github.dtprj.dongting.raft.server.RaftExecTimeoutException;
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
        this.serverConfig = gc.getServerConfig();
        this.groupConfig = gc.getGroupConfig();
        this.raftStatus = gc.getRaftStatus();
        this.ts = raftStatus.getTs();
        this.perfCallback = gc.getGroupConfig().getPerfCallback();
    }

    public void postInit() {
        this.raftLog = gc.getRaftLog();
    }

    public void init(FiberChannel<RaftTask> taskChannel) {
        this.taskChannel = taskChannel;
        Fiber f = new Fiber("linearTaskRunner", groupConfig.getFiberGroup(),
                new RunnerFrame(), true, 50);
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
            return taskChannel.takeAll(serverConfig.getHeartbeatInterval(), list, this::afterTakeAll);
        }

        private FrameCallResult afterTakeAll(Void unused) {
            if (raftStatus.getTransferLeaderCondition() != null) {
                raftStatus.getTransferLeaderCondition().await(this::afterTakeAll);
            }
            if (!list.isEmpty()) {
                raftExec(list);
                list.clear();
            } else if (raftStatus.getRole() == RaftRole.leader) {
                sendHeartBeat();
            }
            // loop
            return Fiber.resume(null, this);
        }
    }

    public void submitRaftTaskInBizThread(RaftInput input, RaftCallback callback) {
        int type = input.isReadOnly() ? LogItem.TYPE_LOG_READ : LogItem.TYPE_NORMAL;
        RaftTask t = new RaftTask(raftStatus.getTs(), type, input, callback);
        input.setPerfTime(perfCallback.takeTime(PerfConsts.RAFT_D_LEADER_RUNNER_FIBER_LATENCY));
        if (!taskChannel.fireOffer(t)) {
            RaftUtil.release(input);
            t.callFail(new RaftException("submit raft task failed"));
        }
    }

    public static long lastIndex(RaftStatusImpl raftStatus) {
        TailCache tailCache = raftStatus.getTailCache();
        if (tailCache.size() == 0) {
            log.info("tail cache is empty, use last log index {}", raftStatus.getLastLogIndex());
            return raftStatus.getLastLogIndex();
        } else {
            return tailCache.getLastIndex();
        }
    }

    public void raftExec(List<RaftTask> inputs) {
        RaftStatusImpl raftStatus = this.raftStatus;
        if (raftStatus.getRole() != RaftRole.leader) {
            for (RaftTask t : inputs) {
                RaftUtil.release(t.getInput());
                t.callFail(new NotLeaderException(raftStatus.getCurrentLeaderNode()));
            }
            return;
        }
        long newIndex = lastIndex(raftStatus);

        int prevTerm = raftStatus.getLastLogTerm();
        int currentTerm = raftStatus.getCurrentTerm();
        for (int len = inputs.size(), i = 0; i < len; i++) {
            RaftTask rt = inputs.get(i);
            RaftInput input = rt.getInput();
            if (input.getPerfTime() != 0) {
                perfCallback.fireTime(PerfConsts.RAFT_D_LEADER_RUNNER_FIBER_LATENCY, input.getPerfTime());
            }

            if (input.getDeadline() != null && input.getDeadline().isTimeout(ts)) {
                RaftUtil.release(input);
                rt.callFail(new RaftExecTimeoutException("timeout "
                        + input.getDeadline().getTimeout(TimeUnit.MILLISECONDS) + "ms"));
                // not removed from list, filter in submitTasks()
                continue;
            }

            newIndex++;
            LogItem item = new LogItem();
            item.setType(rt.getType());
            item.setBizType(input.getBizType());
            item.setTerm(currentTerm);
            item.setIndex(newIndex);
            item.setPrevLogTerm(prevTerm);
            prevTerm = currentTerm;
            item.setTimestamp(ts.getWallClockMillis());

            item.setHeader(input.getHeader(), input.isHeadReleasable());
            item.setBody(input.getBody(), input.isBodyReleasable());

            rt.setItem(item);
        }

        RaftUtil.resetElectTimer(raftStatus);

        submitTasks(raftStatus, inputs);
    }

    public void submitTasks(RaftStatusImpl raftStatus, List<RaftTask> inputs) {
        TailCache tailCache = raftStatus.getTailCache();
        ArrayList<LogItem> logItems = new ArrayList<>(inputs.size());
        for (int len = inputs.size(), i = 0; i < len; i++) {
            RaftTask rt = inputs.get(i);
            LogItem li = rt.getItem();
            if (li == null) {
                // filer timeout items, released, see raftExec()
                continue;
            }
            long index = li.getIndex();

            // successful change owner to TailCache and release in TailCache.release(RaftTask)
            tailCache.put(index, rt);

            logItems.add(li);

            if (i == len - 1) {
                raftStatus.setLastLogIndex(index);
                raftStatus.setLastLogTerm(li.getTerm());
            }
        }
        raftLog.append(logItems);
        raftStatus.getDataArrivedCondition().signalAll();
    }

    public void sendHeartBeat() {
        DtTime deadline = new DtTime(ts, raftStatus.getElectTimeoutNanos(), TimeUnit.NANOSECONDS);
        RaftInput input = new RaftInput(0, null, null, deadline, false);
        RaftTask rt = new RaftTask(ts, LogItem.TYPE_HEARTBEAT, input, null);
        raftExec(Collections.singletonList(rt));
    }
}
