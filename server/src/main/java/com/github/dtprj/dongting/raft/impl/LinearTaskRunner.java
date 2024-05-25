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
import com.github.dtprj.dongting.raft.server.RaftExecTimeoutException;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftOutput;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class LinearTaskRunner {

    private static final DtLog log = DtLogs.getLogger(LinearTaskRunner.class);

    private final GroupComponents gc;

    private ApplyManager applyManager;

    private final RaftServerConfig serverConfig;
    private final RaftGroupConfigEx groupConfig;
    private final RaftStatusImpl raftStatus;

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
        this.applyManager = gc.getApplyManager();
    }

    public void init(FiberChannel<RaftTask> taskChannel) {
        this.taskChannel = taskChannel;
        Fiber f = new Fiber("linearTaskRunner", groupConfig.getFiberGroup(), new RunnerFrame(), true);
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
            } else {
                sendHeartBeat();
            }
            // loop
            return Fiber.resume(null, this);
        }
    }

    public CompletableFuture<RaftOutput> submitRaftTaskInBizThread(RaftInput input) {
        CompletableFuture<RaftOutput> f = new CompletableFuture<>();
        RaftTask t = new RaftTask(raftStatus.getTs(), LogItem.TYPE_NORMAL, input, f);
        if (taskChannel.fireOffer(t)) {
            return f;
        } else {
            RaftUtil.release(input);
            return CompletableFuture.failedFuture(new RaftException("submit raft task failed"));
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
                if (t.getFuture() != null) {
                    t.getFuture().completeExceptionally(new NotLeaderException(raftStatus.getCurrentLeaderNode()));
                }
            }
            return;
        }
        long newIndex = lastIndex(raftStatus);
        TailCache tailCache = raftStatus.getTailCache();

        int oldTerm = raftStatus.getLastLogTerm();
        int currentTerm = raftStatus.getCurrentTerm();
        boolean hasWrite = false;
        for (int len = inputs.size(), i = 0; i < len; i++) {
            RaftTask rt = inputs.get(i);
            RaftInput input = rt.getInput();
            if (input.getPerfTime() != 0) {
                perfCallback.fireTime(PerfConsts.RAFT_D_LEADER_RUNNER_FIBER_LATENCY, input.getPerfTime());
            }

            if (input.getDeadline() != null && input.getDeadline().isTimeout(ts)) {
                RaftUtil.release(input);
                rt.getFuture().completeExceptionally(new RaftExecTimeoutException("timeout "
                        + input.getDeadline().getTimeout(TimeUnit.MILLISECONDS) + "ms"));
                continue;
            }

            if (!input.isReadOnly()) {
                // write task
                newIndex++;
                LogItem item = new LogItem();
                item.setType(rt.getType());
                item.setBizType(input.getBizType());
                item.setTerm(currentTerm);
                item.setIndex(newIndex);
                item.setPrevLogTerm(oldTerm);
                item.setTimestamp(ts.getWallClockMillis());

                item.setHeader(input.getHeader());
                item.setBody(input.getBody());

                item.calcHeaderBodySize();

                rt.setItem(item);

                hasWrite = true;
                try {
                    tailCache.put(newIndex, rt);
                    // successful change owner to TailCache and release in TailCache.release(RaftTask)
                } catch (RuntimeException | Error e) {
                    item.release();
                }
            } else {
                // read task, no LogItem, release res in execRead
                if (newIndex <= raftStatus.getLastApplied()) {
                    applyManager.execRead(newIndex, rt);
                } else {
                    RaftTask newTask = tailCache.get(newIndex);
                    if (newTask == null) {
                        tailCache.put(newIndex, rt);
                    } else {
                        newTask.setNextReader(rt);
                    }
                }
            }
        }

        if (hasWrite) {
            raftStatus.setLastLogIndex(newIndex);
            raftStatus.setLastLogTerm(currentTerm);
            RaftUtil.resetElectTimer(raftStatus);

            raftStatus.getDataArrivedCondition().signalAll();
        }
    }

    public void sendHeartBeat() {
        DtTime deadline = new DtTime(ts, raftStatus.getElectTimeoutNanos(), TimeUnit.NANOSECONDS);
        RaftInput input = new RaftInput(0, null, null, deadline);
        RaftTask rt = new RaftTask(ts, LogItem.TYPE_HEARTBEAT, input, null);
        raftExec(Collections.singletonList(rt));
    }
}
