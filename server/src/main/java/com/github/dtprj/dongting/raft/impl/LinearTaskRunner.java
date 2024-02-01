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
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberChannel;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
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
import java.util.function.BiConsumer;

/**
 * @author huangli
 */
public class LinearTaskRunner implements BiConsumer<EventType, Object> {

    private static final DtLog log = DtLogs.getLogger(LinearTaskRunner.class);

    private final ApplyManager applyManager;

    private final RaftServerConfig serverConfig;
    private final RaftGroupConfigEx groupConfig;
    private final RaftStatusImpl raftStatus;

    private final Timestamp ts;

    private FiberChannel<RaftTask> taskChannel;

    public LinearTaskRunner(RaftServerConfig serverConfig, RaftGroupConfigEx groupConfig,
                            RaftStatusImpl raftStatus, ApplyManager applyManager) {
        this.serverConfig = serverConfig;
        this.groupConfig = groupConfig;
        this.raftStatus = raftStatus;
        this.ts = raftStatus.getTs();

        this.applyManager = applyManager;
    }

    @Override
    public void accept(EventType eventType, Object o) {
        if (eventType == EventType.raftExec) {
            raftExec((List<RaftTask>) o);
        }
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
            if (list.size() > 0) {
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
        CompletableFuture f = new CompletableFuture<>();
        RaftTask t = new RaftTask(raftStatus.getTs(), LogItem.TYPE_NORMAL, input, f);
        taskChannel.fireOffer(t);
        return f;
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

    @SuppressWarnings("ForLoopReplaceableByForEach")
    private void raftExec(List<RaftTask> inputs) {
        RaftStatusImpl raftStatus = this.raftStatus;
        if (raftStatus.getRole() != RaftRole.leader) {
            for (RaftTask t : inputs) {
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
        int writeCount = 0;
        for (int i = 0; i < inputs.size(); i++) {
            RaftTask rt = inputs.get(i);
            RaftInput input = rt.getInput();

            if (input.getDeadline() != null && input.getDeadline().isTimeout(ts)) {
                rt.getFuture().completeExceptionally(new RaftExecTimeoutException("timeout "
                        + input.getDeadline().getTimeout(TimeUnit.MILLISECONDS) + "ms"));
                continue;
            }

            if (!input.isReadOnly()) {
                newIndex++;
                LogItem item = new LogItem(null);
                item.setType(rt.getType());
                item.setBizType(input.getBizType());
                item.setTerm(currentTerm);
                item.setIndex(newIndex);
                item.setPrevLogTerm(oldTerm);
                item.setTimestamp(ts.getWallClockMillis());

                Object header = input.getHeader();
                item.setHeader(header);

                Object body = input.getBody();
                item.setBody(body);

                rt.setItem(item);

                writeCount++;
                tailCache.put(newIndex, rt);
                raftStatus.setLastLogIndex(newIndex);
                raftStatus.setLastLogTerm(currentTerm);
            } else {
                // read
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

        if (writeCount == 0) {
            return;
        }

        raftStatus.getDataArrivedCondition().signalAll();
    }

    public void sendHeartBeat() {
        DtTime deadline = new DtTime(ts, raftStatus.getElectTimeoutNanos(), TimeUnit.NANOSECONDS);
        RaftInput input = new RaftInput(0, null, null, deadline, 0);
        RaftTask rt = new RaftTask(ts, LogItem.TYPE_HEARTBEAT, input, null);
        raftExec(Collections.singletonList(rt));
    }
}
