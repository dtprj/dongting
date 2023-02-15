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

import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.client.RaftException;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftOutput;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class RaftThread extends Thread {
    private static final DtLog log = DtLogs.getLogger(RaftThread.class);

    private final Random random = new Random();

    private final RaftServerConfig config;
    private final RaftStatus raftStatus;
    private final Raft raft;
    private final GroupConManager groupConManager;

    private final long heartbeatIntervalNanos;
    private final long electTimeoutNanos;

    // TODO optimise blocking queue
    private final LinkedBlockingQueue<Object> queue;

    private volatile boolean stop;

    private final CompletableFuture<Void> initFuture = new CompletableFuture<>();

    public RaftThread(RaftContainer container, Raft raft, GroupConManager groupConManager) {
        this.config = container.getConfig();
        this.raftStatus = container.getRaftStatus();
        this.queue = container.getRaftExecutor().getQueue();
        this.raft = raft;
        this.groupConManager = groupConManager;

        electTimeoutNanos = Duration.ofMillis(config.getElectTimeout()).toNanos();
        raftStatus.setElectTimeoutNanos(electTimeoutNanos);
        heartbeatIntervalNanos = Duration.ofMillis(config.getHeartbeatInterval()).toNanos();
    }

    protected boolean init() {
        try {
            groupConManager.initRaftGroup(raftStatus.getElectQuorum(),
                    RaftUtil.parseServers(config.getServers()), 1000);
            initFuture.complete(null);
            return true;
        } catch (Throwable e) {
            initFuture.completeExceptionally(e);
            return false;
        }
    }

    @Override
    public void run() {
        try {
            run0();
        } catch (Throwable e) {
            BugLog.getLog().error("raft thread error", e);
        }
    }

    private void run0() {
        if (!init()) {
            return;
        }
        Timestamp ts = raftStatus.getTs();
        ArrayList<RaftTask> tasks = new ArrayList<>(32);
        ArrayList<Object> queueData = new ArrayList<>(32);
        boolean poll = true;
        while (!stop) {
            long oldNanos = ts.getNanoTime();
            if (poll) {
                try {
                    Object o = queue.poll(50, TimeUnit.MILLISECONDS);
                    if (o != null) {
                        queueData.add(o);
                    }
                } catch (InterruptedException e) {
                    return;
                }
            } else {
                queue.drainTo(queueData);
            }

            ts.refresh(1);
            poll = ts.getNanoTime() - oldNanos > 2 * 1000 * 1000 || queueData.size() == 0;

            for (Object o : queueData) {
                if (o instanceof RaftTask) {
                    tasks.add((RaftTask) o);
                } else if (o instanceof Runnable) {
                    if (tasks.size() > 0) {
                        raft.raftExec(tasks);
                        tasks.clear();
                    }
                    raftStatus.copyShareStatus();
                    ((Runnable) o).run();
                    raftStatus.copyShareStatus();
                } else {
                    BugLog.getLog().error("type error: {}", o.getClass());
                    return;
                }
            }

            if (tasks.size() > 0) {
                raft.raftExec(tasks);
                tasks.clear();
            }
            raftStatus.copyShareStatus();

            if (ts.getNanoTime() - oldNanos > 10 * 1000 * 1000) {
                idle(ts);
            }
        }
    }

    public void requestShutdown() {
        Runnable r = () -> {
            stop = true;
            log.info("request raft thread shutdown");
        };
        queue.offer(r);
    }

    private void idle(Timestamp ts) {
        ts.refresh(1);
        long roundTimeNanos = ts.getNanoTime();
        if (roundTimeNanos - raftStatus.getHeartbeatTime() > heartbeatIntervalNanos) {
            raftStatus.setHeartbeatTime(roundTimeNanos);
            groupConManager.pingAllAndUpdateServers();
            if (raftStatus.getRole() == RaftRole.leader) {
                raft.sendHeartBeat();
                raftStatus.copyShareStatus();
            }
        }
        if (raftStatus.getRole() != RaftRole.leader) {
            if (roundTimeNanos - raftStatus.getLastElectTime() > electTimeoutNanos + random.nextInt(200)) {
                startElect();
                raftStatus.copyShareStatus();
            }
        }
    }

    private void startElect() {
        // TODO persist raft status
        RaftUtil.resetStatus(raftStatus);
        if (raftStatus.getRole() != RaftRole.candidate) {
            log.info("change to candidate. oldTerm={}", raftStatus.getCurrentTerm());
            raftStatus.setRole(RaftRole.candidate);
        }

        raftStatus.setCurrentTerm(raftStatus.getCurrentTerm() + 1);
        raftStatus.setVoteFor(config.getId());
        raftStatus.getCurrentVotes().add(config.getId());

        log.info("start elect. newTerm={}", raftStatus.getCurrentTerm());

        for (RaftNode node : raftStatus.getServers()) {
            if (node.isSelf()) {
                continue;
            }
            raft.sendVoteRequest(node);
        }
    }

    public void waitInit() {
        try {
            initFuture.get();
        } catch (Exception e) {
            throw new RaftException(e);
        }
    }

    public CompletableFuture<RaftOutput> submitRaftTask(RaftInput input) {
        CompletableFuture<RaftOutput> f = new CompletableFuture<>();
        RaftTask t = new RaftTask();
        t.future = f;
        t.input = input;
        queue.offer(t);
        return f;
    }
}
