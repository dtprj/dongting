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

import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.client.RaftException;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class RaftThread extends Thread {
    private static final DtLog log = DtLogs.getLogger(RaftThread.class);

    private final RaftServerConfig config;
    private final RaftStatus raftStatus;
    private final long pollTimeout = new Random().nextInt(100) + 50;
    private final Raft raft;
    private final GroupConManager groupConManager;

    private final long heartbeatIntervalNanos;
    private final long leaderTimeoutNanos;
    private final long electTimeoutNanos;

    // TODO optimise blocking queue
    private final LinkedBlockingQueue<Runnable> queue;

    private volatile boolean stop;

    private final CompletableFuture<Void> initFuture = new CompletableFuture<>();

    public RaftThread(RaftServerConfig config, RaftExecutor executor, RaftStatus raftStatus,
                      Raft raft, GroupConManager groupConManager) {
        this.config = config;
        this.raftStatus = raftStatus;
        this.queue = executor.getQueue();
        this.raft = raft;
        this.groupConManager = groupConManager;
        leaderTimeoutNanos = Duration.ofMillis(config.getLeaderTimeout()).toNanos();
        heartbeatIntervalNanos = Duration.ofMillis(config.getHeartbeatInterval()).toNanos();
        electTimeoutNanos = leaderTimeoutNanos;
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
        if (!init()) {
            return;
        }
        long roundTimestampMillis = System.currentTimeMillis();
        long roundTimeNanos;
        while (!stop) {
            Runnable t;
            try {
                t = queue.poll(pollTimeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                return;
            }
            if (t != null) {
                t.run();
            }
            long now = System.currentTimeMillis();
            if (now < roundTimestampMillis || now - roundTimestampMillis > 50) {
                roundTimeNanos = System.nanoTime();
                roundTimestampMillis = now;
                idle(roundTimeNanos);
            }
        }
    }

    public void requestShutdown() {
        queue.offer(() -> {
            stop = true;
            log.info("request raft thread shutdown");
        });
    }

    private void idle(long roundTimeNanos) {
        if (roundTimeNanos - raftStatus.getHeartbeatTime() > heartbeatIntervalNanos) {
            raftStatus.setHeartbeatTime(roundTimeNanos);
            groupConManager.pingAllAndUpdateServers();
            if (raftStatus.getRole() == RaftRole.leader) {
                raft.sendHeartBeat();
            }
        }
        if (raftStatus.getRole() == RaftRole.follower || raftStatus.getRole() == RaftRole.candidate) {
            if (roundTimeNanos - raftStatus.getLastLeaderActiveTime() > leaderTimeoutNanos) {
                if (roundTimeNanos - raftStatus.getLastElectTime() > electTimeoutNanos) {
                    startElect();
                }
            }
        }
    }

    private void startElect() {
        // TODO persist raft status
        raftStatus.setCurrentTerm(raftStatus.getCurrentTerm() + 1);
        raftStatus.setVoteFor(config.getId());
        raftStatus.setRole(RaftRole.candidate);
        raftStatus.getCurrentVotes().clear();
        raftStatus.getCurrentVotes().add(config.getId());
        raftStatus.setLastElectTime(System.nanoTime());
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
}
