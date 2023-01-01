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
import com.github.dtprj.dongting.raft.server.RaftServerConfig;

import java.time.Duration;
import java.util.Random;
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
    private final RaftRpc raftRpc;
    private final GroupConManager groupConManager;

    private final long heartbeatIntervalNanos;
    private final long leaderTimeoutNanos;

    // TODO optimise blocking queue
    private final LinkedBlockingQueue<Runnable> queue;

    private volatile boolean stop;

    public RaftThread(RaftServerConfig config, RaftExecutor executor, RaftStatus raftStatus,
                      RaftRpc raftRpc, GroupConManager groupConManager) {
        this.config = config;
        this.raftStatus = raftStatus;
        this.queue = executor.getQueue();
        this.raftRpc = raftRpc;
        this.groupConManager = groupConManager;
        leaderTimeoutNanos = Duration.ofMillis(config.getLeaderTimeout()).toNanos();
        heartbeatIntervalNanos = Duration.ofMillis(config.getHeartbeatInterval()).toNanos();
    }

    @Override
    public void run() {
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

    private boolean leaderTimeout(long currentNanos) {
        return currentNanos - raftStatus.getLastLeaderActiveTime() > leaderTimeoutNanos;
    }

    public static void checkTerm(int remoteTerm, RaftStatus raftStatus) {
        if (remoteTerm > raftStatus.getCurrentTerm()) {
            log.info("update term from {} to {}, change from {} to follower",
                    raftStatus.getCurrentTerm(), remoteTerm, raftStatus.getRole());
            raftStatus.setCurrentTerm(remoteTerm);
            raftStatus.setVoteFor(0);
            raftStatus.setRole(RaftRole.follower);
            raftStatus.getCurrentVotes().clear();
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
        }
        switch (raftStatus.getRole()) {
            case follower:
            case candidate:
                if (leaderTimeout(roundTimeNanos)) {
                    startElect();
                }
                break;
            case leader:
                if (roundTimeNanos - raftStatus.getHeartbeatTime() > heartbeatIntervalNanos) {
                    raftStatus.setHeartbeatTime(roundTimeNanos);
                    sendHeartBeat();
                }
                break;
        }
    }

    private void sendHeartBeat() {
        for (RaftNode node : groupConManager.getServers()) {
            if (node.isSelf()) {
                continue;
            }
            raftRpc.sendHeartBeat(node);
        }
    }

    private void startElect() {
        raftStatus.setCurrentTerm(raftStatus.getCurrentTerm() + 1);
        raftStatus.setVoteFor(config.getId());
        raftStatus.setRole(RaftRole.candidate);
        raftStatus.getCurrentVotes().clear();
        raftStatus.getCurrentVotes().add(config.getId());
        raftStatus.setLastLeaderActiveTime(System.nanoTime());
        for (RaftNode node : groupConManager.getServers()) {
            if (node.isSelf()) {
                continue;
            }
            raftRpc.sendVoteRequest(node, () -> leaderTimeout(System.nanoTime()));
        }
    }


}
