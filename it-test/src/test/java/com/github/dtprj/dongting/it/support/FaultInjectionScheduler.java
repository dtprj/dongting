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
package com.github.dtprj.dongting.it.support;

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.it.support.BootstrapProcessManager.ProcessInfo;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.QueryStatusResp;
import com.github.dtprj.dongting.raft.admin.AdminRaftClient;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Fault injection scheduler that periodically injects random faults into the cluster.
 *
 * @author huangli
 */
public class FaultInjectionScheduler extends Thread {
    private static final DtLog log = DtLogs.getLogger(FaultInjectionScheduler.class);

    private final int groupId;
    private final int[] memberIds;
    private final int intervalSeconds;
    private final BootstrapProcessManager processManager;
    private final ClusterValidator clusterValidator;
    private final AtomicBoolean stopped;

    public long transferLeaderCount;
    public long gracefulStopCount;
    public long forceKillCount;
    public long failCount;

    private final Random random = new Random();

    public FaultInjectionScheduler(int groupId, int[] memberIds, int intervalSeconds,
                                   BootstrapProcessManager processManager,
                                   ClusterValidator clusterValidator, AtomicBoolean stopped) {
        this.groupId = groupId;
        this.memberIds = memberIds;
        this.intervalSeconds = intervalSeconds;
        this.stopped = stopped;
        this.processManager = processManager;
        this.clusterValidator = clusterValidator;
    }

    @Override
    public void run() {
        try {
            log.info("FaultInjectionScheduler started with interval {} seconds", intervalSeconds);

            long lastFaultTime = System.currentTimeMillis();
            while (!stopped.get()) {
                Thread.sleep(1000);
                if (System.currentTimeMillis() - lastFaultTime >= intervalSeconds * 1000L) {
                    injectRandomFault();
                    lastFaultTime = System.currentTimeMillis();
                }
            }

            log.info("FaultInjectionScheduler stopped");
        } catch (InterruptedException e) {
            log.info("FaultInjectionScheduler interrupted");
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            failCount++;
            log.error("FaultInjectionScheduler encountered unexpected error", e);
            throw new RuntimeException(e);
        } catch (Error e) {
            failCount++;
            log.error("FaultInjectionScheduler encountered unexpected error", e);
            throw e;
        }
    }

    private void injectRandomFault() throws Exception {
        int leaderId = waitForConvergence(30);
        if (leaderId == 0) {
            return;
        }
        int faultType = random.nextInt(3);
        log.info("Injecting fault type {}: {}", faultType,
                faultType == 0 ? "TransferLeader" : faultType == 1 ? "GracefulStop" : "ForceKill");

        switch (faultType) {
            case 0:
                transferLeader(leaderId);
                break;
            case 1:
                restartNode(false);
                break;
            case 2:
                restartNode(true);
                break;
        }
    }

    private void transferLeader(int currentLeaderId) throws InterruptedException {
        // Select a random follower as new leader
        int newLeaderId = -1;
        for (int i = 0; i < 10; i++) {
            int candidate = memberIds[random.nextInt(memberIds.length)];
            if (candidate != currentLeaderId) {
                newLeaderId = candidate;
                break;
            }
        }

        if (newLeaderId <= 0) {
            log.warn("Failed to select new leader");
            return;
        }

        log.info("Transferring leader from {} to {}", currentLeaderId, newLeaderId);

        try {
            clusterValidator.getAdminClient().transferLeader(groupId, currentLeaderId, newLeaderId,
                    new DtTime(30, TimeUnit.SECONDS)).get(35, TimeUnit.SECONDS);
            transferLeaderCount++;
            log.info("Transfer leader completed");
        } catch (Exception e) {
            failCount++;
            log.error("Transfer leader failed", e);
        }

        long waitReadyTimeout = 30;
        while (!checkServerGroupReady(newLeaderId, waitReadyTimeout)) {
            failCount++;
            log.error("Wait server group ready timeout {}, timeout={}", newLeaderId, waitReadyTimeout);
        }

        // Wait for cluster convergence
        waitForConvergence(30);
    }

    private void restartNode(boolean force) throws Exception {
        // Select a random node
        ProcessInfo targetProcess = processManager.getProcesses().get(random.nextInt(processManager.getProcesses().size()));
        int nodeId = targetProcess.config.nodeId;

        if (!targetProcess.process.isAlive()) {
            log.error("Node {} not found or already stopped", nodeId);
            failCount++;
            return;
        }

        String opType = force ? "Force" : "Graceful";

        log.info("{} stopping node {}", opType, nodeId);
        long stopTimeout = 180;
        while (!processManager.stopNode(targetProcess, stopTimeout)) {
            log.error("Failed to {} stop node {}, timeout={}", opType, nodeId, stopTimeout);
            failCount++;
        }
        gracefulStopCount++;

        // Restart node
        long startTimeout = force ? 180 : 120;
        log.info("Restarting node {}", nodeId);
        if (!processManager.startNode(targetProcess.config, startTimeout)) {
            log.error("Failed to restart node {}, timeout={}", nodeId, startTimeout);
            failCount++;
            throw new RuntimeException("Failed to restart node " + nodeId + ", timeout=" + startTimeout);
        }

        while (!checkServerGroupReady(nodeId, startTimeout)) {
            failCount++;
            log.error("Wait server group ready timeout {}, timeout={}", nodeId, startTimeout);
        }

        log.info("Node {} restarted", nodeId);

        // Wait for cluster convergence
        waitForConvergence(120);
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean checkServerGroupReady(int nodeId, long timeout) throws InterruptedException {
        long t = System.currentTimeMillis();
        while (System.currentTimeMillis() - t < timeout * 1000) {
            QueryStatusResp resp = null;
            try {
                AdminRaftClient c = clusterValidator.getAdminClient();
                CompletableFuture<QueryStatusResp> f = c.queryRaftServerStatus(nodeId, groupId);
                resp = f.get();
            } catch (Exception e) {
                log.warn("Failed to check server ready. {}", e.toString());
            }
            if (resp != null && resp.isInitFinished()) {
                if (resp.isInitFailed()) {
                    throw new AssertionError("init failed");
                }
                return true;
            }
            Thread.sleep(500);
        }
        return false;
    }

    private int waitForConvergence(long timeoutSeconds) {
        try {
            log.info("Wait cluster convergence, timeout {} seconds ...", timeoutSeconds);
            int leaderId = clusterValidator.waitForClusterConsistency(groupId, memberIds, 30);
            log.info("Cluster converged, leaderId: {}", leaderId);
            return leaderId;
        } catch (Exception e) {
            log.error("Cluster convergence failed", e);
            failCount++;
            return 0;
        }
    }
}
