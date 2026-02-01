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
import com.github.dtprj.dongting.it.StressIT;
import com.github.dtprj.dongting.it.support.BootstrapProcessManager.ProcessInfo;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.QueryStatusResp;
import com.github.dtprj.dongting.raft.RaftNode;
import com.github.dtprj.dongting.raft.admin.AdminRaftClient;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.singletonList;

/**
 * Fault injection scheduler that periodically injects random faults into the cluster.
 *
 * @author huangli
 */
@SuppressWarnings("BusyWait")
public class FaultInjector extends Thread {
    private static final DtLog log = DtLogs.getLogger(FaultInjector.class);

    private static final int OBSERVER_NODE_ID = StressIT.OBSERVER_ID;
    private static final long CATCH_UP_CHECK_INTERVAL_MILLIS = 1000;
    private static final long CATCH_UP_THRESHOLD = 5000;
    private static final long TIMEOUT_SECONDS = 60;
    private static final long OBSERVER_CATCH_UP_TIMEOUT_SECONDS = 600;

    private final int groupId;
    private final int[] memberIds;
    public final int intervalSeconds;
    private final BootstrapProcessManager processManager;
    private final ClusterValidator clusterValidator;
    private final AtomicBoolean stopped;
    private Path baseDir;
    private boolean fullSize;

    public long transferLeaderCount;
    public long gracefulStopCount;
    public long forceKillCount;
    public long addObserverCount;
    public long removeObserverCount;
    public long changeMembersCount;
    public long failCount;

    private final Random random = new Random();

    private boolean observerActive;

    public FaultInjector(int groupId, int[] memberIds, BootstrapProcessManager processManager,
                         ClusterValidator clusterValidator, AtomicBoolean stopped) {
        this.groupId = groupId;
        this.memberIds = memberIds;
        this.intervalSeconds = Integer.parseInt(System.getProperty("faultInterval", "60"));
        this.stopped = stopped;
        this.processManager = processManager;
        this.clusterValidator = clusterValidator;
    }

    public void setBaseDir(Path baseDir) {
        this.baseDir = baseDir;
    }


    public void setFullSize(boolean fullSize) {
        this.fullSize = fullSize;
    }

    @Override
    public void run() {
        try {
            log.info("FaultInjector started with interval {} seconds", intervalSeconds);

            // Detect observer status at startup
            detectObserverStatus();

            long lastFaultTime = System.currentTimeMillis();
            while (!stopped.get()) {
                Thread.sleep(1000);
                if (System.currentTimeMillis() - lastFaultTime >= intervalSeconds * 1000L) {
                    injectRandomFault();
                    lastFaultTime = System.currentTimeMillis();
                }
            }

            log.info("FaultInjector stopped");
        } catch (InterruptedException e) {
            log.info("FaultInjector interrupted");
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            failCount++;
            log.error("FaultInjector encountered unexpected error", e);
            throw new RuntimeException(e);
        } catch (Error e) {
            failCount++;
            log.error("FaultInjector encountered unexpected error", e);
            throw e;
        }
    }

    /**
     * Detect observer status by querying cluster nodes.
     * This ensures observerActive is correctly initialized even after restart.
     * We verify both the cluster configuration and the actual process status.
     */
    private void detectObserverStatus() {
        QueryStatusResp leaderStatus = getLeaderStatus();
        if (leaderStatus != null && leaderStatus.isGroupReady()
                && leaderStatus.observers.contains(OBSERVER_NODE_ID)) {
            log.info("Observer node {} found in cluster config, starting process",
                    OBSERVER_NODE_ID);
            clusterValidator.getAdminClient().clientAddNode(ItUtil.formatReplicateServers(
                    new int[]{OBSERVER_NODE_ID}));
            startObserverProcess();
            observerActive = true;
        } else {
            log.info("No observer node {} detected in cluster", OBSERVER_NODE_ID);
            observerActive = false;
        }
    }

    private void injectRandomFault() throws Exception {
        int leaderId = waitForConvergence(30);
        if (leaderId == 0) {
            return;
        }

        int faultType = System.getProperty("faultType") == null ? random.nextInt(5) :
                Integer.parseInt(System.getProperty("faultType"));
        switch (faultType) {
            case 0:
                log.info("Injecting event: TransferLeader");
                transferLeader(leaderId);
                break;
            case 1:
                log.info("Injecting fault: GracefulStop");
                restartNode(false);
                break;
            case 2:
                log.info("Injecting fault: ForceKill");
                restartNode(true);
                break;
            case 3:
                if (observerActive) {
                    log.info("Injecting event: RemoveObserver");
                    removeObserver();
                } else {
                    log.info("Injecting event: AddObserver");
                    addObserver();
                }
                break;
            case 4:
                log.info("Injecting event: ChangeMembers");
                changeMembers(leaderId);
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
        while (!stopped.get() && targetProcess.process.isAlive()) {
            if (force) {
                processManager.forceStopNode(targetProcess, true);
            } else {
                long stopTimeout = 180;
                processManager.stopNode(targetProcess, stopTimeout);
            }
            if (targetProcess.process.isAlive()) {
                log.error("Failed to {} stop node {}", opType, nodeId);
                failCount++;
            }
        }
        if (force) {
            forceKillCount++;
        } else {
            gracefulStopCount++;
        }


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

    private void addObserver() {
        log.info("Adding observer node {} to cluster", OBSERVER_NODE_ID);

        try {
            // Start the observer node
            try {
                startObserverProcess();
            } catch (Exception e) {
                log.error("Failed to start observer node {} process: {}", OBSERVER_NODE_ID, e.getMessage());
                failCount++;
                return;
            }

            AdminRaftClient adminClient = clusterValidator.getAdminClient();

            // Add node definition to adminClient local (for idempotency, always call this)
            adminClient.clientAddNode(ItUtil.formatReplicateServers(new int[]{OBSERVER_NODE_ID}));

            // Prepare and commit config change to add observer
            Set<Integer>[] sets = buildMemberSets(false, true);
            long prepareIndex = adminClient.prepareChange(groupId,
                    sets[0], sets[1], sets[2], sets[3],
                    new DtTime(TIMEOUT_SECONDS, TimeUnit.SECONDS)).get();
            log.info("Prepared config change to add observer, prepareIndex={}", prepareIndex);

            adminClient.commitChange(groupId, prepareIndex, new DtTime(TIMEOUT_SECONDS, TimeUnit.SECONDS)).get();
            log.info("Committed config change to add observer node {}", OBSERVER_NODE_ID);

            // Wait for cluster consistency
            int[] allNodeIds = StressIT.ALL_NODE_IDS;
            clusterValidator.waitForClusterConsistency(groupId, allNodeIds, TIMEOUT_SECONDS);

            // Wait for observer to catch up
            if (!waitForObserverCatchUp()) {
                log.error("Observer node {} failed to catch up, removing it", OBSERVER_NODE_ID);
                cleanupObserverState();
                return;
            }

            // All steps successful, set observerActive
            observerActive = true;
            addObserverCount++;
            log.info("Observer node {} added and caught up", OBSERVER_NODE_ID);

        } catch (Exception e) {
            log.error("Error adding observer node {}: {}", OBSERVER_NODE_ID, e.getMessage(), e);
            failCount++;
            // Clean up partial state
            cleanupObserverState();
        }
    }

    private void removeObserver() {
        if (!observerActive) {
            log.debug("Observer not active, skipping removal");
            return;
        }

        log.info("Removing observer node {} from cluster", OBSERVER_NODE_ID);

        try {
            AdminRaftClient adminClient = clusterValidator.getAdminClient();

            // Prepare and commit config change to remove observer
            Set<Integer>[] sets = buildMemberSets(true, false);
            long prepareIndex = adminClient.prepareChange(groupId,
                    sets[0], sets[1], sets[2], sets[3],
                    new DtTime(TIMEOUT_SECONDS, TimeUnit.SECONDS)).get();
            log.info("Prepared config change to remove observer, prepareIndex={}", prepareIndex);

            adminClient.commitChange(groupId, prepareIndex, new DtTime(TIMEOUT_SECONDS, TimeUnit.SECONDS)).get();
            log.info("Committed config change to remove observer node {}", OBSERVER_NODE_ID);

            // Wait for cluster consistency
            clusterValidator.waitForClusterConsistency(groupId, memberIds, TIMEOUT_SECONDS);

            // Stop the observer process
            stopObserverProcess();

            // Remove node definition from adminClient local (for idempotency)
            adminClient.clientRemoveNode(OBSERVER_NODE_ID);

            // All steps successful, clear observerActive
            observerActive = false;
            removeObserverCount++;
            log.info("Successfully removed observer node {} from cluster", OBSERVER_NODE_ID);

        } catch (Exception e) {
            log.error("Error removing observer node: {}", e.getMessage(), e);
            failCount++;
            // Try to stop observer process and clear flag to avoid state inconsistency
            stopObserverProcess();
            observerActive = false;
        }
    }

    private QueryStatusResp getLeaderStatus() {
        AdminRaftClient adminClient = clusterValidator.getAdminClient();
        try {
            RaftNode leaderNode = adminClient.fetchLeader(groupId).get(5, TimeUnit.SECONDS);
            if (leaderNode == null) {
                log.warn("No leader found for group {}", groupId);
                return null;
            }
            return adminClient.queryRaftServerStatus(leaderNode.nodeId, groupId)
                    .get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("Failed to get leader status: {}", e.getMessage());
            return null;
        }
    }

    private boolean waitForObserverCatchUp() throws InterruptedException {
        AdminRaftClient adminClient = clusterValidator.getAdminClient();
        long startTime = System.currentTimeMillis();

        while (!stopped.get() && System.currentTimeMillis() - startTime
                < OBSERVER_CATCH_UP_TIMEOUT_SECONDS * 1000) {
            try {
                QueryStatusResp leaderStatus = getLeaderStatus();

                if (leaderStatus == null) {
                    log.debug("Could not get leader status to check catch up");
                    Thread.sleep(CATCH_UP_CHECK_INTERVAL_MILLIS);
                    continue;
                }

                QueryStatusResp observerStatus = adminClient.queryRaftServerStatus(OBSERVER_NODE_ID, groupId)
                        .get(5, TimeUnit.SECONDS);

                long leaderCommitIndex = leaderStatus.commitIndex;
                long observerCommitIndex = observerStatus.commitIndex;
                long diff = leaderCommitIndex - observerCommitIndex;

                log.debug("Observer catch up check - leader: {}, observer: {}, diff: {}",
                        leaderCommitIndex, observerCommitIndex, diff);

                if (diff <= CATCH_UP_THRESHOLD && observerStatus.applyLagMillis <= 1000) {
                    log.info("Observer node {} has caught up. Leader commit: {}, Observer commit: {}",
                            OBSERVER_NODE_ID, leaderCommitIndex, observerCommitIndex);
                    return true;
                }

            } catch (Exception e) {
                log.debug("Error checking observer catch up status: {}", e.getMessage());
            }

            Thread.sleep(CATCH_UP_CHECK_INTERVAL_MILLIS);
        }

        log.error("Timeout waiting for observer node {} to catch up", OBSERVER_NODE_ID);
        return false;
    }

    private void startObserverProcess() {
        try {
            ProcessInfo existingProcess = findObserverProcess();
            if (existingProcess != null && existingProcess.process.isAlive()) {
                log.info("Observer node {} process already running", OBSERVER_NODE_ID);
                return;
            }

            String serversStr = formatReplicateServers(StressIT.ALL_NODE_IDS);
            String membersStr = ItUtil.formatMemberIds(memberIds);

            ConfigFileGenerator.ProcessConfig config = new ConfigFileGenerator.ProcessConfigBuilder(
                    OBSERVER_NODE_ID, baseDir, serversStr,
                    singletonList(new ConfigFileGenerator.GroupDefinition(groupId, membersStr, String.valueOf(OBSERVER_NODE_ID))))
                    .fullSize(fullSize)
                    .build();

            log.info("Starting observer node {} process", OBSERVER_NODE_ID);
            if (!processManager.startNode(config, TIMEOUT_SECONDS)) {
                log.error("Failed to start observer node {} process", OBSERVER_NODE_ID);
                throw new RuntimeException("Failed to start observer process");
            }
            log.info("Observer node {} process started", OBSERVER_NODE_ID);

        } catch (Exception e) {
            log.error("Error starting observer process: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to start observer process", e);
        }
    }

    private void stopObserverProcess() {
        ProcessInfo processInfo = findObserverProcess();
        if (processInfo != null && processInfo.process.isAlive()) {
            log.info("Stopping observer node {} process", OBSERVER_NODE_ID);
            try {
                processManager.stopNode(processInfo, 30);
                log.info("Observer node {} process stopped", OBSERVER_NODE_ID);
            } catch (Exception e) {
                log.error("Error stopping observer node process: {}", e.getMessage());
                try {
                    processManager.forceStopNode(processInfo, true);
                } catch (Exception e2) {
                    log.error("Error force stopping observer node process: {}", e2.getMessage());
                }
            }
        }
    }

    private ProcessInfo findObserverProcess() {
        for (ProcessInfo pi : processManager.getProcesses()) {
            if (pi.config.nodeId == OBSERVER_NODE_ID) {
                return pi;
            }
        }
        return null;
    }

    private String formatReplicateServers(int[] nodeIds) {
        return ItUtil.formatReplicateServers(nodeIds);
    }

    /**
     * Build member and observer sets for config change.
     *
     * @param withObserver whether to include observer in oldObservers
     * @param keepObserver whether to include observer in newObservers
     * @return array of [oldMembers, oldObservers, newMembers, newObservers]
     */
    private Set<Integer>[] buildMemberSets(boolean withObserver, boolean keepObserver) {
        Set<Integer> oldMembers = new HashSet<>();
        for (int id : memberIds) {
            oldMembers.add(id);
        }
        Set<Integer> oldObservers = new HashSet<>();
        if (withObserver) {
            oldObservers.add(OBSERVER_NODE_ID);
        }

        Set<Integer> newMembers = new HashSet<>(oldMembers);
        Set<Integer> newObservers = new HashSet<>();
        if (keepObserver) {
            newObservers.add(OBSERVER_NODE_ID);
        }

        @SuppressWarnings("unchecked")
        Set<Integer>[] result = new Set[]{oldMembers, oldObservers, newMembers, newObservers};
        return result;
    }

    /**
     * Clean up observer state when errors occur.
     * This ensures the system is in a consistent state.
     */
    private void cleanupObserverState() {
        try {
            AdminRaftClient adminClient = clusterValidator.getAdminClient();

            // First, try to remove observer from cluster config
            try {
                Set<Integer>[] sets = buildMemberSets(true, false);
                long prepareIndex = adminClient.prepareChange(groupId, sets[0], sets[1], sets[2], sets[3],
                        new DtTime(TIMEOUT_SECONDS, TimeUnit.SECONDS)).get();
                adminClient.commitChange(groupId, prepareIndex, new DtTime(TIMEOUT_SECONDS, TimeUnit.SECONDS)).get();
                log.info("Cleaned up observer from cluster config");
            } catch (Exception e) {
                log.warn("Failed to clean up observer from cluster config: {}", e.getMessage());
            }

            // Then stop the observer process
            stopObserverProcess();

            // Remove node definition from all existing nodes
            for (int memberId : memberIds) {
                try {
                    adminClient.serverRemoveNode(memberId, OBSERVER_NODE_ID).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                } catch (Exception e) {
                    log.warn("Failed to remove node definition from node {}: {}", memberId, e.getMessage());
                }
            }

            // Remove node definition from adminClient local
            adminClient.clientRemoveNode(OBSERVER_NODE_ID);

            // Finally, clear the flag
            observerActive = false;

            log.info("Observer state cleanup completed");

        } catch (Exception e) {
            log.error("Error during observer state cleanup: {}", e.getMessage());
        }
    }

    /**
     * Change cluster members between 2-node and 3-node configuration.
     * If current is 3 nodes (1,2,3), remove a follower to become 2 nodes.
     * If current is 2 nodes, add the missing node from (1,2,3) to become 3 nodes.
     */
    private void changeMembers(int currentLeaderId) {
        try {
            // Get current members from leader status
            QueryStatusResp leaderStatus = getLeaderStatus();
            if (leaderStatus == null || !leaderStatus.isGroupReady()) {
                log.warn("Cannot get leader status or group not ready, skipping members change");
                failCount++;
                return;
            }

            Set<Integer> currentMembersSet = new HashSet<>(leaderStatus.members);

            Set<Integer> targetMembers;
            int nodeToRemoveOrAdd;

            if (currentMembersSet.size() == 3) {
                // Current is 3 nodes, need to remove a follower (not leader)
                nodeToRemoveOrAdd = -1;
                for (int nodeId : currentMembersSet) {
                    if (nodeId != currentLeaderId) {
                        nodeToRemoveOrAdd = nodeId;
                        break;
                    }
                }
                if (nodeToRemoveOrAdd < 0) {
                    throw new RuntimeException("Cannot find a non-leader node to remove");
                }

                targetMembers = new HashSet<>(currentMembersSet);
                targetMembers.remove(nodeToRemoveOrAdd);
                log.info("Changing members from {} to {}, removing node {}",
                        currentMembersSet, targetMembers, nodeToRemoveOrAdd);
            } else if (currentMembersSet.size() == 2) {
                // Current is 2 nodes, need to add the missing node from {1,2,3}
                nodeToRemoveOrAdd = -1;
                for (int nodeId : memberIds) {
                    if (!currentMembersSet.contains(nodeId)) {
                        nodeToRemoveOrAdd = nodeId;
                        break;
                    }
                }
                if (nodeToRemoveOrAdd < 0) {
                    throw new RuntimeException("Cannot find a node to add from memberIds " + Arrays.toString(memberIds));
                }

                targetMembers = new HashSet<>(currentMembersSet);
                targetMembers.add(nodeToRemoveOrAdd);
                log.info("Changing members from {} to {}, adding node {}",
                        currentMembersSet, targetMembers, nodeToRemoveOrAdd);
            } else {
                throw new RuntimeException("Unexpected member count: " + currentMembersSet.size());
            }

            AdminRaftClient adminClient = clusterValidator.getAdminClient();

            // Prepare config change
            Set<Integer> oldObservers = new HashSet<>();
            if (observerActive) {
                oldObservers.add(OBSERVER_NODE_ID);
            }
            Set<Integer> newObservers = new HashSet<>(oldObservers);

            long prepareIndex = adminClient.prepareChange(groupId,
                    currentMembersSet, oldObservers, targetMembers, newObservers,
                    new DtTime(TIMEOUT_SECONDS, TimeUnit.SECONDS)).get();
            log.info("Prepared config change, prepareIndex={}", prepareIndex);

            // Commit config change
            adminClient.commitChange(groupId, prepareIndex, new DtTime(TIMEOUT_SECONDS, TimeUnit.SECONDS)).get();
            log.info("Committed config change, members changed from {} to {}", currentMembersSet, targetMembers);

            // Wait for cluster consistency with new member configuration
            int[] newMemberIds = targetMembers.stream().mapToInt(Integer::intValue).sorted().toArray();
            clusterValidator.waitForClusterConsistency(groupId, newMemberIds, TIMEOUT_SECONDS);

            changeMembersCount++;
            log.info("Members change completed successfully");

        } catch (Exception e) {
            log.error("Error changing members: {}", e.getMessage(), e);
            failCount++;
        }
    }

}
