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
package com.github.dtprj.dongting.it;

import com.github.dtprj.dongting.it.support.BootstrapProcessManager;
import com.github.dtprj.dongting.it.support.BootstrapProcessManager.ProcessInfo;
import com.github.dtprj.dongting.it.support.ConfigFileGenerator;
import com.github.dtprj.dongting.it.support.ConfigFileGenerator.ProcessConfig;
import com.github.dtprj.dongting.it.support.ItUtil;
import com.github.dtprj.dongting.it.support.Validator;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.QueryStatusResp;
import com.github.dtprj.dongting.test.TestDir;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This test start a 3-node RAFT cluster using Bootstrap,
 * verify leader election, and properly cleanup resources.
 *
 * @author huangli
 */
public class Raft3NodeSimpleIT {
    private static final DtLog log = DtLogs.getLogger(Raft3NodeSimpleIT.class);

    private static final int GROUP_ID = 0;
    private static final int[] NODE_IDS = {1, 2, 3};

    private static final long ELECT_TIMEOUT = 1500;
    private static final long RPC_TIMEOUT = 500;
    private static final long CONNECT_TIMEOUT = 500;
    private static final long HEARTBEAT_INTERVAL = 700;
    private static final long PING_INTERVAL = 700;

    /**
     * Test three-node cluster startup and leader election.
     * This is the first and reference integration test for the project.
     */
    @Test
    @Timeout(value = 45, unit = TimeUnit.SECONDS)
    void testThreeNodeClusterStartupAndLeaderElection() throws Exception {
        File tempDirFile = TestDir.createTestDir(Raft3NodeSimpleIT.class.getSimpleName());
        Path tempDir = tempDirFile.toPath();

        log.info("=== Starting " + Raft3NodeSimpleIT.class.getSimpleName() + " ===");
        log.info("Temp directory: {}", tempDir);

        BootstrapProcessManager processManager = new BootstrapProcessManager();
        Validator validator = null;
        List<ProcessInfo> startedProcesses = new ArrayList<>();

        try {
            log.info("Step 1: Generating configuration files for 3-node cluster with optimized timeouts");
            List<ProcessConfig> configs = new ConfigFileGenerator.ClusterConfigBuilder(NODE_IDS, GROUP_ID, tempDir)
                    .electTimeout(ELECT_TIMEOUT)
                    .rpcTimeout(RPC_TIMEOUT)
                    .connectTimeout(CONNECT_TIMEOUT)
                    .heartbeatInterval(HEARTBEAT_INTERVAL)
                    .pingInterval(PING_INTERVAL)
                    .build();

            log.info("Step 2: Starting all nodes");
            for (ProcessConfig config : configs) {
                ProcessInfo processInfo = processManager.startNode(config);
                startedProcesses.add(processInfo);
                log.info("Node {} started successfully", config.nodeId);
            }

            // Verify all processes are alive
            for (ProcessInfo processInfo : startedProcesses) {
                assertTrue(processInfo.process.isAlive(),
                        "Process for node " + processInfo.config.nodeId + " should be alive");
            }

            log.info("Step 3: Initializing AdminRaftClient and waiting for leader election");

            validator = new Validator();
            validator.initialize(NODE_IDS, GROUP_ID);

            log.info("Step 4: Waiting for leader election (up to 60 seconds)");
            int leaderId = validator.waitForClusterConsistency(GROUP_ID, NODE_IDS, 30);

            log.info("Step 5: Running DtKV functional tests");
            runDtKvFunctionalTests(NODE_IDS, GROUP_ID);

            log.info("Step 6: Killing leader and waiting for leader election");
            ProcessInfo leader = startedProcesses.stream().filter(p -> p.config.nodeId == leaderId).findFirst().get();
            int oldLeaderId = leader.config.nodeId;
            processManager.forceStopNode(leader);

            int[] restNodes = IntStream.of(NODE_IDS).filter(n -> n != oldLeaderId).toArray();
            validator.waitForClusterConsistency(GROUP_ID, restNodes, 30);

            log.info("Step 7: Restarting old leader {} and waiting for cluster convergence", oldLeaderId);
            ProcessInfo restartedLeader = processManager.restartNode(leader, 30);
            int leaderIndex = -1;
            for (int i = 0; i < startedProcesses.size(); i++) {
                if (startedProcesses.get(i).config.nodeId == oldLeaderId) {
                    leaderIndex = i;
                    break;
                }
            }
            if (leaderIndex >= 0) {
                startedProcesses.set(leaderIndex, restartedLeader);
            }

            log.info("Step 8: Waiting for all 3 nodes to converge (same leader, term, members)");
            validator.waitForClusterConsistency(GROUP_ID, NODE_IDS, 60);

            log.info("Step 9: Gracefully stopping a follower and restarting it");
            Map<Integer, QueryStatusResp> statusMap = validator.queryAllNodeStatus(GROUP_ID, NODE_IDS);
            QueryStatusResp leaderStatus = statusMap.get(statusMap.values().iterator().next().leaderId);
            final int[] followerIdHolder = new int[]{-1};
            for (int nodeId : NODE_IDS) {
                if (nodeId != leaderStatus.leaderId && nodeId != oldLeaderId) {
                    followerIdHolder[0] = nodeId;
                    break;
                }
            }
            if (followerIdHolder[0] < 0) {
                throw new RuntimeException("Could not find a follower to stop");
            }
            final int followerId = followerIdHolder[0];
            ProcessInfo follower = startedProcesses.stream().filter(p -> p.config.nodeId == followerId).findFirst().get();
            ProcessInfo restartedFollower = processManager.restartNode(follower, 30);
            for (int i = 0; i < startedProcesses.size(); i++) {
                if (startedProcesses.get(i).config.nodeId == followerId) {
                    startedProcesses.set(i, restartedFollower);
                    break;
                }
            }

            log.info("Step 10: Waiting for follower {} to rejoin and cluster to converge", followerId);
            validator.waitForClusterConsistency(GROUP_ID, NODE_IDS, 30);

        } catch (Exception e) {
            log.error("Test failed with exception", e);

            // Collect diagnostic information
            log.error("=== Diagnostic Information ===");
            for (ProcessInfo processInfo : startedProcesses) {
                log.error("Process {} is alive: {}",
                        processInfo.config.nodeId, processInfo.process.isAlive());
                String logs = processManager.collectLogs(processInfo);
                log.error("Process {} logs:\n{}", processInfo.config.nodeId, logs);
            }

            throw e;

        } finally {
            log.info("Step 11: Cleaning up resources");
            if (validator != null) {
                try {
                    validator.close();
                } catch (Exception e) {
                    log.warn("Error closing validator", e);
                }
            }
            processManager.stopAllNodes();
            log.info("=== Raft3NodeSimpleIT completed ===");
        }
    }

    /**
     * Run DtKV functional tests with specific node IDs
     */
    private void runDtKvFunctionalTests(int[] nodeIds, int groupId) {
        try {
            String serversStr = ItUtil.formatServiceServers(nodeIds);
            DtKvValidator dtKvTests = new DtKvValidator(groupId, serversStr);
            dtKvTests.runAllTests();
            log.info("All DtKV functional tests passed successfully");
        } catch (Exception e) {
            log.error("DtKV functional tests failed", e);
            throw new RuntimeException("DtKV functional tests failed", e);
        }
    }
}
