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
import com.github.dtprj.dongting.it.support.ClusterValidator;
import com.github.dtprj.dongting.it.support.ConfigFileGenerator;
import com.github.dtprj.dongting.it.support.ConfigFileGenerator.ProcessConfig;
import com.github.dtprj.dongting.it.support.DtKvValidator;
import com.github.dtprj.dongting.it.support.ItUtil;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.QueryStatusResp;
import com.github.dtprj.dongting.test.TestDir;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.nio.file.Path;
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
public class SimpleRaftGroupIT {
    private static final DtLog log = DtLogs.getLogger(SimpleRaftGroupIT.class);

    private static final int GROUP_ID = 0;
    private static final int[] MEMBER_IDS = {1, 2, 3};
    private static final int[] OBSERVER_IDS = {4};
    private static final int[] ALL_NODE_IDS = {1, 2, 3, 4};

    private static final long ELECT_TIMEOUT = 1500;
    private static final long RPC_TIMEOUT = 500;
    private static final long CONNECT_TIMEOUT = 500;
    private static final long HEARTBEAT_INTERVAL = 700;
    private static final long PING_INTERVAL = 700;
    private static final long WATCH_TIMEOUT = 5000;

    private static final long CLIENT_RPC_TIMEOUT = 2000;
    private static final long CLIENT_WATCH_HEARTBEAT_MILLIS = 1000;

    /**
     * Test three-node cluster startup and leader election.
     * This is the first and reference integration test for the project.
     */
    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void testThreeNodeClusterStartupAndLeaderElection() throws Exception {
        File tempDirFile = TestDir.createTestDir(SimpleRaftGroupIT.class.getSimpleName());
        Path tempDir = tempDirFile.toPath();

        log.info("=== Starting " + SimpleRaftGroupIT.class.getSimpleName() + " ===");
        log.info("Temp directory: {}", tempDir);

        BootstrapProcessManager processManager = new BootstrapProcessManager();
        ClusterValidator validator = null;
        List<ProcessInfo> startedProcesses = processManager.getProcesses();

        try {
            log.info("Step 1: Generating configuration files for 3-node cluster with optimized timeouts");
            List<ProcessConfig> configs = new ConfigFileGenerator.ClusterConfigBuilder(MEMBER_IDS, GROUP_ID, tempDir)
                    .observerIds(OBSERVER_IDS)
                    .electTimeout(ELECT_TIMEOUT)
                    .rpcTimeout(RPC_TIMEOUT)
                    .connectTimeout(CONNECT_TIMEOUT)
                    .heartbeatInterval(HEARTBEAT_INTERVAL)
                    .pingInterval(PING_INTERVAL)
                    .watchTimeoutMillis(WATCH_TIMEOUT)
                    .build();

            log.info("Step 2: Starting all nodes");
            for (ProcessConfig config : configs) {
                assertTrue(processManager.startNode(config, 10));
                log.info("Node {} started successfully", config.nodeId);
            }

            // Verify all startedProcesses are alive
            for (ProcessInfo processInfo : startedProcesses) {
                assertTrue(processInfo.process.isAlive(),
                        "Process for node " + processInfo.config.nodeId + " should be alive");
            }

            log.info("Step 3: Initializing AdminRaftClient and waiting for leader election");

            validator = new ClusterValidator();
            validator.initialize(ALL_NODE_IDS, GROUP_ID);

            log.info("Step 4: Waiting for leader election (up to 60 seconds)");
            int leaderId = validator.waitForClusterConsistency(GROUP_ID, ALL_NODE_IDS, 30);

            log.info("Step 5: Running DtKV functional tests");
            runDtKvFunctionalTests();

            log.info("Step 6: Killing leader and waiting for leader election");
            ProcessInfo leader = startedProcesses.stream().filter(p -> p.config.nodeId == leaderId).findFirst().get();
            int oldLeaderId = leader.config.nodeId;
            processManager.forceStopNode(leader, true);

            int[] restMembers = IntStream.of(ALL_NODE_IDS).filter(n -> n != oldLeaderId).toArray();
            validator.waitForClusterConsistency(GROUP_ID, restMembers, 30);

            log.info("Step 7: Restarting old leader {} and waiting for cluster convergence", oldLeaderId);
            assertTrue(processManager.startNode(leader.config, 30));


            log.info("Step 8: Waiting for all nodes to converge (same leader, term, members)");
            validator.waitForClusterConsistency(GROUP_ID, ALL_NODE_IDS, 60);

            log.info("Step 9: Gracefully stopping a follower and restarting it");
            Map<Integer, QueryStatusResp> statusMap = validator.queryAllNodeStatus(GROUP_ID, ALL_NODE_IDS);
            QueryStatusResp leaderStatus = statusMap.get(statusMap.values().iterator().next().leaderId);
            final int[] followerIdHolder = new int[]{-1};
            for (int nodeId : MEMBER_IDS) {
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
            assertTrue(processManager.restartNode(follower, 30));


            log.info("Step 10: Waiting for follower {} to rejoin and cluster to converge", followerId);
            validator.waitForClusterConsistency(GROUP_ID, ALL_NODE_IDS, 30);

        } catch (Exception e) {
            log.error("Test failed with exception", e);

            // Collect diagnostic information
            log.error("=== Diagnostic Information ===");
            for (ProcessInfo processInfo : startedProcesses) {
                log.error("Process {} is alive: {}",
                        processInfo.config.nodeId, processInfo.process.isAlive());
                StringBuilder logs = processManager.collectLogs(processInfo);
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
            processManager.stopAllNodes(10);
            log.info("=== Raft3NodeSimpleIT completed ===");
        }
    }

    /**
     * Run DtKV functional tests with specific node IDs
     */
    private void runDtKvFunctionalTests() {
        DtKvValidator dtKvTests = null;
        try {
            String serversStr = ItUtil.formatServiceServers(ALL_NODE_IDS);
            dtKvTests = new DtKvValidator(GROUP_ID, serversStr, CLIENT_RPC_TIMEOUT, CLIENT_WATCH_HEARTBEAT_MILLIS);
            dtKvTests.start();
            dtKvTests.runAllTests(100);
            log.info("All DtKV functional tests passed successfully");
        } catch (Exception e) {
            log.error("DtKV functional tests failed", e);
            throw new RuntimeException("DtKV functional tests failed", e);
        } finally {
            if (dtKvTests != null) {
                dtKvTests.stop();
            }
        }
    }
}
