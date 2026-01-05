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
import com.github.dtprj.dongting.it.support.Validator;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.test.TestDir;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

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

    /**
     * Test three-node cluster startup and leader election.
     * This is the first and reference integration test for the project.
     */
    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void testThreeNodeClusterStartupAndLeaderElection() throws Exception {
        File tempDirFile = TestDir.createTestDir(Raft3NodeSimpleIT.class.getSimpleName());
        Path tempDir = tempDirFile.toPath();

        log.info("=== Starting FirstIntegrationTest ===");
        log.info("Temp directory: {}", tempDir);

        BootstrapProcessManager processManager = new BootstrapProcessManager();
        Validator validator = null;
        List<ProcessInfo> startedProcesses = new ArrayList<>();

        try {
            // Step 1: Generate configuration files
            log.info("Step 1: Generating configuration files for 3-node cluster");
            List<ProcessConfig> configs = ConfigFileGenerator.generateClusterConfig(NODE_IDS, GROUP_ID, tempDir);

            // Step 2: Start all nodes
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

            // Step 3: Initialize AdminRaftClient and wait for leader election
            log.info("Step 3: Initializing AdminRaftClient and waiting for leader election");

            validator = new Validator();
            validator.initialize(NODE_IDS, GROUP_ID);

            // Step 4: Wait for leader election
            log.info("Step 4: Waiting for leader election (up to 60 seconds)");
            validator.waitForLeaderElection(GROUP_ID);

            log.info("=== Leader election and consistency verification passed ===");

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
            // Step 6: Cleanup
            log.info("Step 6: Cleaning up resources");
            if (validator != null) {
                try {
                    validator.close();
                } catch (Exception e) {
                    log.warn("Error closing validator", e);
                }
            }
            processManager.stopAllNodes();
            log.info("=== FirstIntegrationTest completed ===");
        }
    }
}
