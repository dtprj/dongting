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

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.dtkv.KvClient;
import com.github.dtprj.dongting.dtkv.KvClientConfig;
import com.github.dtprj.dongting.it.support.BootstrapProcessManager;
import com.github.dtprj.dongting.it.support.BootstrapProcessManager.ProcessInfo;
import com.github.dtprj.dongting.it.support.ClusterValidator;
import com.github.dtprj.dongting.it.support.ConfigFileGenerator;
import com.github.dtprj.dongting.it.support.ConfigFileGenerator.ProcessConfig;
import com.github.dtprj.dongting.it.support.DtKvValidator;
import com.github.dtprj.dongting.it.support.ItUtil;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.raft.RaftClientConfig;
import com.github.dtprj.dongting.test.TestDir;
import com.github.dtprj.dongting.test.Tick;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Stress test with concurrent operations and validation.
 * This test runs background put/get operations while continuously validating
 * correctness with DtKvValidator to ensure system stability under load.
 *
 * @author huangli
 */
public class SimpleStressTestIT {
    private static final DtLog log = DtLogs.getLogger(SimpleStressTestIT.class);
    private static final int TEST_DURATION_SECONDS = 30;

    private static final int GROUP_ID = 0;
    private static final int[] MEMBER_IDS = {1, 2, 3};
    private static final int[] OBSERVER_IDS = {4};
    private static final int[] ALL_NODE_IDS = {1, 2, 3, 4};

    private static final int KEY_COUNT = Math.max(10, 100 / Tick.tick(1));

    private static final int THREAD_COUNT = Math.max(2, 100 / Tick.tick(1));
    private static final int MAX_VALUE_SIZE = Math.max(8 * 1024, (64 * 1024) / Tick.tick(1));
    private static final int MIN_VALUE_SIZE = 1;

    private static final AtomicInteger stressErrors = new AtomicInteger(0);

    private volatile boolean stopped = false;

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void testStressWithValidation() throws Exception {
        File baseDirFile = TestDir.createTestDir(SimpleStressTestIT.class.getSimpleName());
        Path baseDirPath = baseDirFile.toPath();

        log.info("=== Starting " + SimpleStressTestIT.class.getSimpleName() + " ===");
        log.info("Temp directory: {}", baseDirPath);
        log.info("Test duration: {} seconds", TEST_DURATION_SECONDS);
        log.info("Thread count: {}", THREAD_COUNT);
        log.info("Key count: {}", KEY_COUNT);
        log.info("Value size range: {} - {} bytes", MIN_VALUE_SIZE, MAX_VALUE_SIZE);

        BootstrapProcessManager processManager = new BootstrapProcessManager();
        ClusterValidator validator = null;
        List<ProcessInfo> startedProcesses = new ArrayList<>();
        KvClient kvClient = null;
        ExecutorService stressExecutor = null;
        CountDownLatch stressLatch = new CountDownLatch(THREAD_COUNT);

        try {
            log.info("Step 1: Generating configuration files with default timeouts");
            List<ProcessConfig> configs = new ConfigFileGenerator.ClusterConfigBuilder(MEMBER_IDS, GROUP_ID, baseDirPath)
                    .observerIds(OBSERVER_IDS)
                    .build();

            log.info("Step 2: Starting all nodes");
            for (ProcessConfig config : configs) {
                ProcessInfo processInfo = processManager.startNode(config);
                startedProcesses.add(processInfo);
                log.info("Node {} started successfully", config.nodeId);
            }

            for (ProcessInfo processInfo : startedProcesses) {
                assertTrue(processInfo.process.isAlive(),
                        "Process for node " + processInfo.config.nodeId + " should be alive");
            }

            log.info("Step 3: Waiting for leader election");
            validator = new ClusterValidator();
            validator.initialize(ALL_NODE_IDS, GROUP_ID);
            int leaderId = validator.waitForClusterConsistency(GROUP_ID, ALL_NODE_IDS, 30);
            log.info("Leader elected: {}", leaderId);

            log.info("Step 4: Creating KvClient");
            kvClient = createKvClient();
            final KvClient finalKvClient = kvClient;

            log.info("Step 5: Starting stress threads");
            stressExecutor = Executors.newFixedThreadPool(THREAD_COUNT);
            for (int i = 0; i < THREAD_COUNT; i++) {
                final int taskId = i;
                stressExecutor.submit(() -> stressTask(finalKvClient, taskId, stressLatch));
            }
            log.info("Started {} stress threads", THREAD_COUNT);

            log.info("Step 6: Running validation tests under stress for {} seconds", TEST_DURATION_SECONDS);
            DtKvValidator dtKvTests = null;
            try {
                String serversStr = ItUtil.formatServiceServers(ALL_NODE_IDS);
                dtKvTests = new DtKvValidator(GROUP_ID, serversStr);
                dtKvTests.start();

                long endTime = System.currentTimeMillis() + (TEST_DURATION_SECONDS * 1000L);
                int iteration = 0;
                while (System.currentTimeMillis() < endTime) {
                    iteration++;
                    dtKvTests.runAllTests(500);
                    if (iteration % 10 == 0) {
                        log.info("Validation iteration {}, errors so far: {}", iteration, stressErrors.get());
                    }
                }

                log.info("Validation completed {} iterations", iteration);

            } finally {
                if (dtKvTests != null) {
                    dtKvTests.stop();
                }
            }
        } catch (Throwable e) {
            log.error("Test failed with exception", e);

            log.error("=== Diagnostic Information ===");
            for (ProcessInfo processInfo : startedProcesses) {
                log.error("Process {} is alive: {}",
                        processInfo.config.nodeId, processInfo.process.isAlive());
                String logs = processManager.collectLogs(processInfo);
                log.error("Process {} logs:\n{}", processInfo.config.nodeId, logs);
            }

            throw e;

        } finally {
            log.info("Step 7: Stopping stress threads");
            stopped = true;
            if (!stressLatch.await(3, TimeUnit.SECONDS)) {
                log.warn("Stress threads did not complete within 3 seconds");
            }

            int totalErrors = stressErrors.get();
            log.info("Total stress errors: {}", totalErrors);
            assertEquals(0, totalErrors, "Stress test should complete without errors");


            log.info("Step 8: Cleaning up resources");
            if (stressExecutor != null) {
                stressExecutor.shutdownNow();
                try {
                    if (!stressExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                        log.warn("Stress executor did not terminate gracefully");
                    }
                } catch (InterruptedException e) {
                    log.warn("Interrupted while waiting for executor termination", e);
                }
            }

            if (kvClient != null) {
                try {
                    kvClient.stop(new DtTime(5, TimeUnit.SECONDS));
                } catch (Exception e) {
                    log.warn("Error stopping KvClient", e);
                }
            }

            if (validator != null) {
                try {
                    validator.close();
                } catch (Exception e) {
                    log.warn("Error closing validator", e);
                }
            }
            processManager.stopAllNodes();
            log.info("=== " + SimpleStressTestIT.class.getSimpleName() + " completed ===");
        }
    }

    private KvClient createKvClient() {
        RaftClientConfig raftClientConfig = new RaftClientConfig();

        KvClientConfig kvClientConfig = new KvClientConfig();

        KvClient client = new KvClient(kvClientConfig, raftClientConfig, new NioClientConfig());

        client.start();

        String serversStr = ItUtil.formatServiceServers(ALL_NODE_IDS);
        client.getRaftClient().clientAddNode(serversStr);
        client.getRaftClient().clientAddOrUpdateGroup(GROUP_ID, new int[]{1, 2, 3});
        return client;
    }

    private void stressTask(KvClient client, int taskId, CountDownLatch stressLatch) {
        Random random = new Random();
        try {
            client.mkdir(GROUP_ID, String.valueOf(taskId).getBytes(StandardCharsets.UTF_8));
            while (!stopped) {
                int keyIndex = random.nextInt(KEY_COUNT);
                byte[] key = (taskId + "." + keyIndex).getBytes(StandardCharsets.UTF_8);

                int valueSize = MIN_VALUE_SIZE + random.nextInt(MAX_VALUE_SIZE - MIN_VALUE_SIZE);
                byte[] value = new byte[valueSize];
                random.nextBytes(value);

                try {
                    client.put(GROUP_ID, key, value);
                    client.get(GROUP_ID, key);
                } catch (Exception e) {
                    stressErrors.incrementAndGet();
                    log.warn("Stress thread {} operation failed: {}", taskId, e.getMessage());
                }
            }
        } catch (Throwable e) {
            log.error("Stress thread {} encountered unexpected error", taskId, e);
            stressErrors.incrementAndGet();
        } finally {
            stressLatch.countDown();
        }
    }
}
