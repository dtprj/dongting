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

import com.github.dtprj.dongting.dtkv.KvClient;
import com.github.dtprj.dongting.it.support.BenchmarkProcessManager;
import com.github.dtprj.dongting.it.support.BenchmarkProcessManager.BenchmarkConfig;
import com.github.dtprj.dongting.it.support.BenchmarkProcessManager.BenchmarkProcessInfo;
import com.github.dtprj.dongting.it.support.BootstrapProcessManager;
import com.github.dtprj.dongting.it.support.BootstrapProcessManager.ProcessInfo;
import com.github.dtprj.dongting.it.support.ClusterValidator;
import com.github.dtprj.dongting.it.support.ConfigFileGenerator;
import com.github.dtprj.dongting.it.support.ConfigFileGenerator.ProcessConfig;
import com.github.dtprj.dongting.it.support.FaultInjectionScheduler;
import com.github.dtprj.dongting.it.support.ItUtil;
import com.github.dtprj.dongting.it.support.StressLockValidator;
import com.github.dtprj.dongting.it.support.StressRwValidator;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.test.TestDir;
import com.github.dtprj.dongting.test.Tick;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Extreme stress test for linearizability under high pressure and fault injection.
 * This test is disabled by default and should be enabled manually for long-running tests.
 *
 * @author huangli
 */
public class StressIT {
    private static final DtLog log = DtLogs.getLogger(StressIT.class);

    // Test configuration
    private static final int QUICK_MODE_DURATION_SECONDS = 300;
    private static final int FAULT_INJECTION_INTERVAL_SECONDS = 60;
    private static final int WRITE_READ_VALIDATOR_THREADS = 2;
    private static final int LOCK_VALIDATORS = 2;
    private static final int TRANSACTION_VALIDATOR_THREADS = 2;
    private static final int VALIDATOR_KEY_SPACE = 10000;
    private static final int BACKGROUND_MAX_PENDING = 2000 / Tick.tick(1);
    private static final long LOCK_LEASE_MILLIS = 45000;

    // Cluster configuration
    private static final int GROUP_ID = 0;
    private static final int[] MEMBER_IDS = {1, 2, 3};

    // Statistics counters
    private final AtomicLong writeReadVerifyCount = new AtomicLong(0);
    private final AtomicLong writeReadViolationCount = new AtomicLong(0);
    private final AtomicLong writeReadFailureCount = new AtomicLong(0);

    private final AtomicLong lockVerifyCount = new AtomicLong(0);
    private final AtomicLong lockViolationCount = new AtomicLong(0);
    private final AtomicLong lockFailureCount = new AtomicLong(0);

    private boolean failed;

    private final AtomicBoolean stop = new AtomicBoolean();
    private FaultInjectionScheduler faultInjector;

    @Test
    @Timeout(value = 365, unit = TimeUnit.DAYS)
    void test() throws Exception {
        String s = System.getProperty("stressMode");
        if (s == null) {
            // default not run
            return;
        }
        if (s.equals("quick")) {
            runStressTest(true);
        } else if (s.equals("full")) {
            runStressTest(false);
        }

    }

    private void runStressTest(boolean quickMode) throws Exception {
        File baseDirFile = TestDir.createTestDir(StressIT.class.getSimpleName());
        Path baseDirPath = baseDirFile.toPath();

        log.info("=== Starting StressIT ===");
        log.info("Temp directory: {}", baseDirPath);

        if (quickMode) {
            log.info("Test duration: FOREVER");
        } else {
            log.info("Test duration: QUICK");
        }

        log.info("Fault injection interval: {} seconds", FAULT_INJECTION_INTERVAL_SECONDS);
        log.info("Write-Read validators: {}", WRITE_READ_VALIDATOR_THREADS);
        log.info("Lock validator pairs: {}", LOCK_VALIDATORS);
        log.info("Transaction validators: {}", TRANSACTION_VALIDATOR_THREADS);

        BootstrapProcessManager processManager = new BootstrapProcessManager();
        BenchmarkProcessManager benchmarkManager = new BenchmarkProcessManager();
        ClusterValidator validator = null;
        BenchmarkProcessInfo putProcess = null;
        BenchmarkProcessInfo getProcess = null;

        try {
            // Step 1: Generate configuration and start cluster
            log.info("Step 1: Starting 3-node cluster");
            List<ProcessConfig> configs = new ConfigFileGenerator.ClusterConfigBuilder(MEMBER_IDS, GROUP_ID, baseDirPath)
                    .stressTest(!quickMode)
                    .build();

            for (ProcessConfig config : configs) {
                assertTrue(processManager.startNode(config, 10));
                log.info("Node {} started successfully", config.nodeId);
            }

            log.info("Step 2: Waiting for leader election");
            validator = new ClusterValidator();
            validator.initialize(MEMBER_IDS, GROUP_ID);
            int leaderId = validator.waitForClusterConsistency(GROUP_ID, MEMBER_IDS, 60);
            log.info("Cluster ready, leader is {}", leaderId);


            // Step 3: Start validator threads
            log.info("Step 3: Starting validator threads");

            // Start StressRwValidator
            List<Thread> writeReadValidatorThreads = new ArrayList<>();
            for (int i = 0; i < WRITE_READ_VALIDATOR_THREADS; i++) {
                StressRwValidator wrValidator = new StressRwValidator(
                        i, GROUP_ID, VALIDATOR_KEY_SPACE, this::createKvClient,
                        writeReadVerifyCount, writeReadViolationCount, writeReadFailureCount, stop);
                Thread t = new Thread(wrValidator);
                t.start();
                writeReadValidatorThreads.add(t);
            }

            // Start StressLockValidator (in pairs)
            List<Thread> lockValidatorThreads = new ArrayList<>();
            for (int i = 0; i < LOCK_VALIDATORS; i++) {
                StressLockValidator lockValidator = new StressLockValidator(GROUP_ID, i, LOCK_LEASE_MILLIS,
                        this::createKvClient, lockVerifyCount, lockViolationCount, lockFailureCount, stop);
                Thread t = new Thread(lockValidator);
                t.start();
                lockValidatorThreads.add(t);
            }

            log.info("All validators started");

            // Step 4: Start background pressure processes
            log.info("Step 4: Starting background pressure processes");
            String servers = ItUtil.formatServiceServers(MEMBER_IDS);

            File putBenchDir = new File(baseDirPath.toFile(), "benchmark-put");
            assertTrue(putBenchDir.mkdirs());
            BenchmarkConfig putConfig = new BenchmarkConfig("put", BACKGROUND_MAX_PENDING, 1,
                    servers, GROUP_ID, putBenchDir);
            putProcess = benchmarkManager.startBenchmark(putConfig);
            log.info("PUT benchmark process started");

            File getBenchDir = new File(baseDirPath.toFile(), "benchmark-get");
            assertTrue(getBenchDir.mkdirs());
            BenchmarkConfig getConfig = new BenchmarkConfig("get", BACKGROUND_MAX_PENDING, 1,
                    servers, GROUP_ID, getBenchDir);
            getProcess = benchmarkManager.startBenchmark(getConfig);
            log.info("GET benchmark process started");

            // Step 5: Start fault injection scheduler
            log.info("Step 5: Starting fault injection scheduler");
            faultInjector = new FaultInjectionScheduler(GROUP_ID, MEMBER_IDS, FAULT_INJECTION_INTERVAL_SECONDS,
                    processManager, validator, stop);
            faultInjector.start();
            log.info("Fault injection scheduler started");

            // Step 6: Run for specified duration
            long testDurationMillis;
            if (quickMode) {
                testDurationMillis = QUICK_MODE_DURATION_SECONDS * 1000L;
            } else {
                testDurationMillis = 0;
            }
            log.info("Step 6: Running test for {} ", quickMode ? 5 + " minutes" : "FOREVER");
            long startTime = System.currentTimeMillis();

            while (!quickMode || (System.currentTimeMillis() - startTime < testDurationMillis)) {
                Thread.sleep(5000);

                // Check for consistency violations
                if (writeReadViolationCount.get() > 0 || lockViolationCount.get() > 0) {
                    log.error("Consistency violation detected, stopping test");
                    break;
                }
                if (!faultInjector.isAlive()) {
                    failed = true;
                    log.error("Fault injection thread is not alive, stopping test");
                    break;
                }
                for (Thread thread : writeReadValidatorThreads) {
                    if (!thread.isAlive()) {
                        failed = true;
                        log.error("WriteReadValidator thread is not alive, stopping test");
                        break;
                    }
                }
                for (Thread thread : lockValidatorThreads) {
                    if (!thread.isAlive()) {
                        failed = true;
                        log.error("LockValidator thread is not alive, stopping test");
                        break;
                    }
                }
            }

            log.info("Test duration completed or stopped");

            // Signal validators to stop gracefully
            stop.set(true);

            // Step 7: Stop fault injector
            log.info("Step 7: Stopping fault injector");
            faultInjector.interrupt();
            faultInjector.join(60 * 1000);

            // Step 8: Stop validators
            log.info("Step 8: Stopping validators");
            stopValidatorThreads(writeReadValidatorThreads);
            stopValidatorThreads(lockValidatorThreads);
        } catch (Throwable e) {
            failed = true;
            log.error("Test failed with exception", e);

            log.error("=== Diagnostic Information ===");
            for (ProcessInfo processInfo : processManager.getProcesses()) {
                log.error("Process {} is alive: {}",
                        processInfo.config.nodeId, processInfo.process.isAlive());
                StringBuilder logs = processManager.collectLogs(processInfo);
                log.error("Process {} logs:\n{}", processInfo.config.nodeId, logs);
            }

            throw e;

        } finally {
            log.info("Cleaning up resources");

            // Stop benchmark processes
            if (putProcess != null && putProcess.process.isAlive()) {
                benchmarkManager.stopBenchmark(putProcess);
            }
            if (getProcess != null && getProcess.process.isAlive()) {
                benchmarkManager.stopBenchmark(getProcess);
            }

            // Close validator
            if (validator != null) {
                try {
                    validator.close();
                } catch (Exception e) {
                    log.warn("Error closing validator", e);
                }
            }

            // Stop all cluster nodes
            assertTrue(processManager.stopAllNodes(30));

            log.info("=== StressIT completed ===");
            printTestReport();
        }
        assertEquals(0, writeReadViolationCount.get());
        assertEquals(0, lockViolationCount.get());
        assertFalse(failed);
    }

    private KvClient createKvClient(String name) {
        KvClient client = new KvClient();
        client.getRaftClient().getNioClient().getConfig().name = name;
        client.start();

        String serversStr = ItUtil.formatServiceServers(MEMBER_IDS);
        client.getRaftClient().clientAddNode(serversStr);
        client.getRaftClient().clientAddOrUpdateGroup(GROUP_ID, MEMBER_IDS);
        return client;
    }

    private void printTestReport() {
        log.info("=== StressIT Test Report ===");
        // Note: TEST_DURATION_HOURS might not reflect actual duration in quick test

        log.info("--- Verification Statistics ---");
        log.info("Write-Read validation:");
        log.info("  - Total verifications: {}", writeReadVerifyCount.get());
        log.info("  - Consistency violations: {}", writeReadViolationCount.get());
        log.info("  - Operation failures: {} (allowed)", writeReadFailureCount.get());

        log.info("Distributed lock validation:");
        log.info("  - Total verifications: {}", lockVerifyCount.get());
        log.info("  - Lock violations: {}", lockViolationCount.get());
        log.info("  - Operation failures: {} (allowed)", lockFailureCount.get());

        log.info("--- Fault Injection Statistics ---");
        log.info("  - Transfer Leader count: {}", faultInjector.transferLeaderCount);
        log.info("  - Graceful stop count: {}", faultInjector.gracefulStopCount);
        log.info("  - Force kill count: {}", faultInjector.forceKillCount);
        log.info("  - fail count: {}", faultInjector.failCount);

        log.info("=== Test Report End ===");

        // Check for violations
        long totalViolations = writeReadViolationCount.get() + lockViolationCount.get();
        if (totalViolations > 0 || failed) {
            log.error("TEST FAILED: {} consistency violations detected", totalViolations);
        } else {
            log.info("TEST PASSED: No consistency violations detected");
        }
    }

    private void stopValidatorThreads(List<Thread> threads) {
        for (Thread thread : threads) {
            thread.interrupt();
            try {
                thread.join(10 * 1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
