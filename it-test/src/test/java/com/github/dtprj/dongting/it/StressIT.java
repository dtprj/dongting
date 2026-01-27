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

import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.dtkv.KvClient;
import com.github.dtprj.dongting.dtkv.KvClientConfig;
import com.github.dtprj.dongting.it.support.BenchmarkProcessManager;
import com.github.dtprj.dongting.it.support.BenchmarkProcessManager.BenchmarkConfig;
import com.github.dtprj.dongting.it.support.BenchmarkProcessManager.BenchmarkProcessInfo;
import com.github.dtprj.dongting.it.support.BootstrapProcessManager;
import com.github.dtprj.dongting.it.support.BootstrapProcessManager.ProcessInfo;
import com.github.dtprj.dongting.it.support.ClusterValidator;
import com.github.dtprj.dongting.it.support.ConfigFileGenerator;
import com.github.dtprj.dongting.it.support.ConfigFileGenerator.ProcessConfig;
import com.github.dtprj.dongting.it.support.FaultInjector;
import com.github.dtprj.dongting.it.support.ItUtil;
import com.github.dtprj.dongting.it.support.StressLockValidator;
import com.github.dtprj.dongting.it.support.StressRwValidator;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.raft.RaftClientConfig;
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
    private static final long DEFAULT_RUN_SECONDS = 30;
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

    private boolean failed;

    private final AtomicBoolean stop = new AtomicBoolean();

    private final BootstrapProcessManager processManager = new BootstrapProcessManager();
    private final BenchmarkProcessManager benchmarkManager = new BenchmarkProcessManager();
    private final ClusterValidator validator = new ClusterValidator();
    private final FaultInjector faultInjector = new FaultInjector(GROUP_ID, MEMBER_IDS,
            FAULT_INJECTION_INTERVAL_SECONDS, processManager, validator, stop);
    private BenchmarkProcessInfo putProcess;
    private BenchmarkProcessInfo getProcess;
    private List<Thread> writeReadValidatorThreads;
    private List<Thread> lockValidatorThreads;

    @Test
    @Timeout(value = 365, unit = TimeUnit.DAYS)
    void test() throws Exception {
        long duration = Long.parseLong(System.getProperty("duration", String.valueOf(DEFAULT_RUN_SECONDS)));
        boolean mockFault = "".equals(System.getProperty("mockFault"))
                || "true".equalsIgnoreCase(System.getProperty("mockFault"));
        boolean benchmark = "".equals(System.getProperty("benchmark"))
                || "true".equalsIgnoreCase(System.getProperty("benchmark"));
        runStressTest(duration, mockFault, benchmark);
    }

    private void runStressTest(long seconds, boolean mockFault, boolean benchmark) throws Exception {
        File baseDirFile = TestDir.createTestDir(StressIT.class.getSimpleName());
        Path baseDirPath = baseDirFile.toPath();

        log.info("=== Starting StressIT ===");
        log.info("Temp directory: {}", baseDirPath);

        if (seconds == 0) {
            log.info("Test duration: FOREVER");
        } else {
            log.info("Test duration: {} seconds", seconds);
        }

        log.info("Fault injection interval: {} seconds", FAULT_INJECTION_INTERVAL_SECONDS);
        log.info("Write-Read validators: {}", WRITE_READ_VALIDATOR_THREADS);
        log.info("Lock validator pairs: {}", LOCK_VALIDATORS);
        log.info("Transaction validators: {}", TRANSACTION_VALIDATOR_THREADS);

        try {
            // Step 1: Generate configuration and start cluster
            log.info("Step 1: Starting 3-node cluster");
            List<ProcessConfig> configs = new ConfigFileGenerator.ClusterConfigBuilder(MEMBER_IDS, GROUP_ID, baseDirPath)
                    .fullSize(seconds == 0)
                    .build();

            for (ProcessConfig config : configs) {
                assertTrue(processManager.startNode(config, 10));
                log.info("Node {} started successfully", config.nodeId);
            }

            log.info("Step 2: Waiting for leader election");
            validator.initialize(MEMBER_IDS, GROUP_ID);
            int leaderId = validator.waitForClusterConsistency(GROUP_ID, MEMBER_IDS, 60);
            log.info("Cluster ready, leader is {}", leaderId);


            // Step 3: Start validator threads
            log.info("Step 3: Starting validator threads");

            // Start StressRwValidator
            writeReadValidatorThreads = new ArrayList<>();
            for (int i = 0; i < WRITE_READ_VALIDATOR_THREADS; i++) {
                StressRwValidator wrValidator = new StressRwValidator(
                        GROUP_ID, i, VALIDATOR_KEY_SPACE, this::createKvClient, stop);
                Thread t = new Thread(wrValidator);
                t.start();
                writeReadValidatorThreads.add(t);
            }

            // Start StressLockValidator (in pairs)
            lockValidatorThreads = new ArrayList<>();
            for (int i = 0; i < LOCK_VALIDATORS; i++) {
                StressLockValidator lockValidator = new StressLockValidator(GROUP_ID, i, LOCK_LEASE_MILLIS,
                        this::createKvClient, stop);
                Thread t = new Thread(lockValidator);
                t.start();
                lockValidatorThreads.add(t);
            }

            log.info("All validators started");

            // Step 4: Start background pressure processes
            if (benchmark) {
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
            }

            // Step 5: Start fault injection scheduler
            if (mockFault) {
                log.info("Step 5: Starting fault injection scheduler");
                faultInjector.start();
                log.info("Fault injection scheduler started");
            }

            DtUtil.SCHEDULED_SERVICE.scheduleAtFixedRate(this::printTestReport, 1, 1, TimeUnit.MINUTES);

            // Step 6: Run for specified duration
            log.info("Step 6: Running test for {} ", seconds == 0 ? "FOREVER" : (seconds + " seconds"));
            long startTime = System.currentTimeMillis();
            while (seconds == 0 || System.currentTimeMillis() - startTime < seconds * 1000) {
                Thread.sleep(1000);

                // Check for consistency violations
                if (StressRwValidator.violationCount.get() > 0 || StressLockValidator.violationCount.get() > 0) {
                    log.error("Consistency violation detected, stopping test");
                    break;
                }
                if (mockFault && !faultInjector.isAlive()) {
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

            log.info("Test duration completed");
        } catch (InterruptedException e) {
            log.info("Test interrupted");
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
            shutdown(mockFault, benchmark);
        }
        assertEquals(0, StressRwValidator.violationCount.get());
        assertEquals(0, StressLockValidator.violationCount.get());
        assertFalse(failed);
    }

    private void shutdown(boolean mockFault, boolean benchmark) throws InterruptedException {
        // Signal validators to stop gracefully
        stop.set(true);

        log.info("Shutting down StressIT");

        // Step 7: Stop fault injector
        if (mockFault) {
            faultInjector.interrupt();
            faultInjector.join(60 * 1000);
        }

        // Step 8: Stop validators
        if (writeReadValidatorThreads != null) {
            stopValidatorThreads(writeReadValidatorThreads);
        }
        if (lockValidatorThreads != null) {
            stopValidatorThreads(lockValidatorThreads);
        }

        // Stop benchmark processes
        if (benchmark) {
            if (putProcess != null && putProcess.process.isAlive()) {
                benchmarkManager.stopBenchmark(putProcess);
            }
            if (getProcess != null && getProcess.process.isAlive()) {
                benchmarkManager.stopBenchmark(getProcess);
            }
        }

        // Close validator
        try {
            validator.close();
        } catch (Exception e) {
            log.warn("Error closing validator", e);
        }

        // Stop all cluster nodes
        processManager.stopAllNodes(30);

        log.info("=== StressIT completed ===");

        printTestReport();

        // Check for violations
        long totalViolations = StressRwValidator.violationCount.get() + StressLockValidator.violationCount.get();
        if (totalViolations > 0 || failed) {
            log.error("TEST FAILED: {} consistency violations detected", totalViolations);
        } else {
            log.info("TEST PASSED: No consistency violations detected");
        }
    }

    private KvClient createKvClient(String name) {
        KvClient client = new KvClient(new KvClientConfig(), new RaftClientConfig(), new NioClientConfig(name));
        client.start();

        String serversStr = ItUtil.formatServiceServers(MEMBER_IDS);
        client.getRaftClient().clientAddNode(serversStr);
        client.getRaftClient().clientAddOrUpdateGroup(GROUP_ID, MEMBER_IDS);
        return client;
    }

    private void printTestReport() {
        String report = "\n=== StressIT Test Report ===\n"
                + "\n"
                + "--- Verification Statistics ---\n"
                + "Write-Read validation:\n"
                + "  - Total verifications: " + StressRwValidator.verifyCount.get() + "\n"
                + "  - Consistency violations: " + StressRwValidator.violationCount.get() + "\n"
                + "  - Operation failures: " + StressRwValidator.failureCount.get() + " (allowed)\n"
                + "\n"
                + "Distributed lock validation:\n"
                + "  - Total verifications: " + StressLockValidator.verifyCount.get() + "\n"
                + "  - Lock violations: " + StressLockValidator.violationCount.get() + "\n"
                + "  - Operation failures: " + StressLockValidator.failureCount.get() + " (allowed)\n"
                + "\n"
                + "--- Fault Injection Statistics ---\n"
                + "  - Transfer Leader count: " + faultInjector.transferLeaderCount + "\n"
                + "  - Graceful stop count: " + faultInjector.gracefulStopCount + "\n"
                + "  - Force kill count: " + faultInjector.forceKillCount + "\n"
                + "  - fail count: " + faultInjector.failCount + "\n"
                + "\n"
                + "=== Test Report End ===";
        log.info(report);
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
