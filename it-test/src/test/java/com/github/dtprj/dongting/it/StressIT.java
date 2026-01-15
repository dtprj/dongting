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
import com.github.dtprj.dongting.it.support.LockExclusivityValidator;
import com.github.dtprj.dongting.it.support.TransactionAtomicityValidator;
import com.github.dtprj.dongting.it.support.WriteReadValidator;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.raft.RaftClientConfig;
import com.github.dtprj.dongting.test.TestDir;
import com.github.dtprj.dongting.test.Tick;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Extreme stress test for linearizability under high pressure and fault injection.
 * This test is disabled by default and should be enabled manually for long-running tests.
 *
 * @author huangli
 */
public class StressIT {
    private static final DtLog log = DtLogs.getLogger(StressIT.class);

    // Test configuration
    private static final int FAULT_INJECTION_INTERVAL_SECONDS = Integer.getInteger("stressTest.faultInterval", 60);
    private static final int WRITE_READ_VALIDATOR_THREADS = Integer.getInteger("stressTest.writeReadThreads", 2);
    private static final int LOCK_VALIDATOR_PAIRS = Integer.getInteger("stressTest.lockPairs", 2);
    private static final int TRANSACTION_VALIDATOR_THREADS = Integer.getInteger("stressTest.txnThreads", 2);
    private static final int VALIDATOR_KEY_SPACE = 10000;
    private static final int BACKGROUND_MAX_PENDING = Integer.getInteger("stressTest.maxPending", 2000) / Tick.tick(1);
    private static final long LOCK_LEASE_MILLIS = 45000;
    private static final int TRANSACTION_OPERATION_COUNT = 5;

    // Cluster configuration
    private static final int GROUP_ID = 0;
    private static final int[] MEMBER_IDS = {1, 2, 3};

    // Statistics counters
    private final AtomicLong writeReadVerifyCount = new AtomicLong(0);
    private final AtomicLong writeReadViolationCount = new AtomicLong(0);
    private final AtomicLong writeReadFailureCount = new AtomicLong(0);

    private final AtomicLong lockVerifyCount = new AtomicLong(0);
    private final AtomicLong lockConflictCount = new AtomicLong(0);
    private final AtomicLong lockFailureCount = new AtomicLong(0);

    private final AtomicLong txnVerifyCount = new AtomicLong(0);
    private final AtomicLong txnViolationCount = new AtomicLong(0);
    private final AtomicLong txnFailureCount = new AtomicLong(0);

    private final AtomicBoolean stop = new AtomicBoolean();
    private FaultInjectionScheduler faultInjector;

    @Disabled("Long-running stress test, enable manually")
    @Test
    @Timeout(value = 365, unit = TimeUnit.DAYS)
    void testLinearizabilityUnderExtremePressure() throws Exception {
        runStressTest(false);
    }

    /**
     * Quick validation test (5 minutes) to verify basic functionality.
     * This test is NOT disabled and can be used for CI/CD pipelines.
     */
    @Test
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    void testLinearizabilityQuickValidation() throws Exception {
        log.info("Running quick validation test (5 minutes)");
        runStressTest(true);
    }

    private void runStressTest(boolean quickMode) throws Exception {
        File baseDirFile = TestDir.createTestDir(StressIT.class.getSimpleName());
        Path baseDirPath = baseDirFile.toPath();

        log.info("=== Starting StressIT ===");
        log.info("Temp directory: {}", baseDirPath);

        if (quickMode) {
            log.info("Test duration: FOREVER");
        } else {
            log.info("Test duration: {} minutes (quick validation)", 5);
        }

        log.info("Fault injection interval: {} seconds", FAULT_INJECTION_INTERVAL_SECONDS);
        log.info("Write-Read validators: {}", WRITE_READ_VALIDATOR_THREADS);
        log.info("Lock validator pairs: {}", LOCK_VALIDATOR_PAIRS);
        log.info("Transaction validators: {}", TRANSACTION_VALIDATOR_THREADS);

        BootstrapProcessManager processManager = new BootstrapProcessManager();
        BenchmarkProcessManager benchmarkManager = new BenchmarkProcessManager();
        ClusterValidator validator = null;
        List<KvClient> kvClients = new ArrayList<>();
        BenchmarkProcessInfo putProcess = null;
        BenchmarkProcessInfo getProcess = null;

        try {
            // Step 1: Generate configuration and start cluster
            log.info("Step 1: Starting 3-node cluster");
            List<ProcessConfig> configs = new ConfigFileGenerator.ClusterConfigBuilder(MEMBER_IDS, GROUP_ID, baseDirPath)
                    .stressTest(true)
                    .build();

            for (ProcessConfig config : configs) {
                assertTrue(processManager.startNode(config, 10));
                log.info("Node {} started successfully", config.nodeId);
            }

            log.info("Step 2: Waiting for leader election");
            validator = new ClusterValidator();
            validator.initialize(MEMBER_IDS, GROUP_ID);
            int leaderId = validator.waitForClusterConsistency(GROUP_ID, MEMBER_IDS, 30);
            log.info("Cluster ready, leader is {}", leaderId);

            // Step 3: Create KvClients for validators
            log.info("Step 3: Creating KvClients for validators");
            int totalValidators = WRITE_READ_VALIDATOR_THREADS + LOCK_VALIDATOR_PAIRS * 2 + TRANSACTION_VALIDATOR_THREADS;
            for (int i = 0; i < totalValidators; i++) {
                KvClient client = createKvClient();
                kvClients.add(client);
            }
            // Create top-level directories for validators
            log.info("Creating top-level directories for validators");
            KvClient firstClient = kvClients.get(0);
            createValidatorDirectories(firstClient);

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

            // Step 5: Start validator threads
            log.info("Step 5: Starting validator threads");
            CountDownLatch validatorStartLatch = new CountDownLatch(totalValidators);

            int clientIndex = 0;

            // Start WriteReadValidators
            List<Thread> writeReadValidatorThreads = new ArrayList<>();
            for (int i = 0; i < WRITE_READ_VALIDATOR_THREADS; i++) {
                WriteReadValidator wrValidator = new WriteReadValidator(
                        i, GROUP_ID, VALIDATOR_KEY_SPACE, kvClients.get(clientIndex++),
                        validatorStartLatch, writeReadVerifyCount, writeReadViolationCount, writeReadFailureCount, stop);
                Thread t = new Thread(wrValidator);
                t.start();
                writeReadValidatorThreads.add(t);
            }

            // Start LockExclusivityValidators (in pairs)
            List<Thread> lockValidatorThreads = new ArrayList<>();
            for (int i = 0; i < LOCK_VALIDATOR_PAIRS; i++) {
                for (int j = 0; j < 2; j++) {
                    LockExclusivityValidator lockValidator = new LockExclusivityValidator(
                            i, j, GROUP_ID, LOCK_LEASE_MILLIS, kvClients.get(clientIndex++),
                            validatorStartLatch, lockVerifyCount, lockConflictCount, lockFailureCount, stop);
                    Thread t = new Thread(lockValidator);
                    t.start();
                    lockValidatorThreads.add(t);
                }
            }

            // Start TransactionAtomicityValidators (half writers, half readers)
            List<Thread> txnValidatorThreads = new ArrayList<>();
            for (int i = 0; i < TRANSACTION_VALIDATOR_THREADS; i++) {
                boolean isWriter = (i % 2 == 0);
                TransactionAtomicityValidator txnValidator = new TransactionAtomicityValidator(
                        i, GROUP_ID, TRANSACTION_OPERATION_COUNT, LOCK_LEASE_MILLIS, isWriter,
                        kvClients.get(clientIndex++), validatorStartLatch, txnVerifyCount,
                        txnViolationCount, txnFailureCount, stop);
                Thread t = new Thread(txnValidator);
                t.start();
                txnValidatorThreads.add(t);
            }

            // Wait for all validators to start
            Assertions.assertTrue(validatorStartLatch.await(20, TimeUnit.SECONDS));
            log.info("All validators started");

            // Step 6: Start fault injection scheduler
            log.info("Step 6: Starting fault injection scheduler");
            faultInjector = new FaultInjectionScheduler(GROUP_ID, MEMBER_IDS, FAULT_INJECTION_INTERVAL_SECONDS,
                    processManager, validator, stop);
            faultInjector.start();
            log.info("Fault injection scheduler started");

            // Step 7: Run for specified duration
            long testDurationMillis;
            if (quickMode) {
                testDurationMillis = 5 * 60L * 1000L; // 5 minutes for quick test
            } else {
                testDurationMillis = 0;
            }
            log.info("Step 7: Running test for {} ", quickMode ? 5 + " minutes" : "FOREVER");
            long startTime = System.currentTimeMillis();

            while (!quickMode || (System.currentTimeMillis() - startTime < testDurationMillis)) {
                Thread.sleep(5000);

                // Check for consistency violations
                if (writeReadViolationCount.get() > 0 || lockConflictCount.get() > 0 || txnViolationCount.get() > 0) {
                    log.error("Consistency violation detected, stopping test");
                    break;
                }
                if (!faultInjector.isAlive()) {
                    log.error("Fault injection thread is not alive, stopping test");
                    break;
                }
                for (Thread thread : writeReadValidatorThreads) {
                    if (!thread.isAlive()) {
                        log.error("WriteReadValidator thread is not alive, stopping test");
                        break;
                    }
                }
                for (Thread thread : lockValidatorThreads) {
                    if (!thread.isAlive()) {
                        log.error("LockValidator thread is not alive, stopping test");
                        break;
                    }
                }
                for (Thread thread : txnValidatorThreads) {
                    if (!thread.isAlive()) {
                        log.error("TransactionValidator thread is not alive, stopping test");
                        break;
                    }
                }
            }

            log.info("Test duration completed or stopped");

            // Signal validators to stop gracefully
            stop.set(true);

            // Step 8: Stop fault injector
            log.info("Step 8: Stopping fault injector");
            faultInjector.interrupt();
            faultInjector.join(60 * 1000);

            // Step 9: Stop validators
            log.info("Step 9: Stopping validators");
            stopValidatorThreads(writeReadValidatorThreads);
            stopValidatorThreads(lockValidatorThreads);
            stopValidatorThreads(txnValidatorThreads);

        } catch (Throwable e) {
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
            log.info("Step 10: Cleaning up resources");

            // Stop benchmark processes
            if (putProcess != null && putProcess.process.isAlive()) {
                benchmarkManager.stopBenchmark(putProcess);
            }
            if (getProcess != null && getProcess.process.isAlive()) {
                benchmarkManager.stopBenchmark(getProcess);
            }

            // Close KvClients
            for (KvClient client : kvClients) {
                try {
                    client.stop(new DtTime(5, TimeUnit.SECONDS));
                } catch (Exception e) {
                    log.warn("Error stopping KvClient", e);
                }
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
    }

    private KvClient createKvClient() {
        RaftClientConfig raftClientConfig = new RaftClientConfig();
        KvClientConfig kvClientConfig = new KvClientConfig();
        KvClient client = new KvClient(kvClientConfig, raftClientConfig, new NioClientConfig());
        client.start();

        String serversStr = ItUtil.formatServiceServers(MEMBER_IDS);
        client.getRaftClient().clientAddNode(serversStr);
        client.getRaftClient().clientAddOrUpdateGroup(GROUP_ID, MEMBER_IDS);
        return client;
    }

    private void createValidatorDirectories(KvClient client) {
        // Create top-level directory "StressIT"
        client.mkdir(GROUP_ID, "StressIT".getBytes());

        // Create directories for WriteReadValidator: StressIT.WR and StressIT.WR.{threadId}
        client.mkdir(GROUP_ID, "StressIT.WR".getBytes());
        for (int i = 0; i < WRITE_READ_VALIDATOR_THREADS; i++) {
            client.mkdir(GROUP_ID, ("StressIT.WR." + i).getBytes());
        }

        // Create directories for LockExclusivityValidator: StressIT.Lock and StressIT.Lock.{pairId}
        client.mkdir(GROUP_ID, "StressIT.Lock".getBytes());
        for (int i = 0; i < LOCK_VALIDATOR_PAIRS; i++) {
            client.mkdir(GROUP_ID, ("StressIT.Lock." + i).getBytes());
        }

        // Create directories for TransactionAtomicityValidator: StressIT.Txn and StressIT.Txn.{threadId}
        client.mkdir(GROUP_ID, "StressIT.Txn".getBytes());
        for (int i = 0; i < TRANSACTION_VALIDATOR_THREADS; i++) {
            client.mkdir(GROUP_ID, ("StressIT.Txn." + i).getBytes());
            // Create data subdirectory for transaction keys
            client.mkdir(GROUP_ID, ("StressIT.Txn." + i + ".data").getBytes());
        }
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
        log.info("  - Lock conflicts detected: {}", lockConflictCount.get());
        log.info("  - Operation failures: {} (allowed)", lockFailureCount.get());

        log.info("Transaction atomicity validation:");
        log.info("  - Total verifications: {}", txnVerifyCount.get());
        log.info("  - Atomicity violations: {}", txnViolationCount.get());
        log.info("  - Operation failures: {} (allowed)", txnFailureCount.get());

        log.info("--- Fault Injection Statistics ---");
        log.info("  - Transfer Leader count: {}", faultInjector.transferLeaderCount);
        log.info("  - Graceful stop count: {}", faultInjector.gracefulStopCount);
        log.info("  - Force kill count: {}", faultInjector.forceKillCount);
        log.info("  - fail count: {}", faultInjector.failCount);

        log.info("=== Test Report End ===");

        // Check for violations
        long totalViolations = writeReadViolationCount.get() + lockConflictCount.get() + txnViolationCount.get();
        if (totalViolations > 0) {
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
