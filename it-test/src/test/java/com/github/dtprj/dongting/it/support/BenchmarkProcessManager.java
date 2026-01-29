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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Manage DtBenchmark process lifecycle for stress testing.
 *
 * @author huangli
 */
public class BenchmarkProcessManager {
    private static final Logger log = LoggerFactory.getLogger(BenchmarkProcessManager.class);

    private static final long PROCESS_START_TIMEOUT_SECONDS = 10;
    private static final long GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS = 5;

    public static class BenchmarkConfig {
        public final String operation;
        public final int maxPending;
        public final int threads;
        public final String servers;
        public final int groupId;
        public final File nodeDir;

        public BenchmarkConfig(String operation, int maxPending, int threads,
                               String servers, int groupId, File nodeDir) {
            this.operation = operation;
            this.maxPending = maxPending;
            this.threads = threads;
            this.servers = servers;
            this.groupId = groupId;
            this.nodeDir = nodeDir;
        }
    }

    public static class BenchmarkProcessInfo {
        public final Process process;
        public final BenchmarkConfig config;
        public final File stdoutFile;
        public final File stderrFile;

        public BenchmarkProcessInfo(Process process, BenchmarkConfig config,
                                    File stdoutFile, File stderrFile) {
            this.process = process;
            this.config = config;
            this.stdoutFile = stdoutFile;
            this.stderrFile = stderrFile;
        }
    }

    /**
     * Start a DtBenchmark process
     */
    public BenchmarkProcessInfo startBenchmark(BenchmarkConfig config) throws IOException, InterruptedException {
        log.info("Starting DtBenchmark {} process with max-pending={}, threads={}",
                config.operation, config.maxPending, config.threads);

        File distDir = ItUtil.findDistDir();
        File moduleDir = new File(distDir, "lib");
        File logbackFile = new File(new File(distDir, "conf"), "logback-benchmark.xml");
        File logsFile = new File(config.nodeDir, "logs");
        logsFile.mkdirs();

        // Create client.properties file with servers
        File clientPropsFile = new File(config.nodeDir, "client.properties");
        try (FileWriter writer = new FileWriter(clientPropsFile)) {
            writer.write("servers=" + config.servers + "\n");
        }

        // Build command
        List<String> command = new ArrayList<>();
        command.add(System.getProperty("java.home") + "/bin/java");
        command.add("-Xmx512M");
        command.add("-XX:MaxDirectMemorySize=256M");

        command.add("-Dlogback.configurationFile=" + logbackFile.getAbsolutePath());
        command.add("-DLOG_DIR=" + logsFile.getAbsolutePath());

        command.add("--module-path");
        command.add(moduleDir.getAbsolutePath());
        command.add("--add-exports");
        command.add("java.base/jdk.internal.misc=dongting.client");

        command.add("-m");
        command.add("dongting.dist/com.github.dtprj.dongting.dist.DtBenchmark");

        command.add("-s");
        command.add(clientPropsFile.getAbsolutePath());
        command.add("-g");
        command.add(String.valueOf(config.groupId));
        command.add("--op");
        command.add(config.operation);
        command.add("--max-pending");
        command.add(String.valueOf(config.maxPending));
        command.add("--duration");
        command.add("86400"); // Run for 24 hours (will be stopped externally)

        log.debug("Start command: {}", String.join(" ", command));

        // Redirect stdout/stderr to files to avoid memory overflow
        File stdoutFile = new File(logsFile, "stdout.log");
        File stderrFile = new File(logsFile, "stderr.log");

        // Start process
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.directory(config.nodeDir);
        pb.redirectOutput(ProcessBuilder.Redirect.to(stdoutFile));
        pb.redirectError(ProcessBuilder.Redirect.to(stderrFile));
        Process process = pb.start();

        BenchmarkProcessInfo processInfo = new BenchmarkProcessInfo(process, config, stdoutFile, stderrFile);

        // Wait for process to be ready
        if (!waitForReady(processInfo, PROCESS_START_TIMEOUT_SECONDS)) {
            StringBuilder logs = collectLogs(processInfo);
            log.error("DtBenchmark {} startup logs:\n{}", config.operation, logs);
            stopBenchmark(processInfo, 5);
            throw new IOException("DtBenchmark " + config.operation + " failed to start within "
                    + PROCESS_START_TIMEOUT_SECONDS + " seconds");
        }

        log.info("DtBenchmark {} is ready", config.operation);
        return processInfo;
    }

    /**
     * Wait for process to be ready
     */
    public boolean waitForReady(BenchmarkProcessInfo processInfo, long timeoutSeconds) throws InterruptedException {
        long startTime = System.nanoTime();
        long deadline = startTime + TimeUnit.SECONDS.toNanos(timeoutSeconds);

        long checkInterval = 100;
        long readyWaitNanos = TimeUnit.SECONDS.toNanos(2); // Wait 2 seconds before considering ready

        while (System.nanoTime() < deadline) {
            if (!processInfo.process.isAlive()) {
                log.error("DtBenchmark {} process died during startup", processInfo.config.operation);
                return false;
            }

            // Simple check: if process is alive after 2 seconds, assume ready
            long elapsed = System.nanoTime() - startTime;
            if (elapsed >= readyWaitNanos) {
                log.info("DtBenchmark {} appears ready after {}ms",
                        processInfo.config.operation, TimeUnit.NANOSECONDS.toMillis(elapsed));
                return true;
            }

            Thread.sleep(checkInterval);
        }

        return false;
    }

    public void stopBenchmark(BenchmarkProcessInfo processInfo) {
        stopBenchmark(processInfo, GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS);
    }

    /**
     * Stop a benchmark process
     */
    private void stopBenchmark(BenchmarkProcessInfo processInfo, long timeoutSeconds) {
        if (processInfo == null || !processInfo.process.isAlive()) {
            return;
        }

        log.info("Stopping DtBenchmark {}", processInfo.config.operation);

        try {
            // Try graceful shutdown
            processInfo.process.destroy();
            boolean exited = processInfo.process.waitFor(timeoutSeconds, TimeUnit.SECONDS);

            if (exited) {
                int exitCode = processInfo.process.exitValue();
                log.info("DtBenchmark {} terminated with exit code {}", processInfo.config.operation, exitCode);
            }
        } catch (InterruptedException e) {
            log.warn("Interrupted while stopping DtBenchmark {}", processInfo.config.operation, e);
            Thread.currentThread().interrupt();
        } finally {
            if (processInfo.process.isAlive()) {
                // Force kill
                log.warn("DtBenchmark {} did not exit gracefully, forcing termination", processInfo.config.operation);
                processInfo.process.destroyForcibly();
            }
        }
    }

    /**
     * Collect logs from a process (returns file paths since logs are redirected to files)
     */
    public StringBuilder collectLogs(BenchmarkProcessInfo processInfo) {
        StringBuilder sb = new StringBuilder();
        sb.append("=== DtBenchmark ").append(processInfo.config.operation).append(" Log Files ===\n");
        sb.append("STDOUT: ").append(processInfo.stdoutFile.getAbsolutePath()).append("\n");
        sb.append("STDERR: ").append(processInfo.stderrFile.getAbsolutePath()).append("\n");
        log.debug("Log file locations for DtBenchmark {}:\n{}", processInfo.config.operation, sb);
        return sb;
    }
}
