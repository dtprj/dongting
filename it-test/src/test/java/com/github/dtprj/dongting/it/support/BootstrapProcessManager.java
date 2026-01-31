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

import com.github.dtprj.dongting.it.support.ConfigFileGenerator.ProcessConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Manage Bootstrap process lifecycle for integration tests.
 *
 * @author huangli
 */
public class BootstrapProcessManager {
    private static final Logger log = LoggerFactory.getLogger(BootstrapProcessManager.class);

    private final List<ProcessInfo> processes = new CopyOnWriteArrayList<>();

    public static class ProcessInfo {
        public final Process process;
        public final ProcessConfig config;
        public final File stdoutFile;
        public final File stderrFile;

        public ProcessInfo(Process process, ProcessConfig config, File stdoutFile, File stderrFile) {
            this.process = process;
            this.config = config;
            this.stdoutFile = stdoutFile;
            this.stderrFile = stderrFile;
        }
    }

    /**
     * Start a Bootstrap node process
     */
    public boolean startNode(ProcessConfig config, long startTimeoutSeconds) throws IOException, InterruptedException {
        ProcessInfo processInfo = startNode0(config, startTimeoutSeconds);
        if (processInfo != null) {
            processes.add(processInfo);
            return true;
        }
        return false;
    }

    /**
     * Start a Bootstrap node process
     */
    private ProcessInfo startNode0(ProcessConfig config, long startTimeoutSeconds)
            throws IOException, InterruptedException {
        log.info("Starting node {} with replicate port {}, service port {}",
                config.nodeId, config.replicatePort, config.servicePort);


        File distDir = ItUtil.findDistDir();
        File moduleDir = new File(distDir, "lib");
        File logbackFile = new File(new File(distDir, "conf"), "logback-server.xml");
        File logsFile = new File(config.nodeDir, "logs");

        log.debug("distDir: {}", distDir);
        log.debug("moduleDir: {}", moduleDir);
        log.debug("logbackFile: {}", logbackFile);
        log.debug("logsFile: {}", logsFile);

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
        command.add("dongting.dist/com.github.dtprj.dongting.dist.Bootstrap");

        command.add("-c");
        command.add(config.configFile.getAbsolutePath());
        command.add("-s");
        command.add(config.serversFile.getAbsolutePath());

        log.debug("Start command: {}", String.join(" ", command));

        // Redirect stdout/stderr to files to avoid memory overflow
        //noinspection ResultOfMethodCallIgnored
        if(!logsFile.exists()) {
            assertTrue(logsFile.mkdirs());
        }
        File stdoutFile = new File(logsFile, "stdout.log");
        File stderrFile = new File(logsFile, "stderr.log");

        // Start process
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.directory(config.nodeDir);
        pb.redirectOutput(ProcessBuilder.Redirect.to(stdoutFile));
        pb.redirectError(ProcessBuilder.Redirect.to(stderrFile));
        Process process = pb.start();

        ProcessInfo processInfo = new ProcessInfo(process, config, stdoutFile, stderrFile);

        // Wait for process to be ready
        if (!waitForPortReady(processInfo, startTimeoutSeconds)) {
            stopNode0(processInfo, 5);
            return null;
        }

        log.info("Node {} is ready on replicate port {}", config.nodeId, config.replicatePort);
        return processInfo;
    }

    /**
     * Wait for process to be ready
     */
    private boolean waitForPortReady(ProcessInfo processInfo, long timeoutSeconds) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);

        // First check if process is alive
        long checkInterval = 100;
        while (System.nanoTime() - deadline < 0) {
            if (!processInfo.process.isAlive()) {
                log.error("Node {} process died during startup", processInfo.config.nodeId);
                return false;
            }

            // Check if both replicate port AND service port are ready
            if (isPortListening(processInfo.config.replicatePort) && isPortListening(processInfo.config.servicePort)) {
                return true;
            }

            Thread.sleep(checkInterval);
        }

        return false;
    }

    /**
     * Check if a port is listening
     */
    private boolean isPortListening(int port) {
        try (Socket ignored = new Socket("127.0.0.1", port)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public boolean stopNode(ProcessInfo processInfo, long timeoutSeconds) throws InterruptedException {
        try {
            return stopNode0(processInfo, timeoutSeconds);
        } finally {
            processes.remove(processInfo);
        }
    }

    /**
     * Stop a specific node
     */
    private boolean stopNode0(ProcessInfo processInfo, long timeoutSeconds) throws InterruptedException {
        if (!processInfo.process.isAlive()) {
            log.warn("node {} is not alive", processInfo.config.nodeId);
            return true;
        }

        log.info("Stopping node {}", processInfo.config.nodeId);

        try {
            // Try graceful shutdown
            processInfo.process.destroy();
            boolean exited = processInfo.process.waitFor(timeoutSeconds, TimeUnit.SECONDS);

            if (exited) {
                int exitCode = processInfo.process.exitValue();
                log.info("Node {} terminated with exit code {}", processInfo.config.nodeId, exitCode);
                return true;
            } else {
                return forceStopNode(processInfo, true);
            }
        } finally {
            if (processInfo.process.isAlive()) {
                forceStopNode(processInfo, false);
            }
            processes.remove(processInfo);
        }
    }

    /**
     * Stop a specific node
     */
    public boolean forceStopNode(ProcessInfo processInfo, boolean wait) throws InterruptedException {
        if (!processInfo.process.isAlive()) {
            log.warn("node {} is not alive", processInfo.config.nodeId);
        }
        log.info("Force stopping node {}", processInfo.config.nodeId);
        try {
            // Force kill
            processInfo.process.destroyForcibly();
            if (wait) {
                return processInfo.process.waitFor(30, TimeUnit.SECONDS);
            }
            return false;
        } finally {
            processes.remove(processInfo);
        }
    }

    /**
     * Stop all nodes
     */
    public void stopAllNodes(long timeoutSeconds) throws InterruptedException {
        log.info("Stopping all {} nodes", processes.size());
        try {
            for (ProcessInfo processInfo : processes) {
                stopNode(processInfo, timeoutSeconds);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            for (ProcessInfo processInfo : processes) {
                forceStopNode(processInfo, false);
            }
            throw e;
        }
    }

    /**
     * Restart a node gracefully (stop then start)
     */
    public boolean restartNode(ProcessInfo processInfo, long timeoutSeconds) throws IOException, InterruptedException {
        if (processInfo == null) {
            throw new IllegalArgumentException("ProcessInfo cannot be null");
        }

        int nodeId = processInfo.config.nodeId;
        log.info("Restarting node {}", nodeId);

        // Stop the node gracefully
        if (!stopNode0(processInfo, timeoutSeconds)) {
            return false;
        }

        // Start the node again with the same config
        ProcessInfo newProcessInfo = startNode0(processInfo.config, timeoutSeconds);
        processes.add(newProcessInfo);

        log.info("Node {} restarted successfully", nodeId);
        return true;
    }

    /**
     * Collect logs from a process (returns file paths since logs are redirected to files)
     */
    public StringBuilder collectLogs(ProcessInfo processInfo) {
        StringBuilder sb = new StringBuilder();
        sb.append("=== Node ").append(processInfo.config.nodeId).append(" Log Files ===\n");
        sb.append("STDOUT: ").append(processInfo.stdoutFile.getAbsolutePath()).append("\n");
        sb.append("STDERR: ").append(processInfo.stderrFile.getAbsolutePath()).append("\n");
        log.debug("Log file locations for node {}:\n{}", processInfo.config.nodeId, sb);
        return sb;
    }

    public List<ProcessInfo> getProcesses() {
        return processes;
    }
}
