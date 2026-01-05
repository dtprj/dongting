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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * Manage Bootstrap process lifecycle for integration tests.
 *
 * @author huangli
 */
public class BootstrapProcessManager {
    private static final Logger log = LoggerFactory.getLogger(BootstrapProcessManager.class);

    private static final long PROCESS_START_TIMEOUT_SECONDS = 10;
    private static final long GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS = 10;

    private final List<ProcessInfo> processes = new CopyOnWriteArrayList<>();

    public static class ProcessInfo {
        public final Process process;
        public final ProcessConfig config;
        public final List<String> stdoutLines = new CopyOnWriteArrayList<>();
        public final List<String> stderrLines = new CopyOnWriteArrayList<>();

        public ProcessInfo(Process process, ProcessConfig config) {
            this.process = process;
            this.config = config;
        }
    }

    /**
     * Start a Bootstrap node process
     */
    public ProcessInfo startNode(ProcessConfig config) throws IOException, InterruptedException {
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
        command.add("--add-modules");
        command.add("org.slf4j,ch.qos.logback.classic");
        command.add("--add-reads");
        command.add("dongting.client=org.slf4j");
        command.add("--add-reads");
        command.add("dongting.client=ch.qos.logback.classic");

        command.add("-m");
        command.add("dongting.dist/com.github.dtprj.dongting.dist.Bootstrap");

        command.add("-c");
        command.add(config.configFile.getAbsolutePath());
        command.add("-s");
        command.add(config.serversFile.getAbsolutePath());

        log.debug("Start command: {}", String.join(" ", command));

        // Start process
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.directory(config.nodeDir);
        Process process = pb.start();

        ProcessInfo processInfo = new ProcessInfo(process, config);
        processes.add(processInfo);

        // Start output readers
        startOutputReader(process, processInfo);

        // Wait for process to be ready
        if (!waitForReady(processInfo, PROCESS_START_TIMEOUT_SECONDS)) {
            collectLogs(processInfo);
            stopNode(processInfo, 5);
            throw new IOException("Node " + config.nodeId + " failed to start within "
                    + PROCESS_START_TIMEOUT_SECONDS + " seconds");
        }

        log.info("Node {} is ready on replicate port {}", config.nodeId, config.replicatePort);
        return processInfo;
    }

    /**
     * Wait for process to be ready
     */
    public boolean waitForReady(ProcessInfo processInfo, long timeoutSeconds) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);

        // First check if process is alive
        long checkInterval = 100;
        while (System.nanoTime() - deadline < 0) {
            if (!processInfo.process.isAlive()) {
                log.error("Node {} process died during startup", processInfo.config.nodeId);
                return false;
            }

            // Check if port is ready
            if (isPortListening(processInfo.config.replicatePort)) {
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

    /**
     * Start threads to read process output
     */
    private void startOutputReader(Process process, ProcessInfo processInfo) {
        // Read stdout
        Thread stdoutReader = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    processInfo.stdoutLines.add(line);
                    log.debug("[Node-{}-STDOUT] {}", processInfo.config.nodeId, line);
                }
            } catch (IOException e) {
                log.debug("Error reading stdout from node {}", processInfo.config.nodeId, e);
            }
        }, "stdout-reader-" + processInfo.config.nodeId);
        stdoutReader.setDaemon(true);
        stdoutReader.start();

        // Read stderr
        Thread stderrReader = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    processInfo.stderrLines.add(line);
                    log.warn("[Node-{}-STDERR] {}", processInfo.config.nodeId, line);
                }
            } catch (IOException e) {
                log.debug("Error reading stderr from node {}", processInfo.config.nodeId, e);
            }
        }, "stderr-reader-" + processInfo.config.nodeId);
        stderrReader.setDaemon(true);
        stderrReader.start();
    }

    public void stopNode(ProcessInfo processInfo) {
        stopNode(processInfo, GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS);
    }

    /**
     * Stop a specific node
     */
    private void stopNode(ProcessInfo processInfo, long timeoutSeconds) {
        if (processInfo == null || !processInfo.process.isAlive()) {
            return;
        }

        log.info("Stopping node {}", processInfo.config.nodeId);

        try {
            // Try graceful shutdown
            processInfo.process.destroy();
            boolean exited = processInfo.process.waitFor(timeoutSeconds, TimeUnit.SECONDS);

            if (exited) {
                int exitCode = processInfo.process.exitValue();
                log.info("Node {} terminated with exit code {}", processInfo.config.nodeId, exitCode);
            } else {
                // Force kill
                log.warn("Node {} did not exit gracefully, forcing termination", processInfo.config.nodeId);
                processInfo.process.destroyForcibly();
                processInfo.process.waitFor(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            log.warn("Interrupted while stopping node {}", processInfo.config.nodeId, e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Stop a specific node
     */
    public void forceStopNode(ProcessInfo processInfo) {
        if (processInfo == null || !processInfo.process.isAlive()) {
            return;
        }
        log.info("Force stopping node {}", processInfo.config.nodeId);
        try {
            // Force kill
            processInfo.process.destroyForcibly();
            processInfo.process.waitFor(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.warn("Interrupted while stopping node {}", processInfo.config.nodeId, e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Stop all nodes
     */
    public void stopAllNodes() {
        log.info("Stopping all {} nodes", processes.size());
        for (ProcessInfo processInfo : processes) {
            stopNode(processInfo);
        }
        processes.clear();
    }

    /**
     * Collect logs from a process
     */
    public String collectLogs(ProcessInfo processInfo) {
        StringBuilder sb = new StringBuilder();
        sb.append("=== Node ").append(processInfo.config.nodeId).append(" STDOUT ===\n");
        for (String line : processInfo.stdoutLines) {
            sb.append(line).append("\n");
        }
        sb.append("\n=== Node ").append(processInfo.config.nodeId).append(" STDERR ===\n");
        for (String line : processInfo.stderrLines) {
            sb.append(line).append("\n");
        }
        String logs = sb.toString();
        log.debug("Collected logs from node {}:\n{}", processInfo.config.nodeId, logs);
        return logs;
    }
}
