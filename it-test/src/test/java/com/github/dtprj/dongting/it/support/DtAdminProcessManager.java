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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

/**
 * Execute DtAdmin commands as a subprocess for integration testing.
 *
 * @author huangli
 */
public class DtAdminProcessManager {
    private static final long PROCESS_TIMEOUT_SECONDS = 60;

    public static class AdminResult {
        public int exitCode;
        public final Queue<String> stdoutLines;
        public final Queue<String> stderrLines;

        public AdminResult(ProcessOutputReader.OutputReader outputReader) {
            this.exitCode = -1;
            this.stdoutLines = outputReader.stdoutLines;
            this.stderrLines = outputReader.stderrLines;
        }

        public boolean isSuccess() {
            return exitCode == 0;
        }
    }

    /**
     * Execute a DtAdmin command.
     *
     * @param serversPropertiesFilePath path to servers.properties file
     * @param subCommand the admin subcommand to execute
     * @param args additional command line arguments
     * @return the execution result
     */
    private static AdminResult executeCommand(String serversPropertiesFilePath, String subCommand, String... args)
            throws IOException, InterruptedException {
        File distDir = ItUtil.findDistDir();
        File libDir = new File(distDir, "lib");
        File confDir = new File(distDir, "conf");
        File logbackFile = new File(confDir, "logback-admin.xml");

        // Build command - similar to dongting-admin.sh
        List<String> command = new ArrayList<>();
        command.add(System.getProperty("java.home") + "/bin/java");

        // JVM options (smaller than server to reduce memory usage)
        command.add("-Xmx256M");
        command.add("-XX:MaxDirectMemorySize=128M");

        command.add("-Dlogback.configurationFile=" + logbackFile.getAbsolutePath());

        command.add("--module-path");
        command.add(libDir.getAbsolutePath());
        command.add("--add-exports");
        command.add("java.base/jdk.internal.misc=dongting.client");

        command.add("-m");
        command.add("dongting.dist/com.github.dtprj.dongting.dist.DtAdmin");

        // Add -s option for servers.properties
        command.add("-s");
        command.add(serversPropertiesFilePath);

        // Add subcommand
        command.add(subCommand);

        // Add additional arguments
        Collections.addAll(command, args);

        // Start process
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.directory(distDir);
        Process process = pb.start();

        ProcessOutputReader.OutputReader outputReader = ProcessOutputReader.startOutputReaders(process, "admin");

        // Wait for process completion
        boolean completed = process.waitFor(PROCESS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (!completed) {
            process.destroyForcibly();
            throw new IOException("DtAdmin command timed out after " + PROCESS_TIMEOUT_SECONDS + " seconds");
        }

        AdminResult result = new AdminResult(outputReader);
        result.exitCode = process.exitValue();

        // Wait for output readers to finish
        outputReader.await(1000);

        return result;
    }

    /**
     * Execute a DtAdmin command and verify success.
     *
     * @param serversPropertiesFilePath path to servers.properties file
     * @param subCommand the admin subcommand to execute
     * @param args additional command line arguments
     * @return the execution result
     * @throws AssertionError if command fails
     */
    public static AdminResult executeAndVerify(String serversPropertiesFilePath, String subCommand, String... args)
            throws IOException, InterruptedException {
        AdminResult result = executeCommand(serversPropertiesFilePath, subCommand, args);

        if (!result.isSuccess()) {
            StringBuilder sb = new StringBuilder();
            sb.append("DtAdmin command failed with exit code ").append(result.exitCode).append("\n");
            sb.append("Command: ").append(subCommand).append("\n");
            sb.append("Arguments: ").append(String.join(" ", args)).append("\n");
            sb.append("STDOUT:\n");
            for (String line : result.stdoutLines) {
                sb.append(line).append("\n");
            }
            sb.append("STDERR:\n");
            for (String line : result.stderrLines) {
                sb.append(line).append("\n");
            }
            throw new AssertionError(sb.toString());
        }

        return result;
    }
}
