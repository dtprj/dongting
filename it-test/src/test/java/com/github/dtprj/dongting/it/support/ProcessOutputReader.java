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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Utility class to read stdout and stderr from a subprocess.
 *
 * @author huangli
 */
public class ProcessOutputReader {

    /**
     * Start background threads to read process stdout and stderr.
     *
     * @param process subprocess whose output will be read
     * @param threadPrefix prefix for thread names, e.g., "admin" or "node-1"
     * @return OutputReader containing collected output and reader threads
     */
    public static OutputReader startOutputReaders(Process process, String threadPrefix) {
        return startOutputReaders(process, threadPrefix, null, null);
    }

    /**
     * Start background threads to read process stdout and stderr with optional line callbacks.
     *
     * @param process subprocess whose output will be read
     * @param threadPrefix prefix for thread names, e.g., "admin" or "node-1"
     * @param stdoutLineCallback callback invoked for each stdout line (may be null)
     * @param stderrLineCallback callback invoked for each stderr line (may be null)
     * @return OutputReader containing collected output and reader threads
     */
    public static OutputReader startOutputReaders(Process process,
                                               String threadPrefix,
                                               LineCallback stdoutLineCallback,
                                               LineCallback stderrLineCallback) {
        OutputReader outputReader = new OutputReader();

        outputReader.stdoutReader = createStreamReader(
                process.getInputStream(),
                outputReader.stdoutLines,
                threadPrefix + "-stdout-reader",
                stdoutLineCallback);
        outputReader.stderrReader = createStreamReader(
                process.getErrorStream(),
                outputReader.stderrLines,
                threadPrefix + "-stderr-reader",
                stderrLineCallback);

        return outputReader;
    }

    private static Thread createStreamReader(java.io.InputStream inputStream,
                                          Queue<String> linesQueue,
                                          String threadName,
                                          LineCallback lineCallback) {
        Thread thread = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    linesQueue.add(line);
                    if (lineCallback != null) {
                        lineCallback.onLine(line);
                    }
                }
            } catch (IOException ignored) {
            }
        }, threadName);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    /**
     * Callback interface for processing each line read from process output.
     */
    @FunctionalInterface
    public interface LineCallback {
        void onLine(String line);
    }

    /**
     * Container for collected process output and reader threads.
     */
    public static class OutputReader {
        public final Queue<String> stdoutLines = new ConcurrentLinkedQueue<>();
        public final Queue<String> stderrLines = new ConcurrentLinkedQueue<>();
        public Thread stdoutReader;
        public Thread stderrReader;

        /**
         * Wait for both reader threads to finish.
         *
         * @param timeoutMillis maximum time to wait for each thread
         * @throws InterruptedException if interrupted while waiting
         */
        public void await(long timeoutMillis) throws InterruptedException {
            stdoutReader.join(timeoutMillis);
            stderrReader.join(timeoutMillis);
        }
    }
}
