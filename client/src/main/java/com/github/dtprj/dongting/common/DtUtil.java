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
package com.github.dtprj.dongting.common;

import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author huangli
 */
public class DtUtil {
    private static final DtLog log = DtLogs.getLogger(DtUtil.class);

    private static final int JAVA_VER = majorVersion(System.getProperty("java.specification.version", "1.8"));

    public static final int RPC_MAJOR_VER = 1;
    public static final int RPC_MINOR_VER = 0;

    // 0 no debug code
    // 1 base debug code
    // 2 more debug code
    // 3 all debug code
    public static int DEBUG = Integer.parseInt(System.getProperty("dt.debug", "1"));

    private static final Runtime RUNTIME = Runtime.getRuntime();
    private static int CPU_COUNT = RUNTIME.availableProcessors();
    private static int cpuCountInvoke;

    public final static ScheduledExecutorService SCHEDULED_SERVICE = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "DtSchedule");
        t.setDaemon(true);
        return t;
    });

    public static void close(AutoCloseable... resources) {
        for (AutoCloseable res : resources) {
            close(res);
        }
    }

    public static void close(AutoCloseable res) {
        if (res != null) {
            try {
                res.close();
            } catch (Throwable e) {
                log.error("close fail", e);
            }
        }
    }

    public static void stop(DtTime timeout, LifeCircle... lifeCircles) {
        for (LifeCircle lf : lifeCircles) {
            if (lf != null) {
                try {
                    lf.stop(timeout);
                } catch (Exception e) {
                    log.error("stop fail", e);
                }
            }
        }
    }

    public static int javaVersion() {
        return JAVA_VER;
    }

    // Package-private for testing only
    static int majorVersion(final String javaSpecVersion) {
        final String[] components = javaSpecVersion.split("\\.");
        final int[] version = new int[components.length];
        for (int i = 0; i < components.length; i++) {
            version[i] = Integer.parseInt(components[i]);
        }

        if (version[0] == 1) {
            assert version[1] >= 8;
            return version[1];
        } else {
            return version[0];
        }
    }

    /**
     * Checks that the given argument is strictly positive. If it is not, throws {@link IllegalArgumentException}.
     * Otherwise, returns the argument.
     */
    public static int checkPositive(int i, String name) {
        if (i <= 0) {
            throw new IllegalArgumentException(name + " : " + i + " (expected: > 0)");
        }
        return i;
    }

    public static long checkPositive(long value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " : " + value + " (expected: > 0)");
        }
        return value;
    }

    public static float checkPositive(float i, String name) {
        if (i <= 0) {
            throw new IllegalArgumentException(name + " : " + i + " (expected: > 0)");
        }
        return i;
    }

    public static int checkNotNegative(int i, String name) {
        if (i < 0) {
            throw new IllegalArgumentException(name + " : " + i + " (expected: >= 0)");
        }
        return i;
    }

    public static long checkNotNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " : " + value + " (expected: >= 0)");
        }
        return value;
    }

    public static void restoreInterruptStatus() {
        if (!Thread.currentThread().isInterrupted()) {
            Thread.currentThread().interrupt();
        }
    }

    public static int processorCount() {
        if (cpuCountInvoke++ % 100 == 0) {
            CPU_COUNT = RUNTIME.availableProcessors();
        }
        return CPU_COUNT;
    }

    public static <T> CompletableFuture<T> failedFuture(Throwable e) {
        CompletableFuture<T> f = new CompletableFuture<>();
        f.completeExceptionally(e);
        return f;
    }

    public static Throwable rootCause(Throwable ex) {
        if (ex == null) {
            return null;
        }
        for (int i = 0; i < 1000; i++) {
            if (ex.getCause() == null) {
                return ex;
            }
            ex = ex.getCause();
        }
        throw new IllegalStateException("loop cause?");
    }
}
