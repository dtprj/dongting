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
package com.github.dtprj.dongting.dist;

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.dtkv.KvClient;
import com.github.dtprj.dongting.raft.RaftNode;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class DtBenchmark {

    private static final int DEFAULT_VALUE_SIZE = 256;
    private static final boolean DEFAULT_SYNC = false;
    private static final int DEFAULT_THREAD_COUNT = 128;
    private static final int DEFAULT_DURATION = 10;
    private static final int DEFAULT_CLIENT_COUNT = 1;
    private static final int DEFAULT_KEY_COUNT = 10000;
    private static final int DEFAULT_MAX_PENDING = 2000;
    private static final int WARMUP_SECONDS = 3;

    private String serversFile;
    private int groupId;
    private int valueSize = DEFAULT_VALUE_SIZE;
    private boolean sync = DEFAULT_SYNC;
    private int threadCount = DEFAULT_THREAD_COUNT;
    private boolean isPut = true;
    private int duration = DEFAULT_DURATION;
    private int clientCount = DEFAULT_CLIENT_COUNT;
    private int keyCount = DEFAULT_KEY_COUNT;
    private int maxPending = DEFAULT_MAX_PENDING;
    private boolean useNanos = false;
    private boolean useVirtualThreads = true;

    private final List<KvClient> clients = new ArrayList<>();
    private byte[] value;

    private volatile boolean warmup = true;

    private final LongAdder successCount = new LongAdder();
    private final LongAdder failCount = new LongAdder();
    private final LongAdder totalNanos = new LongAdder();
    private final AtomicLong maxNanos = new AtomicLong();

    private final LongAdder lastSecondSuccessCount = new LongAdder();
    private final LongAdder lastSecondFailCount = new LongAdder();
    private final LongAdder lastSecondTotalNanos = new LongAdder();
    private final AtomicLong lastSecondMaxNanos = new AtomicLong();

    private volatile boolean running = true;
    private final CountDownLatch startLatch = new CountDownLatch(1);

    private ScheduledExecutorService statsExecutor;
    private ExecutorService virtualThreadExecutor;

    private int exitCode;

    public static void main(String[] args) throws Exception {
        DtBenchmark benchmark = new DtBenchmark();
        benchmark.run(args);
        System.exit(benchmark.exitCode);
    }

    private DtBenchmark() {
        this.value = new byte[DEFAULT_VALUE_SIZE];
        new Random().nextBytes(value);
    }

    private void run(String[] args) {
        try {
            parseArgs(args);
            Properties props = loadProperties(serversFile);

            System.out.println("Configuration:");
            System.out.println("  Config file: " + serversFile);
            System.out.println("  Servers: " + props.getProperty("servers"));
            System.out.println("  Group ID: " + groupId);
            System.out.println();

            initClients(props);
            preCreateBenchmarkDir();
            startStatsReporter();

            if (sync && useVirtualThreads && DtUtil.JAVA_VER >= 21) {
                virtualThreadExecutor = createVirtualThreadExecutor();
            }

            printBenchmarkConfig();

            System.out.println("Warming up for " + WARMUP_SECONDS + " seconds...");
            System.out.println();

            int threadsPerClient = sync ? (threadCount / clientCount) : 1;
            int remainingThreads = sync ? (threadCount % clientCount) : 0;
            for (int c = 0; c < clientCount; c++) {
                int clientIndex = c;
                int threadsForThisClient = threadsPerClient + (c < remainingThreads ? 1 : 0);
                if (sync) {
                    for (int t = 0; t < threadsForThisClient; t++) {
                        if (virtualThreadExecutor != null) {
                            virtualThreadExecutor.execute(() -> runTestThread(clientIndex));
                        } else {
                            new Thread(() -> runTestThread(clientIndex), "BenchThread-" + c + "-" + t).start();
                        }
                    }
                } else {
                    new Thread(() -> runTestThread(clientIndex), "BenchThread-" + c).start();
                }
            }

            startLatch.countDown();

            Thread.sleep(WARMUP_SECONDS * 1000L);

            warmup = false;
            System.out.println("Warmup complete, starting benchmark...");
            System.out.println();

            Thread.sleep(duration * 1000L);

            running = false;
            statsExecutor.shutdownNow();

            System.out.println();
            printBenchmarkConfig();
            printStats(true);

            shutdown();
            shutdownVirtualThreadExecutor();
        } catch (UsageEx e) {
            System.err.println("Error: " + e.getMessage());
            System.err.println();
            printUsage();
            exitCode = 1;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            //noinspection CallToPrintStackTrace
            e.printStackTrace();
            exitCode = 1;
        }
    }

    private void parseArgs(String[] args) {
        boolean groupIdSpecified = false;
        for (int i = 0; i < args.length; i++) {
            if ("-s".equals(args[i])) {
                if (i + 1 < args.length) {
                    serversFile = args[i + 1];
                    i++;
                } else {
                    throw new UsageEx("-s option requires a file path");
                }
            } else if ("-g".equals(args[i])) {
                if (i + 1 < args.length) {
                    groupId = parseInt(args[i + 1], "--group-id");
                    groupIdSpecified = true;
                    i++;
                } else {
                    throw new UsageEx("--group-id option requires a value");
                }
            } else if ("--value-size".equals(args[i])) {
                if (i + 1 < args.length) {
                    valueSize = parseInt(args[i + 1], "--value-size");
                    resizeValue();
                    i++;
                } else {
                    throw new UsageEx("--value-size option requires a value");
                }
            } else if ("--sync".equals(args[i])) {
                sync = true;
            } else if ("--async".equals(args[i])) {
                sync = false;
            } else if ("--thread-count".equals(args[i])) {
                if (i + 1 < args.length) {
                    threadCount = parseInt(args[i + 1], "--thread-count");
                    i++;
                } else {
                    throw new UsageEx("--thread-count option requires a value");
                }
            } else if ("--op".equals(args[i])) {
                if (i + 1 < args.length) {
                    String opType = args[i + 1].toLowerCase();
                    if (opType.equals("get")) {
                        isPut = false;
                    } else if (!opType.equals("put")) {
                        throw new UsageEx("--op must be 'put' or 'get'");
                    }
                    i++;
                } else {
                    throw new UsageEx("--op option requires a value");
                }
            } else if ("--duration".equals(args[i])) {
                if (i + 1 < args.length) {
                    duration = parseInt(args[i + 1], "--duration");
                    i++;
                } else {
                    throw new UsageEx("--duration option requires a value");
                }
            } else if ("--client-count".equals(args[i])) {
                if (i + 1 < args.length) {
                    clientCount = parseInt(args[i + 1], "--client-count");
                    i++;
                } else {
                    throw new UsageEx("--client-count option requires a value");
                }
            } else if ("--key-count".equals(args[i])) {
                if (i + 1 < args.length) {
                    keyCount = parseInt(args[i + 1], "--key-count");
                    i++;
                } else {
                    throw new UsageEx("--key-count option requires a value");
                }
            } else if ("--max-pending".equals(args[i])) {
                if (i + 1 < args.length) {
                    maxPending = parseInt(args[i + 1], "--max-pending");
                    i++;
                } else {
                    throw new UsageEx("--max-pending option requires a value");
                }
            } else if ("--use-nanos".equals(args[i])) {
                useNanos = true;
            } else if ("--use-virtual-threads".equals(args[i])) {
                useVirtualThreads = true;
            } else if ("--no-virtual-threads".equals(args[i])) {
                useVirtualThreads = false;
            }
        }

        if (serversFile == null) {
            throw new UsageEx("-s option is required");
        }

        if (!groupIdSpecified) {
            throw new UsageEx("-g option is required");
        }
    }

    private int parseInt(String value, String name) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new UsageEx("Invalid integer value for " + name + ": " + value);
        }
    }

    private void resizeValue() {
        if (value == null || value.length != valueSize) {
            value = new byte[valueSize];
            new Random().nextBytes(value);
        }
    }

    private Properties loadProperties(String file) {
        try (FileInputStream fis = new FileInputStream(file)) {
            Properties props = new Properties();
            props.load(fis);
            return props;
        } catch (IOException e) {
            throw new RuntimeException("Failed to load config file " + file, e);
        }
    }

    private void initClients(Properties props) throws Exception {
        String servers = props.getProperty("servers");
        if (servers == null || servers.trim().isEmpty()) {
            throw new RuntimeException("servers property is required");
        }

        List<RaftNode> nodes = RaftNode.parseServers(servers);
        int[] nodeIds = new int[nodes.size()];
        for (int i = 0; i < nodes.size(); i++) {
            nodeIds[i] = nodes.get(i).nodeId;
        }

        for (int i = 0; i < clientCount; i++) {
            KvClient client = new KvClient();
            client.getRaftClient().getNioClient().getConfig().maxOutRequests = maxPending / clientCount;
            client.start();
            client.getRaftClient().clientAddNode(nodes);
            client.getRaftClient().clientAddOrUpdateGroup(groupId, nodeIds);
            client.getRaftClient().fetchLeader(groupId).get();
            clients.add(client);
        }
    }

    private void preCreateBenchmarkDir() {
        byte[] benchmarkDirKey = "benchmark".getBytes();
        try {
            for (KvClient client : clients) {
                client.mkdir(groupId, benchmarkDirKey);
            }
        } catch (Exception e) {
            System.err.println("Warning: Failed to create benchmark directory: " + e.getMessage());
        }
    }

    private void startStatsReporter() {
        statsExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "StatsReporter");
            t.setDaemon(true);
            return t;
        });
        statsExecutor.scheduleAtFixedRate(() -> printStats(false),
                1, 1, TimeUnit.SECONDS);
    }

    private long currentTime() {
        if (useNanos) {
            return System.nanoTime();
        } else {
            return System.currentTimeMillis() * 1_000_000;
        }
    }

    private void runTestThread(int clientIndex) {
        try {
            startLatch.await();

            Random random = new Random();
            KvClient client = clients.get(clientIndex);

            while (running) {
                int keyIndex = Math.abs(random.nextInt() % keyCount);
                byte[] key = ("benchmark.key" + keyIndex).getBytes();

                long startTime = currentTime();
                if (sync) {
                    boolean success = doSyncOp(client, key);
                    long elapsed = currentTime() - startTime;
                    updateStats(success, elapsed);
                } else {
                    doAsyncOp(client, key, startTime);
                }
            }
        } catch (Exception e) {
            //noinspection CallToPrintStackTrace
            e.printStackTrace();
        }
    }

    private boolean doSyncOp(KvClient client, byte[] key) {
        try {
            if (isPut) {
                client.put(groupId, key, value);
            } else {
                client.get(groupId, key);
            }
            return true;
        } catch (Exception e) {
            System.err.println("Error in doSyncOp: " + e);
            return false;
        }
    }

    private void doAsyncOp(KvClient client, byte[] key, long opStartTime) {
        if (isPut) {
            client.put(groupId, key, value, (result, ex) -> {
                if (ex != null) {
                    System.err.println("Async put error: " + ex);
                }
                long elapsed = currentTime() - opStartTime;
                updateStats(ex == null, elapsed);
            });
        } else {
            client.get(groupId, key, (result, ex) -> {
                if (ex != null) {
                    System.err.println("Async get error: " + ex);
                }
                long elapsed = currentTime() - opStartTime;
                updateStats(ex == null, elapsed);
            });
        }
    }

    private void updateStats(boolean success, long elapsedNanos) {
        if (!running) {
            return;
        }

        if (!warmup) {
            if (success) {
                successCount.increment();
            } else {
                failCount.increment();
            }
            totalNanos.add(elapsedNanos);
            while (elapsedNanos > maxNanos.get()) {
                maxNanos.weakCompareAndSetPlain(maxNanos.get(), elapsedNanos);
            }
        }

        if (success) {
            lastSecondSuccessCount.increment();
        } else {
            lastSecondFailCount.increment();
        }
        lastSecondTotalNanos.add(elapsedNanos);
        while (elapsedNanos > lastSecondMaxNanos.get()) {
            lastSecondMaxNanos.weakCompareAndSetPlain(lastSecondMaxNanos.get(), elapsedNanos);
        }
    }

    private void printStats(boolean isFinal) {
        long sc = isFinal ? successCount.sum() : lastSecondSuccessCount.sumThenReset();
        long fc = isFinal ? failCount.sum() : lastSecondFailCount.sumThenReset();
        long total = sc + fc;
        long tn = isFinal ? totalNanos.sum() : lastSecondTotalNanos.sumThenReset();
        long max = isFinal ? maxNanos.get() : lastSecondMaxNanos.getAndSet(0);

        if (total == 0) {
            return;
        }

        double tps = (double) sc / (isFinal ? duration : 1);
        long avg = tn / total;

        DecimalFormat df = new DecimalFormat("#,###");
        System.out.printf("[%s] TPS: %s, Success: %s, Fail: %s, Avg: %,d us, Max: %,d us%n",
                warmup ? "Warmup" : isFinal ? "Final" : "Now",
                df.format(tps),
                df.format(sc),
                df.format(fc),
                avg / 1000,
                max / 1000);
    }

    private void printBenchmarkConfig() {
        System.out.println("Benchmark config:");
        String threadInfo = sync ? threadCount + (virtualThreadExecutor == null ? " platform" : " virtual")
                + " threads total" : "one thread per client";
        System.out.println("  Java " + DtUtil.JAVA_VER + ", " + (sync ? "sync" : "async") + " "
                + (isPut ? "put" : "get") + ", " + keyCount + " keys, "
                + valueSize + " bytes value, " + maxPending + " maxPending");
        System.out.println("  " + clientCount + " clients, " + threadInfo);
        System.out.println();
    }

    private void shutdown() {
        DtTime timeout = new DtTime(10, TimeUnit.SECONDS);
        for (KvClient client : clients) {
            client.stop(timeout);
        }
    }

    private void shutdownVirtualThreadExecutor() {
        if (virtualThreadExecutor != null) {
            virtualThreadExecutor.shutdownNow();
            try {
                virtualThreadExecutor.awaitTermination(3, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private ExecutorService createVirtualThreadExecutor() {
        try {
            Class<?> executorsClass = Class.forName("java.util.concurrent.Executors");
            Method method = executorsClass.getMethod("newVirtualThreadPerTaskExecutor");
            return (ExecutorService) method.invoke(null);
        } catch (Exception e) {
            System.err.println("Failed to create virtual thread executor: " + e.getMessage());
            return null;
        }
    }

    private static void printUsage() {
        System.out.println("Usage: dongting-benchmark.sh -s <file> -g <id> [options]");
        System.out.println();
        System.out.println("Required Options:");
        System.out.println("  -s <file>               Path to client.properties file");
        System.out.println("  -g <id>                 Group ID for benchmark (required)");
        System.out.println();
        System.out.println("Optional Options:");
        System.out.println("  --value-size <size>      Value size in bytes (default: 256)");
        System.out.println("  --sync                   Use synchronous operations (default: async)");
        System.out.println("  --async                  Use asynchronous operations (default)");
        System.out.println("  --thread-count <count>   Total thread count for sync mode (default: 128)");
        System.out.println("  --op <operation>         Operation: put or get (default: put)");
        System.out.println("  --duration <seconds>     Test duration in seconds (default: 10)");
        System.out.println("  --client-count <count>   Number of KvClient instances (default: 1)");
        System.out.println("  --key-count <count>      Number of keys to cycle through (default: 10000)");
        System.out.println("  --max-pending <count>    Number of max pending requests (default: 2000)");
        System.out.println("  --use-nanos              Use System.nanoTime for RT (default: use millis)");
        System.out.println("  --use-virtual-threads    Use virtual threads for sync mode if available (default set, requires Java 21+)");
        System.out.println("  --no-virtual-threads     Disable virtual threads for sync mode");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  dongting-benchmark.sh -s conf/client.properties -g 0");
        System.out.println("  dongting-benchmark.sh -s conf/client.properties -g 0 --op get --duration 30");
        System.out.println("  dongting-benchmark.sh -s conf/client.properties -g 0 --sync --thread-count 64");
        System.out.println("  dongting-benchmark.sh -s conf/client.properties -g 0 --value-size 1024 --client-count 2");
    }

    private static class UsageEx extends RuntimeException {
        public UsageEx(String message) {
            super(message);
        }
    }
}
