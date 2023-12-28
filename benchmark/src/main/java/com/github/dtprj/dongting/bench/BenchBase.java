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
package com.github.dtprj.dongting.bench;

import java.text.DecimalFormat;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author huangli
 */
public abstract class BenchBase {
    protected static final int STATE_WARMUP = 0;
    protected static final int STATE_TEST = 1;
    protected static final int STATE_BEFORE_SHUTDOWN = 2;
    protected static final int STATE_AFTER_SHUTDOWN = 3;

    protected final int threadCount;
    private final long testTime;
    private final long warmupTime;
    protected AtomicInteger state = new AtomicInteger(STATE_WARMUP);
    private final LongAdder successCount = new LongAdder();
    private final LongAdder failCount = new LongAdder();

    private static final boolean LOG_RT = true;
    private final LongAdder totalNanos = new LongAdder();
    private final AtomicLong maxNanos = new AtomicLong();

    public BenchBase(int threadCount, long testTime) {
        this(threadCount, testTime, 5000);
    }

    public BenchBase(int threadCount, long testTime, long warmupTime) {
        this.threadCount = threadCount;
        this.testTime = testTime;
        this.warmupTime = warmupTime;
    }

    public void init() throws Exception {
    }

    public void shutdown() throws Exception {
    }

    public void start() throws Exception {
        init();
        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            int threadIndex = i;
            threads[i] = new Thread(() -> run(threadIndex));
            threads[i].setName("BenchThread" + i);
            threads[i].start();
        }
        Thread.sleep(warmupTime);
        state.set(STATE_TEST);

        Thread.sleep(testTime);
        state.set(STATE_BEFORE_SHUTDOWN);

        for (Thread t : threads) {
            t.join();
        }
        shutdown();
        state.set(STATE_AFTER_SHUTDOWN);

        long sc = successCount.sum();
        long fc = failCount.sum();

        double ops = sc * 1.0 / testTime * 1000;
        System.out.println("success sc:" + sc + ", ops=" + new DecimalFormat(",###").format(ops));

        ops = fc * 1.0 / testTime * 1000;
        System.out.println("fail sc:" + fc + ", ops=" + new DecimalFormat(",###").format(ops));

        if (LOG_RT) {
            System.out.printf("Max time: %,d ns%n", maxNanos.longValue());
            System.out.printf("Avg time: %,d ns%n", totalNanos.sum() / (sc + fc));
        }

    }

    public void run(int threadIndex) {
        int s;
        while ((s = state.getOpaque()) < STATE_BEFORE_SHUTDOWN) {
            long startTime = 0;
            if (LOG_RT && s == 1) {
                startTime = System.nanoTime();
            }
            try {
                test(threadIndex, startTime, s);
            } catch (Throwable e) {
                failCount.increment();
            }
        }
    }

    public abstract void test(int threadIndex, long startTime, int state);

    protected void logRt(long startTime, int state) {
        if (LOG_RT && state == 1) {
            long x = System.nanoTime() - startTime;
            while (x > maxNanos.longValue()) {
                maxNanos.compareAndSet(maxNanos.longValue(), x);
            }
            totalNanos.add(x);
        }
    }

    protected void success(int state) {
        if (state == 1) {
            successCount.increment();
        }
    }

    protected void fail(int state) {
        if (state == 1) {
            failCount.increment();
        }
    }
}
