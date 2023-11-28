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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author huangli
 */
public abstract class MpscCountBenchBase {

    protected final int threadCount;
    protected final long count;
    protected final long warmupCount;

    private static final boolean LOG_RT = false;
    private final LongAdder totalNanos = new LongAdder();
    private final AtomicLong maxNanos = new AtomicLong();

    protected final CountDownLatch latch;

    public MpscCountBenchBase(int threadCount, long testTime) {
        this(threadCount, testTime, 5000);
    }

    public MpscCountBenchBase(int threadCount, long count, long warmupCount) {
        this.threadCount = threadCount;
        this.count = count;
        this.warmupCount = warmupCount;
        this.latch = new CountDownLatch(threadCount + 1);
    }

    public void start() throws Exception {
        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            int threadIndex = i;
            threads[i] = new Thread(() -> run(threadIndex));
            threads[i].setName("BenchThread" + i);
            threads[i].start();
        }
        Thread c = new Thread(this::consumerRun);
        c.start();

        for (Thread t : threads) {
            t.join();
        }
        c.join();
    }

    public void run(int threadIndex) {
        try {
            long loop = this.warmupCount;
            for (int i = 0; i < loop; i++) {
                long startTime = 0;
                if (LOG_RT) {
                    startTime = System.nanoTime();
                }
                test(threadIndex, startTime);
            }
            latch.countDown();
            latch.await();
            loop = this.count;
            long t = System.currentTimeMillis();
            for (int i = 0; i < loop; i++) {
                long startTime = 0;
                if (LOG_RT) {
                    startTime = System.nanoTime();
                }
                test(threadIndex, startTime);
            }
            t = System.currentTimeMillis() - t;
            System.out.println("producer " + threadIndex + " finished in " + t + "ms");
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    private void consumerRun() {
        try {
            long loop = this.warmupCount * threadCount;
            for (int i = 0; i < loop; ) {
                if (poll()) {
                    i++;
                }
            }
            latch.countDown();
            latch.await();
            loop = this.count * threadCount;
            long t = System.currentTimeMillis();
            for (int i = 0; i < loop; ) {
                if (poll()) {
                    i++;
                }
            }
            t = System.currentTimeMillis() - t;
            System.out.println("consumer finished in " + t + "ms");
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    protected abstract void test(int threadIndex, long startTime);

    protected abstract boolean poll();

    protected void logRt(long startTime) {
        if (LOG_RT) {
            long x = System.nanoTime() - startTime;
            while (x > maxNanos.longValue()) {
                maxNanos.compareAndSet(maxNanos.longValue(), x);
            }
            totalNanos.add(x);
        }
    }
}
