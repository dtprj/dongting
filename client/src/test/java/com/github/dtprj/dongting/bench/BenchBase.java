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
import java.util.concurrent.atomic.LongAdder;

/**
 * @author huangli
 */
public abstract class BenchBase {

    protected final int threadCount;
    private final long testTime;
    private final long warmupTime;
    private Thread[] threads;
    protected volatile boolean stop = false;
    protected LongAdder successCount = new LongAdder();
    protected LongAdder failCount = new LongAdder();

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
        threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            int threadIndex = i;
            threads[i] = new Thread(() -> run(threadIndex));
            threads[i].start();
        }
        Thread.sleep(warmupTime);
        long warmupCount = successCount.sum();
        long warmupFailCount = failCount.sum();
        Thread.sleep(testTime);
        stop = true;
        long sc = successCount.sum() - warmupCount;
        long fc = failCount.sum() - warmupFailCount;
        for (Thread t : threads) {
            t.join();
        }
        shutdown();

        double ops = sc * 1.0 / testTime * 1000;
        System.out.println("success sc:" + sc + ", ops=" + new DecimalFormat(",###").format(ops));

        ops = fc * 1.0 / testTime * 1000;
        System.out.println("fail sc:" + fc + ", ops=" + new DecimalFormat(",###").format(ops));
    }

    public void run(int threadIndex) {
        while (!stop) {
            test(threadIndex);
        }
    }

    public abstract void test(int threadIndex);
}
