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

import com.github.dtprj.dongting.test.Tick;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author huangli
 */
public class SequenceRunnerTest {

    @Test
    public void testSerializedExecution() throws Exception {
        ExecutorService es = Executors.newFixedThreadPool(8);
        try {
            AtomicInteger inRun = new AtomicInteger();
            AtomicInteger maxInRun = new AtomicInteger();
            AtomicInteger runCount = new AtomicInteger();

            SequenceRunner r = new SequenceRunner(es, () -> {
                int c = inRun.incrementAndGet();
                maxInRun.accumulateAndGet(c, Math::max);
                runCount.incrementAndGet();
                try {
                    Thread.sleep(Tick.tick(20));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    inRun.decrementAndGet();
                }
            }, new int[]{Tick.tick(1)});

            int submitThreads = 16;
            int submitsPerThread = 50;
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(submitThreads);
            for (int i = 0; i < submitThreads; i++) {
                es.execute(() -> {
                    try {
                        start.await();
                        for (int j = 0; j < submitsPerThread; j++) {
                            r.submit();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            Assertions.assertTrue(done.await(Tick.tick(5), TimeUnit.SECONDS));

            Assertions.assertTrue(r.awaitFinish(Tick.tick(5_000)));

            Assertions.assertEquals(1, maxInRun.get());
            Assertions.assertEquals(r.getRequestVersion(), r.getFinishedVersion());
            Assertions.assertTrue(runCount.get() >= 1);
        } finally {
            es.shutdownNow();
            Assertions.assertTrue(es.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testCoalesceAndVersionWait() throws Exception {
        ExecutorService es = Executors.newSingleThreadExecutor();
        try {
            AtomicInteger runCount = new AtomicInteger();
            SequenceRunner r = new SequenceRunner(es, runCount::incrementAndGet, new int[]{Tick.tick(1)});

            r.submit();
            r.submit();
            r.submit();

            Assertions.assertTrue(r.awaitFinish(Tick.tick(2_000)));
            Assertions.assertEquals(r.getRequestVersion(), r.getFinishedVersion());

            // task is idempotent, so coalescing is allowed, but must execute at least once.
            Assertions.assertTrue(runCount.get() >= 1);

            // already finished
            Assertions.assertTrue(r.awaitFinish(0));
        } finally {
            es.shutdownNow();
            Assertions.assertTrue(es.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testExceptionNotBreakRunner() throws Exception {
        ExecutorService es = Executors.newSingleThreadExecutor();
        try {
            AtomicInteger runCount = new AtomicInteger();
            // no retry intervals: should not auto retry
            SequenceRunner r = new SequenceRunner(es, () -> {
                runCount.incrementAndGet();
                throw new RuntimeException("mock");
            }, null);

            r.submit();
            Assertions.assertFalse(r.awaitFinish(Tick.tick(50)));

            // submit again should trigger another attempt
            r.submit();
            Assertions.assertFalse(r.awaitFinish(Tick.tick(50)));
            Assertions.assertTrue(runCount.get() >= 2);
        } finally {
            es.shutdownNow();
            Assertions.assertTrue(es.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testTimeout() throws Exception {
        ExecutorService es = Executors.newSingleThreadExecutor();
        try {
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch block = new CountDownLatch(1);
            SequenceRunner r = new SequenceRunner(es, () -> {
                entered.countDown();
                try {
                    block.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, new int[]{Tick.tick(1)});

            r.submit();
            Assertions.assertTrue(entered.await(Tick.tick(2_000), TimeUnit.MILLISECONDS));
            Assertions.assertFalse(r.awaitFinish(Tick.tick(20)));

            block.countDown();
            Assertions.assertTrue(r.awaitFinish(Tick.tick(2_000)));
        } finally {
            es.shutdownNow();
            Assertions.assertTrue(es.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testRetryBackoff() throws Exception {
        ExecutorService es = Executors.newSingleThreadExecutor();
        try {
            AtomicInteger runCount = new AtomicInteger();
            SequenceRunner r = new SequenceRunner(es, () -> {
                int c = runCount.incrementAndGet();
                if (c <= 2) {
                    throw new RuntimeException("mock");
                }
            }, new int[]{Tick.tick(50), Tick.tick(200), Tick.tick(500)});

            r.submit();

            Assertions.assertFalse(r.awaitFinish(Tick.tick(10)));

            Assertions.assertTrue(r.awaitFinish(Tick.tick(3_000)));
            Assertions.assertEquals(r.getRequestVersion(), r.getFinishedVersion());
            Assertions.assertTrue(runCount.get() >= 3);
        } finally {
            es.shutdownNow();
            Assertions.assertTrue(es.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testSubmitAfterFailNoRetry() throws Exception {
        // Test: when task fails and no retry is configured, new submit() should still be processed
        ExecutorService es = Executors.newSingleThreadExecutor();
        try {
            AtomicInteger runCount = new AtomicInteger();
            CountDownLatch firstFailed = new CountDownLatch(1);
            CountDownLatch secondSubmitted = new CountDownLatch(1);

            SequenceRunner r = new SequenceRunner(es, () -> {
                int c = runCount.incrementAndGet();
                if (c == 1) {
                    // first run: fail, then wait for new submit
                    firstFailed.countDown();
                    try {
                        secondSubmitted.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    throw new RuntimeException("mock first failure");
                }
                // second run: success
            }, null); // no retry

            r.submit();

            // wait for first run to fail (before throwing)
            Assertions.assertTrue(firstFailed.await(Tick.tick(2_000), TimeUnit.MILLISECONDS));

            // submit new request while first is about to fail
            r.submit();
            secondSubmitted.countDown();

            // the second request should be processed and succeed
            Assertions.assertTrue(r.awaitFinish(Tick.tick(2_000)));
            Assertions.assertEquals(r.getRequestVersion(), r.getFinishedVersion());
            Assertions.assertEquals(2, runCount.get());
        } finally {
            es.shutdownNow();
            Assertions.assertTrue(es.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
}
