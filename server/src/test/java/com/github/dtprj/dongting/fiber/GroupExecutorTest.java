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
package com.github.dtprj.dongting.fiber;

import com.github.dtprj.dongting.log.BugLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.github.dtprj.dongting.test.Tick.tick;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
class GroupExecutorTest extends AbstractFiberTest {

    @Test
    public void basicTest() throws Exception {
        GroupExecutor e = (GroupExecutor) fiberGroup.getExecutor();
        AtomicBoolean run = new AtomicBoolean(false);
        e.execute(() -> run.set(true));
        waitUtil(run::get);

        assertThrows(UnsupportedOperationException.class, e::shutdown);
        assertThrows(UnsupportedOperationException.class, e::shutdownNow);
        assertThrows(UnsupportedOperationException.class, () -> e.awaitTermination(1, TimeUnit.SECONDS));
        assertThrows(UnsupportedOperationException.class, () -> e.invokeAll(null));
        assertThrows(UnsupportedOperationException.class, () -> e.invokeAll(null, 1, TimeUnit.SECONDS));
        assertThrows(UnsupportedOperationException.class, () -> e.invokeAny(null));
        assertThrows(UnsupportedOperationException.class, () -> e.invokeAny(null, 1, TimeUnit.SECONDS));

        try {
            shutdownDispatcher();
            assertTrue(e.isShutdown());
            assertTrue(e.isTerminated());
        } finally {
            // Reset BugLog because these methods throws UnsupportedOperationException mark BugLog
            BugLog.reset();
        }
    }

    @Test
    public void testSubmit() throws Exception {
        GroupExecutor e = (GroupExecutor) fiberGroup.getExecutor();

        Future<Integer> f1 = e.submit(() -> 1);
        assertEquals(1, f1.get());
        Future<Integer> f2 = e.submit(() -> {
            throw new ArrayStoreException();
        });
        assertFutureFail(f2, ArrayStoreException.class);

        Future<?> f3 = e.submit(() -> {
        });
        assertNull(f3.get());
        Future<?> f4 = e.submit(() -> {
            throw new ArrayStoreException();
        });
        assertFutureFail(f4, ArrayStoreException.class);

        Future<Integer> f5 = e.submit(() -> {
        }, 5);
        assertEquals(5, f5.get());
        Future<Integer> f6 = e.submit(() -> {
            throw new ArrayStoreException();
        }, 5);
        assertFutureFail(f6, ArrayStoreException.class);
    }

    private void assertFutureFail(Future<?> f, Class<? extends Throwable> cause) {
        try {
            f.get();
            fail();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            assertEquals(cause, e.getCause().getClass());
        }
    }


    @Test
    public void testSchedule() throws Exception {
        GroupExecutor e = (GroupExecutor) fiberGroup.getExecutor();

        // schedule runnable
        AtomicInteger result = new AtomicInteger(0);
        ScheduledFuture<?> f1 = e.schedule(() -> result.set(1), 0, TimeUnit.MILLISECONDS);
        f1.get();
        assertEquals(1, result.get());

        long t = System.nanoTime();
        f1 = e.schedule(() -> result.set(2), 2, TimeUnit.MILLISECONDS);
        f1.get();
        assertEquals(2, result.get());
        // Theoretically, should be greater or equals than 2_000_000,
        // but Timestamp.refresh(1) has a maximum error of 1 ms, so we use 1_000_000 here.
        assertTrue(System.nanoTime() - t >= 1_000_000);

        f1 = e.schedule(() -> {
            throw new ArrayStoreException();
        }, 0, TimeUnit.MILLISECONDS);
        assertFutureFail(f1, ArrayStoreException.class);
        f1 = e.schedule(() -> {
            throw new ArrayStoreException();
        }, 1, TimeUnit.MILLISECONDS);
        assertFutureFail(f1, ArrayStoreException.class);

        f1 = e.schedule(() -> {
            throw new ArrayStoreException();
        }, 10000, TimeUnit.MILLISECONDS);
        f1.cancel(true);
        assertFutureFail(f1, CancellationException.class);

        // schedule Callable
        ScheduledFuture<Integer> f2 = e.schedule(() -> 1, 0, TimeUnit.MILLISECONDS);
        assertEquals(1, f2.get());

        t = System.nanoTime();
        f2 = e.schedule(() -> 2, 2, TimeUnit.MILLISECONDS);
        assertEquals(2, f2.get());
        // Theoretically, should be greater or equals than 2_000_000,
        // but Timestamp.refresh(1) has a maximum error of 1 ms, so we use 1_000_000 here.
        assertTrue(System.nanoTime() - t >= 1_000_000);

        f2 = e.schedule(() -> {
            throw new ArrayStoreException();
        }, 0, TimeUnit.MILLISECONDS);
        assertFutureFail(f2, ArrayStoreException.class);

        f2 = e.schedule(() -> {
            throw new ArrayStoreException();
        }, 1, TimeUnit.MILLISECONDS);
        assertFutureFail(f2, ArrayStoreException.class);

        f2 = e.schedule(() -> {
            throw new ArrayStoreException();
        }, 10000, TimeUnit.MILLISECONDS);
        f2.cancel(true);
        assertFutureFail(f2, CancellationException.class);
    }

    @ParameterizedTest
    @ValueSource(ints = {5, 0})
    public void testScheduleWithFixedDelay(long initDelay) throws Exception {
        GroupExecutor e = (GroupExecutor) fiberGroup.getExecutor();
        long[] times = new long[4];
        int[] count = new int[1];
        int[] index = new int[1];
        CountDownLatch latch = new CountDownLatch(2);
        long startTime = System.nanoTime() / 1_000_000;
        Runnable r = () -> {
            try {
                times[index[0]++] = System.nanoTime() / 1_000_000;
                if (count[0]++ == 0) {
                    Thread.sleep(tick(15));
                }
                times[index[0]++] = System.nanoTime() / 1_000_000;
                latch.countDown();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        };
        ScheduledFuture<?> f = e.scheduleWithFixedDelay(r, tick(initDelay), tick(20), TimeUnit.MILLISECONDS);
        assertTrue(latch.await(4, TimeUnit.SECONDS));

        if (initDelay > 0) {
            assertTrue(times[0] - startTime > tick(3));
        }
        assertTrue(times[2] - times[1] >= tick(18));

        assertFalse(f.isDone());
        f.cancel(true);
        assertTrue(f.isCancelled());
        assertFutureFail(f, CancellationException.class);
    }

    @Test
    public void testScheduleAtFixedRate() throws Exception {
        GroupExecutor e = (GroupExecutor) fiberGroup.getExecutor();
        long[] times = new long[6];
        int[] count = new int[1];
        int[] index = new int[1];
        CountDownLatch latch = new CountDownLatch(3);
        Runnable r = () -> {
            try {
                times[index[0]++] = System.nanoTime() / 1_000_000;
                if (count[0]++ == 0) {
                    Thread.sleep(tick(17));
                }
                times[index[0]++] = System.nanoTime() / 1_000_000;
                latch.countDown();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        };
        ScheduledFuture<?> f = e.scheduleAtFixedRate(r, 0, tick(20), TimeUnit.MILLISECONDS);
        assertTrue(latch.await(4, TimeUnit.SECONDS));

        assertTrue(times[2] - times[1] < tick(15));
        assertTrue(times[4] - times[3] > tick(18));

        assertFalse(f.isDone());
        f.cancel(true);
        assertTrue(f.isCancelled());
        assertFutureFail(f, CancellationException.class);
    }

    private void waitUtil(Supplier<Boolean> cond) {
        long t = System.currentTimeMillis();
        while (!cond.get()) {
            if (System.currentTimeMillis() - t < 5000) {
                // no sleep
                Thread.yield();
            } else {
                throw new RuntimeException("wait timeout");
            }
        }
    }
}
