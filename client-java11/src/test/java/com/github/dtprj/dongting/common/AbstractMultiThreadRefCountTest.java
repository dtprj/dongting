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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author huangli
 */
public abstract class AbstractMultiThreadRefCountTest extends  AbstractRefCountTest {

    private static final int THREADS = 200;
    private static ExecutorService execService;

    @BeforeAll
    public static void initExecService() {
        execService = Executors.newFixedThreadPool(THREADS);
    }

    @AfterAll
    public static void closeExecServer() {
        execService.shutdown();
    }

    @Test
    @Timeout(value = 30)
    public void testRetainFromMultipleThreadsThrowsException() throws Exception {
        final LongAdder retainFailCount = new LongAdder();
        RefCount[] array = new RefCount[10000];
        for (int i = 0; i < array.length; i++) {
            array[i] = createInstance();
            array[i].release();
        }
        final CountDownLatch readyLatch = new CountDownLatch(THREADS);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch endLatch = new CountDownLatch(THREADS);
        Runnable r = () -> {
            try {
                readyLatch.countDown();
                startLatch.await();
                for (RefCount refCount : array) {
                    try {
                        refCount.retain();
                    } catch (DtException e) {
                        retainFailCount.increment();
                    }
                }
            } catch (Throwable e) {
                e.printStackTrace();
            } finally {
                endLatch.countDown();
            }
        };
        for (int i = 0; i < THREADS; i++) {
            execService.submit(r);
        }
        readyLatch.await();
        startLatch.countDown();
        endLatch.await();
        assertEquals(THREADS * array.length, retainFailCount.sum());
    }

    @Test
    @Timeout(value = 30)
    public void testReleaseFromMultipleThreadsThrowsException() throws Exception {
        final LongAdder failCount = new LongAdder();
        final LongAdder successCount = new LongAdder();
        RefCount[] array = new RefCount[10000];
        for (int i = 0; i < array.length; i++) {
            array[i] = createInstance();
            array[i].retain(THREADS - 1 - 10);
        }
        final CountDownLatch readyLatch = new CountDownLatch(THREADS);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch endLatch = new CountDownLatch(THREADS);
        Runnable r = () -> {
            try {
                readyLatch.countDown();
                startLatch.await();
                for (RefCount refCount : array) {
                    try {
                        refCount.release();
                        successCount.increment();
                    } catch (DtException e) {
                        failCount.increment();
                    }
                }
            } catch (Throwable e) {
                e.printStackTrace();
            } finally {
                endLatch.countDown();
            }
        };
        for (int i = 0; i < THREADS; i++) {
            execService.submit(r);
        }
        readyLatch.await();
        startLatch.countDown();
        endLatch.await();
        assertEquals((THREADS - 10) * array.length, successCount.sum());
        assertEquals(10 * array.length, failCount.sum());
        for (int i = 0; i < array.length; i++) {
            int X = i;
            assertThrows(DtException.class, () -> array[X].release());
        }
    }

    @Test
    @Timeout(value = 30)
    public void multiThreadTest1() throws Exception {
        final AtomicInteger refCountExceptions = new AtomicInteger();
        RefCount[] array = new RefCount[10000];
        for (int i = 0; i < array.length; i++) {
            array[i] = createInstance();
            array[i].retain(THREADS - 1);
        }
        final CountDownLatch readyLatch = new CountDownLatch(THREADS);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch endLatch = new CountDownLatch(THREADS);
        Runnable r = () -> {
            try {
                readyLatch.countDown();
                startLatch.await();
                for (RefCount refCount : array) {
                    refCount.release();
                }
            } catch (Throwable e) {
                refCountExceptions.incrementAndGet();
            } finally {
                endLatch.countDown();
            }
        };
        for (int i = 0; i < THREADS; i++) {
            execService.submit(r);
        }
        readyLatch.await();
        startLatch.countDown();
        endLatch.await();
        assertEquals(0, refCountExceptions.get());
        for (int i = 0; i < array.length; i++) {
            int X = i;
            assertThrows(DtException.class, () -> array[X].release());
        }
    }

    @Test
    @Timeout(value = 30)
    public void multiThreadTest2() throws Exception {
        final AtomicInteger refCountExceptions = new AtomicInteger();
        RefCount rc = createInstance();
        final CountDownLatch readyLatch = new CountDownLatch(THREADS);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch endLatch = new CountDownLatch(THREADS);
        Runnable r = () -> {
            try {
                readyLatch.countDown();
                startLatch.await();
                for (int i = 0; i < 10000; i++) {
                    rc.retain();
                    rc.release();
                }
            } catch (Throwable e) {
                refCountExceptions.incrementAndGet();
            } finally {
                endLatch.countDown();
            }
        };
        for (int i = 0; i < THREADS; i++) {
            execService.submit(r);
        }
        readyLatch.await();
        startLatch.countDown();
        endLatch.await();
        assertEquals(0, refCountExceptions.get());
        rc.release();
        assertThrows(DtException.class, rc::release);
    }
}
