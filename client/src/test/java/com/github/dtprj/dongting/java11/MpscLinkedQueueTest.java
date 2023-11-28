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
package com.github.dtprj.dongting.java11;

import com.github.dtprj.dongting.common.DtException;
import com.github.dtprj.dongting.queue.MpscLinkedQueue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
public class MpscLinkedQueueTest {

    protected <E> MpscLinkedQueue<E> create() {
        return MpscLinkedQueue.newInstance();
    }

    @Test
    public void simpleTest() {
        MpscLinkedQueue<String> q = create();
        assertNull(q.relaxedPoll());
        q.offer("1");
        q.offer("2");
        assertEquals("1", q.relaxedPoll());
        q.offer("3");
        assertEquals("2", q.relaxedPoll());
        assertEquals("3", q.relaxedPoll());
        assertNull(q.relaxedPoll());
        q.shutdown();
        assertThrows(DtException.class, () -> q.offer("4"));
    }

    @Test
    @Timeout(30)
    public void multiThreadTest() throws Throwable {
        int threads = 100;
        int loop = 20000;
        MpscLinkedQueue<Long> q = create();
        CountDownLatch readyLatch = new CountDownLatch(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threads);
        AtomicReference<Throwable> ex = new AtomicReference<>();
        AtomicInteger threadId = new AtomicInteger(0);
        Runnable producer = () -> {
            try {
                long id = (long) threadId.getAndIncrement() << 32;
                readyLatch.countDown();
                startLatch.await();
                for (int i = 1; i <= loop; i++) {
                    q.offer(id | i);
                }
            } catch (Throwable e) {
                ex.set(e);
            } finally {
                endLatch.countDown();
            }
        };
        Runnable consumer = () -> {
            try {
                readyLatch.countDown();
                startLatch.await();
                consume(threads - 1, loop, q);
            } catch (Throwable e) {
                ex.set(e);
            } finally {
                endLatch.countDown();
            }
        };
        for (int i = 0; i < threads - 1; i++) {
            Thread t = new Thread(producer);
            t.start();
        }
        Thread ct = new Thread(consumer);
        ct.start();
        readyLatch.await();
        startLatch.countDown();
        endLatch.await();

        if (ex.get() != null) {
            throw ex.get();
        }
    }

    private void consume(int producerThreads, int loop, MpscLinkedQueue<Long> q) {
        int[] status = new int[producerThreads];
        long total = (long) producerThreads * loop;
        while (total > 0) {
            Long v = q.relaxedPoll();
            if (v != null) {
                total--;
                int thread = (int) (v >>> 32);
                int count = (int) v.longValue();
                if (status[thread] <= count) {
                    status[thread] = count;
                } else {
                    throw new AssertionError();
                }
            }
        }
    }


}
