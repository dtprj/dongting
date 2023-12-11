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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author huangli
 */
public class FiberQueueTest {
    private FiberQueueTask t1;
    private FiberQueueTask t2;

    @BeforeEach
    public void test() {
        t1 = new EmptyFiberQueueTask();
        t2 = new EmptyFiberQueueTask();
    }

    @Test
    public void testOfferPoll() throws Exception {
        FiberQueue q = new FiberQueue();
        q.offer(t1);
        q.offer(t2);
        Assertions.assertSame(t1, q.poll(1, TimeUnit.SECONDS));
        Assertions.assertSame(t2, q.poll(1, TimeUnit.SECONDS));
        q.offer(t1);
        q.offer(t2);
        Assertions.assertSame(t1, q.poll(1, TimeUnit.SECONDS));
        Assertions.assertSame(t2, q.poll(1, TimeUnit.SECONDS));
    }

    @Test
    public void testDrainTo() {
        FiberQueue q = new FiberQueue();
        q.offer(t1);
        q.offer(t2);
        ArrayList<FiberQueueTask> list = new ArrayList<>();
        q.drainTo(list);
        Assertions.assertSame(t1, list.get(0));
        Assertions.assertSame(t2, list.get(1));
        list.clear();
        q.drainTo(list);
        Assertions.assertEquals(0, list.size());

        q.offer(t1);
        q.offer(t2);
        q.drainTo(list);
        Assertions.assertSame(t1, list.get(0));
        Assertions.assertSame(t2, list.get(1));
    }

    @Test
    public void testDuplicateOffer() {
        FiberQueue q = new FiberQueue();
        Assertions.assertTrue(q.offer(t1));
        Assertions.assertThrows(FiberException.class, () -> q.offer(t1));
    }

    @Test
    public void testSignal() throws Exception {
        FiberQueue q = new FiberQueue();
        AtomicBoolean get = new AtomicBoolean();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            try {
                countDownLatch.countDown();
                q.poll(1, TimeUnit.SECONDS);
                get.set(true);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        t.start();
        countDownLatch.await(1, TimeUnit.SECONDS);
        q.offer(t1);
        t.join(1000);
        Assertions.assertTrue(get.get());
    }

    @Test
    public void testShutdown() {
        FiberQueue q = new FiberQueue();
        Assertions.assertTrue(q.offer(t1));
        q.shutdown();
        Assertions.assertFalse(q.offer(t2));
    }
}
