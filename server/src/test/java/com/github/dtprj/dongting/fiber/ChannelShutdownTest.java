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

import com.github.dtprj.dongting.test.Tick;
import com.github.dtprj.dongting.test.WaitUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author huangli
 */
public class ChannelShutdownTest extends AbstractFiberTest {

    private static class ConsumerFrame extends FiberFrame<Void> {
        private final FiberChannel<Integer> channel;
        private final List<Integer> processed;
        private final ArrayList<Integer> list = new ArrayList<>();

        ConsumerFrame(FiberChannel<Integer> channel, List<Integer> processed) {
            this.channel = channel;
            this.processed = processed;
        }

        @Override
        public FrameCallResult execute(Void input) {
            list.clear();
            return channel.takeAll(list, Tick.tick(10), true, this::afterTakeAll);
        }

        private FrameCallResult afterTakeAll(Void unused) {
            if (isGroupShouldStopPlain()) {
                channel.markShutdown();
                channel.drain(list);
                processed.addAll(list);
                return Fiber.frameReturn();
            }
            processed.addAll(list);
            return Fiber.resume(null, this);
        }
    }

    @Test
    public void testAcceptedTasksAllProcessedAfterStop() throws Exception {
        FiberChannel<Integer> channel = fiberGroup.newChannel();
        List<Integer> processed = new CopyOnWriteArrayList<>();
        Assertions.assertTrue(fiberGroup.fireFiber("consumer", new ConsumerFrame(channel, processed)));

        int count = 1000;
        for (int i = 0; i < count; i++) {
            Assertions.assertTrue(channel.fireOffer(i));
        }
        WaitUtil.waitUtil(() -> !processed.isEmpty());
        fiberGroup.requestShutdown();
        fiberGroup.shutdownFuture.get(5, TimeUnit.SECONDS);

        Assertions.assertEquals(count, processed.size());
        Set<Integer> set = new HashSet<>(processed);
        Assertions.assertEquals(count, set.size());
    }

    @Test
    public void testConcurrentOfferAndMarkShutdown() throws Exception {
        FiberChannel<Integer> channel = fiberGroup.newChannel();
        List<Integer> processed = new CopyOnWriteArrayList<>();
        Assertions.assertTrue(fiberGroup.fireFiber("consumer", new ConsumerFrame(channel, processed)));

        int threadCount = 4;
        int maxIdPerThread = 10000;
        ConcurrentHashMap<Integer, Boolean> results = new ConcurrentHashMap<>();
        CountDownLatch startLatch = new CountDownLatch(threadCount);
        List<Thread> threads = new ArrayList<>();
        for (int t = 0; t < threadCount; t++) {
            int threadIndex = t;
            Thread thread = new Thread(() -> {
                startLatch.countDown();
                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    return;
                }
                for (int i = 0; i < maxIdPerThread; i++) {
                    int id = threadIndex * maxIdPerThread + i;
                    boolean b = channel.fireOffer(id);
                    results.put(id, b);
                    if (!b) {
                        break;
                    }
                }
            });
            threads.add(thread);
            thread.start();
        }
        WaitUtil.waitUtil(() -> results.size() > 100);
        fiberGroup.requestShutdown();
        for (Thread t : threads) {
            t.join(5000);
            Assertions.assertFalse(t.isAlive());
        }
        fiberGroup.shutdownFuture.get(5, TimeUnit.SECONDS);

        List<Integer> accepted = new ArrayList<>();
        results.forEach((id, b) -> {
            if (b) {
                accepted.add(id);
            }
        });
        for (Integer id : processed) {
            Assertions.assertEquals(Boolean.TRUE, results.get(id));
        }
        Set<Integer> processedSet = new HashSet<>(processed);
        Assertions.assertEquals(processed.size(), processedSet.size());
        Assertions.assertEquals(accepted.size(), processed.size());
    }

    @Test
    public void testFireFiberAfterSysChannelShutdown() throws Exception {
        Assertions.assertTrue(fiberGroup.fireFiber("f1", new EmptyFiberFrame()));
        fiberGroup.requestShutdown();
        ReentrantLock lock = dispatcher.shareQueue.lock;
        WaitUtil.waitUtil(() -> {
            lock.lock();
            try {
                return fiberGroup.sysChannel.shutdown;
            } finally {
                lock.unlock();
            }
        });
        Assertions.assertFalse(fiberGroup.fireFiber("f2", new EmptyFiberFrame()));
        fiberGroup.shutdownFuture.get(5, TimeUnit.SECONDS);
        Assertions.assertTrue(fiberGroup.finished);
    }
}
