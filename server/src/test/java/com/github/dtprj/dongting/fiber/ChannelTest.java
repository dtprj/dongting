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

import com.github.dtprj.dongting.raft.test.TestUtil;
import com.github.dtprj.dongting.test.Tick;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author huangli
 */
public class ChannelTest extends AbstractFiberTest {
    @Test
    public void testTake() {
        FiberChannel<Integer> channel = new FiberChannel<>(fiberGroup);
        AtomicReference<Integer> r1 = new AtomicReference<>();
        AtomicReference<Integer> r2 = new AtomicReference<>();
        AtomicBoolean finished = new AtomicBoolean();
        AtomicReference<Throwable> exRef = new AtomicReference<>();
        fiberGroup.fireFiber("f1", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return channel.take(this::resume1);
            }

            private FrameCallResult resume1(Integer r) {
                r1.set(r);
                return channel.take(1, this::resume2);
            }

            private FrameCallResult resume2(Integer r) {
                r2.set(r);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                exRef.set(ex);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult doFinally() {
                finished.set(true);
                return Fiber.frameReturn();
            }
        });
        channel.fireOffer(1);
        TestUtil.waitUtil(() -> Integer.valueOf(1).equals(r1.get()));
        TestUtil.waitUtil(finished::get);
        Assertions.assertNull(r2.get());
        Assertions.assertNull(exRef.get());
    }

    @Test
    public void testTakeAll() {
        FiberChannel<Integer> channel = new FiberChannel<>(fiberGroup);
        List<Integer> r1 = new ArrayList<>();
        List<Integer> r2 = new ArrayList<>();
        AtomicBoolean finished = new AtomicBoolean();
        AtomicReference<Throwable> exRef = new AtomicReference<>();
        fiberGroup.fireFiber("f1", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                channel.offer(1);
                channel.offer(2);
                return channel.takeAll(r1, this::resume1);
            }

            private FrameCallResult resume1(Void v) {
                return channel.takeAll(r2, 1, this::resume2);
            }

            private FrameCallResult resume2(Void v) {
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                exRef.set(ex);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult doFinally() {
                finished.set(true);
                return Fiber.frameReturn();
            }
        });
        TestUtil.waitUtil(() -> r1.size() == 2);
        Assertions.assertEquals(1, r1.get(0));
        Assertions.assertEquals(2, r1.get(1));

        TestUtil.waitUtil(finished::get);
        Assertions.assertTrue(r2.isEmpty());
        Assertions.assertNull(exRef.get());
    }

    @Test
    public void testWakeup() throws Exception {
        FiberChannel<Integer> channel = new FiberChannel<>(fiberGroup);
        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        fiberGroup.fireFiber("f1", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                latch1.countDown();
                return channel.take(this::resume);
            }

            private FrameCallResult resume(Integer r) {
                latch2.countDown();
                return Fiber.frameReturn();
            }
        });
        Assertions.assertTrue(latch1.await(2, TimeUnit.SECONDS));
        Thread.sleep(1); // ensure fiber is in waiting state
        long t = System.nanoTime();
        channel.fireOffer(1);
        Assertions.assertTrue(latch2.await(2, TimeUnit.SECONDS));
        Assertions.assertTrue(System.nanoTime() - t < Duration.ofMillis(Tick.tick(10)).toNanos());
    }
}
