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

import com.github.dtprj.dongting.test.WaitUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
public class ConditionTest extends AbstractFiberTest {

    @Test
    public void testTimeout() {
        FiberCondition c = fiberGroup.newCondition("testCondition");
        AtomicBoolean error = new AtomicBoolean();
        AtomicBoolean finish = new AtomicBoolean();
        Fiber f = new Fiber("f", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return c.await(1, this::resume);
            }

            private FrameCallResult resume(Void v) {
                finish.set(true);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                error.set(true);
                return Fiber.frameReturn();
            }
        });
        fiberGroup.fireFiber(f);
        WaitUtil.waitUtil(finish::get);
        Assertions.assertFalse(error.get());
    }

    @Test
    public void testSignal() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        FiberCondition c = fiberGroup.newCondition("testCondition");
        AtomicBoolean error = new AtomicBoolean();
        AtomicBoolean finish = new AtomicBoolean();
        Fiber f = new Fiber("f", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                latch.countDown();
                return c.await(100000, this::resume);
            }

            private FrameCallResult resume(Void v) {
                finish.set(true);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                error.set(true);
                return Fiber.frameReturn();
            }
        });
        fiberGroup.fireFiber(f);
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        Fiber f2 = new Fiber("f2", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                c.signal();
                return Fiber.frameReturn();
            }
        });
        fiberGroup.fireFiber(f2);

        WaitUtil.waitUtil(finish::get);
        Assertions.assertFalse(error.get());
    }

    @Test
    public void testInterrupt() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        FiberCondition c = fiberGroup.newCondition("testCondition");
        AtomicBoolean interrupt = new AtomicBoolean();
        AtomicBoolean finish = new AtomicBoolean();
        Fiber f = new Fiber("f", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                latch.countDown();
                return c.await(1000000, this::resume);
            }

            private FrameCallResult resume(Void v) {
                finish.set(true);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                if (ex instanceof FiberInterruptException) {
                    interrupt.set(true);
                }
                return Fiber.frameReturn();
            }
        });
        fiberGroup.fireFiber(f);
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        Fiber f2 = new Fiber("f2", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                f.interrupt();
                return Fiber.frameReturn();
            }
        });
        fiberGroup.fireFiber(f2);

        WaitUtil.waitUtil(interrupt::get);
        Assertions.assertFalse(finish.get());
    }

    private static class F extends FiberFrame<Void> {
        private final CountDownLatch startLatch;
        private final CountDownLatch finishLatch;
        private final FiberCondition condition;

        public F(CountDownLatch startLatch, CountDownLatch finishLatch, FiberCondition condition) {
            this.startLatch = startLatch;
            this.finishLatch = finishLatch;
            this.condition = condition;
        }

        @Override
        public FrameCallResult execute(Void input) {
            startLatch.countDown();
            return condition.await(this::resume);
        }

        private FrameCallResult resume(Void unused) {
            finishLatch.countDown();
            return Fiber.frameReturn();
        }
    }

    @Test
    public void testSignalAll() throws Exception {
        CountDownLatch startLatch = new CountDownLatch(2);
        CountDownLatch finishLatch = new CountDownLatch(2);
        FiberCondition c = fiberGroup.newCondition("testCondition");

        Fiber f1 = new Fiber("f1", fiberGroup, new F(startLatch, finishLatch, c));
        Fiber f2 = new Fiber("f2", fiberGroup, new F(startLatch, finishLatch, c));
        fiberGroup.fireFiber(f1);
        fiberGroup.fireFiber(f2);
        assertTrue(startLatch.await(1, TimeUnit.SECONDS));
        Fiber f3 = new Fiber("f3", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                c.signalAll();
                return Fiber.frameReturn();
            }
        });
        fiberGroup.fireFiber(f3);

        assertTrue(finishLatch.await(1, TimeUnit.SECONDS));
    }

    @Test
    public void testWaitMultiConditions() throws Exception {
        FiberCondition c1 = fiberGroup.newCondition("c1");
        FiberCondition c2 = fiberGroup.newCondition("c2");
        FiberCondition c3 = fiberGroup.newCondition("c3");

        CompletableFuture<Void> f1 = new CompletableFuture<>();
        CompletableFuture<Void> f2 = new CompletableFuture<>();
        CompletableFuture<Void> f3 = new CompletableFuture<>();
        CompletableFuture<Void> f4 = new CompletableFuture<>();
        Fiber fiber = new Fiber("waiter", fiberGroup, new FiberFrame<Void>() {
            @Override
            public FrameCallResult execute(Void input) {
                return c1.await(c2, this::resume1);
            }

            private FrameCallResult resume1(Void v) {
                f1.complete(null);
                return c3.await(vv -> c1.await(c2, this::resume2));
            }

            private FrameCallResult resume2(Void v) {
                f2.complete(null);
                return c3.await(vv -> c1.await(1000, c2, this::resume3));
            }

            private FrameCallResult resume3(Void v) {
                f3.complete(null);
                return c3.await(vv -> c1.await(1000, c2, this::resume4));
            }

            private FrameCallResult resume4(Void v) {
                f4.complete(null);
                return Fiber.frameReturn();
            }
        });
        fiberGroup.fireFiber(fiber);
        fireSignal(c1);
        check(f1, c1, c2, c3, fiber);
        fireSignal(c2);
        check(f2, c1, c2, c3, fiber);
        fireSignal(c1);
        check(f3, c1, c2, c3, fiber);
        fireSignal(c2);
        check(f4, c1, c2, c3, fiber);
    }

    private void fireSignal(FiberCondition c) {
        fiberGroup.fireFiber("fire", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                c.signal();
                return Fiber.frameReturn();
            }
        });
    }

    private void check(CompletableFuture<Void> f, FiberCondition c1, FiberCondition c2,
                       FiberCondition c3, Fiber fiber) throws Exception {
        f.get(3, TimeUnit.SECONDS);
        doInFiber(() -> {
            assertTrue(c1.waiters.isEmpty());
            assertTrue(c2.waiters.isEmpty());
            if (fiber.isFinished()) {
                assertNull(fiber.source);
            } else {
                assertSame(c3, fiber.source);
            }
            assertNull(fiber.sourceConditions);
            assertFalse(dispatcher.scheduleQueue.contains(fiber));
            c3.signal();
        });
    }
}
