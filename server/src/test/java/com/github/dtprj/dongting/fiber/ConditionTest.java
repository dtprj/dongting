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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author huangli
 */
public class ConditionTest extends AbstractFiberTest {

    @Test
    public void testTimeout() {
        FiberCondition c = fiberGroup.newCondition();
        AtomicBoolean error = new AtomicBoolean();
        AtomicBoolean finish = new AtomicBoolean();
        Fiber f = new Fiber("f", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return c.awaitOn(1, this::resume);
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
        TestUtil.waitUtil(finish::get);
        Assertions.assertFalse(error.get());
    }

    @Test
    public void testSignal() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        FiberCondition c = fiberGroup.newCondition();
        AtomicBoolean error = new AtomicBoolean();
        AtomicBoolean finish = new AtomicBoolean();
        Fiber f = new Fiber("f", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                latch.countDown();
                return c.awaitOn(100000, this::resume);
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
        Assertions.assertTrue(latch.await(1 , TimeUnit.SECONDS));
        Fiber f2 = new Fiber("f2", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                c.signal();
                return Fiber.frameReturn();
            }
        });
        fiberGroup.fireFiber(f2);

        TestUtil.waitUtil(finish::get);
        Assertions.assertFalse(error.get());
    }

    @Test
    public void testInterrupt() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        FiberCondition c = fiberGroup.newCondition();
        AtomicBoolean interrupt = new AtomicBoolean();
        AtomicBoolean finish = new AtomicBoolean();
        Fiber f = new Fiber("f", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                latch.countDown();
                return c.awaitOn(1000000, this::resume);
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
        Assertions.assertTrue(latch.await(1 , TimeUnit.SECONDS));
        Fiber f2 = new Fiber("f2", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                f.interrupt();
                return Fiber.frameReturn();
            }
        });
        fiberGroup.fireFiber(f2);

        TestUtil.waitUtil(interrupt::get);
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
            return condition.awaitOn(this::resume);
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
        FiberCondition c = fiberGroup.newCondition();

        Fiber f1 = new Fiber("f1", fiberGroup,new F(startLatch, finishLatch, c));
        Fiber f2 = new Fiber("f2", fiberGroup,new F(startLatch, finishLatch, c));
        fiberGroup.fireFiber(f1);
        fiberGroup.fireFiber(f2);
        Assertions.assertTrue(startLatch.await(1, TimeUnit.SECONDS));
        Fiber f3 = new Fiber("f3", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                c.signalAll();
                return Fiber.frameReturn();
            }
        });
        fiberGroup.fireFiber(f3);

        Assertions.assertTrue(finishLatch.await(1, TimeUnit.SECONDS));
    }
}
