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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author huangli
 */
public class SleepTest extends AbstractFiberTest {

    @Test
    public void testSleepAndInterrupt() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean interrupt = new AtomicBoolean();
        AtomicBoolean sleepOk = new AtomicBoolean();
        Fiber f = new Fiber("f", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                latch.countDown();
                return Fiber.sleep(100000, this::resume);
            }

            private FrameCallResult resume(Void v) {
                sleepOk.set(true);
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
        latch.await();
        Fiber f2 = new Fiber("f2", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                f.interrupt();
                return Fiber.frameReturn();
            }
        });
        fiberGroup.fireFiber(f2);

        TestUtil.waitUtil(interrupt::get);
        Assertions.assertFalse(sleepOk.get());
        // since we interrupt f, tear down should be finished soon
    }

    @Test
    public void testSleepUntilShouldStop() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean error = new AtomicBoolean();
        Fiber f = new Fiber("f", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                latch.countDown();
                return Fiber.sleepUntilShouldStop(100000, this::justReturn);
            }

            @Override
            protected FrameCallResult handle(Throwable ex) throws Throwable {
                error.set(true);
                return super.handle(ex);
            }
        });
        fiberGroup.fireFiber(f);
        latch.await();
        fiberGroup.requestShutdown();

        super.shutdownDispatcher();
        Assertions.assertFalse(error.get());
    }

    // call sleepUntilShouldStop() after shutdown
    @Test
    public void testSleepUntilShouldStop2() throws Exception {
        AtomicBoolean error = new AtomicBoolean();
        Fiber f = new Fiber("f", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                getFiberGroup().requestShutdown();
                return Fiber.yield(this::afterYield);
            }

            private FrameCallResult afterYield(Void unused) {
                Assertions.assertTrue(isGroupShouldStopPlain());
                return Fiber.sleepUntilShouldStop(1000, this::justReturn);
            }

            @Override
            protected FrameCallResult handle(Throwable ex) throws Throwable {
                error.set(true);
                return super.handle(ex);
            }
        });
        fiberGroup.fireFiber(f);

        super.shutdownDispatcher();
        Assertions.assertFalse(error.get());
    }
}
