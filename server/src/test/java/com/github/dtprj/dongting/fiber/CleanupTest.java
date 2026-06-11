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
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author huangli
 */
public class CleanupTest extends AbstractFiberTest {

    @Test
    public void testCleanupCalledAfterDoFinally() throws Exception {
        AtomicInteger order = new AtomicInteger();
        AtomicInteger doFinallyOrder = new AtomicInteger();
        AtomicInteger cleanupOrder = new AtomicInteger();

        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult doFinally() {
                doFinallyOrder.set(order.incrementAndGet());
                return Fiber.frameReturn();
            }

            @Override
            protected void cleanup() {
                cleanupOrder.set(order.incrementAndGet());
            }
        });

        assertEquals(1, doFinallyOrder.get(), "doFinally should be called first");
        assertEquals(2, cleanupOrder.get(), "cleanup should be called after doFinally");
    }

    @Test
    public void testCleanupCalledAfterDoFinallySuspend() throws Exception {
        AtomicBoolean cleanupCalled = new AtomicBoolean();
        AtomicInteger doFinallyCount = new AtomicInteger();

        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult doFinally() {
                doFinallyCount.incrementAndGet();
                // suspend once in doFinally
                if (doFinallyCount.get() == 1) {
                    return Fiber.sleep(Tick.tick(1), this::afterSleep);
                }
                return Fiber.frameReturn();
            }

            private FrameCallResult afterSleep(Void v) {
                return Fiber.frameReturn();
            }

            @Override
            protected void cleanup() {
                cleanupCalled.set(true);
            }
        });

        assertEquals(1, doFinallyCount.get(), "doFinally should only be called once");
        assertTrue(cleanupCalled.get(), "cleanup should be called after suspend/resume");
    }

    @Test
    public void testCleanupCalledWhenDoFinallyThrows() {
        AtomicBoolean cleanupCalled = new AtomicBoolean();

        fiberGroup.fireFiber("test", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult doFinally() {
                throw new RuntimeException("test exception from doFinally");
            }

            @Override
            protected void cleanup() {
                cleanupCalled.set(true);
            }
        });
        // wait for the fiber to finish and cleanup to be called
        WaitUtil.waitUtil(cleanupCalled::get);

        assertTrue(cleanupCalled.get(), "cleanup should be called even when doFinally throws");
    }

    @Test
    public void testCleanDaemonFiberCleanedOnShutdown() throws Exception {
        AtomicBoolean cleanupCalled = new AtomicBoolean();

        fiberGroup.fireFiber(new Fiber("daemon", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.sleep(60_000, this);
            }

            @Override
            protected void cleanup() {
                cleanupCalled.set(true);
            }
        }).setDaemon(true));

        fiberGroup.requestShutdown();
        fiberGroup.shutdownFuture.get(5, TimeUnit.SECONDS);

        assertTrue(cleanupCalled.get(), "daemon fiber cleanup should be called on group shutdown");
    }

    @Test
    public void testCleanDaemonFiberFrameStackCleaned() throws Exception {
        AtomicBoolean outerCleanupCalled = new AtomicBoolean();
        AtomicBoolean innerCleanupCalled = new AtomicBoolean();

        FiberFrame<Void> innerFrame = new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.sleep(60_000, this);
            }

            @Override
            protected void cleanup() {
                innerCleanupCalled.set(true);
            }
        };

        fiberGroup.fireFiber(new Fiber("daemon-stack", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(innerFrame, this::afterInner);
            }

            private FrameCallResult afterInner(Void unused) {
                return Fiber.frameReturn();
            }

            @Override
            protected void cleanup() {
                outerCleanupCalled.set(true);
            }
        }).setDaemon(true));

        fiberGroup.requestShutdown();
        fiberGroup.shutdownFuture.get(5, TimeUnit.SECONDS);

        assertTrue(outerCleanupCalled.get(), "outer frame cleanup should be called");
        assertTrue(innerCleanupCalled.get(), "inner frame cleanup should be called");
    }

    @Test
    public void testCleanupIdempotent() throws Exception {
        AtomicBoolean cleanupDone = new AtomicBoolean();

        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.frameReturn();
            }

            @Override
            protected void cleanup() {
                cleanupDone.set(true);
            }
        });

        assertTrue(cleanupDone.get(), "cleanup should have been called");
    }
}
