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
package com.github.dtprj.dongting.raft.store;

import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.fiber.BaseFiberTest;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberInterruptException;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author huangli
 */
public class RetryFrameTest extends BaseFiberTest {
    @Test
    public void test() throws Exception {
        // retry and success
        TestFrame tf = new TestFrame(1, new int[]{1}, false);
        fiberGroup.fireFiber("f", tf);
        tf.future.get(1, TimeUnit.SECONDS);

        // fail 2, retry 1
        tf = new TestFrame(2, new int[]{1}, false);
        fiberGroup.fireFiber("f", tf);
        try {
            tf.future.get(1, TimeUnit.SECONDS);
            fail();
        } catch (ExecutionException e) {
            assertEquals("mock error", DtUtil.rootCause(e).getMessage());
        }

        // fail 2, retry 2
        tf = new TestFrame(2, new int[]{1, 1}, false);
        fiberGroup.fireFiber("f", tf);
        tf.future.get(1, TimeUnit.SECONDS);

        // fail 3, retry forever
        tf = new TestFrame(3, new int[]{1, 1}, true);
        fiberGroup.fireFiber("f", tf);
        tf.future.get(1, TimeUnit.SECONDS);

        // no retry
        tf = new TestFrame(1, null, false);
        fiberGroup.fireFiber("f", tf);
        try {
            tf.future.get(1, TimeUnit.SECONDS);
            fail();
        } catch (ExecutionException e) {
            assertEquals("mock error", DtUtil.rootCause(e).getMessage());
        }

        // no fail
        tf = new TestFrame(0, null, false);
        fiberGroup.fireFiber("f", tf);
        tf.future.get(1, TimeUnit.SECONDS);

        // interrupt fiber
        {
            tf = new TestFrame(1, new int[]{5000}, true);
            Fiber fiber = new Fiber("f", fiberGroup, tf);
            fiberGroup.fireFiber(fiber);
            fiberGroup.fireFiber("f2", new FiberFrame<>() {
                @Override
                public FrameCallResult execute(Void input) {
                    fiber.interrupt();
                    return Fiber.frameReturn();
                }
            });
            try {
                tf.future.get(1, TimeUnit.SECONDS);
                fail();
            } catch (ExecutionException e) {
                assertEquals("mock error", DtUtil.rootCause(e).getMessage());
            }
        }

        // sub frame throw FiberInterruptException, no retry
        {
            tf = new TestFrame(1, new int[]{1}, true, new FiberInterruptException("interrupt"));
            Fiber fiber = new Fiber("f", fiberGroup, tf);
            fiberGroup.fireFiber(fiber);
            try {
                tf.future.get(1, TimeUnit.SECONDS);
                fail();
            } catch (ExecutionException e) {
                assertEquals("interrupt", DtUtil.rootCause(e).getMessage());
            }
        }

        // fiber group shutdown, cancel
        tf = new TestFrame(1, new int[]{5000}, true);
        Fiber fiber = new Fiber("f", fiberGroup, tf);
        fiberGroup.fireFiber(fiber);
        shutdownDispatcher();
        try {
            tf.future.get(1, TimeUnit.SECONDS);
            fail();
        } catch (ExecutionException e) {
            assertEquals("mock error", DtUtil.rootCause(e).getMessage());
        }
    }

    private static class TestFrame extends FiberFrame<Void> {
        private final CompletableFuture<Void> future = new CompletableFuture<>();
        private final int mockFailCount;
        private final int[] retryInterval;
        private final boolean retryForever;
        private final Exception mockEx;

        public TestFrame(int mockFailCount, int[] retryInterval, boolean retryForever) {
            this.mockFailCount = mockFailCount;
            this.retryInterval = retryInterval;
            this.retryForever = retryForever;
            this.mockEx = new SecurityException("mock error");
        }

        public TestFrame(int mockFailCount, int[] retryInterval, boolean retryForever, Exception mockEx) {
            this.mockFailCount = mockFailCount;
            this.retryInterval = retryInterval;
            this.retryForever = retryForever;
            this.mockEx = mockEx;
        }


        @Override
        public FrameCallResult execute(Void input) {
            RetryFrame<Void> f = new RetryFrame<>(new MockFailFrame(mockFailCount, mockEx), retryInterval,
                    retryForever, () -> false);
            return Fiber.call(f, this::resume);
        }

        private FrameCallResult resume(Void unused) {
            future.complete(null);
            return Fiber.frameReturn();
        }

        @Override
        protected FrameCallResult handle(Throwable ex) {
            future.completeExceptionally(ex);
            return Fiber.frameReturn();
        }
    }

    private static class MockFailFrame extends FiberFrame<Void> {
        private final int failCount;
        private int count;
        private final Exception ex;

        public MockFailFrame(int failCount, Exception ex) {
            this.failCount = failCount;
            this.ex = ex;
        }

        @Override
        public FrameCallResult execute(Void input) throws Throwable {
            if (count++ < failCount) {
                throw ex;
            }
            return Fiber.frameReturn();
        }
    }
}
