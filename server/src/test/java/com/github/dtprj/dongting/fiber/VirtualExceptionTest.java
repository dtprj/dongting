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

import java.util.concurrent.atomic.AtomicReference;

/**
 * @author huangli
 */
public class VirtualExceptionTest extends AbstractFiberTest {

    private static class F extends FiberFrame<Void> {

        private final boolean execEx;
        private final boolean handleEx;
        private final boolean finallyEx;
        private final boolean throwFiberEx;

        public F(boolean execEx, boolean handleEx, boolean finallyEx, boolean throwFiberEx) {
            this.execEx = execEx;
            this.handleEx = handleEx;
            this.finallyEx = finallyEx;
            this.throwFiberEx = throwFiberEx;
        }

        private void process(boolean b) {
            if (b) {
                if (throwFiberEx) {
                    throw new FiberException();
                } else {
                    throw new RuntimeException();
                }
            }
        }

        @Override
        public FrameCallResult execute(Void input) {
            process(execEx);
            return Fiber.frameReturn();
        }

        @Override
        protected FrameCallResult handle(Throwable ex) throws Throwable {
            process(handleEx);
            return super.handle(ex);
        }

        @Override
        protected FrameCallResult doFinally() {
            process(finallyEx);
            return super.doFinally();
        }
    }

    private void fireCall(F sub) {
        AtomicReference<Throwable> exRef = new AtomicReference<>();
        fiberGroup.fireFiber("ef", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(sub, this::justReturn);
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                exRef.set(ex);
                return Fiber.frameReturn();
            }
        });
        TestUtil.waitUtil(() -> exRef.get() != null);
        if (sub.execEx && sub.handleEx && sub.finallyEx) {
            Assertions.assertEquals(2, exRef.get().getSuppressed().length);
        } else {
            Assertions.assertEquals(1, exRef.get().getSuppressed().length);
            Assertions.assertEquals(FiberVirtualException.class, exRef.get().getSuppressed()[0].getClass());
        }
    }

    @Test
    public void testCall() {
        fireCall(new F(true, false, false, false));
        fireCall(new F(true, false, false, true));

        fireCall(new F(true, true, false, false));
        fireCall(new F(true, true, false, true));

        fireCall(new F(false, false, true, false));
        fireCall(new F(false, false, true, true));

        fireCall(new F(true, true, true, false));
        fireCall(new F(true, true, true, true));
    }

    @Test
    public void testSuspend() {
        AtomicReference<Throwable> exRef = new AtomicReference<>();
        Fiber f = new Fiber("ef", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.sleep(1000, this::justReturn);
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                exRef.set(ex);
                return Fiber.frameReturn();
            }
        });
        fiberGroup.fireFiber(f);
        fiberGroup.fireFiber("int", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                f.interrupt();
                return Fiber.frameReturn();
            }
        });
        TestUtil.waitUtil(() -> exRef.get() != null);
        Assertions.assertEquals(1, exRef.get().getSuppressed().length);
        Assertions.assertEquals(FiberVirtualException.class, exRef.get().getSuppressed()[0].getClass());
    }
}
