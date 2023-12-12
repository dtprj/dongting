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
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
public class ExecFlowTest extends AbstractFiberTest {

    @Test
    public void testCall() {
        AtomicBoolean f1Called = new AtomicBoolean();
        AtomicBoolean f2Called = new AtomicBoolean();
        AtomicInteger f2Result = new AtomicInteger();
        AtomicBoolean f1FinallyCalled = new AtomicBoolean();
        AtomicBoolean f2FinallyCalled = new AtomicBoolean();

        FiberCondition c = group.newCondition();
        FiberFrame<Integer> f2 = new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return c.awaitOn(this::resume);
            }

            private FrameCallResult resume(Void unused) {
                f2Called.set(true);
                setResult(100);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult doFinally() {
                f2FinallyCalled.set(true);
                return super.doFinally();
            }
        };

        FiberFrame<Void> f1 = new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(f2, this::resume);
            }

            private FrameCallResult resume(Integer r) {
                f2Result.set(r);
                f1Called.set(true);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult doFinally() {
                f1FinallyCalled.set(true);
                return super.doFinally();
            }
        };

        Fiber fiber = new Fiber("fiber", group, f1);
        group.fireFiber(fiber);
        group.fireFiber(new Fiber("signal", group, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                c.signal();
                return Fiber.frameReturn();
            }
        }));
        TestUtil.waitUtil(f1FinallyCalled::get);
        assertTrue(f2FinallyCalled.get());
        assertTrue(f1Called.get());
        assertTrue(f2Called.get());
        assertEquals(100, f2Result.get());
    }

    @Test
    public void testExCatchBySelf() {
        Exception ex = new Exception();
        AtomicReference<Throwable> exRef = new AtomicReference<>();
        AtomicReference<Integer> resultRef = new AtomicReference<>();
        AtomicBoolean subFinallyCalled = new AtomicBoolean();
        AtomicBoolean parentFinallyCalled = new AtomicBoolean();
        FiberFrame<Integer> sub = new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) throws Exception {
                throw ex;
            }
            @Override
            protected FrameCallResult handle(Throwable ex) {
                exRef.set(ex);
                setResult(100);
                return Fiber.frameReturn();
            }
            @Override
            protected FrameCallResult doFinally() {
                subFinallyCalled.set(true);
                return super.doFinally();
            }
        };
        group.fireFiber("f", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(sub, this::resume);
            }

            private FrameCallResult resume(Integer o) {
                resultRef.set(o);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult doFinally() {
                parentFinallyCalled.set(true);
                return super.doFinally();
            }
        });
        TestUtil.waitUtil(parentFinallyCalled::get);
        assertTrue(subFinallyCalled.get());
        assertSame(ex, exRef.get());
        assertEquals(100, resultRef.get());
    }

    @Test
    public void testExCatchByParent() {
        Exception ex = new Exception();
        AtomicReference<Throwable> exRef = new AtomicReference<>();
        AtomicBoolean subFinallyCalled = new AtomicBoolean();
        AtomicBoolean parentFinallyCalled = new AtomicBoolean();
        FiberFrame<Integer> sub = new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) throws Exception {
                throw ex;
            }
            @Override
            protected FrameCallResult doFinally() {
                subFinallyCalled.set(true);
                return super.doFinally();
            }
        };
        group.fireFiber("f", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(sub, this::resume);
            }
            private FrameCallResult resume(Integer o) {
                return Fiber.frameReturn();
            }
            @Override
            protected FrameCallResult handle(Throwable ex) {
                exRef.set(ex);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult doFinally() {
                parentFinallyCalled.set(true);
                return super.doFinally();
            }
        });
        TestUtil.waitUtil(parentFinallyCalled::get);
        assertTrue(subFinallyCalled.get());
        assertSame(ex, exRef.get());
    }

    @Test
    public void testExNotCatch() {
        Exception ex = new Exception("Mock Exception");
        AtomicBoolean subFinallyCalled = new AtomicBoolean();
        AtomicBoolean parentFinallyCalled = new AtomicBoolean();
        FiberFrame<Integer> sub = new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) throws Exception {
                throw ex;
            }
            @Override
            protected FrameCallResult doFinally() {
                subFinallyCalled.set(true);
                return super.doFinally();
            }
        };
        group.fireFiber("f", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(sub, this::resume);
            }
            private FrameCallResult resume(Integer o) {
                return Fiber.frameReturn();
            }
            @Override
            protected FrameCallResult doFinally() {
                parentFinallyCalled.set(true);
                return super.doFinally();
            }
        });
        TestUtil.waitUtil(parentFinallyCalled::get);
        assertTrue(subFinallyCalled.get());
    }

    @Test
    public void testHandlerEx1() {
        Exception ex1 = new Exception("mock ex1");
        Exception ex2 = new Exception("mock ex2");
        AtomicReference<Throwable> exRef = new AtomicReference<>();
        AtomicBoolean subFinallyCalled = new AtomicBoolean();
        AtomicBoolean parentFinallyCalled = new AtomicBoolean();
        FiberFrame<Void> sub = new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) throws Exception {
                throw ex1;
            }
            @Override
            protected FrameCallResult handle(Throwable ex) throws Throwable {
                throw ex2;
            }
            @Override
            protected FrameCallResult doFinally() {
                subFinallyCalled.set(true);
                return super.doFinally();
            }
        };
        group.fireFiber("f", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(sub, this::justReturn);
            }
            @Override
            protected FrameCallResult handle(Throwable ex) {
                exRef.set(ex);
                return Fiber.frameReturn();
            }
            @Override
            protected FrameCallResult doFinally() {
                parentFinallyCalled.set(true);
                return super.doFinally();
            }
        });
        TestUtil.waitUtil(parentFinallyCalled::get);
        assertTrue(subFinallyCalled.get());
        assertSame(ex2, exRef.get());
    }

    // error occurs after handle resume
    @Test
    public void testHandlerEx2() {
        Exception ex1 = new Exception("mock ex1");
        Exception ex2 = new Exception("mock ex2");
        AtomicReference<Throwable> exRef = new AtomicReference<>();
        AtomicBoolean subFinallyCalled = new AtomicBoolean();
        AtomicBoolean parentFinallyCalled = new AtomicBoolean();
        FiberFrame<Void> sub = new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) throws Exception {
                throw ex1;
            }
            @Override
            protected FrameCallResult handle(Throwable ex) {
                return Fiber.sleep(1, this::resume);
            }
            private FrameCallResult resume(Void unused) throws Exception {
                throw ex2;
            }
            @Override
            protected FrameCallResult doFinally() {
                subFinallyCalled.set(true);
                return super.doFinally();
            }
        };
        group.fireFiber("f", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(sub, this::justReturn);
            }
            @Override
            protected FrameCallResult handle(Throwable ex) {
                exRef.set(ex);
                return Fiber.frameReturn();
            }
            @Override
            protected FrameCallResult doFinally() {
                parentFinallyCalled.set(true);
                return super.doFinally();
            }
        });
        TestUtil.waitUtil(parentFinallyCalled::get);
        assertTrue(subFinallyCalled.get());
        assertSame(ex2, exRef.get());
    }

    @Test
    public void testHandlerSuspend() {
        Exception ex1 = new Exception("mock ex1");
        AtomicBoolean subFinallyCalled = new AtomicBoolean();
        AtomicBoolean parentFinallyCalled = new AtomicBoolean();
        AtomicReference<Integer> resultRef = new AtomicReference<>();
        FiberFrame<Integer> sub = new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) throws Exception {
                throw ex1;
            }
            @Override
            protected FrameCallResult handle(Throwable ex) {
                return Fiber.sleep(1, this::resume);
            }
            private FrameCallResult resume(Void unused) {
                setResult(100);
                return Fiber.frameReturn();
            }
            @Override
            protected FrameCallResult doFinally() {
                subFinallyCalled.set(true);
                return super.doFinally();
            }
        };
        group.fireFiber("f", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(sub, this::resume);
            }
            private FrameCallResult resume(Integer integer) {
                resultRef.set(integer);
                return Fiber.frameReturn();
            }
            @Override
            protected FrameCallResult doFinally() {
                parentFinallyCalled.set(true);
                return super.doFinally();
            }
        });
        TestUtil.waitUtil(parentFinallyCalled::get);
        assertTrue(subFinallyCalled.get());
        assertEquals(100, resultRef.get());
    }

    @Test
    public void testFinallySuspend() {
        AtomicBoolean subFinallyCalled = new AtomicBoolean();
        AtomicBoolean parentFinallyCalled = new AtomicBoolean();
        AtomicReference<Integer> resultRef = new AtomicReference<>();
        FiberFrame<Integer> sub = new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                setResult(100);
                return Fiber.frameReturn();
            }
            @Override
            protected FrameCallResult doFinally() {
                subFinallyCalled.set(true);
                return Fiber.sleep(1, this::resume);
            }

            private FrameCallResult resume(Void unused) {
                return Fiber.frameReturn();
            }
        };
        group.fireFiber("f", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(sub, this::resume);
            }
            private FrameCallResult resume(Integer integer) {
                resultRef.set(integer);
                return Fiber.frameReturn();
            }
            @Override
            protected FrameCallResult doFinally() {
                parentFinallyCalled.set(true);
                return super.doFinally();
            }
        });
        TestUtil.waitUtil(parentFinallyCalled::get);
        assertTrue(subFinallyCalled.get());
        assertEquals(100, resultRef.get());
    }

    @Test
    public void testFinallyReturnValue() {
        AtomicBoolean parentFinallyCalled = new AtomicBoolean();
        AtomicReference<Integer> resultRef = new AtomicReference<>();
        FiberFrame<Integer> sub = new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                setResult(100);
                return Fiber.frameReturn();
            }
            @Override
            protected FrameCallResult doFinally() {
                setResult(200);
                return super.doFinally();
            }
        };
        group.fireFiber("f", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(sub, this::resume);
            }
            private FrameCallResult resume(Integer integer) {
                resultRef.set(integer);
                return Fiber.frameReturn();
            }
            @Override
            protected FrameCallResult doFinally() {
                parentFinallyCalled.set(true);
                return super.doFinally();
            }
        });
        TestUtil.waitUtil(parentFinallyCalled::get);
        assertEquals(200, resultRef.get());
    }

    @Test
    public void testFinallyEx1() {
        Exception ex1 = new Exception("mock ex1");
        RuntimeException ex2 = new RuntimeException("mock ex2");

        AtomicReference<Throwable> exRef = new AtomicReference<>();
        AtomicBoolean parentFinallyCalled = new AtomicBoolean();
        FiberFrame<Void> sub = new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) throws Exception {
                throw ex1;
            }
            @Override
            protected FrameCallResult doFinally() {
                throw ex2;
            }
        };
        group.fireFiber("f", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(sub, this::justReturn);
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                exRef.set(ex);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult doFinally() {
                parentFinallyCalled.set(true);
                return super.doFinally();
            }
        });
        TestUtil.waitUtil(parentFinallyCalled::get);
        assertSame(ex2, exRef.get());
    }

    // error occurs after resume
    @Test
    public void testFinallyEx2() {
        Exception ex1 = new Exception("mock ex1");
        RuntimeException ex2 = new RuntimeException("mock ex2");

        AtomicReference<Throwable> exRef = new AtomicReference<>();
        AtomicBoolean parentFinallyCalled = new AtomicBoolean();
        FiberFrame<Void> sub = new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) throws Exception {
                throw ex1;
            }
            @Override
            protected FrameCallResult doFinally() {
                return Fiber.sleep(1, this::resume);
            }
            private FrameCallResult resume(Void unused) {
                throw ex2;
            }
        };
        group.fireFiber("f", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(sub, this::justReturn);
            }
            @Override
            protected FrameCallResult handle(Throwable ex) {
                exRef.set(ex);
                return Fiber.frameReturn();
            }
            @Override
            protected FrameCallResult doFinally() {
                parentFinallyCalled.set(true);
                return super.doFinally();
            }
        });
        TestUtil.waitUtil(parentFinallyCalled::get);
        assertSame(ex2, exRef.get());
    }
}
