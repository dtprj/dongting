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
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
public class FinallyTest extends AbstractFiberTest {

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
        fiberGroup.fireFiber("f", new FiberFrame<>() {
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
    public void testFinallySuspendWithEx() {
        AtomicBoolean subFinallyCalled = new AtomicBoolean();
        AtomicBoolean parentFinallyCalled = new AtomicBoolean();
        AtomicReference<Integer> resultRef = new AtomicReference<>();
        Exception ex = new Exception("mock ex");
        AtomicReference<Throwable> exRef = new AtomicReference<>();
        FiberFrame<Integer> sub = new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) throws Exception {
                setResult(100);
                throw ex;
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
        fiberGroup.fireFiber("f", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(sub, this::resume);
            }
            private FrameCallResult resume(Integer integer) {
                resultRef.set(integer);
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
        assertNull(resultRef.get());
        assertSame(ex, exRef.get());
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
        fiberGroup.fireFiber("f", new FiberFrame<>() {
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
        fiberGroup.fireFiber("f", new FiberFrame<>() {
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
        assertSame(ex1, exRef.get().getSuppressed()[0]);
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
        fiberGroup.fireFiber("f", new FiberFrame<>() {
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
        assertSame(ex1, exRef.get().getSuppressed()[0]);
    }
}
