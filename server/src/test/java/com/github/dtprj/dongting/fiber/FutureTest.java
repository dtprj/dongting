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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
public class FutureTest extends AbstractFiberTest {

    @Test
    public void testTimeout() {
        FiberFuture<Void> f = fiberGroup.newFuture("noName");
        AtomicBoolean timeout = new AtomicBoolean();
        AtomicBoolean finish = new AtomicBoolean();
        fiberGroup.fireFiber(new Fiber("f", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return f.await(1, this::resume);
            }

            private FrameCallResult resume(Void v) {
                finish.set(true);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                if (ex instanceof FiberTimeoutException) {
                    timeout.set(true);
                }
                return Fiber.frameReturn();
            }
        }));
        TestUtil.waitUtil(timeout::get);
        Assertions.assertFalse(finish.get());
    }

    @Test
    public void testInterrupt1() {
        FiberFuture<Void> f = fiberGroup.newFuture("noName");
        AtomicBoolean interrupt = new AtomicBoolean();
        AtomicBoolean finish = new AtomicBoolean();
        Fiber f1 = new Fiber("f", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return f.await(this::resume);
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
        fiberGroup.fireFiber(f1);
        fiberGroup.fireFiber(new Fiber("f2", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                f1.interrupt();
                return Fiber.frameReturn();
            }
        }));
        TestUtil.waitUtil(interrupt::get);
        Assertions.assertFalse(finish.get());
    }

    @Test
    public void testInterrupt2() {
        FiberFuture<Void> f = fiberGroup.newFuture("noName");
        AtomicBoolean interrupt = new AtomicBoolean();
        AtomicBoolean finish = new AtomicBoolean();
        Fiber f1 = new Fiber("f", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return f.await(this::resume);
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
        // interrupt before await
        fiberGroup.fireFiber(new Fiber("f2", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                f1.interrupt();
                return Fiber.frameReturn();
            }
        }));
        fiberGroup.fireFiber(f1);
        TestUtil.waitUtil(interrupt::get);
        Assertions.assertFalse(finish.get());
    }

    @Test
    public void testComplete1() throws Exception {
        FiberFuture<Integer> f = fiberGroup.newFuture("noName");
        AtomicInteger futureResult = new AtomicInteger();
        AtomicReference<Throwable> futureEx = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        Fiber fiber = new Fiber("f", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                latch.countDown();
                return f.await(this::resume);
            }

            private FrameCallResult resume(Integer v) {
                futureResult.set(v);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                futureEx.set(ex);
                return Fiber.frameReturn();
            }
        });
        fiberGroup.fireFiber(fiber);
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        f.fireComplete(100);
        TestUtil.waitUtil(() -> futureResult.get() == 100);
        assertNull(futureEx.get());

        assertEquals(100, f.getResult());
        assertNull(f.getEx());
    }

    @Test
    public void testComplete2() {
        FiberFuture<Integer> f = fiberGroup.newFuture("noName");
        AtomicInteger futureResult = new AtomicInteger();
        AtomicReference<Throwable> futureEx = new AtomicReference<>();
        Fiber fiber = new Fiber("f", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return f.await(this::resume);
            }

            private FrameCallResult resume(Integer v) {
                futureResult.set(v);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                futureEx.set(ex);
                return Fiber.frameReturn();
            }
        });
        // complete before await
        f.fireComplete(100);
        fiberGroup.fireFiber(fiber);
        TestUtil.waitUtil(() -> futureResult.get() == 100);
        assertNull(futureEx.get());

        assertEquals(100, f.getResult());
        assertNull(f.getEx());
    }

    @Test
    public void testCompleteEx1() throws Exception {
        FiberFuture<Integer> f = fiberGroup.newFuture("noName");
        AtomicInteger futureResult = new AtomicInteger();
        AtomicReference<Throwable> futureEx = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        Fiber fiber = new Fiber("f", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                latch.countDown();
                return f.await(this::resume);
            }

            private FrameCallResult resume(Integer v) {
                futureResult.set(v);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                futureEx.set(ex);
                return Fiber.frameReturn();
            }
        });
        fiberGroup.fireFiber(fiber);
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        Exception ex = new Exception();
        f.fireCompleteExceptionally(ex);
        TestUtil.waitUtil(() -> futureEx.get() == ex);
        assertEquals(0, futureResult.get());

        assertNull(f.getResult());
        assertSame(ex, f.getEx());
    }

    @Test
    public void testCompleteEx2() {
        FiberFuture<Integer> f = fiberGroup.newFuture("noName");
        AtomicInteger futureResult = new AtomicInteger();
        AtomicReference<Throwable> futureEx = new AtomicReference<>();
        Fiber fiber = new Fiber("f", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return f.await(this::resume);
            }

            private FrameCallResult resume(Integer v) {
                futureResult.set(v);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                futureEx.set(ex);
                return Fiber.frameReturn();
            }
        });

        // completeEx before await
        Exception ex = new Exception();
        f.fireCompleteExceptionally(ex);

        fiberGroup.fireFiber(fiber);
        TestUtil.waitUtil(() -> futureEx.get() == ex);
        assertEquals(0, futureResult.get());

        assertNull(f.getResult());
        assertSame(ex, f.getEx());
    }

    @Test
    public void testCallback1() throws InterruptedException {
        FiberFuture<Integer> f = fiberGroup.newFuture("noName");
        AtomicReference<Integer> futureResult = new AtomicReference<>();
        AtomicReference<Throwable> futureEx = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        fiberGroup.fireFiber(new Fiber("f", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                latch.countDown();
                f.registerCallback(new FiberFuture.FiberFutureCallback<>() {
                    @Override
                    protected FrameCallResult afterDone(Integer value, Throwable ex) {
                        futureResult.set(value);
                        futureEx.set(ex);
                        return Fiber.frameReturn();
                    }
                });
                return Fiber.frameReturn();
            }
        }));
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        f.fireComplete(100);
        TestUtil.waitUtil(() -> futureResult.get() != null && futureResult.get() == 100);
        assertNull(futureEx.get());
    }

    @Test
    public void testCallback2() {
        FiberFuture<Integer> f = fiberGroup.newFuture("noName");
        AtomicReference<Integer> futureResult = new AtomicReference<>();
        AtomicReference<Throwable> futureEx = new AtomicReference<>();

        // complete before register callback
        f.fireComplete(100);
        fiberGroup.fireFiber(new Fiber("f", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                f.registerCallback(new FiberFuture.FiberFutureCallback<>() {
                    @Override
                    protected FrameCallResult afterDone(Integer value, Throwable ex) {
                        futureResult.set(value);
                        futureEx.set(ex);
                        return Fiber.frameReturn();
                    }
                });
                return Fiber.frameReturn();
            }
        }));

        TestUtil.waitUtil(() -> futureResult.get() != null && futureResult.get() == 100);
        assertNull(futureEx.get());
    }

    @Test
    public void testCallback3() throws InterruptedException {
        FiberFuture<Integer> f = fiberGroup.newFuture("noName");
        AtomicReference<Integer> futureResult = new AtomicReference<>();
        AtomicReference<Throwable> futureEx = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        fiberGroup.fireFiber(new Fiber("f", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                latch.countDown();
                f.registerCallback(new FiberFuture.FiberFutureCallback<>() {
                    @Override
                    protected FrameCallResult afterDone(Integer value, Throwable ex) {
                        futureResult.set(value);
                        futureEx.set(ex);
                        return Fiber.frameReturn();
                    }
                });
                return Fiber.frameReturn();
            }
        }));
        assertTrue(latch.await(1, TimeUnit.SECONDS));

        Exception ex = new Exception();
        f.fireCompleteExceptionally(ex);
        TestUtil.waitUtil(() -> futureEx.get() == ex);
        assertNull(futureResult.get());
    }

    @Test
    public void testCallback4() {
        FiberFuture<Integer> f = fiberGroup.newFuture("noName");
        AtomicReference<Integer> futureResult = new AtomicReference<>();
        AtomicReference<Throwable> futureEx = new AtomicReference<>();

        // complete before register callback
        Exception ex = new Exception();
        f.fireCompleteExceptionally(ex);
        fiberGroup.fireFiber(new Fiber("f", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                f.registerCallback(new FiberFuture.FiberFutureCallback<>() {
                    @Override
                    protected FrameCallResult afterDone(Integer value, Throwable ex) {
                        futureResult.set(value);
                        futureEx.set(ex);
                        return Fiber.frameReturn();
                    }
                });
                return Fiber.frameReturn();
            }
        }));

        TestUtil.waitUtil(() -> futureEx.get() == ex);
        assertNull(futureResult.get());
    }

    @Test
    public void testConvert1() throws InterruptedException {
        FiberFuture<Integer> f = fiberGroup.newFuture("noName");
        AtomicReference<String> futureResult = new AtomicReference<>();
        AtomicReference<Throwable> futureEx = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        fiberGroup.fireFiber(new Fiber("f", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                latch.countDown();
                return f.convert("convert", String::valueOf).await(this::resume);
            }

            private FrameCallResult resume(String s) {
                futureResult.set(s);
                return Fiber.frameReturn();
            }
        }));
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        f.fireComplete(100);
        TestUtil.waitUtil(() -> "100".equals(futureResult.get()));
        assertNull(futureEx.get());
    }

    @Test
    public void testConvert2() {
        FiberFuture<Integer> f = fiberGroup.newFuture("noName");
        AtomicReference<String> futureResult = new AtomicReference<>();
        AtomicReference<Throwable> futureEx = new AtomicReference<>();
        // complete before convert
        f.fireComplete(100);
        fiberGroup.fireFiber(new Fiber("f", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return f.convert("convert", String::valueOf).await(this::resume);
            }

            private FrameCallResult resume(String s) {
                futureResult.set(s);
                return Fiber.frameReturn();
            }
        }));

        TestUtil.waitUtil(() -> "100".equals(futureResult.get()));
        assertNull(futureEx.get());
    }

    @Test
    public void testConvert3() throws InterruptedException {
        FiberFuture<Integer> f = fiberGroup.newFuture("noName");
        AtomicReference<String> futureResult = new AtomicReference<>();
        AtomicReference<Throwable> futureEx = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        fiberGroup.fireFiber(new Fiber("f", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                latch.countDown();
                return f.convert("convert", String::valueOf).await(this::resume);
            }

            private FrameCallResult resume(String s) {
                futureResult.set(s);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                futureEx.set(ex);
                return Fiber.frameReturn();
            }
        }));
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        Exception ex = new Exception();
        f.fireCompleteExceptionally(ex);
        TestUtil.waitUtil(() -> ex == futureEx.get());
        assertNull(futureResult.get());
    }

    @Test
    public void testConvert4() {
        FiberFuture<Integer> f = fiberGroup.newFuture("noName");
        AtomicReference<String> futureResult = new AtomicReference<>();
        AtomicReference<Throwable> futureEx = new AtomicReference<>();
        // complete before convert
        Exception ex = new Exception();
        f.fireCompleteExceptionally(ex);
        fiberGroup.fireFiber(new Fiber("f", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return f.convert("convert", String::valueOf).await(this::resume);
            }

            private FrameCallResult resume(String s) {
                futureResult.set(s);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                futureEx.set(ex);
                return Fiber.frameReturn();
            }
        }));

        TestUtil.waitUtil(() -> ex == futureEx.get());
        assertNull(futureResult.get());
    }

    @Test
    public void testCancel1() throws InterruptedException {
        FiberFuture<Integer> f = fiberGroup.newFuture("noName");
        AtomicReference<Integer> futureResult = new AtomicReference<>();
        AtomicReference<Throwable> futureEx = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        fiberGroup.fireFiber(new Fiber("f", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                latch.countDown();
                return f.await(this::resume);
            }

            private FrameCallResult resume(Integer r) {
                futureResult.set(r);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                futureEx.set(ex);
                return Fiber.frameReturn();
            }
        }));
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        fiberGroup.fireFiber(new Fiber("cancel", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                f.cancel();
                return Fiber.frameReturn();
            }
        }));
        TestUtil.waitUtil(() -> futureEx.get() instanceof FiberCancelException);
        assertNull(futureResult.get());
        assertTrue(f.isCancelled());
    }

    @Test
    public void testCancel2() throws InterruptedException {
        FiberFuture<Integer> f = fiberGroup.newFuture("noName");
        AtomicReference<Integer> futureResult = new AtomicReference<>();
        AtomicReference<Throwable> futureEx = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        // cancel before await
        fiberGroup.fireFiber(new Fiber("cancel", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                latch.countDown();
                f.cancel();
                return Fiber.frameReturn();
            }
        }));
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        fiberGroup.fireFiber(new Fiber("f", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return f.await(this::resume);
            }

            private FrameCallResult resume(Integer r) {
                futureResult.set(r);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                futureEx.set(ex);
                return Fiber.frameReturn();
            }
        }));

        TestUtil.waitUtil(() -> futureEx.get() instanceof FiberCancelException);
        assertNull(futureResult.get());
        assertTrue(f.isCancelled());
    }
}
