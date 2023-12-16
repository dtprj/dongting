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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author huangli
 */
public class LockTest extends AbstractFiberTest {
    @Test
    public void testTryLock() throws Exception {
        FiberLock lock = fiberGroup.newLock();
        AtomicInteger lockCount = new AtomicInteger();
        fiberGroup.fireFiber("f1", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                if (lock.tryLock()) {
                    lockCount.incrementAndGet();
                }
                return Fiber.frameReturn();
            }
        });
        fiberGroup.fireFiber("f2", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                if (lock.tryLock()) {
                    lockCount.incrementAndGet();
                }
                return Fiber.frameReturn();
            }
        });
        super.shutdownDispatcher();
        Assertions.assertEquals(1, lockCount.get());
    }

    private static class TestLockFrame extends FiberFrame<Void> {
        private final FiberLock lock;
        private final AtomicInteger lockCount;
        private final FiberFuture<Void> future;
        private final CountDownLatch beforeLockLatch;
        private final int lockType;

        public TestLockFrame(FiberLock lock, AtomicInteger lockCount, FiberFuture<Void> future,
                             CountDownLatch beforeLockLatch, int lockType) {
            this.lock = lock;
            this.lockCount = lockCount;
            this.future = future;
            this.beforeLockLatch = beforeLockLatch;
            this.lockType = lockType;
        }

        @Override
        public FrameCallResult execute(Void input) {
            beforeLockLatch.countDown();
            if (lockType == 1) {
                return lock.lock(v -> resume(true));
            } else {
                return lock.tryLock(100000, this::resume);
            }
        }

        private FrameCallResult resume(Boolean locked) {
            if (locked) {
                lockCount.incrementAndGet();
                Assertions.assertTrue(lock.isHeldByCurrentFiber());
            }
            return future.await(this::resume2);
        }

        private FrameCallResult resume2(Void unused) {
            lock.unlock();
            return Fiber.frameReturn();
        }
    }

    private void testLockImpl(int lockType) throws Exception {
        FiberLock lock = fiberGroup.newLock();
        AtomicInteger lockCount = new AtomicInteger();
        CountDownLatch beforeLockLatch = new CountDownLatch(2);
        FiberFuture<Void> future = fiberGroup.newFuture();
        fiberGroup.fireFiber("f1", new TestLockFrame(lock, lockCount, future, beforeLockLatch, lockType));
        fiberGroup.fireFiber("f2", new TestLockFrame(lock, lockCount, future, beforeLockLatch, lockType));
        Assertions.assertTrue(beforeLockLatch.await(1, TimeUnit.SECONDS));
        fiberGroup.fireFiber("f3", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                future.complete(null);
                return Fiber.frameReturn();
            }
        });
        TestUtil.waitUtil(() -> lockCount.get() == 2);
    }

    @Test
    public void testLock1() throws Exception {
        testLockImpl(1);
    }

    @Test
    public void testLock2() throws Exception {
        testLockImpl(2);
    }
}
