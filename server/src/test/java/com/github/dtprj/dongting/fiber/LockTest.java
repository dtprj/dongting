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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author huangli
 */
public class LockTest extends AbstractFiberTest {

    @Test
    public void testTryLock1() throws Exception {
        FiberLock lock1 = fiberGroup.newLock("noName");
        FiberLock lock2 = fiberGroup.newLock("noName");
        testTryLock(new Lock[]{lock1, lock2}, new boolean[]{true, true});
    }

    @Test
    public void testTryLock2() throws Exception {
        FiberLock lock = fiberGroup.newLock("noName");
        testTryLock(new Lock[]{lock, lock}, new boolean[]{true, false});
    }

    @Test
    public void testTryLock3() throws Exception {
        FiberLock lock = fiberGroup.newLock("noName");
        FiberReadLock readLock = lock.readLock();
        testTryLock(new Lock[]{lock, readLock}, new boolean[]{true, false});
    }

    @Test
    public void testTryLock4() throws Exception {
        FiberLock lock = fiberGroup.newLock("noName");
        FiberReadLock readLock = lock.readLock();
        testTryLock(new Lock[]{readLock, lock}, new boolean[]{true, false});
    }

    @Test
    public void testTryLock5() throws Exception {
        FiberLock lock = fiberGroup.newLock("noName");
        FiberReadLock readLock = lock.readLock();
        testTryLock(new Lock[]{readLock, readLock, lock}, new boolean[]{true, true, false});
    }

    private void testTryLock(Lock[] locks, boolean[] expectResults) throws Exception {
        Boolean[] locked = new Boolean[locks.length];
        for (int i = 0; i < locks.length; i++) {
            int index = i;
            fiberGroup.fireFiber("f" + i, new FiberFrame<>() {
                @Override
                public FrameCallResult execute(Void input) {
                    if (locks[index].tryLock()) {
                        // test re-entry
                        locked[index] = locks[index].tryLock();
                    } else {
                        locked[index] = false;
                    }
                    return Fiber.frameReturn();
                }
            });
        }
        super.shutdownDispatcher();
        for (int i = 0; i < expectResults.length; i++) {
            Assertions.assertEquals(expectResults[i], locked[i]);
        }
    }

    private static class TestLockFrame extends FiberFrame<Void> {
        private final Lock lock;
        private volatile int lockCount;
        private final int lockType;
        private FiberFuture<Void> future;
        private Fiber ownerFiber;

        public TestLockFrame(Lock lock, int lockType) {
            this.lock = lock;
            this.lockType = lockType;
        }

        @Override
        public FrameCallResult execute(Void input) {
            if (lockType == 1) {
                return lock.lock(v -> resume(true));
            } else {
                return lock.tryLock(100000, this::resume);
            }
        }

        private FrameCallResult resume(Boolean locked) {
            Assertions.assertTrue(locked);
            if (lock instanceof FiberLock) {
                FiberLock l = (FiberLock) lock;
                Assertions.assertTrue(l.isHeldByCurrentFiber());
            }
            lockCount++;
            if (lockCount == 1) {
                // test re-entry
                return Fiber.resume(null, this);
            }
            future = getFiberGroup().newFuture("noName");
            ownerFiber = getFiber();
            return future.await(this::resume2);
        }

        private FrameCallResult resume2(Void unused) {
            lock.unlock();
            lock.unlock();
            return Fiber.frameReturn();
        }

        // not call in fiber group thread
        public void signalReleaseLockAndJoin() {
            CompletableFuture<Void> f = new CompletableFuture<>();
            getFiberGroup().fireFiber("signal-release", new FiberFrame<>() {
                @Override
                public FrameCallResult execute(Void input) {
                    future.complete(null);
                    return ownerFiber.join(this:: afterJoin);
                }
                private FrameCallResult afterJoin(Void unused) {
                    f.complete(null);
                    return Fiber.frameReturn();
                }
            });
            f.join();
        }
    }

    @Test
    public void testLock1() {
        FiberLock lock = fiberGroup.newLock("noName");
        for (int lockType = 1; lockType <= 2; lockType++) {
            TestLockFrame f1 = new TestLockFrame(lock, lockType);
            TestLockFrame f2 = new TestLockFrame(lock, lockType);
            fiberGroup.fireFiber("f1", f1);
            fiberGroup.fireFiber("f2", f2);
            TestUtil.waitUtil(() -> f1.lockCount == 2);
            Assertions.assertEquals(0, f2.lockCount);
            f1.signalReleaseLockAndJoin();
            TestUtil.waitUtil(() -> f2.lockCount == 2);
            f2.signalReleaseLockAndJoin();
        }
    }

    @Test
    public void testLock2() {
        FiberLock lock = fiberGroup.newLock("noName");
        FiberReadLock readLock = lock.readLock();
        for (int lockType = 1; lockType <= 2; lockType++) {
            TestLockFrame f1 = new TestLockFrame(lock, lockType);
            TestLockFrame f2 = new TestLockFrame(readLock, lockType);
            fiberGroup.fireFiber("f1", f1);
            fiberGroup.fireFiber("f2", f2);
            TestUtil.waitUtil(() -> f1.lockCount == 2);
            Assertions.assertEquals(0, f2.lockCount);
            f1.signalReleaseLockAndJoin();
            TestUtil.waitUtil(() -> f2.lockCount == 2);
            f2.signalReleaseLockAndJoin();
        }
    }

    @Test
    public void testLock3() {
        FiberLock lock = fiberGroup.newLock("noName");
        FiberReadLock readLock = lock.readLock();
        for (int lockType = 1; lockType <= 2; lockType++) {
            TestLockFrame f1 = new TestLockFrame(readLock, lockType);
            TestLockFrame f2 = new TestLockFrame(lock, lockType);
            fiberGroup.fireFiber("f1", f1);
            fiberGroup.fireFiber("f2", f2);
            TestUtil.waitUtil(() -> f1.lockCount == 2);
            Assertions.assertEquals(0, f2.lockCount);
            f1.signalReleaseLockAndJoin();
            TestUtil.waitUtil(() -> f2.lockCount == 2);
            f2.signalReleaseLockAndJoin();
        }
    }

    @Test
    public void testLock4() {
        FiberLock lock = fiberGroup.newLock("noName");
        FiberReadLock readLock = lock.readLock();
        for (int lockType = 1; lockType <= 2; lockType++) {
            TestLockFrame f1 = new TestLockFrame(readLock, lockType);
            TestLockFrame f2 = new TestLockFrame(readLock, lockType);
            TestLockFrame f3 = new TestLockFrame(lock, lockType);
            fiberGroup.fireFiber("f1", f1);
            fiberGroup.fireFiber("f2", f2);
            fiberGroup.fireFiber("f3", f3);
            TestUtil.waitUtil(() -> f1.lockCount == 2);
            TestUtil.waitUtil(() -> f2.lockCount == 2);
            Assertions.assertEquals(0, f3.lockCount);
            f1.signalReleaseLockAndJoin();
            f2.signalReleaseLockAndJoin();
            TestUtil.waitUtil(() -> f3.lockCount == 2);
            f3.signalReleaseLockAndJoin();
        }
    }

    @Test
    public void testTimeTryLockFail() {
        FiberLock lock = fiberGroup.newLock("noName");
        fiberGroup.fireFiber("f1", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                lock.tryLock();
                return Fiber.frameReturn();
            }
        });
        AtomicReference<Boolean> locked = new AtomicReference<>();
        fiberGroup.fireFiber("f2" , new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return lock.tryLock(1, this::resume);
            }
            private FrameCallResult resume(Boolean result) {
                locked.set(result);
                return Fiber.frameReturn();
            }
        });
        TestUtil.waitUtil(() -> locked.get() != null && locked.get() == false);
    }
}
