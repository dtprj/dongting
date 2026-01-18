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
package com.github.dtprj.dongting.dtkv.server;

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.VersionFactory;
import com.github.dtprj.dongting.dtkv.AutoRenewalLock;
import com.github.dtprj.dongting.dtkv.AutoRenewalLockListener;
import com.github.dtprj.dongting.test.WaitUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.github.dtprj.dongting.test.Tick.tick;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
class AutoRenewalLockImplTest extends ServerClientLockTest {

    @BeforeAll
    public static void setup() throws Exception {
        setupServerClient();
        client1.mkdir(groupId, "test".getBytes());
    }

    @AfterAll
    public static void teardown() {
        stopServerClient();
    }

    @BeforeEach
    public void beforeEach() {
        client1.reset();
        client2.reset();
        client3.reset();
    }

    static class Listener implements AutoRenewalLockListener {

        final AtomicInteger acquiredCount = new AtomicInteger(0);
        final AtomicInteger lostCount = new AtomicInteger(0);

        @Override
        public void onAcquired(AutoRenewalLock lock) {
            acquiredCount.incrementAndGet();
        }

        @Override
        public void onLost(AutoRenewalLock lock) {
            lostCount.incrementAndGet();
        }
    }

    @Test
    void testBasicAcquireAndHold() {
        Listener listener = new Listener();

        byte[] key = "test.lock1".getBytes();
        AutoRenewalLock lock = client1.createAutoRenewalLock(groupId, key, 5000, listener);
        lock.start();

        WaitUtil.waitUtil(1, listener.acquiredCount::get);
        assertEquals(0, listener.lostCount.get());
        assertTrue(lock.isHeldByCurrentClient());
        assertTrue(lock.getLeaseRestMillis() > 0);

        lock.stop(new DtTime(1, TimeUnit.SECONDS));
    }

    @Test
    void testAutoRenewal() {
        Listener listener = new Listener();

        byte[] key = "test.lock2".getBytes();
        long leaseMillis = tick(30);
        AutoRenewalLock lock = client1.createAutoRenewalLock(groupId, key, leaseMillis, listener);
        lock.start();

        WaitUtil.waitUtil(1, listener.acquiredCount::get);
        AtomicLong leaseRestMillis = new AtomicLong(lock.getLeaseRestMillis());

        WaitUtil.waitUtil(() -> {
            long rest = lock.getLeaseRestMillis();
            if (rest > 0 && rest < leaseRestMillis.get()) {
                leaseRestMillis.set(rest);
                return false;
            } else {
                return true;
            }
        });

        // Lock should still be held
        assertTrue(lock.isHeldByCurrentClient());
        assertEquals(0, listener.lostCount.get());

        // Should not trigger onAcquired again
        assertEquals(1, listener.acquiredCount.get());

        lock.stop(new DtTime(1, TimeUnit.SECONDS));
    }

    @Test
    void testLockExpireAndReacquire() {
        Listener listener = new Listener();

        byte[] key = "test.lock3".getBytes();
        long leaseMillis = tick(30);
        AutoRenewalLock lock = client1.createAutoRenewalLock(groupId, key, leaseMillis, listener);
        lock.start();

        WaitUtil.waitUtil(1, listener.acquiredCount::get);

        // update lease will fail, and first retry fail, and second retry after tick(1000)ms
        client1.mockCount = 2;
        client1.mockFailInCallback = true;

        WaitUtil.waitUtil(1, listener.lostCount::get);
        WaitUtil.waitUtil(2, listener.acquiredCount::get);

        lock.stop(new DtTime(1, TimeUnit.SECONDS));
    }

    @Test
    void testMultipleClientsCompetition() {
        Listener listener1 = new Listener();
        Listener listener2 = new Listener();

        byte[] key = "test.lock4".getBytes();
        long leaseMillis = tick(30);
        AutoRenewalLock lock1 = client1.createAutoRenewalLock(groupId, key, leaseMillis, listener1);
        AutoRenewalLock lock2 = client2.createAutoRenewalLock(groupId, key, leaseMillis, listener2);
        lock1.start();
        lock2.start();

        // One of them should acquire the lock
        WaitUtil.waitUtil(() -> listener1.acquiredCount.get() == 1 || listener2.acquiredCount.get() == 1);

        // Only one should be holding the lock
        if (lock1.isHeldByCurrentClient()) {
            assertFalse(lock2.isHeldByCurrentClient());
            client1.mockCount = 100;
            client1.mockFailSync = true;
            WaitUtil.waitUtil(1, listener1.lostCount::get);
            WaitUtil.waitUtil(1, listener2.acquiredCount::get);
        } else {
            assertFalse(lock1.isHeldByCurrentClient());
            client2.mockCount = 100;
            client2.mockFailSync = true;
            WaitUtil.waitUtil(1, listener2.lostCount::get);
            WaitUtil.waitUtil(1, listener1.acquiredCount::get);
        }
        lock1.stop(new DtTime(1, TimeUnit.SECONDS));
        lock2.stop(new DtTime(1, TimeUnit.SECONDS));
    }

    @Test
    void testCloseWhileHolding() {
        Listener listener = new Listener();

        byte[] key = "test.lock5".getBytes();
        AutoRenewalLock lock = client1.createAutoRenewalLock(groupId, key, tick(30), listener);
        lock.start();

        WaitUtil.waitUtil(1, listener.acquiredCount::get);
        assertTrue(lock.isHeldByCurrentClient());

        // Close while holding
        lock.stop(new DtTime(1, TimeUnit.SECONDS));

        // Should trigger onLost
        WaitUtil.waitUtil(1, listener.lostCount::get);
        assertFalse(lock.isHeldByCurrentClient());
    }

    @Test
    void testCloseBeforeAcquire() throws Exception {
        Listener listener1 = new Listener();
        Listener listener2 = new Listener();

        byte[] key = "test.lock6".getBytes();

        // First client holds the lock
        AutoRenewalLock lock1 = client1.createAutoRenewalLock(groupId, key, 1000, listener1);
        lock1.start();
        WaitUtil.waitUtil(1, listener1.acquiredCount::get);

        // Second client tries to get the same lock (will wait)
        AutoRenewalLock lock2 = client2.createAutoRenewalLock(groupId, key, 1000, listener2);
        lock2.start();

        // Close before acquiring
        lock2.stop(new DtTime(1, TimeUnit.SECONDS));
        Thread.sleep(tick(5));

        assertEquals(0, listener2.acquiredCount.get());
        assertEquals(0, listener2.lostCount.get());

        lock1.stop(new DtTime(1, TimeUnit.SECONDS));
    }

    @Test
    void testRetryAfterUpdateLeaseFailure() throws Exception {
        Listener listener = new Listener();

        byte[] key = "test.lock7".getBytes();
        AutoRenewalLock lock = client1.createAutoRenewalLock(groupId, key, tick(40), listener);
        lock.start();

        WaitUtil.waitUtil(1, listener.acquiredCount::get);

        // update lease fail and retry after tick(5)ms
        client1.mockCount = 1;
        client1.mockFailSync = true;

        Thread.sleep(tick(40));

        assertTrue(lock.isHeldByCurrentClient());
        assertEquals(0, listener.lostCount.get());
        lock.stop(new DtTime(1, TimeUnit.SECONDS));
    }

    @Test
    void testExpireDuringRpcInProgress() {
        Listener listener = new Listener();
        byte[] key = "testExpireDuringRpcInProgress".getBytes();
        AutoRenewalLock lock = client1.createAutoRenewalLock(groupId, key, tick(20), listener);
        lock.start();
        WaitUtil.waitUtil(1, listener.acquiredCount::get);
        client1.mockCount = 1;
        client1.mockDelayMillis = tick(20);
        WaitUtil.waitUtil(1, listener.lostCount::get);
        WaitUtil.waitUtil(2, listener.acquiredCount::get);
        lock.stop(new DtTime(1, TimeUnit.SECONDS));
    }

    @Test
    void testRestLeaseTooSmall() {
        Listener listener = new Listener(){
            @Override
            public void onLost(AutoRenewalLock lock) {
                super.onLost(lock);
                client1.autoRenewalMinValidLeaseMillis = 1;
            }
        };
        byte[] key = "testRestLeaseTooSmall".getBytes();
        AutoRenewalLock lock = client1.createAutoRenewalLock(groupId, key, tick(20), listener);
        lock.start();
        WaitUtil.waitUtil(1, listener.acquiredCount::get);
        client1.autoRenewalMinValidLeaseMillis = 10000;
        VersionFactory.getInstance().fullFence();
        WaitUtil.waitUtil(1, listener.lostCount::get);
        WaitUtil.waitUtil(2, listener.acquiredCount::get);
        lock.stop(new DtTime(1, TimeUnit.SECONDS));
    }

    @Test
    void testDuplicateLockCreation() {
        byte[] key = "test.lock9".getBytes();

        Listener listener = new Listener();

        AutoRenewalLock lock1 = client1.createAutoRenewalLock(groupId, key, tick(5000), listener);
        lock1.start();

        // Try to create another lock with the same key in the same client
        assertThrows(IllegalStateException.class, () -> client1.createAutoRenewalLock(
                groupId, key, tick(5000), listener));
        assertThrows(IllegalStateException.class, () -> client1.createLock(groupId, key));

        lock1.stop(new DtTime(1, TimeUnit.SECONDS));

        AutoRenewalLock lock2 = client1.createAutoRenewalLock(groupId, key, tick(5000), listener);
        lock2.start();
        lock2.stop(new DtTime(1, TimeUnit.SECONDS));
    }

    @Test
    void testInvalidParameters() {
        byte[] key = "test.lock10".getBytes();
        Listener listener = new Listener();

        // Invalid lease time
        assertThrows(IllegalArgumentException.class, () -> client1.createAutoRenewalLock(groupId, key, 0, listener));

        assertThrows(IllegalArgumentException.class, () -> client1.createAutoRenewalLock(groupId, key, -1000, listener));

        assertThrows(IllegalArgumentException.class, () -> client1.createAutoRenewalLock(groupId, key, 1, listener));

        // Null listener
        assertThrows(NullPointerException.class, () -> client1.createAutoRenewalLock(groupId, key, tick(5000), null));
    }
}
