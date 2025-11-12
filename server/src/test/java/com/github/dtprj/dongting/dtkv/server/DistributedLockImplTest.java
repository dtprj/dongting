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

import com.github.dtprj.dongting.dtkv.DistributedLock;
import com.github.dtprj.dongting.net.NetException;
import com.github.dtprj.dongting.test.WaitUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.github.dtprj.dongting.test.Tick.tick;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author huangli
 */
class DistributedLockImplTest extends ServerClientLockTest {

    @BeforeAll
    public static void setup() throws Exception {
        setupServerClient();
    }

    @AfterAll
    public static void teardown() {
        stopServerClient();
    }

    // ========== P0: Basic lock/unlock operations ==========

    @Test
    public void testSingleClientLockUnlock() {
        DistributedLock lock = client1.createLock(groupId, "test-lock-1".getBytes());
        try {
            // acquire lock
            boolean acquired = lock.tryLock(60000, 0);
            assertTrue(acquired, "should acquire lock successfully");
            assertTrue(lock.isHeldByCurrentClient(), "should hold the lock");

            long restMillis = lock.getLeaseRestMillis();
            assertTrue(restMillis > 59000 && restMillis <= 60000,
                    "lease rest time should be close to 60000ms, actual: " + restMillis);

            // unlock
            lock.unlock();
            assertFalse(lock.isHeldByCurrentClient(), "should not hold the lock after unlock");
            assertEquals(0, lock.getLeaseRestMillis(), "lease rest time should be 0 after unlock");
        } finally {
            lock.close();
        }
    }

    @Test
    public void testUnlockWithoutHoldingLock() {
        DistributedLock lock = client1.createLock(groupId, "test-lock-2".getBytes());
        try {
            // unlock without holding lock should not throw exception
            assertDoesNotThrow(() -> lock.unlock(), "unlock without holding lock should not throw");
            assertFalse(lock.isHeldByCurrentClient());
        } finally {
            lock.close();
        }
    }

    // ========== P0: Multi-client competition ==========

    @Test
    public void testTwoClientsCompeteImmediateFail() {
        DistributedLock lock1 = client1.createLock(groupId, "test-lock-3".getBytes());
        DistributedLock lock2 = client2.createLock(groupId, "test-lock-3".getBytes());
        try {
            // client1 acquires lock
            assertTrue(lock1.tryLock(60000, 0));
            assertTrue(lock1.isHeldByCurrentClient());

            // client2 tries to acquire immediately, should fail
            boolean acquired = lock2.tryLock(60000, 0);
            assertFalse(acquired, "client2 should fail to acquire lock");
            assertFalse(lock2.isHeldByCurrentClient());

            // client1 still holds the lock
            assertTrue(lock1.isHeldByCurrentClient());
        } finally {
            lock1.close();
            lock2.close();
        }
    }

    @Test
    public void testTwoClientsCompeteWithTimeout() {
        DistributedLock lock1 = client1.createLock(groupId, "test-lock-4".getBytes());
        DistributedLock lock2 = client2.createLock(groupId, "test-lock-4".getBytes());
        try {
            // client1 acquires lock
            assertTrue(lock1.tryLock(60000, 0));

            // client2 waits for lock with timeout
            long startTime = System.currentTimeMillis();
            boolean acquired = lock2.tryLock(60000, tick(30));
            long elapsed = System.currentTimeMillis() - startTime;

            assertFalse(acquired, "client2 should fail to acquire lock after timeout");
            assertTrue(elapsed >= tick(30));
            assertTrue(elapsed < tick(60));

            // client1 still holds the lock
            assertTrue(lock1.isHeldByCurrentClient());
            assertFalse(lock2.isHeldByCurrentClient());
        } finally {
            lock1.close();
            lock2.close();
        }
    }

    @Test
    public void testLockReleaseAndAutomaticAcquire() throws Exception {
        DistributedLock lock1 = client1.createLock(groupId, "test-lock-5".getBytes());
        DistributedLock lock2 = client2.createLock(groupId, "test-lock-5".getBytes());
        DistributedLock lock3 = client3.createLock(groupId, "test-lock-5".getBytes());
        try {
            // client1 acquires lock
            assertTrue(lock1.tryLock(60000, 0));

            // client2 waits asynchronously
            CompletableFuture<Boolean> f2 = new CompletableFuture<>();
            lock2.tryLock(60000, 1000, (result, ex) -> {
                if (ex != null) {
                    f2.completeExceptionally(ex);
                } else {
                    f2.complete(result);
                }
            });

            Thread.sleep(tick(20));

            CompletableFuture<Boolean> f3 = new CompletableFuture<>();
            lock3.tryLock(60000, 1000, (result, ex) -> {
                if (ex != null) {
                    f3.completeExceptionally(ex);
                } else {
                    f3.complete(result);
                }
            });

            // client1 unlocks, client2 should acquire the lock
            lock1.unlock();
            assertTrue(f2.get(500, TimeUnit.MILLISECONDS));
            assertFalse(lock1.isHeldByCurrentClient());
            assertTrue(lock2.isHeldByCurrentClient());
            assertFalse(lock3.isHeldByCurrentClient());

            // client3 should acquire the lock
            lock2.unlock();
            assertTrue(f3.get(500, TimeUnit.MILLISECONDS));
            assertFalse(lock1.isHeldByCurrentClient());
            assertFalse(lock2.isHeldByCurrentClient());
            assertTrue(lock3.isHeldByCurrentClient());
        } finally {
            lock1.close();
            lock2.close();
            lock3.close();
        }
    }

    // ========== P0: Parameter validation ==========

    @Test
    public void testInvalidLeaseTime() {
        DistributedLock lock = client1.createLock(groupId, "test-lock-6".getBytes());
        try {
            // zero lease time
            assertThrows(IllegalArgumentException.class, () -> lock.tryLock(0, 0));

            // negative lease time
            assertThrows(IllegalArgumentException.class, () -> lock.tryLock(-1000, 0));

            // updateLease with zero
            assertThrows(IllegalArgumentException.class, () -> lock.updateLease(0));

            // updateLease with negative
            assertThrows(IllegalArgumentException.class, () -> lock.updateLease(-5000));
        } finally {
            lock.close();
        }
    }

    @Test
    public void testWaitTimeoutGreaterThanLease() {
        DistributedLock lock = client1.createLock(groupId, "test-lock-7".getBytes());
        try {
            assertThrows(IllegalArgumentException.class,
                    () -> lock.tryLock(5000, 10000),
                    "waitLockTimeoutMillis must be less than or equal to leaseMillis");
        } finally {
            lock.close();
        }
    }

    @Test
    public void testNegativeWaitTimeout() {
        DistributedLock lock = client1.createLock(groupId, "test-lock-8".getBytes());
        try {
            assertThrows(IllegalArgumentException.class,
                    () -> lock.tryLock(10000, -100));
        } finally {
            lock.close();
        }
    }

    // ========== P0: Lock instance lifecycle ==========

    @Test
    public void testCloseLockInstance() {
        DistributedLock lock1 = client1.createLock(groupId, "test-lock1-9".getBytes());

        // acquire and close
        assertTrue(lock1.tryLock(60000, 0));
        lock1.close();

        // operations after close should throw exception
        assertThrows(IllegalStateException.class, () -> lock1.tryLock(60000, 0));
        assertThrows(IllegalStateException.class, () -> lock1.updateLease(60000));
        assertThrows(IllegalStateException.class, lock1::unlock);
    }

    @Test
    public void testCloseDoesNotAffectOtherClients() throws Exception {
        DistributedLock lock1 = client1.createLock(groupId, "test-lock-10".getBytes());

        // client1 acquires and closes
        assertTrue(lock1.tryLock(60000, 0));
        lock1.close();

        // the close method release lock asynchronously, wait a moment
        Thread.sleep(tick(20));

        // client2 should be able to acquire the same lock
        DistributedLock lock2 = client2.createLock(groupId, "test-lock-10".getBytes());
        try {
            boolean acquired = lock2.tryLock(60000, 0);
            assertTrue(acquired, "client2 should acquire the lock after client1 closes");
        } finally {
            lock2.close();
        }
    }

    @Test
    public void testRecreateAfterClose() {
        DistributedLock lock1 = client1.createLock(groupId, "test-lock-11".getBytes());
        lock1.close();

        // recreate lock with the same key
        DistributedLock lock2 = client1.createLock(groupId, "test-lock-11".getBytes());
        try {
            assertNotSame(lock1, lock2, "should return a new instance");
            assertTrue(lock2.tryLock(60000, 0), "new lock instance should work");
        } finally {
            lock2.close();
        }
    }

    // ========== P1: Lease management ==========

    @Test
    public void testUpdateLeaseSuccess() {
        DistributedLock lock = client1.createLock(groupId, "test-lock-12".getBytes());
        try {
            // acquire lock with 10s lease
            assertTrue(lock.tryLock(1000, 0));

            // update lease to 15s
            lock.updateLease(15000);

            // check remaining time
            long restMillis = lock.getLeaseRestMillis();
            assertTrue(restMillis > 14000 && restMillis <= 15000,
                    "lease rest time should be close to 15000ms after update, actual: " + restMillis);
        } finally {
            lock.close();
        }
    }

    @Test
    public void testUpdateLeaseWithoutHolding() {
        DistributedLock lock = client1.createLock(groupId, "test-lock-13".getBytes());
        try {
            // try to update lease without holding lock
            assertThrows(IllegalStateException.class,
                    () -> lock.updateLease(10000),
                    "should throw when updating lease without holding lock");
        } finally {
            lock.close();
        }
    }

    @Test
    public void testUpdateLeaseAfterExpired() throws Exception {
        DistributedLock lock = client1.createLock(groupId, "test-lock-14".getBytes());
        try {
            // acquire lock with very short lease
            assertTrue(lock.tryLock(tick(20), 0));

            // wait for expiration
            Thread.sleep(tick(21));

            // try to update lease after expiration
            assertThrows(IllegalStateException.class,
                    () -> lock.updateLease(10000),
                    "should throw when updating lease after expiration");
        } finally {
            lock.close();
        }
    }

    @Test
    public void testLeaseNaturalExpiration() throws Exception {
        AtomicBoolean expireFlag = new AtomicBoolean(false);
        DistributedLock lock = client1.createLock(groupId, "test-lock-15".getBytes(),
                () -> expireFlag.set(true));
        try {
            // acquire lock with short lease
            assertTrue(lock.tryLock(tick(20), 0));

            // wait for expiration
            Thread.sleep(tick(21));

            // verify expire listener was called
            WaitUtil.waitUtil(expireFlag::get);
            assertTrue(expireFlag.get(), "expire listener should be called");
            assertFalse(lock.isHeldByCurrentClient(), "should not hold lock after expiration");
        } finally {
            lock.close();
        }
    }

    // ========== P1: Async operations ==========

    @Test
    public void testAsyncTryLockSuccess() throws Exception {
        DistributedLock lock = client1.createLock(groupId, "test-lock-16".getBytes());
        try {
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            lock.tryLock(60000, 0, (result, ex) -> {
                if (ex != null) {
                    future.completeExceptionally(ex);
                } else {
                    future.complete(result);
                }
            });

            Boolean acquired = future.get(tick(5000), TimeUnit.MILLISECONDS);
            assertTrue(acquired, "async tryLock should succeed");
            assertTrue(lock.isHeldByCurrentClient());
        } finally {
            lock.close();
        }
    }

    @Test
    public void testAsyncTryLockFail() throws Exception {
        DistributedLock lock1 = client1.createLock(groupId, "test-lock-17".getBytes());
        DistributedLock lock2 = client2.createLock(groupId, "test-lock-17".getBytes());
        try {
            // client1 acquires lock
            assertTrue(lock1.tryLock(60000, 0));

            // client2 tries asynchronously
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            lock2.tryLock(60000, 0, (result, ex) -> {
                if (ex != null) {
                    future.completeExceptionally(ex);
                } else {
                    future.complete(result);
                }
            });

            Boolean acquired = future.get(500, TimeUnit.MILLISECONDS);
            assertFalse(acquired, "async tryLock should fail when lock is held");
            assertFalse(lock2.isHeldByCurrentClient());
        } finally {
            lock1.close();
            lock2.close();
        }
    }

    @Test
    public void testAsyncUnlock() throws Exception {
        DistributedLock lock = client1.createLock(groupId, "test-lock-18".getBytes());
        try {
            // acquire lock
            assertTrue(lock.tryLock(60000, 0));

            // async unlock
            CompletableFuture<Void> future = new CompletableFuture<>();
            lock.unlock((result, ex) -> {
                if (ex != null) {
                    future.completeExceptionally(ex);
                } else {
                    future.complete(null);
                }
            });

            future.get(tick(5000), TimeUnit.MILLISECONDS);
            assertFalse(lock.isHeldByCurrentClient(), "should not hold lock after async unlock");
        } finally {
            lock.close();
        }
    }

    @Test
    public void testAsyncUpdateLease() throws Exception {
        DistributedLock lock = client1.createLock(groupId, "test-lock-19".getBytes());
        try {
            // acquire lock
            assertTrue(lock.tryLock(10000, 0));

            // async update lease
            CompletableFuture<Void> future = new CompletableFuture<>();
            lock.updateLease(20000, (result, ex) -> {
                if (ex != null) {
                    future.completeExceptionally(ex);
                } else {
                    future.complete(null);
                }
            });

            future.get(500, TimeUnit.MILLISECONDS);
            long restMillis = lock.getLeaseRestMillis();
            assertTrue(restMillis > 19000 && restMillis <= 20000,
                    "lease should be updated, actual rest: " + restMillis);
        } finally {
            lock.close();
        }
    }

    // ========== P1: Concurrency control ==========

    @Test
    public void testOperationSerialization() {
        DistributedLock lock1 = client1.createLock(groupId, "test-lock-20".getBytes());
        DistributedLock lock2 = client2.createLock(groupId, "test-lock-20".getBytes());
        try {
            // client2 holds the lock
            assertTrue(lock2.tryLock(60000, 0));

            // client1 starts waiting asynchronously
            lock1.tryLock(60000, 1000, (result, ex) -> {
            });

            // try to update lease while another operation is in progress
            assertThrows(IllegalStateException.class,
                    () -> lock1.tryLock(2000, 1000),
                    "should throw when another operation is in progress");
            assertThrows(IllegalStateException.class,
                    () -> lock1.updateLease(2000),
                    "should throw when another operation is in progress");
            assertDoesNotThrow(() -> lock1.unlock());
        } finally {
            lock1.close();
            lock2.close();
        }
    }

    @Test
    public void testUnlockCancelsCurrentOperation() {
        DistributedLock lock1 = client1.createLock(groupId, "test-lock-21".getBytes());
        DistributedLock lock2 = client2.createLock(groupId, "test-lock-21".getBytes());
        try {
            // client2 holds the lock
            assertTrue(lock2.tryLock(60000, 0));

            // client1 starts waiting
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            lock1.tryLock(60000, 1000, (result, ex) -> {
                if (ex != null) {
                    future.completeExceptionally(ex);
                } else {
                    future.complete(result);
                }
            });

            // unlock (even though not holding) should cancel the waiting operation
            lock1.unlock();

            // wait for the async operation to complete
            try {
                future.get(tick(2000), TimeUnit.MILLISECONDS);
                fail("should throw exception");
            } catch (Exception e) {
                // expected
                assertTrue(e.getCause().getMessage().contains("canceled by unlock"));
            }

            // client2 should still hold the lock
            assertTrue(lock2.isHeldByCurrentClient());
        } finally {
            lock1.close();
            lock2.close();
        }
    }

    // ========== P1: Exception scenarios ==========

    @Test
    public void testNetworkTimeout() throws Exception {
        server.stopServers();
        try {
            DistributedLock lock = client1.createLock(groupId, "test-lock-22".getBytes());
            try {
                assertThrows(NetException.class,
                        () -> lock.tryLock(60000, 0),
                        "should throw NetException when server is down");
            } finally {
                lock.close();
            }
        } finally {
            // restart server
            server = new Server();
            server.startServers();
        }
    }

    @Test
    @Disabled // this test is not stable enough
    public void testClientLeaseTimeoutWhenServerGrant() {
        DistributedLock lock = client1.createLock(groupId, "test-lock-23".getBytes());
        try {
            // acquire lock with very short lease
            assertFalse(lock.tryLock(1, 0));
        } finally {
            lock.close();
        }
    }

}
