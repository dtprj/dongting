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
package com.github.dtprj.dongting.it.support;

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.dtkv.AutoRenewalLock;
import com.github.dtprj.dongting.dtkv.AutoRenewalLockListener;
import com.github.dtprj.dongting.dtkv.DistributedLock;
import com.github.dtprj.dongting.dtkv.KvClient;
import com.github.dtprj.dongting.dtkv.KvNode;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * Validator for distributed lock exclusivity.
 * Verifies that two clients cannot hold the same lock simultaneously.
 *
 * @author huangli
 */
public class StressLockValidator implements Runnable {
    private static final DtLog log = DtLogs.getLogger(StressLockValidator.class);
    public static final String PREFIX = "StressIT_Lock";


    private final int groupId;
    private final int validatorId;
    private final long lockLeaseMillis;
    public static final AtomicLong verifyCount = new AtomicLong();
    public static final AtomicLong violationCount = new AtomicLong();
    public static final AtomicLong failureCount = new AtomicLong();
    private final AtomicBoolean stop;

    private final KvClient client1;
    private final KvClient client2;

    private final AutoLockThread autoLockThread1;
    private final AutoLockThread autoLockThread2;
    private final Thread thread1;
    private final Thread thread2;

    private final AutoRenewalLock autoLock1;
    private final AutoRenewalLock autoLock2;
    private final DistributedLock lock1;
    private final DistributedLock lock2;

    public StressLockValidator(int groupId, int validatorId, long lockLeaseMillis,
                               BiFunction<String, UUID, KvClient> clientFactory, AtomicBoolean stop) {
        this.groupId = groupId;
        this.validatorId = validatorId;
        this.lockLeaseMillis = lockLeaseMillis;
        this.stop = stop;

        long most = 234324325663423L;
        this.client1 = clientFactory.apply("StressLockValidator" + validatorId + "_1",
                new UUID(most, validatorId * 10L + 1));
        this.client2 = clientFactory.apply("StressLockValidator" + validatorId + "_2",
                new UUID(most, validatorId * 10L + 2));


        client1.mkdir(groupId, PREFIX.getBytes());
        String autoRenewalKey = key("autoRenewalLock");

        autoLockThread1 = new AutoLockThread(client1);
        autoLockThread2 = new AutoLockThread(client2);
        autoLock1 = client1.createAutoRenewalLock(groupId, autoRenewalKey.getBytes(),
                lockLeaseMillis, autoLockThread1);
        autoLock2 = client2.createAutoRenewalLock(groupId, autoRenewalKey.getBytes(),
                lockLeaseMillis, autoLockThread2);
        autoLockThread1.myLock = autoLock1;
        autoLockThread1.otherLock = autoLock2;
        autoLockThread2.myLock = autoLock2;
        autoLockThread2.otherLock = autoLock1;

        String lockKey = key("generalLock");
        lock1 = client1.createLock(groupId, lockKey.getBytes());
        lock2 = client2.createLock(groupId, lockKey.getBytes());

        thread1 = new Thread(() -> runValidator(lock1, lock2, client1));
        thread2 = new Thread(() -> runValidator(lock2, lock1, client2));
    }

    @Override
    public void run() {
        try {
            // Create directory
            log.info("LockExclusivityValidator started");
            thread1.start();
            thread2.start();
            autoLockThread1.start();
            autoLockThread2.start();
            while (!stop.get() && violationCount.get() == 0) {
                Thread.sleep(100);
            }
        } catch (InterruptedException e) {
            log.debug("LockExclusivityValidator interrupted");
        } catch (Throwable e) {
            log.error("LockExclusivityValidator encountered unexpected error", e);
            throw new RuntimeException(e);
        } finally {
            try {
                thread1.interrupt();
                thread2.interrupt();
                autoLockThread1.interrupt();
                autoLockThread2.interrupt();

                thread1.join();
                thread2.join();
                autoLockThread1.join();
                autoLockThread2.join();

                lock1.close();
                lock2.close();
                autoLock1.stop(new DtTime(5, TimeUnit.SECONDS));
                autoLock2.stop(new DtTime(5, TimeUnit.SECONDS));
                client1.stop(new DtTime(10, TimeUnit.SECONDS));
                client2.stop(new DtTime(10, TimeUnit.SECONDS));
            } catch (Exception e) {
                log.error("LockExclusivityValidator stop failed", e);
            }
            log.info("LockExclusivityValidator stopped");
        }
    }

    private void processAllowedEx(Exception e) throws InterruptedException {
        if (e instanceof IllegalArgumentException) {
            throw (IllegalArgumentException) e;
        }
        Throwable root = DtUtil.rootCause(e);
        if (root instanceof InterruptedException) {
            throw (InterruptedException) root;
        } else {
            Thread.sleep(500);
        }
        failureCount.incrementAndGet();
    }

    private void runValidator(DistributedLock myLock, DistributedLock otherLock, KvClient myClient) {
        try {
            while (!stop.get() && violationCount.get() == 0) {
                boolean locked;
                if (myLock.isHeldByCurrentClient()) {
                    locked = true;
                } else {
                    try {
                        locked = myLock.tryLock(lockLeaseMillis, 5000);
                    } catch (Exception e) {
                        processAllowedEx(e);
                        continue;
                    }
                }
                if (locked) {
                    try {
                        runCheckOnce("lockTestKey", myClient, myLock::isHeldByCurrentClient,
                                otherLock::isHeldByCurrentClient);
                    } finally {
                        try {
                            myLock.unlock();
                        } catch (Exception e) {
                            processAllowedEx(e);
                        }
                    }
                }
            }
        } catch (InterruptedException e) {
            log.debug("LockExclusivityValidator interrupted");
        } catch (Throwable e) {
            BugLog.log("LockExclusivityValidator encountered unexpected error", e);
            violationCount.incrementAndGet();
        }
    }

    private class AutoLockThread extends Thread implements AutoRenewalLockListener {
        private final KvClient myClient;
        private AutoRenewalLock myLock;
        private AutoRenewalLock otherLock;
        private final ReentrantLock reentrantLock = new ReentrantLock();
        private final Condition cond = reentrantLock.newCondition();

        private AutoLockThread(KvClient myClient) {
            this.myClient = myClient;
        }

        @Override
        public void onAcquired(AutoRenewalLock lock) {
            log.info("auto lock acquired: {}", lock);
            reentrantLock.lock();
            try {
                cond.signalAll();
            } finally {
                reentrantLock.unlock();
            }
        }

        @Override
        public void onLost(AutoRenewalLock lock) {
            log.info("auto lock lost: {}", lock);
            onAcquired(lock);
        }

        @Override
        public void run() {
            try {
                while (!stop.get() && violationCount.get() == 0) {
                    if (myLock.isHeldByCurrentClient()) {
                        runCheckOnce("autoLockTestKey", myClient, myLock::isHeldByCurrentClient,
                                otherLock::isHeldByCurrentClient);
                    } else {
                        reentrantLock.lock();
                        try {
                            cond.await();
                        } finally {
                            reentrantLock.unlock();
                        }
                    }
                }
            } catch (InterruptedException e) {
                log.debug("AutoLockThread interrupted");
            } catch (Throwable e) {
                BugLog.log("AutoLockThread encountered unexpected error", e);
                violationCount.incrementAndGet();
            }
        }
    }

    private void runCheckOnce(String key, KvClient client, Supplier<Boolean> heldBySelf, Supplier<Boolean> heldByOther)
            throws InterruptedException {
        byte[] keyBytes = key(key).getBytes(StandardCharsets.UTF_8);

        KvNode n;
        try {
            n = client.get(groupId, keyBytes);
        } catch (Exception e) {
            processAllowedEx(e);
            return;
        }
        ByteBuffer buf = ByteBuffer.allocate(8);
        long oldValue;
        if (n != null) {
            if (n.data == null || n.data.length != 8) {
                BugLog.log("invalid data length: {}", n.data == null ? 0 : n.data.length);
                violationCount.incrementAndGet();
                return;
            }
            buf.put(n.data);
            buf.flip();
            oldValue = buf.getLong();
        } else {
            oldValue = 0;
        }
        buf.clear();
        buf.putLong(oldValue + 1);
        if (!heldBySelf.get()) {
            return;
        }
        try {
            client.put(groupId, keyBytes, buf.array());
        } catch (Exception e) {
            processAllowedEx(e);
            return;
        }
        Random r = new Random();
        if (r.nextBoolean()) {
            try {
                Thread.sleep(r.nextInt(1000));
            } catch (InterruptedException e) {
                return;
            }
        }
        if (!heldBySelf.get()) {
            return;
        }
        try {
            n = client.get(groupId, keyBytes);
        } catch (Exception e) {
            processAllowedEx(e);
            return;
        }
        if (n == null) {
            BugLog.log("can't get node written before");
            violationCount.incrementAndGet();
            return;
        }
        if (n.data == null || n.data.length != 8) {
            BugLog.log("invalid data length: {}", n.data == null ? 0 : n.data.length);
            violationCount.incrementAndGet();
            return;
        }
        buf.clear();
        buf.put(n.data);
        buf.flip();
        long v = buf.getLong();
        if (v != oldValue + 1) {
            if (heldBySelf.get()) {
                BugLog.log("lock held by self, but value not match. heldByOther={}, oldValue={}, value={}",
                        heldByOther.get(), oldValue, v);
                violationCount.incrementAndGet();
            }
            if (heldByOther.get()) {
                log.info("lock held by other client");
                failureCount.incrementAndGet();
            }
            return;
        }
        verifyCount.incrementAndGet();

    }

    private String key(String k) {
        return PREFIX + "." + k + validatorId;
    }
}
