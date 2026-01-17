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

import com.github.dtprj.dongting.dtkv.DistributedLock;
import com.github.dtprj.dongting.dtkv.KvClient;
import com.github.dtprj.dongting.dtkv.KvNode;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Validator for distributed lock exclusivity.
 * Verifies that two clients cannot hold the same lock simultaneously.
 *
 * @author huangli
 */
public class StressLockValidator implements Runnable {
    private static final DtLog log = DtLogs.getLogger(StressLockValidator.class);
    private static final String PREFIX = "StressIT.Lock";

    private final int pairId;
    private final int clientId; // 0 or 1 within the pair
    private final int groupId;
    private final long lockLeaseMillis;
    private final KvClient client;
    private final CountDownLatch startLatch;
    private final AtomicLong verifyCount;
    private final AtomicLong conflictCount;
    private final AtomicLong failureCount;
    private final AtomicBoolean stop;

    private final Random random = new Random();

    public StressLockValidator(int pairId, int clientId, int groupId, long lockLeaseMillis,
                               KvClient client, CountDownLatch startLatch,
                               AtomicLong verifyCount, AtomicLong conflictCount,
                               AtomicLong failureCount, AtomicBoolean stop) {
        this.pairId = pairId;
        this.clientId = clientId;
        this.groupId = groupId;
        this.lockLeaseMillis = lockLeaseMillis;
        this.client = client;
        this.startLatch = startLatch;
        this.verifyCount = verifyCount;
        this.conflictCount = conflictCount;
        this.failureCount = failureCount;
        this.stop = stop;
    }

    @Override
    public void run() {
        try {
            // Create directory
            byte[] dirKey = makeLockKey("dir");
            client.mkdir(groupId, dirKey);

            startLatch.countDown();
            log.info("LockExclusivityValidator-{}-{} started", pairId, clientId);

            while (!stop.get()) {
                try {
                    runOneLockVerification();
                } catch (Exception e) {
                    log.debug("LockExclusivityValidator-{}-{} operation failed (expected): {}",
                            pairId, clientId, e.getMessage());
                    failureCount.incrementAndGet();
                }

                // Sleep between lock attempts
                Thread.sleep(50);
            }

            log.info("LockExclusivityValidator-{}-{} stopped", pairId, clientId);
        } catch (Exception e) {
            log.error("LockExclusivityValidator-{}-{} encountered unexpected error", pairId, clientId, e);
            throw new RuntimeException(e);
        }
    }

    private void runOneLockVerification() {
        byte[] lockKey = makeLockKey("lock");
        byte[] counterKey = makeLockKey("counter");

        DistributedLock lock = null;
        try {
            lock = client.createLock(groupId, lockKey);

            // Try to acquire lock immediately (no wait)
            boolean acquired = lock.tryLock(lockLeaseMillis, 0);

            if (acquired) {
                verifyCount.incrementAndGet();
                log.debug("LockExclusivityValidator-{}-{} acquired lock", pairId, clientId);

                // Check if lock is still held
                if (!lock.isHeldByCurrentClient()) {
                    log.warn("LockExclusivityValidator-{}-{} lock expired immediately after acquisition",
                            pairId, clientId);
                    return;
                }

                // Write a unique identifier to prove we hold the lock
                long timestamp = System.currentTimeMillis();
                byte[] value = encodeCounterValue(clientId, timestamp);

                try {
                    client.put(groupId, counterKey, value);
                } catch (Exception e) {
                    log.debug("LockExclusivityValidator-{}-{} put failed while holding lock: {}",
                            pairId, clientId, e.getMessage());
                    failureCount.incrementAndGet();
                    return;
                }

                // Check lock is still held
                if (!lock.isHeldByCurrentClient()) {
                    log.warn("LockExclusivityValidator-{}-{} lock expired during operation (expected in extreme stress)",
                            pairId, clientId);
                    return;
                }

                // Sleep a bit while holding the lock
                Thread.sleep(10);

                // Check lock again
                if (!lock.isHeldByCurrentClient()) {
                    log.warn("LockExclusivityValidator-{}-{} lock expired during sleep (expected in extreme stress)",
                            pairId, clientId);
                    return;
                }

                // Read back and verify it's our value
                try {
                    KvNode node = client.get(groupId, counterKey);
                    if (node != null) {
                        int readClientId = decodeClientId(node.data);
                        long readTimestamp = decodeTimestamp(node.data);

                        if (readClientId != clientId || readTimestamp != timestamp) {
                            log.error("LockExclusivityValidator-{}-{} VIOLATION: expected clientId={} timestamp={}, " +
                                            "but read clientId={} timestamp={}",
                                    pairId, clientId, clientId, timestamp, readClientId, readTimestamp);
                            conflictCount.incrementAndGet();
                        } else {
                            log.debug("LockExclusivityValidator-{}-{} verified exclusive access", pairId, clientId);
                        }
                    }
                } catch (Exception e) {
                    log.debug("LockExclusivityValidator-{}-{} get failed: {}", pairId, clientId, e.getMessage());
                    failureCount.incrementAndGet();
                }

            } else {
                // Failed to acquire lock - this is normal
                log.debug("LockExclusivityValidator-{}-{} failed to acquire lock (normal)", pairId, clientId);
            }

        } catch (Exception e) {
            log.debug("LockExclusivityValidator-{}-{} lock operation failed: {}", pairId, clientId, e.getMessage());
            failureCount.incrementAndGet();
        } finally {
            if (lock != null) {
                try {
                    if (lock.isHeldByCurrentClient()) {
                        lock.unlock();
                        log.debug("LockExclusivityValidator-{}-{} released lock", pairId, clientId);
                    }
                    lock.close();
                } catch (Exception e) {
                    log.debug("LockExclusivityValidator-{}-{} unlock/close failed: {}", pairId, clientId, e.getMessage());
                }
            }
        }
    }

    private byte[] makeLockKey(String suffix) {
        String key = PREFIX + "." + pairId + "." + suffix;
        return key.getBytes(StandardCharsets.UTF_8);
    }

    private byte[] encodeCounterValue(int clientId, long timestamp) {
        // Format: clientId(4 bytes) + timestamp(8 bytes)
        ByteBuffer buffer = ByteBuffer.allocate(12);
        buffer.putInt(clientId);
        buffer.putLong(timestamp);
        return buffer.array();
    }

    private int decodeClientId(byte[] data) {
        if (data.length < 4) {
            throw new IllegalArgumentException("Invalid counter value format");
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        return buffer.getInt();
    }

    private long decodeTimestamp(byte[] data) {
        if (data.length < 12) {
            throw new IllegalArgumentException("Invalid counter value format");
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.getInt(); // skip clientId
        return buffer.getLong();
    }
}
