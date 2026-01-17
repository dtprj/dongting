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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Validator for transaction atomicity under distributed locks.
 * Verifies that multiple operations protected by a lock appear atomic to other clients.
 *
 * @author huangli
 */
public class StressTxValidator implements Runnable {
    private static final DtLog log = DtLogs.getLogger(StressTxValidator.class);
    private static final String PREFIX = "StressIT.Txn";

    private final int threadId;
    private final int groupId;
    private final int operationCount;
    private final long lockLeaseMillis;
    private final boolean isWriter;
    private final KvClient client;
    private final CountDownLatch startLatch;
    private final AtomicLong verifyCount;
    private final AtomicLong violationCount;
    private final AtomicLong failureCount;
    private final AtomicBoolean stop;

    private final Random random = new Random();
    private long txnIdCounter = 0;

    public StressTxValidator(int threadId, int groupId, int operationCount,
                             long lockLeaseMillis, boolean isWriter, KvClient client,
                             CountDownLatch startLatch, AtomicLong verifyCount,
                             AtomicLong violationCount, AtomicLong failureCount, AtomicBoolean stop) {
        this.threadId = threadId;
        this.groupId = groupId;
        this.operationCount = operationCount;
        this.lockLeaseMillis = lockLeaseMillis;
        this.isWriter = isWriter;
        this.client = client;
        this.startLatch = startLatch;
        this.verifyCount = verifyCount;
        this.violationCount = violationCount;
        this.failureCount = failureCount;
        this.stop = stop;
    }

    @Override
    public void run() {
        try {
            // Create directory
            byte[] dirKey = makeTxnKey("dir");
            client.mkdir(groupId, dirKey);

            startLatch.countDown();
            log.info("TransactionAtomicityValidator-{} ({}) started", threadId, isWriter ? "writer" : "reader");

            while (!stop.get()) {
                try {
                    if (isWriter) {
                        runWriteTransaction();
                    } else {
                        runReadTransaction();
                    }
                } catch (Exception e) {
                    log.debug("TransactionAtomicityValidator-{} operation failed (expected): {}",
                            threadId, e.getMessage());
                    failureCount.incrementAndGet();
                }

                // Sleep between transactions
                Thread.sleep(isWriter ? 50 : 100);
            }

            log.info("TransactionAtomicityValidator-{} ({}) stopped", threadId, isWriter ? "writer" : "reader");
        } catch (Exception e) {
            log.error("TransactionAtomicityValidator-{} encountered unexpected error", threadId, e);
            throw new RuntimeException(e);
        }
    }

    private void runWriteTransaction() {
        byte[] lockKey = makeTxnKey("lock");
        DistributedLock lock = null;

        try {
            lock = client.createLock(groupId, lockKey);

            // Try to acquire lock with short wait
            boolean acquired = lock.tryLock(lockLeaseMillis, 10000);
            if (!acquired) {
                log.debug("TransactionAtomicityValidator-{} failed to acquire lock for write", threadId);
                return;
            }

            verifyCount.incrementAndGet();
            log.debug("TransactionAtomicityValidator-{} acquired lock for write transaction", threadId);

            // Check lock is held
            if (!lock.isHeldByCurrentClient()) {
                log.warn("TransactionAtomicityValidator-{} lock expired after acquisition", threadId);
                return;
            }

            // Generate transaction ID
            long txnId = ++txnIdCounter;

            // Write multiple keys with the same transaction ID
            List<byte[]> keys = new ArrayList<>();
            for (int i = 0; i < operationCount; i++) {
                byte[] key = makeTxnKey("data." + i);
                keys.add(key);

                byte[] value = encodeTxnValue(threadId, txnId, i);

                try {
                    client.put(groupId, key, value);
                    log.debug("TransactionAtomicityValidator-{} wrote key {} with txnId {}", threadId, i, txnId);
                } catch (Exception e) {
                    log.warn("TransactionAtomicityValidator-{} put failed during transaction: {}", threadId, e.getMessage());
                    failureCount.incrementAndGet();
                    // Continue with partial write - we'll detect if lock expired
                }

                // Check lock after each operation
                if (!lock.isHeldByCurrentClient()) {
                    log.warn("TransactionAtomicityValidator-{} lock expired during transaction (partial write is expected)",
                            threadId);
                    return;
                }
            }

            log.debug("TransactionAtomicityValidator-{} completed write transaction with txnId {}", threadId, txnId);

        } catch (Exception e) {
            log.debug("TransactionAtomicityValidator-{} write transaction failed: {}", threadId, e.getMessage());
            failureCount.incrementAndGet();
        } finally {
            if (lock != null) {
                try {
                    if (lock.isHeldByCurrentClient()) {
                        lock.unlock();
                    }
                    lock.close();
                } catch (Exception e) {
                    log.debug("TransactionAtomicityValidator-{} unlock/close failed: {}", threadId, e.getMessage());
                }
            }
        }
    }

    private void runReadTransaction() {
        byte[] lockKey = makeTxnKey("lock");
        DistributedLock lock = null;

        try {
            lock = client.createLock(groupId, lockKey);

            // Try to acquire lock with longer wait for reading
            boolean acquired = lock.tryLock(lockLeaseMillis, 20000);
            if (!acquired) {
                log.debug("TransactionAtomicityValidator-{} failed to acquire lock for read", threadId);
                return;
            }

            verifyCount.incrementAndGet();
            log.debug("TransactionAtomicityValidator-{} acquired lock for read transaction", threadId);

            // Read all keys
            List<byte[]> keys = new ArrayList<>();
            for (int i = 0; i < operationCount; i++) {
                byte[] key = makeTxnKey("data." + i);
                keys.add(key);
            }

            try {
                List<KvNode> nodes = client.batchGet(groupId, keys);

                // Check atomicity: all keys should have the same txnId or all be null
                Long firstTxnId = null;
                int nullCount = 0;
                int nonNullCount = 0;

                for (int i = 0; i < nodes.size(); i++) {
                    KvNode node = nodes.get(i);
                    if (node == null) {
                        nullCount++;
                    } else {
                        nonNullCount++;
                        long txnId = decodeTxnId(node.data);
                        if (firstTxnId == null) {
                            firstTxnId = txnId;
                        } else if (firstTxnId != txnId) {
                            log.error("TransactionAtomicityValidator-{} VIOLATION: inconsistent txnIds in transaction, " +
                                            "first={} current={} at index={}",
                                    threadId, firstTxnId, txnId, i);
                            violationCount.incrementAndGet();
                            return;
                        }
                    }
                }

                // It's OK if all are null (no transaction yet) or all have same txnId
                // Partial nulls might indicate a transaction in progress or lock expired during write
                if (nullCount > 0 && nonNullCount > 0) {
                    log.debug("TransactionAtomicityValidator-{} read partial transaction: {} nulls, {} non-nulls " +
                                    "(this can happen if writer's lock expired)",
                            threadId, nullCount, nonNullCount);
                } else {
                    log.debug("TransactionAtomicityValidator-{} verified atomic transaction with txnId {}",
                            threadId, firstTxnId);
                }

            } catch (Exception e) {
                log.debug("TransactionAtomicityValidator-{} batchGet failed: {}", threadId, e.getMessage());
                failureCount.incrementAndGet();
            }

        } catch (Exception e) {
            log.debug("TransactionAtomicityValidator-{} read transaction failed: {}", threadId, e.getMessage());
            failureCount.incrementAndGet();
        } finally {
            if (lock != null) {
                try {
                    if (lock.isHeldByCurrentClient()) {
                        lock.unlock();
                    }
                    lock.close();
                } catch (Exception e) {
                    log.debug("TransactionAtomicityValidator-{} unlock/close failed: {}", threadId, e.getMessage());
                }
            }
        }
    }

    private byte[] makeTxnKey(String suffix) {
        String key = PREFIX + "." + threadId + "." + suffix;
        return key.getBytes(StandardCharsets.UTF_8);
    }

    private byte[] encodeTxnValue(int threadId, long txnId, int operationIndex) {
        // Format: threadId(4 bytes) + txnId(8 bytes) + operationIndex(4 bytes)
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putInt(threadId);
        buffer.putLong(txnId);
        buffer.putInt(operationIndex);
        return buffer.array();
    }

    private long decodeTxnId(byte[] data) {
        if (data.length < 12) {
            throw new IllegalArgumentException("Invalid txn value format");
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.getInt(); // skip threadId
        return buffer.getLong();
    }
}
