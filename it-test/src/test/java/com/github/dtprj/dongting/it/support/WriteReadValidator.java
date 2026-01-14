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

import com.github.dtprj.dongting.dtkv.KvClient;
import com.github.dtprj.dongting.dtkv.KvNode;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Validator for write-then-read consistency (Read Your Writes).
 * Verifies that successfully written values can be read back and are at least as new as written.
 *
 * @author huangli
 */
public class WriteReadValidator implements Runnable {
    private static final DtLog log = DtLogs.getLogger(WriteReadValidator.class);
    private static final String PREFIX = "StressIT.WR";

    private final int threadId;
    private final int groupId;
    private final int keySpace;
    private final KvClient client;
    private final CountDownLatch startLatch;
    private final AtomicLong verifyCount;
    private final AtomicLong violationCount;
    private final AtomicLong failureCount;
    private final AtomicBoolean stop;

    private final ConcurrentHashMap<String, ValueWithVersion> writtenValues = new ConcurrentHashMap<>();
    private final Random random = new Random();
    private long versionCounter = 0;

    public static class ValueWithVersion {
        public final byte[] value;
        public final long version;

        public ValueWithVersion(byte[] value, long version) {
            this.value = value;
            this.version = version;
        }
    }

    public WriteReadValidator(int threadId, int groupId, int keySpace, KvClient client,
                              CountDownLatch startLatch, AtomicLong verifyCount,
                              AtomicLong violationCount, AtomicLong failureCount, AtomicBoolean stop) {
        this.threadId = threadId;
        this.groupId = groupId;
        this.keySpace = keySpace;
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
            // Create directory for this validator
            byte[] dirKey = makeKey("dir");
            client.mkdir(groupId, dirKey);

            startLatch.countDown();
            log.info("WriteReadValidator-{} started", threadId);

            while (!stop.get()) {
                try {
                    runOneVerification();
                } catch (Exception e) {
                    log.debug("WriteReadValidator-{} operation failed (expected): {}", threadId, e.getMessage());
                    failureCount.incrementAndGet();
                }

                // Sleep a bit to avoid overwhelming the system
                Thread.sleep(10);
            }

            log.info("WriteReadValidator-{} stopped", threadId);
        } catch (Exception e) {
            log.error("WriteReadValidator-{} encountered unexpected error", threadId, e);
            throw new RuntimeException(e);
        }
    }

    private void runOneVerification() {
        int keyIndex = random.nextInt(keySpace);
        byte[] key = makeKey(String.valueOf(keyIndex));

        // Generate value with version
        long version = ++versionCounter;
        byte[] value = encodeValue(threadId, version);

        // Write
        try {
            client.put(groupId, key, value);
        } catch (Exception e) {
            log.debug("WriteReadValidator-{} put failed: {}", threadId, e.getMessage());
            failureCount.incrementAndGet();
            return;
        }

        // Record written value
        String keyStr = new String(key, StandardCharsets.UTF_8);
        writtenValues.put(keyStr, new ValueWithVersion(value, version));

        verifyCount.incrementAndGet();

        // Read back
        try {
            KvNode node = client.get(groupId, key);
            if (node == null) {
                log.error("WriteReadValidator-{} VIOLATION: key {} not found after successful write", threadId, keyStr);
                violationCount.incrementAndGet();
                return;
            }

            // Decode and verify version
            long readVersion = decodeVersion(node.data);
            if (readVersion < version) {
                log.error("WriteReadValidator-{} VIOLATION: key {} read version {} < written version {}",
                        threadId, keyStr, readVersion, version);
                violationCount.incrementAndGet();
            } else {
                log.debug("WriteReadValidator-{} verified key {} with version {}", threadId, keyStr, readVersion);
            }
        } catch (Exception e) {
            log.debug("WriteReadValidator-{} get failed (allowed): {}", threadId, e.getMessage());
            failureCount.incrementAndGet();
        }
    }

    private byte[] makeKey(String suffix) {
        String key = PREFIX + "." + threadId + "." + suffix;
        return key.getBytes(StandardCharsets.UTF_8);
    }

    private byte[] encodeValue(int threadId, long version) {
        // Format: threadId(4 bytes) + version(8 bytes) + random data(16 bytes)
        ByteBuffer buffer = ByteBuffer.allocate(28);
        buffer.putInt(threadId);
        buffer.putLong(version);
        byte[] randomData = new byte[16];
        random.nextBytes(randomData);
        buffer.put(randomData);
        return buffer.array();
    }

    private long decodeVersion(byte[] data) {
        if (data.length < 12) {
            throw new IllegalArgumentException("Invalid value format");
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.getInt(); // skip threadId
        return buffer.getLong();
    }
}
