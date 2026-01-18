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
import com.github.dtprj.dongting.dtkv.KvClient;
import com.github.dtprj.dongting.dtkv.KvNode;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Validator for write-then-read consistency (Read Your Writes).
 * Verifies that successfully written values can be read back and are at least as new as written.
 *
 * @author huangli
 */
public class StressRwValidator implements Runnable {
    private static final DtLog log = DtLogs.getLogger(StressRwValidator.class);
    public static final String PREFIX = "StressIT_WR";

    private final int threadId;
    private final int groupId;
    private final int keySpace;
    private final KvClient client;
    private final AtomicLong verifyCount;
    private final AtomicLong violationCount;
    private final AtomicLong failureCount;
    private final AtomicBoolean stop;

    private final Random random = new Random();

    public StressRwValidator(int threadId, int groupId, int keySpace, Function<String, KvClient> clientFactory,
                             AtomicLong verifyCount, AtomicLong violationCount, AtomicLong failureCount,
                             AtomicBoolean stop) {
        this.threadId = threadId;
        this.groupId = groupId;
        this.keySpace = keySpace;
        this.client = clientFactory.apply("StressRwValidator" + threadId);
        this.verifyCount = verifyCount;
        this.violationCount = violationCount;
        this.failureCount = failureCount;
        this.stop = stop;

        // Create directory for this validator
        client.mkdir(groupId, PREFIX.getBytes(StandardCharsets.UTF_8));
        client.mkdir(groupId, (PREFIX + "." + threadId).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void run() {
        try {
            log.info("WriteReadValidator-{} started", threadId);
            while (!stop.get()) {
                runOneVerification();
                if (!stop.get()) {
                    Thread.sleep(10);
                }
            }
            log.info("WriteReadValidator-{} stopped", threadId);
        } catch (InterruptedException e) {
            log.debug("WriteReadValidator-{} interrupted", threadId);
        } catch (Throwable e) {
            log.error("WriteReadValidator-{} encountered unexpected error", threadId, e);
            throw new RuntimeException(e);
        } finally {
            try {
                client.stop(new DtTime(10, TimeUnit.SECONDS));
            } catch (Exception e) {
                log.error("WriteReadValidator-{} stop failed", threadId, e);
            }
        }
    }

    private void processAllowedEx(Exception e) {
        Throwable root = DtUtil.rootCause(e);
        if (root instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        failureCount.incrementAndGet();
    }

    private void runOneVerification() {
        int keyIndex = random.nextInt(keySpace);
        byte[] key = makeKey(String.valueOf(keyIndex));

        // Generate value with version
        byte[] value = new byte[random.nextInt(2048)];
        random.nextBytes(value);

        // Write
        try {
            client.put(groupId, key, value);
        } catch (Exception e) {
            processAllowedEx(e);
            return;
        }

        // Record written value
        String keyStr = new String(key, StandardCharsets.UTF_8);

        verifyCount.incrementAndGet();

        // Read back
        KvNode node;
        try {
            node = client.get(groupId, key);
        } catch (Exception e) {
            processAllowedEx(e);
            return;
        }
        if (node == null) {
            log.error("WriteReadValidator-{} VIOLATION: key {} not found after successful write", threadId, keyStr);
            violationCount.incrementAndGet();
            return;
        }
        if (node.data == null) {
            log.error("WriteReadValidator-{} VIOLATION: key {} found but data is null after successful write", threadId, keyStr);
            violationCount.incrementAndGet();
            return;
        }
        if (!Arrays.equals(value, node.data)) {
            log.error("WriteReadValidator-{} VIOLATION: key {} found but data is different after successful write", threadId, keyStr);
            violationCount.incrementAndGet();
        }
    }

    private byte[] makeKey(String suffix) {
        String key = PREFIX + "." + threadId + "." + suffix;
        return key.getBytes(StandardCharsets.UTF_8);
    }
}
