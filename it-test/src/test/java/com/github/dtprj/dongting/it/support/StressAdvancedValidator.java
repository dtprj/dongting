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
import com.github.dtprj.dongting.dtkv.DistributedLock;
import com.github.dtprj.dongting.dtkv.KvClient;
import com.github.dtprj.dongting.dtkv.KvCodes;
import com.github.dtprj.dongting.dtkv.KvException;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * @author huangli
 */
@SuppressWarnings("BusyWait")
public class StressAdvancedValidator implements Runnable {

    private static final DtLog log = DtLogs.getLogger(StressAdvancedValidator.class);

    private static final int MAX_DEPTH = 4;
    private static final int MAX_CHILDREN = 4;

    private final int groupId;
    private final long lockLeaseMillis;
    private final AtomicBoolean stop;
    private final KvClient client;

    private static final String PREFIX = "StressAdvancedValidator";

    public static final AtomicLong verifyCount = new AtomicLong();
    public static final AtomicLong violationCount = new AtomicLong();
    public static final AtomicLong failureCount = new AtomicLong();

    public StressAdvancedValidator(int groupId, long lockLeaseMillis, Function<String, KvClient> clientFactory,
                                   AtomicBoolean stop) {
        this.groupId = groupId;
        this.lockLeaseMillis = lockLeaseMillis;
        this.stop = stop;
        this.client = clientFactory.apply("StressAdvancedValidator");
        client.mkdir(groupId, PREFIX.getBytes());
    }

    @Override
    public void run() {
        try {
            log.info("StressAdvancedValidator started");
            while (!stop.get() && violationCount.get() == 0) {
                runOnce();
            }
            log.info("StressAdvancedValidator stopped");
        } catch (InterruptedException e) {
            log.debug("StressAdvancedValidator interrupted");
        } catch (Throwable e) {
            BugLog.getLog().error("StressAdvancedValidator encountered unexpected error", e);
            violationCount.incrementAndGet();
        } finally {
            try {
                client.stop(new DtTime(10, TimeUnit.SECONDS));
            } catch (Exception e) {
                log.error("StressAdvancedValidator stop failed", e);
            }
        }
    }

    private void runOnce() throws InterruptedException {
        TestNode root = new TestNode(TestNode.TYPE_DIR, 0, false, PREFIX.getBytes());
        ArrayDeque<TestNode> queue = new ArrayDeque<>();
        queue.add(root);
        while (!queue.isEmpty()) {
            TestNode p = queue.pop();
            if (p.depth >= MAX_DEPTH) {
                continue;
            }
            for (int i = 0; i < MAX_CHILDREN; i++) {
                TestNode child = p.createChild(i);
                if (remoteCreateChild(child)) {
                    verifyCount.incrementAndGet();
                    if (child.isDir()) {
                        queue.add(child);
                    }
                } else {
                    // temp parent is expired
                    child.writeFail = true;
                    break;
                }
            }
        }

        ArrayDeque<TestNode> stack = new ArrayDeque<>();
        ArrayDeque<TestNode> output = new ArrayDeque<>();
        stack.push(root);
        while (!stack.isEmpty()) {
            TestNode current = stack.pop();
            output.push(current);
            if (current.isDir()) {
                for (TestNode child : current.children) {
                    stack.push(child);
                }
            }
        }
        while (!output.isEmpty()) {
            TestNode current = output.pop();
            if (current != root) {
                remoteRemoveChild(current);
                verifyCount.incrementAndGet();
            }
        }
    }

    /**
     * return if it should retry.
     */
    private boolean processCreateEx(Exception e) throws InterruptedException {
        if(e instanceof IllegalArgumentException){
            throw (IllegalArgumentException) e;
        }
        if (e instanceof KvException) {
            KvException kve = (KvException) e;
            if (kve.getCode() == KvCodes.PARENT_DIR_NOT_EXISTS) {
                return false;
            } else {
                throw new RuntimeException(e);
            }
        }
        Throwable root = DtUtil.rootCause(e);
        if (root instanceof InterruptedException) {
            throw (InterruptedException) root;
        }
        // the server may fail, but the cluster should be ok eventually
        failureCount.incrementAndGet();
        return true;
    }

    private boolean remoteCreateChild(TestNode child) throws InterruptedException {
        while (true) {
            switch (child.type) {
                case TestNode.TYPE_DIR:
                    try {
                        client.mkdir(groupId, child.fullKey);
                        return true;
                    } catch (Exception e) {
                        if (processCreateEx(e)) {
                            Thread.sleep(50);
                            continue;
                        } else {
                            return false;
                        }
                    }
                case TestNode.TYPE_VALUE:
                    try {
                        client.put(groupId, child.fullKey, child.fullKey);
                        return true;
                    } catch (Exception e) {
                        if (processCreateEx(e)) {
                            Thread.sleep(50);
                            continue;
                        } else {
                            return false;
                        }
                    }
                case TestNode.TYPE_TEMP_DIR:
                    try {
                        client.makeTempDir(groupId, child.fullKey, lockLeaseMillis);
                        return true;
                    } catch (Exception e) {
                        if (processCreateEx(e)) {
                            Thread.sleep(50);
                            continue;
                        } else {
                            return false;
                        }
                    }
                case TestNode.TYPE_TEMP_VALUE:
                    try {
                        client.putTemp(groupId, child.fullKey, child.fullKey, lockLeaseMillis);
                        return true;
                    } catch (Exception e) {
                        if (processCreateEx(e)) {
                            Thread.sleep(50);
                            continue;
                        } else {
                            return false;
                        }
                    }
                case TestNode.TYPE_LOCK:
                    child.lock = client.createLock(groupId, child.fullKey);
                    try {
                        // ignore return value, the unlock operation is idempotent
                        child.lock.tryLock(lockLeaseMillis, 0);
                        return true;
                    } catch (Exception e) {
                        if (processCreateEx(e)) {
                            Thread.sleep(50);
                            continue;
                        } else {
                            return false;
                        }
                    }
                default:
                    throw new IllegalStateException();
            }
        }
    }

    private void remoteRemoveChild(TestNode child) throws InterruptedException {
        if (child.writeFail) {
            return;
        }
        while (true) {
            switch (child.type) {
                case TestNode.TYPE_DIR:
                case TestNode.TYPE_TEMP_DIR:
                case TestNode.TYPE_VALUE:
                case TestNode.TYPE_TEMP_VALUE:
                    try {
                        client.remove(groupId, child.fullKey);
                        return;
                    } catch (Exception e) {
                        Throwable root = DtUtil.rootCause(e);
                        if (root instanceof InterruptedException) {
                            throw (InterruptedException) root;
                        }
                        Thread.sleep(50);
                        failureCount.incrementAndGet();
                        continue;
                    }
                case TestNode.TYPE_LOCK:
                    try {
                        // the unlock operation is idempotent
                        child.lock.unlock();
                    } catch (Exception e) {
                        Throwable root = DtUtil.rootCause(e);
                        if (root instanceof InterruptedException) {
                            throw (InterruptedException) root;
                        }
                        Thread.sleep(50);
                        failureCount.incrementAndGet();
                        continue;
                    }
                    Assertions.assertFalse(child.lock.isHeldByCurrentClient());
                    child.lock.close(); // should not fail
                    return;
                default:
                    throw new IllegalStateException();

            }
        }
    }

    private static class TestNode {
        static final int TYPE_DIR = 1;
        static final int TYPE_VALUE = 2;
        static final int TYPE_TEMP_DIR = 3;
        static final int TYPE_TEMP_VALUE = 4;
        static final int TYPE_LOCK = 5;
        final int type;
        final int depth;
        final boolean tempDirOrUnderTempDir;
        final byte[] fullKey;

        DistributedLock lock;
        boolean writeFail;

        final ArrayList<TestNode> children;

        private static final Random RANDOM = new Random();

        private TestNode(int type, int depth, boolean tempDirOrUnderTempDir, byte[] fullKey) {
            this.type = type;
            this.depth = depth;
            this.tempDirOrUnderTempDir = tempDirOrUnderTempDir;
            this.fullKey = fullKey;
            if (isDir()) {
                children = new ArrayList<>();
            } else {
                children = null;
            }
        }

        public boolean isDir() {
            return type == TYPE_DIR || type == TYPE_TEMP_DIR;
        }

        private byte[] concatKey(byte[] parent, byte[] child) {
            byte[] bs = new byte[parent.length + child.length + 1];
            System.arraycopy(parent, 0, bs, 0, parent.length);
            bs[parent.length] = '.';
            System.arraycopy(child, 0, bs, parent.length + 1, child.length);
            return bs;
        }

        public TestNode createChild(int childIndex) {
            if (!isDir()) {
                throw new IllegalStateException("not a dir");
            }
            int subType = nextSubType();
            TestNode child = new TestNode(subType, depth + 1,
                    tempDirOrUnderTempDir || subType == TYPE_TEMP_DIR,
                    concatKey(fullKey, String.valueOf(childIndex).getBytes()));
            children.add(child);
            return child;
        }

        private int nextSubType() {
            int x = RANDOM.nextInt(100);
            if (depth >= MAX_DEPTH - 1) {
                if (x < 50) {
                    return TYPE_VALUE;
                } else if (x < 75) {
                    return TYPE_TEMP_VALUE;
                } else {
                    return tempDirOrUnderTempDir ? TYPE_VALUE : TYPE_LOCK;
                }
            } else {
                if (x < 60) {
                    return TYPE_DIR;
                } else if (x < 70) {
                    return TYPE_TEMP_DIR;
                } else if (x < 80) {
                    return TYPE_VALUE;
                } else if (x < 90) {
                    return TYPE_TEMP_VALUE;
                } else {
                    return tempDirOrUnderTempDir ? TYPE_VALUE : TYPE_LOCK;
                }
            }
        }
    }
}
