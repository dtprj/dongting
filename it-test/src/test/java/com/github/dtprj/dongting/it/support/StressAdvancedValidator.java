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
import com.github.dtprj.dongting.common.FutureCallback;
import com.github.dtprj.dongting.dtkv.DistributedLock;
import com.github.dtprj.dongting.dtkv.KvClient;
import com.github.dtprj.dongting.dtkv.KvCodes;
import com.github.dtprj.dongting.dtkv.KvException;
import com.github.dtprj.dongting.dtkv.KvNode;
import com.github.dtprj.dongting.dtkv.KvResult;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.*;

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

    private long round = 1;

    public StressAdvancedValidator(int groupId, long lockLeaseMillis, BiFunction<String, UUID, KvClient> clientFactory,
                                   AtomicBoolean stop) {
        this.groupId = groupId;
        this.lockLeaseMillis = lockLeaseMillis;
        this.stop = stop;
        this.client = clientFactory.apply("StressAdvancedValidator", new UUID(4574397593475L, 1));
        client.getWatchManager().setListener(event -> verifyCount.incrementAndGet(), DtUtil.SCHEDULED_SERVICE);
        client.mkdir(groupId, PREFIX.getBytes());
    }

    @Override
    public void run() {
        try {
            log.info("StressAdvancedValidator started");
            clearResidualData();
            addWatchesForAllPossibleKeys();
            while (!stop.get() && violationCount.get() == 0) {
                runOnce();
                round++;
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

    private void clearResidualData() throws InterruptedException {
        // Delete from root using list, depth-first (children first, then parent)
        // Fault injection hasn't started, any error should fail the test
        clearDirRecursive(PREFIX.getBytes());
        // Note: root node (PREFIX) is not removed, it will be reused
        log.info("Residual data cleared");
    }

    /**
     * Recursively clear directory contents. Throws exception on any error.
     */
    private void clearDirRecursive(byte[] dirKey) throws InterruptedException {
        List<KvResult> children;
        try {
            children = client.list(groupId, dirKey);
        } catch (KvException e) {
            if (e.getCode() == KvCodes.NOT_FOUND || e.getCode() == KvCodes.PARENT_NOT_DIR) {
                // temp dir may expire
                return;
            }
            throw e;
        }

        for (KvResult child : children) {
            byte[] childKey = concatKey(dirKey, child.getKeyInDir().getData());
            boolean isDir = child.getNode().isDir();
            boolean isLock = (child.getNode().flag & KvNode.FLAG_LOCK_MASK) != 0;

            if (isDir && !isLock) {
                // Recursively clear subdirectory first (depth-first)
                clearDirRecursive(childKey);
            }

            // Now delete this child
            if (isLock) {
                log.debug("unlock... {}", new String(childKey));
                // For lock: createLock -> unlock -> close
                // Any failure will propagate and fail the test
                DistributedLock lock = client.createLock(groupId, childKey);
                CompletableFuture<Void> f = new CompletableFuture<>();
                lock.unlock(FutureCallback.fromFuture(f), true);
                try {
                    f.get(5, TimeUnit.SECONDS);
                } catch (ExecutionException | TimeoutException e) {
                    throw new RuntimeException(e);
                }
                lock.close();
            } else {
                log.debug("remove... {}", new String(childKey));
                client.remove(groupId, childKey);
            }
        }
    }

    private void addWatchesForAllPossibleKeys() {
        ArrayList<byte[]> allKeys = new ArrayList<>();
        generateKeys(PREFIX.getBytes(), 0, allKeys);
        client.getWatchManager().addWatch(groupId, allKeys.toArray(new byte[0][]));
        log.info("All possible keys added to watch");
    }

    private void generateKeys(byte[] key, int depth, ArrayList<byte[]> allKeys) {
        allKeys.add(key);
        if (depth < MAX_DEPTH) {
            for (int j = 0; j < MAX_CHILDREN; j++) {
                generateKeys(concatKey(key, String.valueOf(j).getBytes()), depth + 1, allKeys);
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
                    if (child.isDirOrTmpDir()) {
                        queue.add(child);
                    }
                } else {
                    // temp parent is expired
                    child.writeFail = true;
                    break;
                }
            }
        }

        check(root);

        ArrayDeque<TestNode> stack = new ArrayDeque<>();
        ArrayDeque<TestNode> output = new ArrayDeque<>();
        stack.push(root);
        while (!stack.isEmpty()) {
            TestNode current = stack.pop();
            output.push(current);
            if (current.isDirOrTmpDir()) {
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

    private void check(TestNode dir) throws InterruptedException {
        if (dir.writeFail) {
            return;
        }
        while (true) {
            List<KvResult> remoteList;
            List<byte[]> subFullKeys;
            List<KvNode> batchGetResults;
            try {
                KvNode n = client.get(groupId, dir.fullKey);
                assertTrue(n.isDir());
                assertEquals(0, (n.flag & KvNode.FLAG_LOCK_MASK));

                remoteList = client.list(groupId, dir.fullKey);
                assertEquals(dir.children.size(), remoteList.size());

                subFullKeys = dir.children.stream().map(node -> node.fullKey).collect(toList());
                batchGetResults = client.batchGet(groupId, subFullKeys);
                assertEquals(dir.children.size(), batchGetResults.size());
            } catch (Exception e) {
                if (processCreateEx(e, dir)) {
                    Thread.sleep(50);
                    continue;
                } else {
                    return;
                }
            }

            for (int i = 0; i < dir.children.size(); i++) {
                TestNode child = dir.children.get(i);
                if (child.writeFail) {
                    continue;
                }
                boolean shouldHasLockBit = child.type == TestNode.TYPE_LOCK;
                boolean shouldHasDirBit = child.isDirOrTmpDir() | shouldHasLockBit;
                KvNode nodeInList = remoteList.stream()
                        .filter(node -> Arrays.equals(concatKey(dir.fullKey, node.getKeyInDir().getData()), child.fullKey))
                        .findFirst()
                        .map(KvResult::getNode)
                        .orElse(null);
                String failMsg = "round " + round + " " + new String(child.fullKey) + " check fail";
                if (child.tempDirOrUnderTempDir) {
                    if (nodeInList != null) {
                        assertEquals(shouldHasDirBit, nodeInList.isDir(), failMsg);
                        assertEquals(shouldHasLockBit, (nodeInList.flag & KvNode.FLAG_LOCK_MASK) != 0, failMsg);
                    }
                } else {
                    assertNotNull(nodeInList);
                    assertEquals(shouldHasDirBit, nodeInList.isDir(), failMsg);
                    assertEquals(shouldHasLockBit, (nodeInList.flag & KvNode.FLAG_LOCK_MASK) != 0, failMsg);
                }

                KvNode nodeInBatchGet = batchGetResults.get(i);
                if (child.tempDirOrUnderTempDir) {
                    if (nodeInBatchGet != null) {
                        assertArrayEquals(child.fullKey, subFullKeys.get(i));
                        assertEquals(shouldHasDirBit, nodeInBatchGet.isDir(), failMsg);
                        assertEquals(shouldHasLockBit, (nodeInBatchGet.flag & KvNode.FLAG_LOCK_MASK) != 0, failMsg);
                    }
                } else {
                    assertNotNull(nodeInBatchGet);
                    assertArrayEquals(child.fullKey, subFullKeys.get(i));
                    assertEquals(shouldHasDirBit, nodeInBatchGet.isDir(), failMsg);
                    assertEquals(shouldHasLockBit, (nodeInBatchGet.flag & KvNode.FLAG_LOCK_MASK) != 0, failMsg);
                }

                if (child.isDirOrTmpDir() && !shouldHasLockBit) {
                    check(child);
                }
                return;
            }
        }
    }

    /**
     * return if it should retry.
     */
    private boolean processCreateEx(Exception e, TestNode child) throws InterruptedException {
        if (e instanceof IllegalArgumentException) {
            throw (IllegalArgumentException) e;
        }
        if (e instanceof KvException) {
            KvException kve = (KvException) e;
            if (child.tempDirOrUnderTempDir && (kve.getCode() == KvCodes.PARENT_DIR_NOT_EXISTS
                    || kve.getCode() == KvCodes.NOT_FOUND)) {
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
                        if (processCreateEx(e, child)) {
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
                        if (processCreateEx(e, child)) {
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
                        if (processCreateEx(e, child)) {
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
                        if (processCreateEx(e, child)) {
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
                        if (processCreateEx(e, child)) {
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

    private static byte[] concatKey(byte[] parent, byte[] child) {
        byte[] bs = new byte[parent.length + child.length + 1];
        System.arraycopy(parent, 0, bs, 0, parent.length);
        bs[parent.length] = '.';
        System.arraycopy(child, 0, bs, parent.length + 1, child.length);
        return bs;
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
            if (isDirOrTmpDir()) {
                children = new ArrayList<>();
            } else {
                children = null;
            }
        }

        public boolean isDirOrTmpDir() {
            return type == TYPE_DIR || type == TYPE_TEMP_DIR;
        }

        public TestNode createChild(int childIndex) {
            if (!isDirOrTmpDir()) {
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
