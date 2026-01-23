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
import com.github.dtprj.dongting.common.FutureCallback;
import com.github.dtprj.dongting.dtkv.DistributedLock;
import com.github.dtprj.dongting.dtkv.KvClient;
import com.github.dtprj.dongting.dtkv.KvClientConfig;
import com.github.dtprj.dongting.dtkv.KvNode;
import com.github.dtprj.dongting.dtkv.KvResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.raft.RaftClientConfig;
import com.github.dtprj.dongting.test.WaitUtil;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.github.dtprj.dongting.test.Tick.tick;
import static org.junit.jupiter.api.Assertions.*;

/**
 * DtKV functional tests for integration testing.
 * This class provides test methods for various DtKV features using KvClient.
 *
 * @author huangli
 */
public class DtKvValidator {
    private static final DtLog log = DtLogs.getLogger(DtKvValidator.class);

    private final int groupId;
    private final String serversStr;
    private final long rpcTimeoutMillis;
    private final long watchHeartbeatMillis;

    private static final String PREFIX = DtKvValidator.class.getSimpleName();

    private KvClient client;
    private KvClient client2;


    public DtKvValidator(int groupId, String serversStr) {
        this.groupId = groupId;
        this.serversStr = serversStr;
        this.rpcTimeoutMillis = 0;
        this.watchHeartbeatMillis = 0;
    }

    public DtKvValidator(int groupId, String serversStr, long rpcTimeoutMillis, long watchHeartbeatMillis) {
        this.groupId = groupId;
        this.serversStr = serversStr;
        this.rpcTimeoutMillis = rpcTimeoutMillis;
        this.watchHeartbeatMillis = watchHeartbeatMillis;
    }

    public void start() {
        client = createClient(rpcTimeoutMillis, watchHeartbeatMillis);
        client2 = createClient(rpcTimeoutMillis, watchHeartbeatMillis);
    }

    private KvClient createClient(long rpcTimeoutMillis, long watchHeartbeatMillis) {
        RaftClientConfig raftClientConfig = new RaftClientConfig();
        if (rpcTimeoutMillis > 0) {
            raftClientConfig.rpcTimeoutMillis = rpcTimeoutMillis;
        }

        KvClientConfig kvClientConfig = new KvClientConfig();
        if (watchHeartbeatMillis > 0) {
            kvClientConfig.watchHeartbeatMillis = watchHeartbeatMillis;
        }

        KvClient client = new KvClient(kvClientConfig, raftClientConfig, new NioClientConfig("DtKvValidatorClient"));

        client.start();

        client.getRaftClient().clientAddNode(serversStr);
        client.getRaftClient().clientAddOrUpdateGroup(groupId, new int[]{1, 2, 3});
        client.mkdir(groupId, PREFIX.getBytes(StandardCharsets.UTF_8));
        return client;
    }

    public void stop() {
        stopClient(client);
        stopClient(client2);
    }

    private void stopClient(KvClient client) {
        if (client != null) {
            try {
                client.stop(new DtTime(5, TimeUnit.SECONDS));
            } catch (Throwable e) {
                log.warn("Error stopping client", e);
            }
        }
    }

    private byte[] key(String str) {
        return (PREFIX + "." + str).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Test basic KV operations: put, get, remove
     */
    private void testBasicKvOperations() {
        log.debug("=== Testing basic KV operations ===");

        // Test put and get
        byte[] key1 = key("key1");
        byte[] value1 = "value1".getBytes(StandardCharsets.UTF_8);
        client.put(groupId, key1, value1);

        KvNode node = client.get(groupId, key1);
        assertNotNull(node, "Node should exist after put");
        assertArrayEquals(value1, node.data, "Value should match");

        // Test get non-existent key
        KvNode node2 = client.get(groupId, key("nonexistent"));
        assertNull(node2, "Non-existent key should return null");

        // Test remove
        client.remove(groupId, key1);
        KvNode node3 = client.get(groupId, key1);
        assertNull(node3, "Key should be null after remove");

        log.debug("Basic KV operations test passed");
    }

    /**
     * Test directory operations: mkdir, list
     */
    private void testDirectoryOperations() {
        log.debug("=== Testing directory operations ===");

        // Test mkdir dir first (first level directory)
        byte[] dir1Path = key("dir");
        client.mkdir(groupId, dir1Path);

        // Test mkdir b.dir.subDir1 (second level directory)
        byte[] dir2Path = key("dir.subDir1");
        client.mkdir(groupId, dir2Path);

        // Test list root
        List<KvResult> rootList = client.list(groupId, key("dir"));
        assertTrue(rootList.stream().anyMatch(r -> new String(r.getKeyInDir().getData()).equals("subDir1")),
                "Directory should contain subDir1");

        // Test put in directory
        byte[] key1 = key("dir.subDir1.key1");
        byte[] value1 = "value1".getBytes(StandardCharsets.UTF_8);
        client.put(groupId, key1, value1);

        byte[] key2 = key("dir.subDir1.key2");
        byte[] value2 = "value2".getBytes(StandardCharsets.UTF_8);
        client.put(groupId, key2, value2);

        // Test list directory
        List<KvResult> dirList = client.list(groupId, dir2Path);
        assertEquals(2, dirList.size(), "Directory should contain 2 keys");
        assertTrue(dirList.stream().anyMatch(r -> new String(r.getKeyInDir().getData()).equals("key1")),
                "Should contain key1");
        assertTrue(dirList.stream().anyMatch(r -> new String(r.getKeyInDir().getData()).equals("key2")),
                "Should contain key2");

        log.debug("Directory operations test passed");
    }

    /**
     * Test temporary node: putTemp and automatic expiration
     */
    private void testTemporaryNode(long tempTtlMillis) {
        log.debug("=== Testing temporary node ===");


        // Test putTemp
        byte[] tempKey = key("tempKey1");
        byte[] value = "tempValue".getBytes(StandardCharsets.UTF_8);
        long ttl = tick(tempTtlMillis);
        client.putTemp(groupId, tempKey, value, ttl);

        // Get immediately should succeed
        KvNode node = client.get(groupId, tempKey);
        assertNotNull(node, "Temporary node should exist immediately after putTemp");
        assertArrayEquals(value, node.data, "Value should match");

        WaitUtil.waitUtil(() -> client.get(groupId, tempKey) == null, 10000);

        log.debug("Temporary node test passed");
    }

    /**
     * Test batch operations: batchPut, batchGet, batchRemove
     */
    private void testBatchOperations() {
        log.debug("=== Testing batch operations ===");

        // Prepare test data
        byte[] key1 = key("batchKey1");
        byte[] value1 = "value1".getBytes(StandardCharsets.UTF_8);
        byte[] key2 = key("batchKey2");
        byte[] value2 = "value2".getBytes(StandardCharsets.UTF_8);
        byte[] key3 = key("batchKey3");
        byte[] value3 = "value3".getBytes(StandardCharsets.UTF_8);

        List<byte[]> keys = Arrays.asList(key1, key2, key3);
        List<byte[]> values = Arrays.asList(value1, value2, value3);

        // Test batchPut
        List<KvResult> putResults = client.batchPut(groupId, keys, values);
        assertEquals(3, putResults.size(), "Should return 3 results");
        for (KvResult r : putResults) {
            assertEquals(0, r.getBizCode(), "All puts should succeed");
        }

        // Test batchGet
        List<KvNode> getResults = client.batchGet(groupId, keys);
        assertEquals(3, getResults.size(), "Should return 3 results");
        assertArrayEquals(value1, getResults.get(0).data, "First value should match");
        assertArrayEquals(value2, getResults.get(1).data, "Second value should match");
        assertArrayEquals(value3, getResults.get(2).data, "Third value should match");

        // Test batchRemove
        List<KvResult> removeResults = client.batchRemove(groupId, keys);
        assertEquals(3, removeResults.size(), "Should return 3 results");

        // Verify keys are removed
        List<KvNode> getAfterRemove = client.batchGet(groupId, keys);
        for (KvNode node : getAfterRemove) {
            assertNull(node, "All keys should be null after batchRemove");
        }

        log.debug("Batch operations test passed");
    }

    /**
     * Test compare and set (CAS) operation
     */
    private void testCompareAndSet() {
        log.debug("=== Testing CAS operation ===");

        byte[] key = key("casKey1");
        byte[] value1 = "value1".getBytes(StandardCharsets.UTF_8);
        byte[] value2 = "value2".getBytes(StandardCharsets.UTF_8);
        byte[] value3 = "value3".getBytes(StandardCharsets.UTF_8);

        // CAS when key doesn't exist (expect null)
        boolean result1 = client.compareAndSet(groupId, key, null, value1);
        assertTrue(result1, "CAS should succeed when key doesn't exist");

        // Verify value
        KvNode node1 = client.get(groupId, key);
        assertArrayEquals(value1, node1.data, "Value should be value1");

        // CAS with correct expected value
        boolean result2 = client.compareAndSet(groupId, key, value1, value2);
        assertTrue(result2, "CAS should succeed with correct expected value");

        // Verify value updated
        KvNode node2 = client.get(groupId, key);
        assertArrayEquals(value2, node2.data, "Value should be value2");

        // CAS with wrong expected value
        boolean result3 = client.compareAndSet(groupId, key, value1, value3);
        assertFalse(result3, "CAS should fail with wrong expected value");

        // Verify value not changed
        KvNode node3 = client.get(groupId, key);
        assertArrayEquals(value2, node3.data, "Value should still be value2");

        // CAS to delete (set new value to null/empty)
        boolean result4 = client.compareAndSet(groupId, key, value2, new byte[0]);
        assertTrue(result4, "CAS should succeed to delete key");

        // Verify key deleted
        KvNode node4 = client.get(groupId, key);
        assertNull(node4, "Key should be deleted after CAS with null");

        log.debug("CAS operation test passed");
    }

    /**
     * Test distributed lock with two clients competing for same lock
     */
    private void testDistributedLock(long shortLeaseMillis) {
        log.debug("=== Testing distributed lock ===");
        final byte[] lockKey = key("lockKey1");
        final long leaseMillis = tick(shortLeaseMillis);

        DistributedLock lock1 = null;
        DistributedLock lock2 = null;
        try {
            lock1 = client.createLock(groupId, lockKey);
            lock2 = client2.createLock(groupId, lockKey);

            // Client1 acquires lock
            boolean acquired1 = lock1.tryLock(leaseMillis, 0);
            assertTrue(acquired1, "Client1 should acquire lock successfully");
            assertTrue(lock1.isHeldByCurrentClient(), "Client1 should hold the lock");

            // Client2 tries to acquire immediately, should fail
            boolean acquired2 = lock2.tryLock(leaseMillis, 0);
            assertFalse(acquired2, "Client2 should fail to acquire lock immediately");
            assertFalse(lock2.isHeldByCurrentClient(), "Client2 should not hold the lock");

            // Client1 releases lock
            lock1.unlock();
            assertFalse(lock1.isHeldByCurrentClient(), "Client1 should not hold lock after unlock");

            // Client2 acquires lock
            boolean acquired3 = lock2.tryLock(leaseMillis, 0);
            assertTrue(acquired3, "Client2 should acquire lock after client1 releases");
            assertTrue(lock2.isHeldByCurrentClient(), "Client2 should hold the lock");

            // Client1 tries again, should fail
            boolean acquired4 = lock1.tryLock(leaseMillis, 0);
            assertFalse(acquired4, "Client1 should fail to acquire lock when client2 holds it");

            // Client2 releases lock
            lock2.unlock();
            assertFalse(lock2.isHeldByCurrentClient(), "Client2 should not hold lock after unlock");

            log.debug("Distributed lock test passed");
        } finally {
            if (lock1 != null) {
                lock1.close();
            }
            if (lock2 != null) {
                lock2.close();
            }
        }
    }

    /**
     * Test distributed lock with wait timeout: when one client releases lock,
     * another waiting client should acquire it immediately.
     */
    private void testDistributedLockWithWait() {
        log.debug("=== Testing distributed lock with wait ===");
        final byte[] lockKey = key("lockKey2");
        final long leaseMillis = 10000;
        final long waitTimeoutMillis = 1000;

        DistributedLock lock1 = null;
        DistributedLock lock2 = null;
        try {
            lock1 = client.createLock(groupId, lockKey);
            lock2 = client2.createLock(groupId, lockKey);

            // Client1 acquires lock
            boolean acquired1 = lock1.tryLock(leaseMillis, 0);
            assertTrue(acquired1, "Client1 should acquire lock successfully");

            // Client2 tries to acquire with wait timeout (async to avoid blocking main thread)
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            long acquireStartNanos = System.nanoTime();
            lock2.tryLock(leaseMillis, waitTimeoutMillis, FutureCallback.fromFuture(future));

            // Client1 releases lock
            lock1.unlock();

            // Client2 should acquire lock quickly (not waiting until timeout)
            Boolean acquired2 = future.get(waitTimeoutMillis, TimeUnit.MILLISECONDS);

            assertTrue(acquired2, "Client2 should acquire lock after client1 releases");
            assertTrue(lock2.isHeldByCurrentClient(), "Client2 should hold the lock");

            // Verify that client2 acquired lock quickly (much less than waitTimeoutMillis)
            long waitTime = (System.nanoTime() - acquireStartNanos) / 1000 / 1000;
            log.debug("Client2 waited {}ms to acquire lock", waitTime);
            assertTrue(waitTime < waitTimeoutMillis,
                    "Client2 should acquire lock quickly, actual wait: " + waitTime + "ms");

            // Cleanup
            lock2.unlock();

            log.debug("Distributed lock with wait test passed");
        } catch (Throwable e) {
            throw new RuntimeException(e);
        } finally {
            if (lock1 != null) {
                lock1.close();
            }
            if (lock2 != null) {
                lock2.close();
            }
        }
    }

    /**
     * Run all DtKV functional tests
     */
    public void runAllTests(long shortTimeoutMillis) {
        testBasicKvOperations();
        testDirectoryOperations();
        testTemporaryNode(shortTimeoutMillis);
        testBatchOperations();
        testCompareAndSet();
        testDistributedLock(shortTimeoutMillis);
        testDistributedLockWithWait();
    }
}
