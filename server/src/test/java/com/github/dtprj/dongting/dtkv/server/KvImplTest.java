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

import com.github.dtprj.dongting.common.ByteArray;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.dtkv.KvCodes;
import com.github.dtprj.dongting.dtkv.KvResult;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.raft.sm.Snapshot;
import com.github.dtprj.dongting.raft.sm.SnapshotInfo;
import com.github.dtprj.dongting.raft.test.TestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
class KvImplTest {

    private KvImpl kv;
    private static Timestamp ts;
    private int ver;
    private final UUID selfUuid = UUID.randomUUID();

    @BeforeEach
    void setUp() {
        ver = 1;
        ts = new Timestamp();
        TtlManager tm = new TtlManager(ts, null);
        kv = new KvImpl(null, tm, ts, 0, 16, 0.75f);
    }

    static ByteArray ba(String str) {
        return new ByteArray(str.getBytes());
    }

    private void initOpContext(int bizType, long ttlMillis) {
        kv.opContext.init(bizType, selfUuid, ttlMillis, ts.wallClockMillis, ts.nanoTime);
    }

    private KvResult put(long index, ByteArray key, byte[] data) {
        initOpContext(DtKV.BIZ_TYPE_PUT, 0);
        return kv.put(index, key, data);
    }

    private KvResult mkdir(long index, ByteArray key) {
        initOpContext(DtKV.BIZ_TYPE_MKDIR, 0);
        return kv.mkdir(index, key);
    }

    private KvResult remove(long index, ByteArray key) {
        initOpContext(DtKV.BIZ_TYPE_REMOVE, 0);
        return kv.remove(index, key);
    }

    private Pair<Integer, List<KvResult>> batchRemove(long index, List<byte[]> keys) {
        initOpContext(DtKV.BIZ_TYPE_BATCH_REMOVE, 0);
        return kv.batchRemove(index, keys);
    }

    private Pair<Integer, List<KvResult>> batchPut(long index, List<byte[]> keys, List<byte[]> values) {
        initOpContext(DtKV.BIZ_TYPE_BATCH_PUT, 0);
        return kv.batchPut(index, keys, values);
    }

    private KvResult compareAndSet(long index, ByteArray key, byte[] expectedValue, byte[] newValue) {
        initOpContext(DtKV.BIZ_TYPE_CAS, 0);
        return kv.compareAndSet(index, key, expectedValue, newValue);
    }

    @Test
    void testGetPut() {
        KvResult r = kv.get(ba("key1"));
        assertEquals(KvCodes.NOT_FOUND, r.getBizCode());
        assertNull(r.getNode());

        assertEquals(KvCodes.SUCCESS, put(1, ba("key1"), "value1".getBytes()).getBizCode());
        r = kv.get(ba("key1"));
        assertEquals(KvCodes.SUCCESS, r.getBizCode());
        assertArrayEquals("value1".getBytes(), r.getNode().data);
        assertFalse(r.getNode().isDir);
        assertEquals(ts.getWallClockMillis(), r.getNode().createTime);
        assertEquals(ts.getWallClockMillis(), r.getNode().updateTime);
        assertEquals(1, r.getNode().createIndex);
        assertEquals(1, r.getNode().updateIndex);

        TestUtil.updateTimestamp(ts, ts.getNanoTime() + 1, ts.getWallClockMillis() + 1);
        assertEquals(KvCodes.SUCCESS_OVERWRITE, put(2, ba("key1"), "value2".getBytes()).getBizCode());
        r = kv.get(ba("key1"));
        assertEquals(KvCodes.SUCCESS, r.getBizCode());
        assertArrayEquals("value2".getBytes(), r.getNode().data);
        assertFalse(r.getNode().isDir);
        assertEquals(ts.getWallClockMillis() - 1, r.getNode().createTime);
        assertEquals(ts.getWallClockMillis(), r.getNode().updateTime);
        assertEquals(1, r.getNode().createIndex);
        assertEquals(2, r.getNode().updateIndex);

        assertEquals(KvCodes.VALUE_EXISTS, mkdir(3, ba("key1")).getBizCode());
    }

    @Test
    void testGetPutInDir() {
        mkdir(1, ba("parent"));
        assertEquals(KvCodes.NOT_FOUND, kv.get(ba("parent.key1")).getBizCode());

        TestUtil.updateTimestamp(ts, ts.getNanoTime() + 1, ts.getWallClockMillis() + 1);
        assertEquals(KvCodes.SUCCESS, put(2, ba("parent.key1"), "value1".getBytes()).getBizCode());
        assertEquals(KvCodes.PARENT_NOT_DIR, put(3, ba("parent.key1.key2"), "xxx".getBytes()).getBizCode());
        assertEquals(KvCodes.PARENT_DIR_NOT_EXISTS, put(3, ba("xxx.yyy"), "xxx".getBytes()).getBizCode());
        KvResult r = kv.get(ba("parent.key1"));
        assertEquals(KvCodes.SUCCESS, r.getBizCode());
        assertArrayEquals("value1".getBytes(), r.getNode().data);
        r = kv.get(ba("parent"));
        assertEquals(KvCodes.SUCCESS, r.getBizCode());
        assertTrue(r.getNode().isDir);
        assertEquals(ts.getWallClockMillis() - 1, r.getNode().createTime);
        assertEquals(ts.getWallClockMillis(), r.getNode().updateTime);
        assertEquals(1, r.getNode().createIndex);
        assertEquals(2, r.getNode().updateIndex);
        r = kv.get(ba(""));
        assertEquals(0, r.getNode().createTime);
        assertEquals(ts.getWallClockMillis(), r.getNode().updateTime);
        assertEquals(0, r.getNode().createIndex);
        assertEquals(2, r.getNode().updateIndex);
    }

    @Test
    void testRemove() {
        assertEquals(KvCodes.NOT_FOUND, remove(1, ba("key1")).getBizCode());
        put(2, ba("key1"), "value1".getBytes());
        assertEquals(KvCodes.SUCCESS, remove(3, ba("key1")).getBizCode());
        assertEquals(KvCodes.NOT_FOUND, kv.get(ba("key1")).getBizCode());
    }

    @Test
    void testRemoveInDir() {
        mkdir(1, ba("parent"));
        assertEquals(KvCodes.NOT_FOUND, remove(2, ba("parent.key1")).getBizCode());
        assertEquals(1, kv.get(ba("parent")).getNode().updateIndex);
        assertEquals(1, kv.get(ba("")).getNode().updateIndex);

        put(3, ba("parent.key1"), "value1".getBytes());
        TestUtil.updateTimestamp(ts, ts.getNanoTime() + 1, ts.getWallClockMillis() + 1);
        assertEquals(KvCodes.SUCCESS, remove(4, ba("parent.key1")).getBizCode());
        assertEquals(KvCodes.NOT_FOUND, kv.get(ba("parent.key1")).getBizCode());

        assertEquals(1, kv.get(ba("parent")).getNode().createIndex);
        assertEquals(4, kv.get(ba("parent")).getNode().updateIndex);
        assertEquals(ts.getWallClockMillis() - 1, kv.get(ba("parent")).getNode().createTime);
        assertEquals(ts.getWallClockMillis(), kv.get(ba("parent")).getNode().updateTime);

        assertEquals(0, kv.get(ba("")).getNode().createIndex);
        assertEquals(4, kv.get(ba("")).getNode().updateIndex);
        assertEquals(0, kv.get(ba("")).getNode().createTime);
        assertEquals(ts.getWallClockMillis(), kv.get(ba("")).getNode().updateTime);
    }

    @Test
    void testRemoveDir() {
        mkdir(1, ba("parent"));
        put(2, ba("parent.key1"), "value1".getBytes());
        put(3, ba("parent.key2"), "value2".getBytes());
        assertEquals(KvCodes.HAS_CHILDREN, remove(4, ba("parent")).getBizCode());
        remove(5, ba("parent.key1"));
        assertEquals(KvCodes.HAS_CHILDREN, remove(6, ba("parent")).getBizCode());
        remove(7, ba("parent.key2"));
        assertEquals(KvCodes.SUCCESS, remove(8, ba("parent")).getBizCode());
    }

    @Test
    void testList() {
        put(1, ba("key1"), "a".getBytes());
        put(2, ba("key2"), "b".getBytes());
        mkdir(3, ba("dir1"));
        Pair<Integer, List<KvResult>> list = kv.list(ba(""));
        assertEquals(KvCodes.SUCCESS, list.getLeft());
        assertEquals(3, list.getRight().size());
        remove(4, ba("key1"));
        list = kv.list(ba(""));
        assertEquals(2, list.getRight().size());
    }

    @Test
    void testList2() {
        assertEquals(KvCodes.INVALID_KEY, kv.list(ba("..")).getLeft());
        assertEquals(KvCodes.NOT_FOUND, kv.list(ba("aaa")).getLeft());
        mkdir(1, ba("dir1"));
        remove(2, ba("dir1"));
        assertEquals(KvCodes.NOT_FOUND, kv.list(ba("dir1")).getLeft());
        mkdir(3, ba("dir1"));
        takeSnapshot();
        remove(4, ba("dir1"));
        assertEquals(KvCodes.NOT_FOUND, kv.list(ba("dir1")).getLeft());
        put(5, ba("dir1"), "a".getBytes());
        assertEquals(KvCodes.PARENT_NOT_DIR, kv.list(ba("dir1")).getLeft());
    }

    @Test
    void testList3() {
        for (int i = 0; i < 11; i++) {
            put(i, ba("key" + i), ("value" + i).getBytes());
        }
        Pair<Integer, List<KvResult>> list = kv.list(ba(""));
        assertEquals(KvCodes.SUCCESS, list.getLeft());
        assertEquals(11, list.getRight().size());
    }

    @Test
    void testListInDir() {
        mkdir(1, ba("parent"));
        put(2, ba("parent.key1"), "a".getBytes());
        mkdir(3, ba("parent.dir1"));
        Pair<Integer, List<KvResult>> list = kv.list(ba("parent"));
        assertEquals(KvCodes.SUCCESS, list.getLeft());
        assertEquals(2, list.getRight().size());
    }

    @Test
    void testMkdir() {
        assertEquals(KvCodes.SUCCESS, mkdir(1, ba("dir1")).getBizCode());
        TestUtil.updateTimestamp(ts, ts.getNanoTime() + 1, ts.getWallClockMillis() + 1);
        assertEquals(KvCodes.DIR_EXISTS, mkdir(2, ba("dir1")).getBizCode());
        KvResult r = kv.get(ba("dir1"));
        assertEquals(KvCodes.SUCCESS, r.getBizCode());
        assertTrue(r.getNode().isDir);
        assertEquals(ts.getWallClockMillis() - 1, r.getNode().createTime);
        assertEquals(r.getNode().createTime, r.getNode().updateTime);
        assertEquals(1, r.getNode().createIndex);
        assertEquals(1, r.getNode().updateIndex);

        assertEquals(KvCodes.DIR_EXISTS, put(3, ba("dir1"), "value1".getBytes()).getBizCode());
    }

    @Test
    void testInvalidKeyValue() {
        assertEquals(KvCodes.SUCCESS, kv.get(ba("")).getBizCode());
        assertTrue(kv.get(ba("")).getNode().isDir);
        assertEquals(KvCodes.SUCCESS, kv.get(null).getBizCode());
        assertTrue(kv.get(null).getNode().isDir);

        assertEquals(KvCodes.INVALID_KEY, put(1, ba(""), "value1".getBytes()).getBizCode());
        assertEquals(KvCodes.INVALID_KEY, put(1, null, "value1".getBytes()).getBizCode());

        assertEquals(KvCodes.INVALID_KEY, remove(1, ba("")).getBizCode());
        assertEquals(KvCodes.INVALID_KEY, remove(1, null).getBizCode());

        assertEquals(KvCodes.INVALID_VALUE, put(1, ba("key1"), "".getBytes()).getBizCode());
        assertEquals(KvCodes.INVALID_VALUE, put(1, ba("key1"), null).getBizCode());

        assertEquals(KvCodes.INVALID_KEY, kv.get(ba(".")).getBizCode());
        assertEquals(KvCodes.INVALID_KEY, kv.get(ba("a.b.")).getBizCode());

        kv.maxKeySize = 5;
        kv.maxValueSize = 5;
        assertEquals(KvCodes.KEY_TOO_LONG, put(1, ba("123456"), "a".getBytes()).getBizCode());
        assertEquals(KvCodes.VALUE_TOO_LONG, put(1, ba("key1"), "123456".getBytes()).getBizCode());
    }

    private Snapshot takeSnapshot() {
        return takeSnapshot(kv);
    }

    static Snapshot takeSnapshot(KvImpl kv) {
        long lastIndex = kv.root.latest.updateIndex;
        SnapshotInfo si = new SnapshotInfo(lastIndex, 1, null, null,
                null, null, 0);
        Snapshot s = new Snapshot(si) {
            @Override
            public FiberFuture<Integer> readNext(ByteBuffer buffer) {
                return null;
            }

            @Override
            protected void doClose() {
                kv.closeSnapshot(this);
                Supplier<Boolean> gc = kv.createGcTask();
                //noinspection StatementWithEmptyBody
                while (gc.get()) {
                }
            }
        };
        kv.openSnapshot(s);
        return s;
    }

    @Test
    void testWithSnapshot() {
        mkdir(ver++, ba("parent"));
        put(ver++, ba("key1"), "a".getBytes());
        put(ver++, ba("key2"), "b".getBytes());
        put(ver++, ba("parent.key1"), "c".getBytes());
        takeSnapshot();
        assertEquals(KvCodes.SUCCESS, remove(ver++, ba("key1")).getBizCode());
        assertEquals(KvCodes.NOT_FOUND, remove(ver++, ba("key1")).getBizCode());
        assertEquals(KvCodes.NOT_FOUND, kv.get(ba("key1")).getBizCode());
        assertEquals(KvCodes.SUCCESS_OVERWRITE, put(ver++, ba("key2"), "b2".getBytes()).getBizCode());
        assertArrayEquals("b2".getBytes(), kv.get(ba("key2")).getNode().data);
        takeSnapshot();
        assertEquals(KvCodes.SUCCESS_OVERWRITE, put(ver++, ba("key2"), "b3".getBytes()).getBizCode());
        assertArrayEquals("b3".getBytes(), kv.get(ba("key2")).getNode().data);
        takeSnapshot();
        assertEquals(KvCodes.SUCCESS, remove(ver++, ba("key2")).getBizCode());
        assertEquals(KvCodes.SUCCESS, remove(ver++, ba("parent.key1")).getBizCode());
        assertEquals(KvCodes.SUCCESS, remove(ver++, ba("parent")).getBizCode());
        assertEquals(KvCodes.NOT_FOUND, kv.get(ba("key2")).getBizCode());
        assertEquals(KvCodes.NOT_FOUND, kv.get(ba("parent.key1")).getBizCode());
        assertEquals(KvCodes.NOT_FOUND, kv.get(ba("parent")).getBizCode());
        assertEquals(KvCodes.PARENT_DIR_NOT_EXISTS, put(ver++, ba("parent.key1"), "a".getBytes()).getBizCode());
        takeSnapshot();
        // change key2 to dir
        assertEquals(KvCodes.SUCCESS, mkdir(ver++, ba("key2")).getBizCode());
        assertEquals(KvCodes.SUCCESS, put(ver++, ba("key2.key1"), "d".getBytes()).getBizCode());
        assertEquals("d", new String(kv.get(ba("key2.key1")).getNode().data));
        //change parent to string
        assertEquals(KvCodes.SUCCESS, put(ver++, ba("parent"), "e".getBytes()).getBizCode());
        assertEquals("e", new String(kv.get(ba("parent")).getNode().data));
    }

    private void assertNodeCount(int expect, String key) {
        KvNodeHolder holder = kv.map.get(ba(key));
        if (expect == 0) {
            assertNull(holder);
        } else {
            assertNotNull(holder);
            int count = 0;
            KvNodeEx n = holder.latest;
            while (n != null) {
                count++;
                n = n.previous;
            }
            assertEquals(expect, count);
        }
    }

    @Test
    void testGc1() {
        put(ver++, ba("key1"), "a".getBytes());
        Snapshot s1 = takeSnapshot();

        put(ver++, ba("key1"), "b".getBytes());
        assertNodeCount(2, "key1");
        put(ver++, ba("key1"), "c".getBytes());
        assertNodeCount(2, "key1");
        Snapshot s2 = takeSnapshot();

        put(ver++, ba("key1"), "d".getBytes());
        assertNodeCount(3, "key1");
        s1.close();
        assertNodeCount(2, "key1");
        s2.close();
        assertNodeCount(1, "key1");
    }

    @Test
    void testGc2() {
        put(1, ba("key1"), "a".getBytes());
        Snapshot s1 = takeSnapshot();

        put(2, ba("key2"), "b".getBytes());
        remove(3, ba("key2"));
        assertNodeCount(0, "key2");

        put(4, ba("key1"), "a2".getBytes());
        put(5, ba("key1"), "a3".getBytes());
        remove(6, ba("key1"));
        assertNodeCount(2, "key1");

        s1.close();
        assertNodeCount(0, "key1");
    }

    @Test
    void testGc3() {
        put(1, ba("key1"), "a".getBytes());
        Snapshot s1 = takeSnapshot();

        remove(2, ba("key1"));
        takeSnapshot();

        put(3, ba("key1"), "b".getBytes());
        assertNodeCount(3, "key1");
        takeSnapshot();

        s1.close();
        assertNodeCount(1, "key1");
    }

    @Test
    void testGc4() {
        put(1, ba("key1"), "a".getBytes());
        Snapshot s1 = takeSnapshot();

        put(2, ba("key1"), "b".getBytes());
        assertNodeCount(2, "key1");
        takeSnapshot();

        s1.close();
        assertNodeCount(1, "key1");
    }

    @Test
    void testGc5() {
        put(1, ba("key1"), "a".getBytes());
        Snapshot s1 = takeSnapshot();

        put(2, ba("key1"), "b".getBytes());
        assertNodeCount(2, "key1");
        takeSnapshot();

        remove(3, ba("key1"));

        s1.close();
        assertNodeCount(2, "key1");
    }

    @Test
    void testBatchGetPut() {
        // Test invalid input
        assertEquals(KvCodes.INVALID_KEY, kv.batchGet(null).getLeft());
        assertEquals(KvCodes.INVALID_KEY, kv.batchGet(List.of()).getLeft());
        assertEquals(KvCodes.INVALID_KEY, batchPut(1, null, List.of("a".getBytes())).getLeft());
        assertEquals(KvCodes.INVALID_KEY, batchPut(1, List.of(), List.of("a".getBytes())).getLeft());
        assertEquals(KvCodes.INVALID_VALUE, batchPut(1, List.of("a".getBytes()), null).getLeft());
        assertEquals(KvCodes.INVALID_VALUE, batchPut(1, List.of("a".getBytes()), List.of()).getLeft());

        // Prepare test data
        List<byte[]> keys = List.of(
                "key1".getBytes(),
                "key2".getBytes(),
                "parent.key1".getBytes()
        );
        List<byte[]> values = List.of(
                "value1".getBytes(),
                "value2".getBytes(),
                "value3".getBytes()
        );

        // Test initial get on non-existing keys
        Pair<Integer, List<KvResult>> getResult = kv.batchGet(keys);
        assertEquals(KvCodes.SUCCESS, getResult.getLeft());
        assertEquals(3, getResult.getRight().size());
        getResult.getRight().forEach(r -> assertEquals(KvCodes.NOT_FOUND, r.getBizCode()));

        // Create parent directory for "parent.key1"
        assertEquals(KvCodes.SUCCESS, mkdir(1, ba("parent")).getBizCode());

        // Test batch put
        Pair<Integer, List<KvResult>> putResult = batchPut(2, keys, values);
        assertEquals(KvCodes.SUCCESS, putResult.getLeft());
        assertEquals(3, putResult.getRight().size());
        putResult.getRight().forEach(r -> assertEquals(KvCodes.SUCCESS, r.getBizCode()));

        // Verify values with batch get
        getResult = kv.batchGet(keys);
        assertEquals(KvCodes.SUCCESS, getResult.getLeft());
        assertEquals(3, getResult.getRight().size());
        List<KvResult> results = getResult.getRight();

        // Verify individual results
        assertEquals(KvCodes.SUCCESS, results.get(0).getBizCode());
        assertArrayEquals("value1".getBytes(), results.get(0).getNode().data);
        assertEquals(2, results.get(0).getNode().createIndex);

        assertEquals(KvCodes.SUCCESS, results.get(1).getBizCode());
        assertArrayEquals("value2".getBytes(), results.get(1).getNode().data);
        assertEquals(2, results.get(1).getNode().createIndex);

        assertEquals(KvCodes.SUCCESS, results.get(2).getBizCode());
        assertArrayEquals("value3".getBytes(), results.get(2).getNode().data);
        assertEquals(2, results.get(2).getNode().createIndex);

        // Test batch put with updates
        List<byte[]> newValues = List.of(
                "updated1".getBytes(),
                "updated2".getBytes(),
                "updated3".getBytes()
        );
        putResult = batchPut(3, keys, newValues);
        assertEquals(KvCodes.SUCCESS, putResult.getLeft());
        putResult.getRight().forEach(r -> assertEquals(KvCodes.SUCCESS_OVERWRITE, r.getBizCode()));

        // Verify updated values
        keys = List.of(
                "key1".getBytes(),
                "key2".getBytes(),
                ".....".getBytes()
        );
        getResult = kv.batchGet(keys);
        assertEquals(KvCodes.SUCCESS, getResult.getLeft());
        results = getResult.getRight();

        assertArrayEquals("updated1".getBytes(), results.get(0).getNode().data);
        assertEquals(2, results.get(0).getNode().createIndex);
        assertEquals(3, results.get(0).getNode().updateIndex);

        assertArrayEquals("updated2".getBytes(), results.get(1).getNode().data);
        assertEquals(2, results.get(1).getNode().createIndex);
        assertEquals(3, results.get(1).getNode().updateIndex);

        assertNull(results.get(2).getNode());
        assertEquals(KvCodes.INVALID_KEY, results.get(2).getBizCode());
    }

    @Test
    public void testBatchRemove() {
        // Test invalid input
        assertEquals(KvCodes.INVALID_KEY, batchRemove(ver++, null).getLeft());
        assertEquals(KvCodes.INVALID_KEY, batchRemove(ver++, List.of()).getLeft());

        // Prepare test data
        mkdir(ver++, ba("parent"));
        List<byte[]> keys = List.of(
                "key1".getBytes(),
                "key2".getBytes(),
                "parent.key1".getBytes()
        );
        List<byte[]> values = List.of(
                "value1".getBytes(),
                "value2".getBytes(),
                "value3".getBytes()
        );
        batchPut(ver++, keys, values);

        // Test batch remove
        List<byte[]> removeKeys = List.of(
                "key1".getBytes(),
                "nonexistent".getBytes(),
                "parent.key1".getBytes()
        );
        Pair<Integer, List<KvResult>> removeResult = batchRemove(ver++, removeKeys);
        assertEquals(KvCodes.SUCCESS, removeResult.getLeft());
        assertEquals(3, removeResult.getRight().size());

        // Verify individual results
        List<KvResult> results = removeResult.getRight();
        assertEquals(KvCodes.SUCCESS, results.get(0).getBizCode());  // key1 removed
        assertEquals(KvCodes.NOT_FOUND, results.get(1).getBizCode());  // nonexistent key
        assertEquals(KvCodes.SUCCESS, results.get(2).getBizCode());  // parent.key1 removed

        // Verify removals with gets
        assertEquals(KvCodes.NOT_FOUND, kv.get(ba("key1")).getBizCode());
        assertEquals(KvCodes.SUCCESS, kv.get(ba("key2")).getBizCode());
        assertEquals(KvCodes.NOT_FOUND, kv.get(ba("parent.key1")).getBizCode());
    }

    @Test
    public void testCompareAndSet() {
        // Test initial CAS with null expected value (should succeed)
        assertEquals(KvCodes.SUCCESS, compareAndSet(ver++, ba("key1"), null, "value1".getBytes()).getBizCode());
        assertArrayEquals("value1".getBytes(), kv.get(ba("key1")).getNode().data);

        // Test CAS with wrong expected value (should fail)
        assertEquals(KvCodes.CAS_MISMATCH,
                compareAndSet(ver++, ba("key1"), "wrongvalue".getBytes(), "value2".getBytes()).getBizCode());
        assertArrayEquals("value1".getBytes(), kv.get(ba("key1")).getNode().data);

        // Test CAS with correct expected value (should succeed)
        assertEquals(KvCodes.SUCCESS,
                compareAndSet(ver++, ba("key1"), "value1".getBytes(), "value2".getBytes()).getBizCode());
        assertArrayEquals("value2".getBytes(), kv.get(ba("key1")).getNode().data);

        // test CAS with null expectValue and null newValue
        assertEquals(KvCodes.INVALID_VALUE,
                compareAndSet(ver++, ba("xxx"), null, null).getBizCode());

        // test CAS delete
        assertEquals(KvCodes.SUCCESS,
                compareAndSet(ver++, ba("key1"), "value2".getBytes(), null).getBizCode());
        assertEquals(KvCodes.NOT_FOUND, kv.get(ba("key1")).getBizCode());

        // Test CAS with non-existent key
        assertEquals(KvCodes.CAS_MISMATCH,
                compareAndSet(ver++, ba("nonexistent"), "any".getBytes(), "value".getBytes()).getBizCode());

        // Test CAS with invalid inputs
        assertEquals(KvCodes.INVALID_KEY,
                compareAndSet(ver++, null, "value1".getBytes(), "value2".getBytes()).getBizCode());

        // Test CAS on a directory
        mkdir(ver++, ba("dir1"));
        assertEquals(KvCodes.CAS_MISMATCH,
                compareAndSet(ver++, ba("dir1"), "any".getBytes(), "value".getBytes()).getBizCode());

        // Test CAS with parent dir checks
        mkdir(ver++, ba("parent"));
        assertEquals(KvCodes.SUCCESS,
                compareAndSet(ver++, ba("parent.key1"), null, "value1".getBytes()).getBizCode());
        assertEquals(KvCodes.SUCCESS,
                compareAndSet(ver++, ba("parent.key1"), "value1".getBytes(), "value2".getBytes()).getBizCode());
        assertArrayEquals("value2".getBytes(), kv.get(ba("parent.key1")).getNode().data);

        // Test CAS with non-existent parent directory
        assertEquals(KvCodes.PARENT_DIR_NOT_EXISTS,
                compareAndSet(ver++, ba("nonexistent.key1"), null, "value".getBytes()).getBizCode());
    }

    @Test
    void testTempNode() {
        ByteArray key = ba("temp1");
        initOpContext(DtKV.BIZ_TYPE_PUT_TEMP_NODE, 5);
        long createIndex = ver++;
        assertEquals(KvCodes.SUCCESS, kv.put(createIndex, key, "tempValue1".getBytes()).getBizCode());
        assertEquals(5, kv.map.get(ba("temp1")).latest.ttlInfo.ttlMillis);

        initOpContext(DtKV.BIZ_TYPE_PUT_TEMP_NODE, 10);
        assertEquals(KvCodes.SUCCESS_OVERWRITE, kv.put(ver++, key, "tempValue2".getBytes()).getBizCode());
        assertEquals(10, kv.map.get(key).latest.ttlInfo.ttlMillis);

        initOpContext(DtKV.BIZ_TYPE_PUT_TEMP_NODE, 0);
        assertEquals(KvCodes.SUCCESS_OVERWRITE, kv.put(ver++, key, "tempValue3".getBytes()).getBizCode());
        assertEquals(10, kv.map.get(key).latest.ttlInfo.ttlMillis);

        initOpContext(DtKV.BIZ_TYPE_UPDATE_TTL, 20);
        assertEquals(KvCodes.SUCCESS, kv.updateTtl(ver++, key).getBizCode());
        assertEquals(20, kv.map.get(key).latest.ttlInfo.ttlMillis);

        assertNotNull(kv.map.get(key));

        initOpContext(DtKV.BIZ_TYPE_EXPIRE, 0);
        assertEquals(KvCodes.SUCCESS, kv.expire(ver++, key, createIndex).getBizCode());

        assertNull(kv.map.get(key));
    }

    @Test
    void testExpire() {
        ByteArray key = ba("temp1");

        initOpContext(DtKV.BIZ_TYPE_EXPIRE, 0);
        assertEquals(KvCodes.NOT_FOUND, kv.expire(ver++, key, 0).getBizCode());

        initOpContext(DtKV.BIZ_TYPE_PUT_TEMP_NODE, 5);
        long createIndex = ver++;
        assertEquals(KvCodes.SUCCESS, kv.put(createIndex, key, "tempValue1".getBytes()).getBizCode());
        assertEquals(5, kv.map.get(ba("temp1")).latest.ttlInfo.ttlMillis);

        initOpContext(DtKV.BIZ_TYPE_REMOVE, 0);
        kv.remove(ver++, key);
        initOpContext(DtKV.BIZ_TYPE_PUT_TEMP_NODE, 5);
        assertEquals(KvCodes.SUCCESS, kv.put(ver++, key, "tempValue1".getBytes()).getBizCode());

        initOpContext(DtKV.BIZ_TYPE_EXPIRE, 0);
        assertEquals(KvCodes.CREATE_INDEX_MISMATCH, kv.expire(ver++, key, createIndex).getBizCode());

        assertNotNull(kv.map.get(key));
    }

}

