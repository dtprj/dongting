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
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.dtkv.KvCodes;
import com.github.dtprj.dongting.dtkv.KvNode;
import com.github.dtprj.dongting.dtkv.KvResult;
import com.github.dtprj.dongting.raft.test.TestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for distributed lock operations: tryLock, unlock, updateLockLease.
 *
 * @author huangli
 */
class KvLockTest {

    private KvImpl kv;
    private static Timestamp ts;
    private int ver;
    private final UUID owner1 = UUID.randomUUID();
    private final UUID owner2 = UUID.randomUUID();

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

    private void initOpContext(UUID uuid, int bizType, long ttlMillis) {
        kv.opContext.init(bizType, uuid, ttlMillis, ts.wallClockMillis, ts.nanoTime);
    }

    private byte[] buildLockValue(long holdTtlMillis, int lockId, int opId) {
        byte[] value = new byte[16];
        ByteBuffer bb = ByteBuffer.wrap(value);
        bb.putLong(holdTtlMillis);
        bb.putInt(lockId);
        bb.putInt(opId);
        return value;
    }

    @Test
    void testTryLockFirstTime() {
        // first time acquire lock
        initOpContext(owner1, DtKV.BIZ_TYPE_TRY_LOCK, 1000);
        byte[] value = buildLockValue(5000, 1, 1);
        KvResult r = kv.tryLock(ver++, ba("lock1"), value);
        assertEquals(KvCodes.SUCCESS, r.getBizCode());

        // verify lock dir created
        KvResult dirResult = kv.get(ba("lock1"));
        assertEquals(KvCodes.SUCCESS, dirResult.getBizCode());
        assertTrue(dirResult.getNode().isDir());
        assertEquals(KvNode.FLAG_LOCK_MASK, dirResult.getNode().flag & KvNode.FLAG_LOCK_MASK);

        // verify lock sub node created with TTL
        ByteArray lockKey = KvServerUtil.buildLockKey(ba("lock1"),
                owner1.getMostSignificantBits(), owner1.getLeastSignificantBits());
        KvNodeHolder holder = kv.map.get(lockKey);
        assertNotNull(holder);
        assertNotNull(holder.latest.ttlInfo);
        assertEquals(5000, holder.latest.ttlInfo.ttlMillis);
        assertEquals(owner1, holder.latest.ttlInfo.owner);
    }

    @Test
    void testTryLockDirExists() {
        // create lock dir first
        initOpContext(owner1, DtKV.BIZ_TYPE_TRY_LOCK, 1000);
        byte[] value1 = buildLockValue(5000, 1, 1);
        kv.tryLock(ver++, ba("lock1"), value1);

        // second client tries to acquire
        initOpContext(owner2, DtKV.BIZ_TYPE_TRY_LOCK, 2000);
        byte[] value2 = buildLockValue(5000, 2, 1);
        KvResult r = kv.tryLock(ver++, ba("lock1"), value2);
        
        // should return LOCK_BY_OTHER because owner1 holds the lock
        assertEquals(KvCodes.LOCK_BY_OTHER, r.getBizCode());
    }

    @Test
    void testTryLockByOther() {
        // owner1 acquires lock
        initOpContext(owner1, DtKV.BIZ_TYPE_TRY_LOCK, 1000);
        byte[] value1 = buildLockValue(5000, 1, 1);
        kv.tryLock(ver++, ba("lock1"), value1);

        // owner2 tries to acquire
        initOpContext(owner2, DtKV.BIZ_TYPE_TRY_LOCK, 2000);
        byte[] value2 = buildLockValue(5000, 2, 1);
        KvResult r = kv.tryLock(ver++, ba("lock1"), value2);
        
        assertEquals(KvCodes.LOCK_BY_OTHER, r.getBizCode());

        // verify owner1 still holds the lock
        ByteArray lockKey1 = KvServerUtil.buildLockKey(ba("lock1"),
                owner1.getMostSignificantBits(), owner1.getLeastSignificantBits());
        KvNodeHolder holder = kv.map.get(lockKey1);
        assertNotNull(holder);
        assertFalse(holder.latest.removed);
    }

    @Test
    void testTryLockBySelf() {
        // owner1 acquires lock
        initOpContext(owner1, DtKV.BIZ_TYPE_TRY_LOCK, 1000);
        byte[] value1 = buildLockValue(5000, 1, 1);
        kv.tryLock(ver++, ba("lock1"), value1);

        // owner1 tries to acquire again (wait mode with ttl > 0)
        initOpContext(owner1, DtKV.BIZ_TYPE_TRY_LOCK, 2000);
        byte[] value2 = buildLockValue(6000, 1, 2);
        KvResult r = kv.tryLock(ver++, ba("lock1"), value2);
        
        // when ttl > 0 (wait mode) and lock is held by same client, returns LOCK_BY_SELF
        assertEquals(KvCodes.LOCK_BY_SELF, r.getBizCode());
        ByteArray lockKey1 = KvServerUtil.buildLockKey(ba("lock1"),
                owner1.getMostSignificantBits(), owner1.getLeastSignificantBits());
        KvNodeHolder holder = kv.map.get(lockKey1);
        assertNotNull(holder);
        assertFalse(holder.latest.removed);
        assertEquals(6000, holder.latest.ttlInfo.ttlMillis); // TTL updated
    }

    @Test
    void testTryLockParentNotExists() {
        initOpContext(owner1, DtKV.BIZ_TYPE_TRY_LOCK, 1000);
        byte[] value = buildLockValue(5000, 1, 1);
        KvResult r = kv.tryLock(ver++, ba("parent.lock1"), value);
        
        assertEquals(KvCodes.PARENT_DIR_NOT_EXISTS, r.getBizCode());
    }

    @Test
    void testTryLockInvalidTtl() {
        // test with invalid hold ttl (less than wait ttl)
        initOpContext(owner1, DtKV.BIZ_TYPE_TRY_LOCK, 5000);
        byte[] value = buildLockValue(3000, 1, 1); // holdTtl < waitTtl
        
        String errorMsg = KvImpl.checkTtl(5000, value, true);
        assertNotNull(errorMsg);
        assertTrue(errorMsg.contains("less than ttl"));
    }

    @Test
    void testTryLockValueValidation() {
        // test value field validation
        byte[] value = buildLockValue(5000, 1, 1);
        String errorMsg = KvImpl.checkTtl(1000, value, true);
        assertNull(errorMsg);

        // test missing value
        errorMsg = KvImpl.checkTtl(1000, null, true);
        assertNotNull(errorMsg);
        assertTrue(errorMsg.contains("no hold ttl"));

        // test short value
        errorMsg = KvImpl.checkTtl(1000, new byte[4], true);
        assertNotNull(errorMsg);
    }

    @Test
    void testUnlockSuccess() {
        // acquire lock first
        initOpContext(owner1, DtKV.BIZ_TYPE_TRY_LOCK, 1000);
        byte[] value = buildLockValue(5000, 1, 1);
        kv.tryLock(ver++, ba("lock1"), value);

        // unlock
        initOpContext(owner1, DtKV.BIZ_TYPE_UNLOCK, 0);
        KvImpl.KvResultWithNewOwnerInfo r = kv.opContext.getKvResultWithNewOwnerInfo(
                kv.unlock(ver++, ba("lock1")));
        
        assertEquals(KvCodes.SUCCESS, r.result.getBizCode());

        // verify lock sub node deleted
        ByteArray lockKey = KvServerUtil.buildLockKey(ba("lock1"),
                owner1.getMostSignificantBits(), owner1.getLeastSignificantBits());
        assertNull(kv.map.get(lockKey));

        // verify lock dir deleted
        assertNull(kv.map.get(ba("lock1")));
    }

    @Test
    void testUnlockByNonHolder() {
        // owner1 acquires lock
        initOpContext(owner1, DtKV.BIZ_TYPE_TRY_LOCK, 1000);
        byte[] value = buildLockValue(5000, 1, 1);
        kv.tryLock(ver++, ba("lock1"), value);

        // owner2 tries to unlock
        initOpContext(owner2, DtKV.BIZ_TYPE_UNLOCK, 0);
        KvResult r = kv.unlock(ver++, ba("lock1"));
        
        // returns LOCK_BY_OTHER because owner2's lock sub node does not exist
        assertEquals(KvCodes.LOCK_BY_OTHER, r.getBizCode());

        // verify owner1's lock still exists
        ByteArray lockKey1 = KvServerUtil.buildLockKey(ba("lock1"),
                owner1.getMostSignificantBits(), owner1.getLeastSignificantBits());
        KvNodeHolder holder = kv.map.get(lockKey1);
        assertNotNull(holder);
        assertFalse(holder.latest.removed);
    }

    @Test
    void testUnlockNotFound() {
        initOpContext(owner1, DtKV.BIZ_TYPE_UNLOCK, 0);
        KvResult r = kv.unlock(ver++, ba("nonexistent"));
        
        assertEquals(KvCodes.NOT_FOUND, r.getBizCode());
    }

    @Test
    void testUnlockWithWaitingQueue() {
        // owner1 acquires lock
        initOpContext(owner1, DtKV.BIZ_TYPE_TRY_LOCK, 1000);
        byte[] value1 = buildLockValue(5000, 1, 1);
        kv.tryLock(ver++, ba("lock1"), value1);

        // owner2 waits in queue
        initOpContext(owner2, DtKV.BIZ_TYPE_TRY_LOCK, 2000);
        byte[] value2 = buildLockValue(6000, 2, 1);
        kv.tryLock(ver++, ba("lock1"), value2);

        // owner1 unlocks
        TestUtil.updateTimestamp(ts, ts.nanoTime + 100_000_000L, ts.wallClockMillis + 100);
        initOpContext(owner1, DtKV.BIZ_TYPE_UNLOCK, 0);
        KvImpl.KvResultWithNewOwnerInfo r = kv.opContext.getKvResultWithNewOwnerInfo(
                kv.unlock(ver++, ba("lock1")));
        
        assertEquals(KvCodes.SUCCESS, r.result.getBizCode());

        // verify next owner notified
        assertNotNull(r.newOwner);
        assertEquals(owner2, r.newOwner.ttlInfo.owner);
        assertTrue(r.newOwnerServerSideWaitNanos >= 0);

        // verify owner1's lock removed
        ByteArray lockKey1 = KvServerUtil.buildLockKey(ba("lock1"),
                owner1.getMostSignificantBits(), owner1.getLeastSignificantBits());
        assertNull(kv.map.get(lockKey1));

        // verify owner2 now holds the lock
        ByteArray lockKey2 = KvServerUtil.buildLockKey(ba("lock1"),
                owner2.getMostSignificantBits(), owner2.getLeastSignificantBits());
        KvNodeHolder holder2 = kv.map.get(lockKey2);
        assertNotNull(holder2);
        assertFalse(holder2.latest.removed);
    }

    @Test
    void testUnlockClearEmptyDir() {
        // acquire and release lock, verify dir cleaned up
        initOpContext(owner1, DtKV.BIZ_TYPE_TRY_LOCK, 1000);
        byte[] value = buildLockValue(5000, 1, 1);
        kv.tryLock(ver++, ba("lock1"), value);

        initOpContext(owner1, DtKV.BIZ_TYPE_UNLOCK, 0);
        kv.opContext.getKvResultWithNewOwnerInfo(kv.unlock(ver++, ba("lock1")));

        // both sub node and dir should be removed
        ByteArray lockKey = KvServerUtil.buildLockKey(ba("lock1"),
                owner1.getMostSignificantBits(), owner1.getLeastSignificantBits());
        assertNull(kv.map.get(lockKey));
        assertNull(kv.map.get(ba("lock1")));
    }

    @Test
    void testUpdateLockLeaseSuccess() {
        // acquire lock first
        initOpContext(owner1, DtKV.BIZ_TYPE_TRY_LOCK, 1000);
        byte[] value = buildLockValue(5000, 1, 1);
        kv.tryLock(ver++, ba("lock1"), value);

        // update lease
        TestUtil.updateTimestamp(ts, ts.nanoTime + 100_000_000L, ts.wallClockMillis + 100);
        initOpContext(owner1, DtKV.BIZ_TYPE_UPDATE_LOCK_LEASE, 8000);
        KvResult r = kv.updateLockLease(ver++, ba("lock1"));
        
        assertEquals(KvCodes.SUCCESS, r.getBizCode());

        // verify TTL updated
        ByteArray lockKey = KvServerUtil.buildLockKey(ba("lock1"),
                owner1.getMostSignificantBits(), owner1.getLeastSignificantBits());
        KvNodeHolder holder = kv.map.get(lockKey);
        assertNotNull(holder);
        assertEquals(8000, holder.latest.ttlInfo.ttlMillis);
    }

    @Test
    void testUpdateLockLeaseByNonOwner() {
        // owner1 acquires lock
        initOpContext(owner1, DtKV.BIZ_TYPE_TRY_LOCK, 1000);
        byte[] value = buildLockValue(5000, 1, 1);
        kv.tryLock(ver++, ba("lock1"), value);

        // owner2 tries to update lease
        initOpContext(owner2, DtKV.BIZ_TYPE_UPDATE_LOCK_LEASE, 6000);
        KvResult r = kv.updateLockLease(ver++, ba("lock1"));
        
        // updateLockLease builds lock key with owner2's UUID, which doesn't exist
        assertEquals(KvCodes.LOCK_BY_OTHER, r.getBizCode());
    }

    @Test
    void testUpdateLockLeaseNotFound() {
        initOpContext(owner1, DtKV.BIZ_TYPE_UPDATE_LOCK_LEASE, 5000);
        KvResult r = kv.updateLockLease(ver++, ba("nonexistent"));
        
        assertEquals(KvCodes.NOT_FOUND, r.getBizCode());
    }

    @Test
    void testUpdateLockLeaseInvalidTtl() {
        // acquire lock first
        initOpContext(owner1, DtKV.BIZ_TYPE_TRY_LOCK, 1000);
        byte[] value = buildLockValue(5000, 1, 1);
        kv.tryLock(ver++, ba("lock1"), value);

        // try to update with invalid TTL (zero)
        initOpContext(owner1, DtKV.BIZ_TYPE_UPDATE_LOCK_LEASE, 0);
        KvResult r = kv.updateLockLease(ver++, ba("lock1"));
        
        assertEquals(KvCodes.INVALID_TTL, r.getBizCode());

        // try to update with negative TTL
        initOpContext(owner1, DtKV.BIZ_TYPE_UPDATE_LOCK_LEASE, -1);
        r = kv.updateLockLease(ver++, ba("lock1"));
        
        assertEquals(KvCodes.INVALID_TTL, r.getBizCode());
    }

    @Test
    void testUpdateLockLeaseOnNonLockNode() {
        // create regular node
        initOpContext(owner1, DtKV.BIZ_TYPE_PUT, 0);
        kv.put(ver++, ba("regular"), "value".getBytes());

        // try to update lease on non-lock node
        initOpContext(owner1, DtKV.BIZ_TYPE_UPDATE_LOCK_LEASE, 5000);
        KvResult r = kv.updateLockLease(ver++, ba("regular"));
        
        // should fail because regular node is not a lock
        assertEquals(KvCodes.NOT_LOCK_NODE, r.getBizCode());
    }
}
