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
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.raft.impl.RaftRole;
import com.github.dtprj.dongting.util.MockRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.github.dtprj.dongting.dtkv.server.KvImplTest.ba;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
class TtlManagerTest {

    private TtlManager manager;
    private Timestamp ts;
    private List<TtlInfo> expiredList;
    private AtomicInteger expireCallbackCount;
    private RuntimeException expireCallbackEx;
    private long ver;

    @BeforeEach
    void setUp() {
        ver = 1;
        ts = new Timestamp();
        expiredList = new ArrayList<>();
        expireCallbackCount = new AtomicInteger(0);
        expireCallbackEx = null;

        Consumer<TtlInfo> expireCallback = ttlInfo -> {
            expireCallbackCount.incrementAndGet();
            expiredList.add(ttlInfo);
            if (expireCallbackEx != null) {
                throw expireCallbackEx;
            }
        };

        manager = new TtlManager(ts, expireCallback);
        manager.roleChange(RaftRole.leader);
    }

    @Test
    void testTtlInfoCompareTo() {
        UUID owner = UUID.randomUUID();

        TtlInfo t1 = new TtlInfo(ba("k1"), 1, owner, 1, 5, 100000, 1);
        TtlInfo t2 = new TtlInfo(ba("k2"), 2, owner, 1, 5, 200000, 2);
        TtlInfo t3 = new TtlInfo(ba("k3"), 3, owner, 1, 5, -1, 3);

        TreeSet<TtlInfo> q = new TreeSet<>();
        q.add(t2);
        q.add(t1);
        q.add(t3);
        assertSame(t3, q.first());
        q.remove(t3);
        assertSame(t1, q.first());

        q.clear();
        TtlInfo t4 = new TtlInfo(ba("k1"), 1, owner, 1, 5, 1000, 4);
        TtlInfo t5 = new TtlInfo(ba("k1"), 1, owner, 1, 5, 1000, 5);
        q.add(t4);
        q.add(t5);
        assertSame(t4, q.first());

        q.clear();
        TtlInfo t6 = new TtlInfo(ba("k1"), 1, owner, 1, 5, 1000, Integer.MAX_VALUE);
        TtlInfo t7 = new TtlInfo(ba("k1"), 1, owner, 1, 5, 1000, Integer.MAX_VALUE + 1);
        q.add(t6);
        q.add(t7);
        assertSame(t6, q.first());
    }

    @Test
    void testInitTtlWithZeroTtl() {
        ByteArray key = new ByteArray("test".getBytes());
        KvNodeEx node = createKvNode(key);
        KvImpl.OpContext ctx = createOpContext(UUID.randomUUID(), 0);
        manager.initTtl(ver++, key, node, ctx);
        assertNull(node.ttlInfo);
    }

    @Test
    void testInitTtl() {
        ByteArray key = ba("test");
        KvNodeEx node = createKvNode(key);
        UUID owner = UUID.randomUUID();
        KvImpl.OpContext ctx = createOpContext(owner, 2);

        manager.initTtl(ver++, key, node, ctx);
        assertEquals(2_000_000, manager.task.execute());
        assertEquals(0, expiredList.size());

        ts.wallClockMillis += 2000;
        ts.nanoTime += 2000 * 1000 * 1000;

        assertEquals(manager.defaultDelayNanos, manager.task.execute());
        assertEquals(1, expiredList.size());
    }

    @Test
    void testUpdateTtl() {
        ByteArray key = ba("test");
        KvNodeEx node = createKvNode(key);
        UUID owner = UUID.randomUUID();

        KvImpl.OpContext initCtx = createOpContext(owner, 5);
        manager.initTtl(ver++, key, node, initCtx);
        assertEquals(5_000_000, manager.task.execute());
        assertEquals(0, expiredList.size());

        KvImpl.OpContext updateCtx = createOpContext(owner, 3);
        manager.updateTtl(ver++, key, node, updateCtx);

        assertEquals(3, node.ttlInfo.ttlMillis);
        assertEquals(ts.nanoTime + 3_000_000L, node.ttlInfo.expireNanos);
        assertEquals(3_000_000, manager.task.execute());
        assertEquals(0, expiredList.size());

        ts.wallClockMillis += 2000;
        ts.nanoTime += 2000 * 1000 * 1000;

        assertEquals(manager.defaultDelayNanos, manager.task.execute());
        assertEquals(1, expiredList.size());
    }

    @Test
    void testUpdateTtlInPendingQueue() {
        ByteArray key = ba("test");
        KvNodeEx node = createKvNode(key);
        UUID owner = UUID.randomUUID();

        KvImpl.OpContext initCtx = createOpContext(owner, 5);
        manager.initTtl(ver++, key, node, initCtx);
        ts.wallClockMillis += 6;
        ts.nanoTime += 6 * 1000 * 1000;
        assertEquals(manager.defaultDelayNanos, manager.task.execute()); // goto pendingQueue
        assertEquals(1, expiredList.size());

        expiredList.clear();
        KvImpl.OpContext updateCtx = createOpContext(owner, 3);
        manager.updateTtl(ver++, key, node, updateCtx);
        assertEquals(3, node.ttlInfo.ttlMillis);
        assertEquals(ts.nanoTime + 3_000_000L, node.ttlInfo.expireNanos);
        assertEquals(3_000_000, manager.task.execute());
        assertEquals(0, expiredList.size());
    }

    @Test
    void testRemove() {
        ByteArray key = ba("test");
        KvNodeEx node = createKvNode(key);
        UUID owner = UUID.randomUUID();
        KvImpl.OpContext ctx = createOpContext(owner, 3);

        manager.initTtl(ver++, key, node, ctx);
        manager.remove(node);
        assertEquals(manager.defaultDelayNanos, manager.task.execute());
        assertEquals(0, expiredList.size());

        ts.wallClockMillis += 2000;
        ts.nanoTime += 2000 * 1000 * 1000;

        assertEquals(manager.defaultDelayNanos, manager.task.execute());
        assertEquals(0, expiredList.size());
    }

    @Test
    void testRemoveInPendingQueue() {
        ByteArray key = ba("test");
        KvNodeEx node = createKvNode(key);
        UUID owner = UUID.randomUUID();

        KvImpl.OpContext initCtx = createOpContext(owner, 5);
        manager.initTtl(ver++, key, node, initCtx);
        ts.wallClockMillis += 6;
        ts.nanoTime += 6 * 1000 * 1000;
        assertEquals(manager.defaultDelayNanos, manager.task.execute()); // goto pendingQueue
        assertEquals(1, expiredList.size());

        expiredList.clear();
        manager.remove(node);

        assertEquals(0, manager.ttlQueue.size());
        assertEquals(0, manager.pendingQueue.size());
    }


    @Test
    void testRetry() {
        try {
            ByteArray key = ba("test");
            KvNodeEx node = createKvNode(key);
            UUID owner = UUID.randomUUID();
            KvImpl.OpContext ctx = createOpContext(owner, 3);

            manager.initTtl(ver++, key, node, ctx);

            expireCallbackEx = new MockRuntimeException();
            ts.wallClockMillis += 2000;
            ts.nanoTime += 2000 * 1000 * 1000;

            assertEquals(manager.defaultDelayNanos, manager.task.execute());
            assertEquals(1, expiredList.size());

            manager.retry(expiredList.remove(0), expireCallbackEx);

            ts.nanoTime += manager.retryDelayNanos / 2;
            ts.wallClockMillis = ts.nanoTime / 1_000_000 / 2;
            assertEquals(manager.defaultDelayNanos, manager.task.execute());
            assertEquals(0, expiredList.size());

            ts.nanoTime += manager.retryDelayNanos * 2;
            ts.wallClockMillis = ts.nanoTime / 1_000_000 * 2;
            assertEquals(manager.defaultDelayNanos, manager.task.execute());
            assertEquals(1, expiredList.size());
        } finally {
            // the mock ex may trigger BugLog
            BugLog.reset();
        }

    }

    @Test
    void testRoleChange() {
        ByteArray key = ba("test");
        KvNodeEx node = createKvNode(key);
        UUID owner = UUID.randomUUID();

        KvImpl.OpContext initCtx = createOpContext(owner, 5);
        manager.initTtl(ver++, key, node, initCtx);
        ts.wallClockMillis += 6;
        ts.nanoTime += 6 * 1000 * 1000;
        assertEquals(manager.defaultDelayNanos, manager.task.execute()); // goto pendingQueue
        assertEquals(1, expiredList.size());

        manager.roleChange(RaftRole.follower);
        assertTrue(manager.task.shouldPause());
        assertEquals(1, manager.ttlQueue.size());
        assertEquals(0, manager.pendingQueue.size());
    }

    @Test
    void testExecuteBatch() {
        UUID owner = UUID.randomUUID();
        for (int i = 0; i < TtlManager.MAX_EXPIRE_BATCH + 1; i++) {
            ByteArray key = ba("test" + i);
            KvNodeEx node = createKvNode(key);
            KvImpl.OpContext initCtx = createOpContext(owner, 5);
            manager.initTtl(ver++, key, node, initCtx);
        }
        ts.wallClockMillis += 6;
        ts.nanoTime += 6 * 1000 * 1000;
        assertEquals(0, manager.task.execute());
        assertEquals(TtlManager.MAX_EXPIRE_BATCH, expiredList.size());
    }

    @Test
    void testExecuteBatchInPending() {
        UUID owner = UUID.randomUUID();
        for (int i = 0; i < TtlManager.MAX_RETRY_BATCH + 1; i++) {
            ByteArray key = ba("test" + i);
            KvNodeEx node = createKvNode(key);
            KvImpl.OpContext initCtx = createOpContext(owner, 5);
            manager.initTtl(ver++, key, node, initCtx);
        }
        ts.wallClockMillis += 6;
        ts.nanoTime += 6 * 1000 * 1000;
        while (manager.task.execute() == 0) {
        }
        Throwable ex = new MockRuntimeException();
        for (TtlInfo ttlInfo : expiredList) {
            manager.retry(ttlInfo, ex);
        }
        expiredList.clear();
        ts.wallClockMillis += manager.retryDelayNanos / 1_000_000 + 1;
        ts.nanoTime += manager.retryDelayNanos + 1;
        assertEquals(0, manager.task.execute());
        assertEquals(TtlManager.MAX_RETRY_BATCH, expiredList.size());
    }

    // Helper methods

    private KvNodeEx createKvNode(ByteArray key) {
        // ttl manager does not distinguish between dir and data node, so use a simple data node for testing
        return new KvNodeEx(1, ts.wallClockMillis, 1,
                ts.wallClockMillis, 0, key.getData());
    }

    private KvImpl.OpContext createOpContext(UUID operator, long ttlMillis) {
        KvImpl.OpContext ctx = new KvImpl.OpContext();
        ctx.operator = operator;
        ctx.ttlMillis = ttlMillis;
        ctx.leaderCreateTimeMillis = ts.wallClockMillis;
        ctx.localCreateNanos = ts.nanoTime;
        return ctx;
    }
}
