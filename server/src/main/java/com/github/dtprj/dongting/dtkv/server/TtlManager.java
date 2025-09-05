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
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.impl.RaftRole;

import java.util.Iterator;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * @author huangli
 */
class TtlManager {
    private static final DtLog log = DtLogs.getLogger(TtlManager.class);
    private final Timestamp ts;
    private final Consumer<TtlInfo> expireCallback;

    final TreeSet<TtlInfo> ttlQueue = new TreeSet<>();
    final TreeSet<TtlInfo> pendingQueue = new TreeSet<>();
    final TtlTask task;
    boolean stop;
    private RaftRole role;

    long defaultDelayNanos = 1_000_000_000L; // 1 second
    long retryDelayNanos = 1_000_000_000L; // 1 second

    static final int MAX_RETRY_BATCH = 10;
    static final int MAX_EXPIRE_BATCH = 50;

    // may overflow, but not a problem
    private int ttlInfoIndex;

    public TtlManager(Timestamp ts, Consumer<TtlInfo> expireCallback) {
        this.ts = ts;
        this.expireCallback = expireCallback;
        this.task = new TtlTask();
    }

    class TtlTask extends DtKVExecutor.DtKVExecutorTask {

        @Override
        protected long execute() {
            boolean yield = false;
            if (!pendingQueue.isEmpty()) {
                Iterator<TtlInfo> it = pendingQueue.iterator();
                int count = 0;
                while (it.hasNext()) {
                    TtlInfo ttlInfo = it.next();
                    if (ttlInfo.expireFailed && ts.nanoTime - ttlInfo.lastFailNanos > retryDelayNanos) {
                        if (count++ >= MAX_RETRY_BATCH) {
                            yield = true;
                            break;
                        }
                        it.remove();
                        ttlQueue.add(ttlInfo);
                    } else {
                        break;
                    }
                }
            }
            if (!ttlQueue.isEmpty()) {
                Iterator<TtlInfo> it = ttlQueue.iterator();
                int count = 0;
                while (it.hasNext()) {
                    if (count++ >= MAX_EXPIRE_BATCH) {
                        yield = true;
                        break;
                    }
                    TtlInfo ttlInfo = it.next();
                    if (ttlInfo.expireNanos - ts.nanoTime > 0) {
                        return ttlInfo.expireNanos - ts.nanoTime;
                    }
                    it.remove();
                    pendingQueue.add(ttlInfo);
                    try {
                        ttlInfo.expireFailed = false;
                        ttlInfo.lastFailNanos = 0;
                        expireCallback.accept(ttlInfo);
                    } catch (Throwable e) {
                        ttlInfo.expireFailed = true;
                        ttlInfo.lastFailNanos = ts.nanoTime;
                        BugLog.log(e);
                        return defaultDelayNanos();
                    }
                }
            }

            return yield ? 0 : defaultDelayNanos();
        }

        @Override
        protected boolean shouldPause() {
            return role != RaftRole.leader;
        }

        @Override
        protected boolean shouldStop() {
            return stop;
        }

        @Override
        protected long defaultDelayNanos() {
            return defaultDelayNanos;
        }
    }

    public void retry(TtlInfo ttlInfo, Throwable ex) {
        if (stop) {
            return;
        }
        ttlInfo.expireFailed = true;
        ttlInfo.lastFailNanos = ts.nanoTime;
        // if already in pending queue, no need to add again
        log.warn("expire failed: {}", ex.toString());
    }

    public void initTtl(long raftIndex, ByteArray key, KvNodeEx n, KvImpl.OpContext ctx) {
        if (ctx.ttlMillis <= 0) {
            return;
        }
        if (addNodeTtlAndAddToQueue(raftIndex, key, n, ctx)) {
            task.signal();
        }
    }

    // this method should be idempotent
    public void updateTtl(long raftIndex, ByteArray key, KvNodeEx newNode, KvImpl.OpContext ctx) {
        if (ctx.ttlMillis <= 0) {
            return;
        }
        TtlInfo ttlInfo = newNode.ttlInfo;
        if (ttlInfo == null) {
            return;
        }
        if (ttlInfo.raftIndex >= raftIndex) {
            // this occurs after install snapshot, since KvImpl.updateTtl/doPutInLock may not create new KvNodeEx, and
            // takeSnapshot may save newer ttlInfo than snapshot lastIncludedIndex.
            return;
        }
        doRemove(ttlInfo);
        if (addNodeTtlAndAddToQueue(raftIndex, key, newNode, ctx)) {
            task.signal();
        }
    }

    private boolean addNodeTtlAndAddToQueue(long raftIndex, ByteArray key, KvNodeEx n, KvImpl.OpContext ctx) {
        TtlInfo ttlInfo = new TtlInfo(key, raftIndex, ctx.operator, ctx.leaderCreateTimeMillis, ctx.ttlMillis,
                ctx.localCreateNanos + ctx.ttlMillis * 1_000_000, ttlInfoIndex++);
        n.ttlInfo = ttlInfo;

        // assert not in ttl queue and pending queue pending queue
        if (!ttlQueue.add(ttlInfo)) {
            BugLog.getLog().error("TtlInfo exists {}, {}", key, ttlInfo.raftIndex);
        }
        return ttlQueue.first() == ttlInfo;
    }

    public void remove(KvNodeEx n) {
        doRemove(n.ttlInfo);
    }

    private void doRemove(TtlInfo ti) {
        if (ti == null) {
            return;
        }
        if (!ttlQueue.remove(ti)) {
            pendingQueue.remove(ti);
        }
    }

    public void roleChange(RaftRole newRole) {
        try {
            role = newRole;
            ttlQueue.addAll(pendingQueue);
            pendingQueue.clear();
            task.signal();
        } catch (Throwable e) {
            BugLog.log(e);
        }
    }
}


final class TtlInfo implements Comparable<TtlInfo> {

    final ByteArray key;
    final long raftIndex;
    final UUID owner;
    final long leaderTtlStartMillis;
    final long ttlMillis;
    final long expireNanos;
    private final int ttlInfoIndex;

    boolean expireFailed;
    long lastFailNanos;

    TtlInfo(ByteArray key, long raftIndex, UUID owner, long leaderTtlStartMillis, long ttlMillis,
            long expireNanos, int ttlInfoIndex) {
        this.key = key;
        this.raftIndex = raftIndex;
        this.owner = owner;
        this.leaderTtlStartMillis = leaderTtlStartMillis;
        this.ttlMillis = ttlMillis;
        this.expireNanos = expireNanos;
        this.ttlInfoIndex = ttlInfoIndex;
    }

    @Override
    public int compareTo(TtlInfo o) {
        long x = this.expireNanos - o.expireNanos;
        if (x < 0) {
            return -1;
        } else if (x > 0) {
            return 1;
        } else {
            int y = this.ttlInfoIndex - o.ttlInfoIndex;
            //noinspection UseCompareMethod
            return y < 0 ? -1 : y > 0 ? 1 : 0;
        }
    }
}
