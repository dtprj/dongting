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
import com.github.dtprj.dongting.dtkv.KvResult;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.impl.RaftRole;

import java.util.Iterator;
import java.util.Objects;
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

    private final TreeSet<TtlInfo> ttlQueue = new TreeSet<>();
    private final TreeSet<TtlInfo> pendingQueue = new TreeSet<>();
    final DtKVExecutor.DtKVExecutorTask task;
    boolean stop;
    private RaftRole role;

    long defaultDelayNanos = 1_000_000_000L; // 1 second

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
                    if (count++ >= 10) {
                        yield = true;
                        break;
                    }
                    TtlInfo ttlInfo = it.next();
                    if (ttlInfo.expireFailed && ts.nanoTime - ttlInfo.lastFailNanos > defaultDelayNanos) {
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
                    if (count++ >= 50) {
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
        log.warn("expire failed: {}", ex.toString());
    }

    public void initTtl(ByteArray key, KvNodeEx n, KvImpl.OpContext ctx) {
        if (ctx.ttlMillis <= 0) {
            return;
        }
        if (addNodeTtlAndAddToQueue(key, n, ctx)) {
            task.signal();
        }
    }

    public KvResult checkOwner(KvNodeEx n, UUID currentOperator, long ttlMillis) {
        if (n.removed) {
            return null;
        }
        if (n.ttlInfo == null) {
            return ttlMillis <= 0 ? null : new KvResult(KvCodes.CODE_NOT_TEMP_NODE);
        }
        return n.ownerUuid.equals(currentOperator) ? null : new KvResult(KvCodes.CODE_NOT_OWNER);
    }

    public void updateTtl(ByteArray key, KvNodeEx n, KvImpl.OpContext ctx) {
        if (ctx.ttlMillis <= 0) {
            return;
        }
        TtlInfo ttlInfo = n.ttlInfo;
        if (ttlInfo == null) {
            return;
        }
        doRemove(ttlInfo);
        if (addNodeTtlAndAddToQueue(key, n, ctx)) {
            task.signal();
        }
    }

    private boolean addNodeTtlAndAddToQueue(ByteArray key, KvNodeEx n, KvImpl.OpContext ctx) {
        long sleepNanos = ctx.ttlMillis * 1_000_000;

        TtlInfo ttlInfo = new TtlInfo(key, n.createIndex, ctx.localCreateNanos + sleepNanos);
        n.ttlInfo = ttlInfo;

        n.ownerUuid = ctx.operator;
        n.ttlMillis = ctx.ttlMillis;
        n.expireTime = ctx.leaderCreateTimeMillis + ctx.ttlMillis;

        // assert not in ttl queue and pending queue pending queue
        ttlQueue.add(ttlInfo);
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
    final long createIndex;
    final long expireNanos;
    private int hash;

    boolean expireFailed;
    long lastFailNanos;

    TtlInfo(ByteArray key, long createIndex, long expireNanos) {
        this.key = key;
        this.createIndex = createIndex;
        this.expireNanos = expireNanos;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TtlInfo ttlInfo = (TtlInfo) o;
        return createIndex == ttlInfo.createIndex &&
                Objects.equals(key, ttlInfo.key);
    }

    @Override
    public int hashCode() {
        if (hash == 0) {
            hash = Objects.hash(key, createIndex);
        }
        return hash;
    }

    @Override
    public int compareTo(TtlInfo o) {
        long x = this.expireNanos - o.expireNanos;
        return x < 0 ? -1 : (x > 0 ? 1 : 0);
    }
}
