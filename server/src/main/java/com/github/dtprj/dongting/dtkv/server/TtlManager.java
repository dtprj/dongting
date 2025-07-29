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
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;

import java.util.Objects;
import java.util.PriorityQueue;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @author huangli
 */
class TtlManager {
    private final Timestamp ts;
    private final RaftGroupConfigEx groupConfig;
    private final ScheduledExecutorService executorService;
    private final Function<TtlInfo, Boolean> expireCallback;

    private final FiberCondition cond;
    private ScheduledFuture<?> scheduledFuture;

    final PriorityQueue<TtlInfo> ttlQueue = new PriorityQueue<>();

    public TtlManager(Timestamp ts, RaftGroupConfigEx groupConfig, ScheduledExecutorService executorService,
                      Function<TtlInfo, Boolean> expireCallback) {
        this.ts = ts;
        this.groupConfig = groupConfig;
        this.executorService = executorService;
        this.expireCallback = expireCallback;
        if (executorService == null) {
            cond = groupConfig.fiberGroup.newCondition("expire" + groupConfig.groupId);
        } else {
            cond = null;
        }
    }

    public void start() {
        if (executorService == null) {
            Fiber f = new Fiber("expire" + groupConfig.groupId, groupConfig.fiberGroup, new FiberFrame<>() {
                @Override
                public FrameCallResult execute(Void input) {
                    long t = removeExpired();
                    return cond.await(t, TimeUnit.NANOSECONDS, this);
                }
            }, true);
            f.start();
        } else {
            executorService.execute(this::expireTaskInExecutor);
        }
    }

    private void expireTaskInExecutor() {
        long t = removeExpired();
        scheduledFuture = executorService.schedule(this::expireTaskInExecutor, t, TimeUnit.NANOSECONDS);
    }

    private void updateExpireTask() {
        if (executorService == null) {
            cond.signal();
        } else {
            scheduledFuture.cancel(false);
            executorService.execute(this::expireTaskInExecutor);
        }
    }

    public long removeExpired() {
        PriorityQueue<TtlInfo> ttlQueue = this.ttlQueue;
        while (!ttlQueue.isEmpty()) {
            TtlInfo ttlInfo = ttlQueue.peek();
            if (ttlInfo.expireNanos - ts.nanoTime > 0) {
                return ttlInfo.expireNanos - ts.nanoTime;
            }
            if (!expireCallback.apply(ttlInfo)) {
                // group is stopped, so submit raft task failed
                return 1_000_000_000L;
            }
            ttlQueue.remove();
        }
        return 1_000_000_000L;
    }

    public void initTtl(ByteArray key, KvNodeEx n, UUID ownerUuid, long ttlMillis) {
        if (ttlMillis <= 0) {
            return;
        }
        if (addNodeTtlAndAddToQueue(key, n, ownerUuid, ttlMillis)) {
            updateExpireTask();
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

    public void updateTtl(ByteArray key, KvNodeEx n, long newTtlMillis) {
        if (newTtlMillis <= 0) {
            return;
        }
        TtlInfo ttlInfo = n.ttlInfo;
        if (ttlInfo == null) {
            return;
        }
        ttlQueue.remove(ttlInfo);
        if (addNodeTtlAndAddToQueue(key, n, n.ownerUuid, newTtlMillis)) {
            updateExpireTask();
        }
    }

    private boolean addNodeTtlAndAddToQueue(ByteArray key, KvNodeEx n, UUID owner, long ttlMillis) {
        long sleepNanos = ttlMillis * 1_000_000;

        TtlInfo ttlInfo = new TtlInfo(key, n.createIndex, ts.nanoTime + sleepNanos);
        n.ttlInfo = ttlInfo;

        n.ownerUuid = owner;
        n.ttlMillis = ttlMillis;
        n.expireTime = ts.wallClockMillis + ttlMillis;

        ttlQueue.add(ttlInfo);
        return ttlQueue.peek() == ttlInfo;
    }

    public void remove(KvNodeEx n) {
        if (n.ttlInfo != null) {
            ttlQueue.remove(n.ttlInfo);
        }
    }
}


final class TtlInfo implements Comparable<TtlInfo> {

    final ByteArray key;
    final long createIndex;
    final long expireNanos;
    private int hash;

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
