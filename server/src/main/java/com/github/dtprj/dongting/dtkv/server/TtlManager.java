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
import com.github.dtprj.dongting.raft.impl.RaftRole;

import java.util.Objects;
import java.util.PriorityQueue;
import java.util.UUID;
import java.util.function.Function;

/**
 * @author huangli
 */
class TtlManager {
    private final Timestamp ts;
    private final DtKVExecutor dtKVExecutor;
    private final Function<TtlInfo, Boolean> expireCallback;

    final PriorityQueue<TtlInfo> ttlQueue = new PriorityQueue<>();
    private final DtKVExecutor.DtKVExecutorTask task;
    private final String taskName;
    private boolean stop;
    private boolean paused;

    public TtlManager(int groupId, Timestamp ts, DtKVExecutor dtKVExecutor,
                      Function<TtlInfo, Boolean> expireCallback) {
        this.ts = ts;
        this.dtKVExecutor = dtKVExecutor;
        this.expireCallback = expireCallback;
        this.taskName = "expireTask" + groupId;
        this.task = dtKVExecutor.new DtKVExecutorTask() {
            @Override
            protected long execute() {
                return removeExpired();
            }

            @Override
            protected boolean shouldPause() {
                return paused;
            }

            @Override
            protected boolean shouldStop() {
                return stop;
            }

            @Override
            protected long defaultDelayNanos() {
                return 1_000_000_000L;
            }
        };
    }


    public void start() {
        dtKVExecutor.startDaemonTask(taskName, task);
    }

    public void updatePauseStatus(boolean pause) {
        dtKVExecutor.submitTaskInAnyThread(() -> this.paused = pause);
    }

    public void stop() {
        dtKVExecutor.submitTaskInAnyThread(() -> stop = true);
    }

    public long removeExpired() {
        PriorityQueue<TtlInfo> ttlQueue = this.ttlQueue;
        while (!ttlQueue.isEmpty()) {
            TtlInfo ttlInfo = ttlQueue.peek();
            if (ttlInfo.expireNanos - ts.nanoTime > 0) {
                return ttlInfo.expireNanos - ts.nanoTime;
            }
            ttlInfo.shouldExpire = true;
            // expire operation should execute in state machine after write quorum is reached,
            // this is, submit as a raft task.
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
            dtKVExecutor.signalTask(task);
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
            dtKVExecutor.signalTask(task);
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

    public void roleChange(RaftRole oldRole, RaftRole newRole) {

    }
}


final class TtlInfo implements Comparable<TtlInfo> {

    final ByteArray key;
    final long createIndex;
    final long expireNanos;
    boolean shouldExpire;
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
