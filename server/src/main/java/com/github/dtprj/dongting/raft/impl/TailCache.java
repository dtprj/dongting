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
package com.github.dtprj.dongting.raft.impl;

import com.github.dtprj.dongting.common.IndexedQueue;
import com.github.dtprj.dongting.common.LongObjMap;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;

import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class TailCache {
    private static final DtLog log = DtLogs.getLogger(TailCache.class);
    private static final long TIMEOUT = TimeUnit.SECONDS.toNanos(10);
    private final RaftGroupConfig groupConfig;
    private final RaftStatusImpl raftStatus;
    private long firstIndex = -1;
    private int pending;
    private long pendingBytes;
    private final IndexedQueue<RaftTask> cache = new IndexedQueue<>(1024);

    private int putCount;

    public TailCache(RaftGroupConfig groupConfig, RaftStatusImpl raftStatus) {
        this.groupConfig = groupConfig;
        this.raftStatus = raftStatus;
    }

    public RaftTask get(long index) {
        if (firstIndex < 0 || index < firstIndex || index >= nextWriteIndex()) {
            return null;
        }
        return cache.get((int) (index - firstIndex));
    }

    private long nextWriteIndex() {
        if (firstIndex <= 0) {
            return -1;
        }
        return firstIndex + cache.size();
    }

    public void put(long index, RaftTask value) {
        if (cache.size() == 0) {
            firstIndex = index;
        } else {
            if (index != nextWriteIndex()) {
                throw new IllegalArgumentException("index " + index + " is not nextWriteIndex " + nextWriteIndex());
            }
        }
        value.getItem().retain();
        cache.addLast(value);
        pending++;
        pendingBytes += value.getInput().getFlowControlSize();
        if ((putCount++ & 0x0F) == 0) { // call cleanPending 1/16
            cleanPending();
        }
    }

    /**
     * truncate tail to index (inclusive)
     */
    public void truncate(long index) {
        if (firstIndex < 0) {
            return;
        }
        if (index < firstIndex) {
            throw new IllegalArgumentException("index " + index + " is less than firstIndex " + firstIndex);
        }
        if (index > getLastIndex()) {
            throw new IllegalArgumentException("index " + index + " is greater than lastIndex " + getLastIndex());
        }
        long nextWriteIndex = nextWriteIndex();
        if (index >= nextWriteIndex) {
            throw new IllegalArgumentException("index " + index + " is greater than nextWriteIndex " + nextWriteIndex);
        }

        log.info("truncate tail cache to {}, old nextWriteIndex={}", index, nextWriteIndex);
        while (size() > 0 && index < nextWriteIndex()) {
            RaftTask raftTask = cache.removeLast();
            if (cache.size() == 0) {
                firstIndex = -1;
            }
            release(raftTask);
        }
    }

    private void release(RaftTask t) {
        pending--;
        pendingBytes = Math.max(pendingBytes - t.getInput().getFlowControlSize(), 0);
        RaftTask x = t;
        while (x != null) {
            x.getItem().release();
            x = x.getNextReader();
        }
    }

    private void remove(long index) {
        if (index != firstIndex) {
            throw new IllegalArgumentException("index " + index + " is not firstIndex " + firstIndex);
        }
        RaftTask t = cache.removeFirst();
        if (t != null) {
            release(t);
        } else {
            throw new IllegalStateException("pending is empty: index=" + index);
        }
        if (cache.size() == 0) {
            firstIndex = -1;
        } else {
            firstIndex++;
        }
    }

    public void forEach(LongObjMap.ReadOnlyVisitor<RaftTask> visitor) {
        int len = cache.size();
        long index = firstIndex;
        for (int i = 0; i < len; i++, index++) {
            visitor.visit(index, cache.get(i));
        }
    }

    public void cleanAll() {
        while (firstIndex >= 0) {
            remove(firstIndex);
        }
    }

    private void cleanPending() {
        if (firstIndex <= 0) {
            return;
        }
        long boundIndex = raftStatus.getLastApplied();
        long timeBound = raftStatus.getTs().getNanoTime() - TIMEOUT;
        int len = cache.size();
        long index = firstIndex;
        for (int i = 0; i < len; i++, index++) {
            if (index >= boundIndex) {
                break;
            }
            RaftTask t = cache.get(0);
            if (pending <= groupConfig.getMaxPendingWrites() && pendingBytes <= groupConfig.getMaxPendingWrites()) {
                if (t.getCreateTimeNanos() - timeBound >= 0) {
                    // this item not timeout, so next items not timeout
                    break;
                } else {
                    remove(index);
                }
            } else {
                remove(index);
            }
        }
    }

    public int size() {
        return cache.size();
    }

    public long getFirstIndex() {
        return firstIndex;
    }

    public long getLastIndex() {
        if (firstIndex == -1) {
            return -1;
        }
        return firstIndex + cache.size() - 1;
    }
}

