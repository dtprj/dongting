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
package com.github.dtprj.dongting.raft.sm;

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author huangli
 */
public abstract class Snapshot implements AutoCloseable {
    private static final DtLog log = DtLogs.getLogger(Snapshot.class);
    private static final AtomicLong NEXT_ID = new AtomicLong();
    private final long id = NEXT_ID.incrementAndGet();
    private final long lastIncludedIndex;
    private final int lastIncludedTerm;
    private AtomicBoolean closed = new AtomicBoolean();

    public Snapshot(long lastIncludedIndex, int lastIncludedTerm) {
        this.lastIncludedTerm = lastIncludedTerm;
        this.lastIncludedIndex = lastIncludedIndex;
    }

    public long getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    public int getLastIncludedTerm() {
        return lastIncludedTerm;
    }


    public abstract CompletableFuture<RefBuffer> readNext();

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            log.warn("snapshot iterator already closed");
            return;
        }
        doClose();
    }

    protected abstract void doClose();

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Snapshot) {
            Snapshot other = (Snapshot) obj;
            return id == other.id;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(id);
    }

    public long getId() {
        return id;
    }
}
