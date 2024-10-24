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

import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author huangli
 */
public abstract class Snapshot {
    private static final DtLog log = DtLogs.getLogger(Snapshot.class);
    private final AtomicBoolean closed = new AtomicBoolean();
    private final SnapshotInfo snapshotInfo;

    public Snapshot(SnapshotInfo snapshotInfo) {
        this.snapshotInfo = snapshotInfo;
    }

    public SnapshotInfo getSnapshotInfo() {
        return snapshotInfo;
    }

    public abstract FiberFuture<Integer> readNext(ByteBuffer buffer);

    public void close() {
        if (!closed.compareAndSet(false, true)) {
            log.warn("snapshot iterator already closed");
            return;
        }
        doClose();
    }

    protected abstract void doClose();
}
