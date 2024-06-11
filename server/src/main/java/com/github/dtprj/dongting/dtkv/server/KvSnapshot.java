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

import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.sm.Snapshot;
import com.github.dtprj.dongting.raft.sm.SnapshotInfo;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author huangli
 */
class KvSnapshot extends Snapshot {
    private final Supplier<KvStatus> statusSupplier;
    private final Consumer<Snapshot> closeCallback;
    private final Iterator<Map.Entry<String, Value>> iterator;
    private final int epoch;

    private Value currentValue;

    private final EncodeStatus encodeStatus = new EncodeStatus();

    public KvSnapshot(SnapshotInfo si, Supplier<KvStatus> statusSupplier, Consumer<Snapshot> closeCallback) {
        super(si);
        this.statusSupplier = statusSupplier;
        this.closeCallback = closeCallback;
        KvStatus kvStatus = statusSupplier.get();
        this.iterator = kvStatus.kvImpl.getMap().entrySet().iterator();
        this.epoch = kvStatus.epoch;
    }

    @Override
    public FiberFuture<Void> readNext(ByteBuffer buffer) {
        FiberGroup fiberGroup = FiberGroup.currentGroup();
        KvStatus current = statusSupplier.get();
        if (current.status != KvStatus.RUNNING || current.epoch != epoch) {
            return FiberFuture.failedFuture(fiberGroup, new RaftException("the snapshot is expired"));
        }

        while (true) {
            if (currentValue == null) {
                nextValue();
            }
            if (currentValue == null) {
                // no more data
                buffer.flip();
                return FiberFuture.completedFuture(fiberGroup, null);
            }

            if (encodeStatus.writeToBuffer(buffer)) {
                encodeStatus.reset();
                currentValue = null;
            } else {
                // buffer is full
                buffer.flip();
                return FiberFuture.completedFuture(fiberGroup, null);
            }
        }
    }

    private void nextValue() {
        while (iterator.hasNext()) {
            Map.Entry<String, Value> en = iterator.next();
            Value value = en.getValue();
            while (value != null && value.getRaftIndex() > getSnapshotInfo().getLastIncludedIndex()) {
                value = value.getPrevious();
            }
            if (value != null && value.getData() != null) {
                String key = en.getKey();
                encodeStatus.keyBytes = key.getBytes(StandardCharsets.UTF_8);
                encodeStatus.valueBytes = value.getData();
                encodeStatus.raftIndex = value.getRaftIndex();
                currentValue = value;
                break;
            }
        }
    }

    @Override
    protected void doClose() {
        closeCallback.accept(this);
    }
}
