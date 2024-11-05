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

import com.github.dtprj.dongting.common.IndexedQueue;
import com.github.dtprj.dongting.dtkv.KvNode;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.sm.Snapshot;
import com.github.dtprj.dongting.raft.sm.SnapshotInfo;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author huangli
 */
class KvSnapshot extends Snapshot {
    final Supplier<Boolean> cancel;
    private final KvImpl kv;
    private final Consumer<Supplier<Boolean>> gcExecutor;
    private final long lastIncludeRaftIndex;

    private final Iterator<KvNodeHolder> iterator;
    private final IndexedQueue<KvNodeHolder> stack = new IndexedQueue<>(16);
    private final HashSet<ByteArray> processedDirs = new HashSet<>();
    private KvNode currentKvNode;

    private final EncodeStatus encodeStatus = new EncodeStatus();

    public KvSnapshot(SnapshotInfo si, KvImpl kv, Supplier<Boolean> cancel, Consumer<Supplier<Boolean>> gcExecutor) {
        super(si);
        this.kv = kv;
        this.cancel = cancel;
        this.gcExecutor = gcExecutor;
        this.lastIncludeRaftIndex = si.getLastIncludedIndex();
        this.iterator = kv.map.values().iterator();
    }

    @Override
    public FiberFuture<Integer> readNext(ByteBuffer buffer) {
        FiberGroup fiberGroup = FiberGroup.currentGroup();
        if (cancel.get()) {
            return FiberFuture.failedFuture(fiberGroup, new RaftException("canceled"));
        }

        int startPos = buffer.position();
        while (true) {
            if (currentKvNode == null) {
                loadNextNode();
            }
            if (currentKvNode == null) {
                // no more data
                return FiberFuture.completedFuture(fiberGroup, buffer.position() - startPos);
            }

            if (encodeStatus.writeToBuffer(buffer)) {
                encodeStatus.reset();
                currentKvNode = null;
            } else {
                // buffer is full
                return FiberFuture.completedFuture(fiberGroup, buffer.position() - startPos);
            }
        }
    }

    private void loadNextNode() {
        while (stack.size() > 0 || iterator.hasNext()) {
            KvNodeHolder h;
            KvNodeEx n;
            // should process parent dir first
            if (stack.size() > 0) {
                h = stack.removeLast();
                n = getNode(h);
            } else {
                h = iterator.next();
                n = getNode(h);
                if (n == null) {
                    continue;
                }
                if (h.parent != null && !processedDirs.contains(h.parent.key)) {
                    while (h.parent != null && !processedDirs.contains(h.parent.key)) {
                        stack.addLast(h);
                        h = h.parent;
                    }
                    n = getNode(h);
                }
            }
            if (Objects.requireNonNull(n).isDir()) {
                processedDirs.add(h.key);
            }
            encodeStatus.keyBytes = h.key.getData();
            encodeStatus.valueBytes = n.getData();
            encodeStatus.createIndex = n.getCreateIndex();
            encodeStatus.createTime = n.getCreateTime();
            encodeStatus.updateIndex = n.getUpdateIndex();
            encodeStatus.updateTime = n.getUpdateTime();
            currentKvNode = n;
            return;
        }
    }

    private KvNodeEx getNode(KvNodeHolder h) {
        KvNodeEx n = h.latest;
        while (n != null && n.getCreateIndex() > lastIncludeRaftIndex) {
            n = n.previous;
        }
        if (n == null || n.removeAtIndex > 0) {
            return null;
        }
        return n;
    }

    @Override
    protected void doClose() {
        kv.closeSnapshot(this, gcExecutor);
    }
}
