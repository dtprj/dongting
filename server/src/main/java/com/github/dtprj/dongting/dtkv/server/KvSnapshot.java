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
import com.github.dtprj.dongting.common.IndexedQueue;
import com.github.dtprj.dongting.dtkv.KvNode;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.sm.Snapshot;
import com.github.dtprj.dongting.raft.sm.SnapshotInfo;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * @author huangli
 */
class KvSnapshot extends Snapshot {
    final Supplier<Boolean> cancel;
    private final int groupId;
    private final KvImpl kv;
    private final DtKVExecutor dtkvExecutor;
    private final long lastIncludeRaftIndex;

    private final Iterator<KvNodeHolder> iterator;
    private final IndexedQueue<KvNodeHolder> stack = new IndexedQueue<>(16);
    private final HashSet<ByteArray> processedDirs = new HashSet<>();
    private KvNode currentKvNode;

    private final EncodeStatus encodeStatus = new EncodeStatus();

    public KvSnapshot(int groupId, SnapshotInfo si, KvImpl kv,
                      Supplier<Boolean> cancel, DtKVExecutor dtkvExecutor) {
        super(si);
        this.groupId = groupId;
        this.kv = kv;
        this.cancel = cancel;
        this.lastIncludeRaftIndex = si.lastIncludedIndex;
        this.iterator = kv.map.iterator();
        this.dtkvExecutor = dtkvExecutor;
        kv.openSnapshot(this);
    }

    @Override
    public FiberFuture<Integer> readNext(ByteBuffer buffer) {
        FiberGroup fiberGroup = FiberGroup.currentGroup();
        FiberFuture<Integer> f = fiberGroup.newFuture("readNext");
        // no read lock, since we run in dtKvExecutor or raft thread.
        dtkvExecutor.submitTaskInFiberThread(f, () -> {
            try {
                if (cancel.get()) {
                    f.fireCompleteExceptionally(new RaftException("canceled"));
                    return;
                }

                f.fireComplete(readNext0(buffer));
            } catch (Throwable e) {
                BugLog.log(e);
                f.fireCompleteExceptionally(e);
            }
        });

        return f;
    }

    int readNext0(ByteBuffer buffer) {
        int startPos = buffer.position();
        while (true) {
            if (currentKvNode == null) {
                loadNextNode();
            }
            if (currentKvNode == null) {
                // no more data
                return buffer.position() - startPos;
            }

            if (encodeStatus.writeToBuffer(buffer)) {
                encodeStatus.reset();
                currentKvNode = null;
            } else {
                // buffer is full
                return buffer.position() - startPos;
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
                // not check null, because parent path of an existing node must exist
                n = getNode(h);
            } else {
                h = iterator.next();
                n = getNode(h);
                if (n == null) {
                    continue;
                }
                if ((n.flag & KvNode.FLAG_DIR_MASK) != 0 && processedDirs.contains(h.key)) {
                    continue;
                }
                if (h.parent != null && !processedDirs.contains(h.parent.key)) {
                    while (h.parent != null && !processedDirs.contains(h.parent.key)) {
                        // push parent node to stack
                        stack.addLast(h);
                        h = h.parent;
                    }
                    // not check null, because parent path of an existing node must exist
                    n = getNode(h);
                }
            }
            if ((Objects.requireNonNull(n).flag & KvNode.FLAG_DIR_MASK) != 0) {
                processedDirs.add(h.key);
            }
            encodeStatus.keyBytes = h.key.getData();
            encodeStatus.valueBytes = n.data;
            encodeStatus.createIndex = n.createIndex;
            encodeStatus.createTime = n.createTime;
            encodeStatus.updateIndex = n.updateIndex;
            encodeStatus.updateTime = n.updateTime;
            encodeStatus.flag = n.flag;
            if (n.ttlInfo != null) {
                encodeStatus.uuid1 = n.ttlInfo.owner.getMostSignificantBits();
                encodeStatus.uuid2 = n.ttlInfo.owner.getLeastSignificantBits();
                encodeStatus.ttlRaftIndex = n.ttlInfo.raftIndex;
                encodeStatus.leaderTtlStartTime = n.ttlInfo.leaderTtlStartMillis;
                encodeStatus.ttlMillis = n.ttlInfo.ttlMillis;
            }
            currentKvNode = n;
            return;
        }
    }

    private KvNodeEx getNode(KvNodeHolder h) {
        KvNodeEx n = h.latest;
        while (n != null && n.updateIndex > lastIncludeRaftIndex) {
            n = n.previous;
        }
        if (n == null || n.removed) {
            return null;
        }
        return n;
    }

    @Override
    protected void doClose() {
        // ignore submit failure (stopped)
        dtkvExecutor.submitTaskInFiberThread(() -> {
            kv.closeSnapshot(this);
            Supplier<Boolean> gcTask = kv.createGcTask();
            // ignore submit failure (stopped)
            dtkvExecutor.startDaemonTask("gcTask" + groupId, new DtKVExecutor.DtKVExecutorTask() {

                private boolean finished;

                @Override
                protected long execute() {
                    boolean hasNext = gcTask.get();
                    if (!hasNext) {
                        finished = true;
                    }
                    return 0;
                }

                @Override
                protected boolean shouldPause() {
                    return false;
                }

                @Override
                protected boolean shouldStop() {
                    return finished || cancel.get();
                }

                @Override
                protected long defaultDelayNanos() {
                    return 0;
                }
            });
        });
    }
}
