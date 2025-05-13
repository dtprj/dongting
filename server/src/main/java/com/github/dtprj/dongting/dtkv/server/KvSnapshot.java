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
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.sm.Snapshot;
import com.github.dtprj.dongting.raft.sm.SnapshotInfo;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * @author huangli
 */
class KvSnapshot extends Snapshot {
    final Supplier<Boolean> cancel;
    private final int groupId;
    private final KvImpl kv;
    private final Executor dtkvExecutor;
    private final long lastIncludeRaftIndex;

    private final Iterator<KvNodeHolder> iterator;
    private final IndexedQueue<KvNodeHolder> stack = new IndexedQueue<>(16);
    private final HashSet<ByteArray> processedDirs = new HashSet<>();
    private KvNode currentKvNode;

    private final EncodeStatus encodeStatus = new EncodeStatus();

    public KvSnapshot(int groupId, SnapshotInfo si, KvImpl kv,
                      Supplier<Boolean> cancel, Executor dtkvExecutor) {
        super(si);
        this.groupId = groupId;
        this.kv = kv;
        this.cancel = cancel;
        this.lastIncludeRaftIndex = si.getLastIncludedIndex();
        this.iterator = kv.map.values().iterator();
        this.dtkvExecutor = dtkvExecutor;
        run(() -> kv.openSnapshot(this));
    }

    private void run(Runnable r) {
        if (dtkvExecutor != null) {
            try {
                dtkvExecutor.execute(r);
            } catch (Exception ex) {
                BugLog.log(ex);
            }
        } else {
            r.run();
        }
    }

    @Override
    public FiberFuture<Integer> readNext(ByteBuffer buffer) {
        FiberGroup fiberGroup = FiberGroup.currentGroup();
        FiberFuture<Integer> f = fiberGroup.newFuture("readNext");
        // no read lock, since we run in dtKvExecutor or raft thread.
        run(() -> {
            if (cancel.get()) {
                f.fireCompleteExceptionally(new RaftException("canceled"));
                return;
            }

            int startPos = buffer.position();
            while (true) {
                if (currentKvNode == null) {
                    loadNextNode();
                }
                if (currentKvNode == null) {
                    // no more data
                    f.fireComplete(buffer.position() - startPos);
                    return;
                }

                if (encodeStatus.writeToBuffer(buffer)) {
                    encodeStatus.reset();
                    currentKvNode = null;
                } else {
                    // buffer is full
                    f.fireComplete(buffer.position() - startPos);
                    return;
                }
            }
        });

        return f;
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
                if (n.isDir && processedDirs.contains(h.key)) {
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
            if (Objects.requireNonNull(n).isDir) {
                processedDirs.add(h.key);
            }
            encodeStatus.keyBytes = h.key.getData();
            encodeStatus.valueBytes = n.data;
            encodeStatus.createIndex = n.createIndex;
            encodeStatus.createTime = n.createTime;
            encodeStatus.updateIndex = n.updateIndex;
            encodeStatus.updateTime = n.updateTime;
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
        run(() -> kv.closeSnapshot(this));
        doGcInExecutor(kv.createGcTask(cancel));
    }

    private void doGcInExecutor(Supplier<Boolean> gcTask) {
        if (dtkvExecutor != null) {
            dtkvExecutor.execute(() -> {
                if (gcTask.get()) {
                    doGcInExecutor(gcTask);
                }
            });
        } else {
            Fiber f = new Fiber("gcTask" + groupId, FiberGroup.currentGroup(), new FiberFrame<>() {
                @Override
                public FrameCallResult execute(Void input) {
                    if (gcTask.get()) {
                        return Fiber.yield(this);
                    } else {
                        return Fiber.frameReturn();
                    }
                }
            }, true);
            f.start();
        }
    }
}
