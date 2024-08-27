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

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.codec.RefBufferDecoderCallback;
import com.github.dtprj.dongting.codec.StrEncoder;
import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.DtThread;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.sm.Snapshot;
import com.github.dtprj.dongting.raft.sm.SnapshotInfo;
import com.github.dtprj.dongting.raft.sm.StateMachine;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author huangli
 */
public class DtKV extends AbstractLifeCircle implements StateMachine {
    public static final int BIZ_TYPE_GET = 0;
    public static final int BIZ_TYPE_PUT = 1;
    public static final int BIZ_TYPE_REMOVE = 2;

    private Executor dtkvExecutor;

    private final FiberGroup mainFiberGroup;
    private final RaftGroupConfigEx config;
    private final boolean useSeparateExecutor;
    private final ArrayList<Snapshot> openSnapshots = new ArrayList<>();
    private long maxOpenSnapshotIndex;

    private final ReentrantLock kvStatusLock = new ReentrantLock();
    private volatile KvStatus kvStatus = new KvStatus(KvStatus.RUNNING, new KvImpl(), 0);
    private EncodeStatus encodeStatus;

    public DtKV(RaftGroupConfigEx config, boolean useSeparateExecutor) {
        this.mainFiberGroup = config.getFiberGroup();
        this.config = config;
        this.useSeparateExecutor = useSeparateExecutor;
    }

    @Override
    public DecoderCallback<? extends Encodable> createHeaderDecoder(int bizType) {
        switch (bizType) {
            case BIZ_TYPE_GET:
            case BIZ_TYPE_REMOVE:
            case BIZ_TYPE_PUT:
                return new StrEncoder.Callback();
            default:
                throw new IllegalArgumentException("unknown bizType " + bizType);
        }
    }

    @Override
    public DecoderCallback<? extends Encodable> createBodyDecoder(int bizType) {
        switch (bizType) {
            case BIZ_TYPE_GET:
            case BIZ_TYPE_REMOVE:
                return null;
            case BIZ_TYPE_PUT:
                return new RefBufferDecoderCallback();
            default:
                throw new IllegalArgumentException("unknown bizType " + bizType);
        }
    }

    @Override
    public FiberFuture<Object> exec(long index, RaftInput input) {
        KvStatus kvStatus = this.kvStatus;
        ensureRunning(kvStatus);
        FiberFuture<Object> f = mainFiberGroup.newFuture("dtkv-exec");
        if (useSeparateExecutor) {
            dtkvExecutor.execute(() -> {
                try {
                    Object r = exec0(index, input, kvStatus);
                    f.fireComplete(r);
                } catch (Exception e) {
                    f.fireCompleteExceptionally(e);
                }
            });
        } else {
            try {
                Object r = exec0(index, input, kvStatus);
                f.complete(r);
            } catch (Exception e) {
                f.completeExceptionally(e);
            }
        }
        return f;
    }

    private Object exec0(long index, RaftInput input, KvStatus kvStatus) {
        StrEncoder key = (StrEncoder) input.getHeader();
        switch (input.getBizType()) {
            case BIZ_TYPE_GET:
                return kvStatus.kvImpl.get(index, key.getStr());
            case BIZ_TYPE_PUT:
                RefBuffer data = (RefBuffer) input.getBody();
                kvStatus.kvImpl.put(index, key.getStr(), data, maxOpenSnapshotIndex);
                return null;
            case BIZ_TYPE_REMOVE:
                return kvStatus.kvImpl.remove(index, key.getStr(), maxOpenSnapshotIndex);
            default:
                throw new IllegalArgumentException("unknown bizType " + input.getBizType());
        }
    }

    /**
     * raft lease read, can read in any threads.
     *
     * @see com.github.dtprj.dongting.raft.server.RaftGroup#getLeaseReadIndex(DtTime)
     */
    public RefBuffer get(long index, String key) {
        KvStatus kvStatus = this.kvStatus;
        ensureRunning(kvStatus);
        return kvStatus.kvImpl.get(index, key);
    }

    @Override
    public FiberFuture<Void> installSnapshot(long lastIncludeIndex, int lastIncludeTerm, long offset,
                                             boolean done, ByteBuffer data) {
        FiberFuture<Void> f = mainFiberGroup.newFuture("dtkv-install-snapshot");
        if (useSeparateExecutor) {
            dtkvExecutor.execute(() -> {
                try {
                    install0(offset, done, data);
                    f.fireComplete(null);
                } catch (Exception ex) {
                    f.fireCompleteExceptionally(ex);
                }
            });
        } else {
            try {
                install0(offset, done, data);
                f.complete(null);
            } catch (Exception ex) {
                f.completeExceptionally(ex);
            }
        }
        return f;
    }

    private void install0(long offset, boolean done, ByteBuffer data) {
        if (offset == 0) {
            newStatus(KvStatus.INSTALLING_SNAPSHOT, new KvImpl());
            encodeStatus = new EncodeStatus(((DtThread) Thread.currentThread()).getHeapPool());
        } else if (kvStatus.status != KvStatus.INSTALLING_SNAPSHOT) {
            throw new IllegalStateException("current status error: " + kvStatus.status);
        }
        KvImpl kvImpl = kvStatus.kvImpl;
        if (data != null && data.hasRemaining()) {
            while (data.hasRemaining()) {
                if (encodeStatus.readFromBuffer(data)) {
                    long raftIndex = encodeStatus.raftIndex;
                    // TODO use byte buffer pool?
                    String key = new String(encodeStatus.keyBytes, StandardCharsets.UTF_8);
                    kvImpl.put(raftIndex, key, encodeStatus.valueBytes, 0);
                    encodeStatus.reset();
                } else {
                    break;
                }
            }
        }
        if (done) {
            newStatus(KvStatus.RUNNING, kvImpl);
            encodeStatus = null;
        }
    }

    @Override
    public Snapshot takeSnapshot(SnapshotInfo si) {
        KvStatus kvStatus = this.kvStatus;
        ensureRunning(kvStatus);
        KvSnapshot snapshot = new KvSnapshot(si, () -> kvStatus, this::closeSnapshot);
        openSnapshots.add(snapshot);
        updateMax();
        return snapshot;
    }

    private void closeSnapshot(Snapshot snapshot) {
        openSnapshots.remove(snapshot);
        updateMax();
        if (maxOpenSnapshotIndex == 0) {
            KvImpl kvImpl = kvStatus.kvImpl;
            if (kvImpl != null) {
                kvImpl.gc();
            }
        }
    }

    private void updateMax() {
        long max = 0;
        for (Snapshot s : openSnapshots) {
            SnapshotInfo si = s.getSnapshotInfo();
            max = Math.max(max, si.getLastIncludedIndex());
        }
        this.maxOpenSnapshotIndex = max;
    }

    private static void ensureRunning(KvStatus kvStatus) {
        switch (kvStatus.status) {
            case KvStatus.RUNNING:
                break;
            case KvStatus.INSTALLING_SNAPSHOT:
                throw new RaftException("state machine is installing snapshot");
            case KvStatus.CLOSED:
                throw new RaftException("state machine is closed");
            default:
                throw new RaftException(String.valueOf(kvStatus.status));
        }
    }

    protected Executor createExecutor() {
        return Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r);
            t.setName("DtKV-" + config.getGroupId());
            return t;
        });
    }

    protected void stopExecutor(Executor executor) {
        ((ExecutorService) executor).shutdown();
    }

    @Override
    protected void doStart() {
        if (useSeparateExecutor) {
            dtkvExecutor = createExecutor();
        }
    }

    @Override
    protected void doStop(DtTime timeout, boolean force) {
        newStatus(KvStatus.CLOSED, null);
        if (dtkvExecutor != null) {
            stopExecutor(dtkvExecutor);
        }
    }

    private void newStatus(int status, KvImpl kvImpl) {
        kvStatusLock.lock();
        try {
            //noinspection NonAtomicOperationOnVolatileField
            kvStatus = new KvStatus(status, kvImpl, kvStatus.epoch + 1);
        } finally {
            kvStatusLock.unlock();
        }
    }
}
