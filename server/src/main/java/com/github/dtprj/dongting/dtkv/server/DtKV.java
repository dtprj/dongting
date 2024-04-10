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

import com.github.dtprj.dongting.buf.ByteBufferPool;
import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.codec.ByteArrayEncoder;
import com.github.dtprj.dongting.codec.Decoder;
import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.codec.StrEncoder;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftStatus;
import com.github.dtprj.dongting.raft.sm.Snapshot;
import com.github.dtprj.dongting.raft.sm.StateMachine;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author huangli
 */
public class DtKV implements StateMachine {
    public static final int BIZ_TYPE_GET = 0;
    public static final int BIZ_TYPE_PUT = 1;
    public static final int BIZ_TYPE_REMOVE = 2;

    private final RaftGroupConfigEx groupConfig;
    private final RaftStatus raftStatus;
    private final ByteBufferPool heapPool;

    private final ArrayList<Snapshot> openSnapshots = new ArrayList<>();
    private long minOpenSnapshotIndex;

    private volatile KvStatus kvStatus = new KvStatus(KvStatus.RUNNING, new KvImpl(), 0);

    public DtKV(RaftGroupConfigEx groupConfig) {
        this.groupConfig = groupConfig;
        this.raftStatus = groupConfig.getRaftStatus();
        this.heapPool = groupConfig.getHeapPool().getPool();
    }

    @Override
    public Decoder<? extends Encodable> createHeaderDecoder(int bizType) {
        switch (bizType) {
            case BIZ_TYPE_GET:
            case BIZ_TYPE_REMOVE:
            case BIZ_TYPE_PUT:
                return StrEncoder.DECODER;
            default:
                throw new IllegalArgumentException("unknown bizType " + bizType);
        }
    }

    @Override
    public Decoder<? extends Encodable> createBodyDecoder(int bizType) {
        switch (bizType) {
            case BIZ_TYPE_GET:
            case BIZ_TYPE_REMOVE:
                return null;
            case BIZ_TYPE_PUT:
                return ByteArrayEncoder.DECODER;
            default:
                throw new IllegalArgumentException("unknown bizType " + bizType);
        }
    }

    @Override
    public Object exec(long index, RaftInput input) {
        KvStatus kvStatus = this.kvStatus;
        ensureRunning(kvStatus);
        StrEncoder key = (StrEncoder) input.getHeader();
        ByteArrayEncoder data = (ByteArrayEncoder) input.getBody();
        switch (input.getBizType()) {
            case BIZ_TYPE_GET:
                return kvStatus.kvImpl.get(key.getStr());
            case BIZ_TYPE_PUT:
                kvStatus.kvImpl.put(index, key.getStr(), data.getData(), minOpenSnapshotIndex);
                return null;
            case BIZ_TYPE_REMOVE:
                return kvStatus.kvImpl.remove(index, key.getStr(), minOpenSnapshotIndex);
            default:
                throw new IllegalArgumentException("unknown bizType " + input.getBizType());
        }
    }

    /**
     * read in other threads.
     */
    public byte[] get(String key) {
        return kvStatus.kvImpl.get(key);
    }

    @Override
    public FiberFuture<Void> installSnapshot(long lastIncludeIndex, int lastIncludeTerm, long offset,
                                             boolean done, RefBuffer data) {
        try {
            if (offset == 0) {
                newStatus(KvStatus.INSTALLING_SNAPSHOT, new KvImpl());
            } else if (kvStatus.status != KvStatus.INSTALLING_SNAPSHOT) {
                return FiberFuture.failedFuture(FiberGroup.currentGroup(), new IllegalStateException(
                        "current status error: " + kvStatus.status));
            }
            ByteBuffer bb = data.getBuffer();
            KvImpl kvImpl = kvStatus.kvImpl;
            ConcurrentSkipListMap<String, Value> map = kvImpl.getMap();
            while (bb.hasRemaining()) {
                long raftIndex = bb.getLong();
                int keyLen = bb.getInt();
                ByteBuffer keyBuf = heapPool.borrow(keyLen);
                String key;
                try {
                    keyBuf.limit(keyLen);
                    keyBuf.put(bb);
                    key = new String(keyBuf.array(), 0, keyLen, StandardCharsets.UTF_8);
                } finally {
                    heapPool.release(keyBuf);
                }
                int valueLen = bb.getInt();
                byte[] value = new byte[valueLen];
                bb.get(value);
                map.put(key, new Value(raftIndex, value));
            }
            if (done) {
                newStatus(KvStatus.RUNNING, kvImpl);
            }
            return FiberFuture.completedFuture(FiberGroup.currentGroup(), null);
        } catch (Throwable ex) {
            return FiberFuture.failedFuture(FiberGroup.currentGroup(), ex);
        }
    }

    @Override
    public Snapshot takeSnapshot() {
        KvStatus kvStatus = this.kvStatus;
        ensureRunning(kvStatus);
        KvSnapshot snapshot = new KvSnapshot(raftStatus.getLastApplied(), raftStatus.getCurrentTerm(), () -> kvStatus,
                kvStatus, groupConfig.getHeapPool(), this::closeSnapshot);
        openSnapshots.add(snapshot);
        updateMin();
        return snapshot;
    }

    private void closeSnapshot(Snapshot snapshot) {
        openSnapshots.remove(snapshot);
        updateMin();
    }

    private void updateMin() {
        long min = 0;
        for (Snapshot s : openSnapshots) {
            min = min == 0 ? s.getLastIncludedIndex() : Math.min(min, s.getLastIncludedIndex());
        }
        this.minOpenSnapshotIndex = min;
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


    @Override
    public void close() throws Exception {
        newStatus(KvStatus.CLOSED, null);
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private void newStatus(int status, KvImpl kvImpl) {
        // close()/installSnapshot() are called in raft thread, so we don't need to use CAS here
        kvStatus = new KvStatus(status, kvImpl, kvStatus.epoch + 1);
    }
}
