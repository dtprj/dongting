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

import com.github.dtprj.dongting.codec.ByteArrayEncoder;
import com.github.dtprj.dongting.codec.Decoder;
import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.codec.StrEncoder;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.sm.Snapshot;
import com.github.dtprj.dongting.raft.sm.SnapshotInfo;
import com.github.dtprj.dongting.raft.sm.StateMachine;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;

/**
 * @author huangli
 */
public class DtKV implements StateMachine {
    public static final int BIZ_TYPE_GET = 0;
    public static final int BIZ_TYPE_PUT = 1;
    public static final int BIZ_TYPE_REMOVE = 2;

    private final ArrayList<Snapshot> openSnapshots = new ArrayList<>();
    private long maxOpenSnapshotIndex;

    private volatile KvStatus kvStatus = new KvStatus(KvStatus.RUNNING, new KvImpl(), 0);
    private EncodeStatus encodeStatus;

    public DtKV() {
    }

    @Override
    public Decoder<? extends Encodable> createHeaderDecoder(int bizType) {
        return switch (bizType) {
            case BIZ_TYPE_GET, BIZ_TYPE_REMOVE, BIZ_TYPE_PUT -> StrEncoder.DECODER;
            default -> throw new IllegalArgumentException("unknown bizType " + bizType);
        };
    }

    @Override
    public Decoder<? extends Encodable> createBodyDecoder(int bizType) {
        return switch (bizType) {
            case BIZ_TYPE_GET, BIZ_TYPE_REMOVE -> null;
            case BIZ_TYPE_PUT -> ByteArrayEncoder.DECODER;
            default -> throw new IllegalArgumentException("unknown bizType " + bizType);
        };
    }

    @Override
    public Object exec(long index, RaftInput input) {
        KvStatus kvStatus = this.kvStatus;
        ensureRunning(kvStatus);
        StrEncoder key = (StrEncoder) input.getHeader();
        ByteArrayEncoder data = (ByteArrayEncoder) input.getBody();
        return switch (input.getBizType()) {
            case BIZ_TYPE_GET -> kvStatus.kvImpl.get(key.getStr());
            case BIZ_TYPE_PUT -> {
                kvStatus.kvImpl.put(index, key.getStr(), data.getData(), maxOpenSnapshotIndex);
                yield null;
            }
            case BIZ_TYPE_REMOVE -> kvStatus.kvImpl.remove(index, key.getStr(), maxOpenSnapshotIndex);
            default -> throw new IllegalArgumentException("unknown bizType " + input.getBizType());
        };
    }

    /**
     * read in other threads.
     */
    public byte[] get(String key) {
        return kvStatus.kvImpl.get(key);
    }

    @Override
    public FiberFuture<Void> installSnapshot(long lastIncludeIndex, int lastIncludeTerm, long offset,
                                             boolean done, ByteBuffer data) {
        try {
            if (offset == 0) {
                newStatus(KvStatus.INSTALLING_SNAPSHOT, new KvImpl());
                encodeStatus = new EncodeStatus();
            } else if (kvStatus.status != KvStatus.INSTALLING_SNAPSHOT) {
                return FiberFuture.failedFuture(FiberGroup.currentGroup(), new IllegalStateException(
                        "current status error: " + kvStatus.status));
            }
            KvImpl kvImpl = kvStatus.kvImpl;
            if (data != null && data.hasRemaining()) {
                Map<String, Value> map = kvImpl.getMap();
                while (data.hasRemaining()) {
                    if (encodeStatus.readFromBuffer(data)) {
                        long raftIndex = encodeStatus.raftIndex;
                        // TODO keyBytes is temporary object, we should use a pool
                        String key = new String(encodeStatus.keyBytes, StandardCharsets.UTF_8);
                        byte[] value = encodeStatus.valueBytes;
                        map.put(key, new Value(raftIndex, key, value));
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
            return FiberFuture.completedFuture(FiberGroup.currentGroup(), null);
        } catch (Throwable ex) {
            return FiberFuture.failedFuture(FiberGroup.currentGroup(), ex);
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
