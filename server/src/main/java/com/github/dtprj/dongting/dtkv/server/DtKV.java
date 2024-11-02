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
import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.codec.StrEncoder;
import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.DtBugException;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.dtkv.KvCodes;
import com.github.dtprj.dongting.dtkv.KvNode;
import com.github.dtprj.dongting.dtkv.KvResult;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.sm.Snapshot;
import com.github.dtprj.dongting.raft.sm.SnapshotInfo;
import com.github.dtprj.dongting.raft.sm.StateMachine;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class DtKV extends AbstractLifeCircle implements StateMachine {
    public static final int BIZ_TYPE_PUT = 0;
    public static final int BIZ_TYPE_GET = 1;
    public static final int BIZ_TYPE_REMOVE = 2;
    public static final int BIZ_TYPE_MKDIR = 3;
    public static final int BIZ_TYPE_LIST = 4;

    private Executor dtkvExecutor;

    private final FiberGroup mainFiberGroup;
    private final RaftGroupConfigEx config;
    private final KvConfig kvConfig;
    private final boolean useSeparateExecutor;
    private final ArrayList<Snapshot> openSnapshots = new ArrayList<>();

    private volatile KvStatus kvStatus;
    private EncodeStatus encodeStatus;

    public DtKV(RaftGroupConfigEx config, KvConfig kvConfig) {
        this.mainFiberGroup = config.getFiberGroup();
        this.config = config;
        this.useSeparateExecutor = kvConfig.isUseSeparateExecutor();
        this.kvConfig = kvConfig;
        KvImpl kvImpl = new KvImpl(config.getTs(), config.getGroupId(), kvConfig.getInitMapCapacity(),
                kvConfig.getLoadFactor());
        updateStatus(false, kvImpl);
    }

    @Override
    public DecoderCallback<? extends Encodable> createHeaderCallback(int bizType, DecodeContext context) {
        return new StrEncoder.Callback();
    }

    @Override
    public DecoderCallback<? extends Encodable> createBodyCallback(int bizType, DecodeContext context) {
        switch (bizType) {
            case BIZ_TYPE_GET:
            case BIZ_TYPE_REMOVE:
            case BIZ_TYPE_MKDIR:
            case BIZ_TYPE_LIST:
                return null;
            case BIZ_TYPE_PUT:
                return new ByteArrayEncoder.Callback();
            default:
                throw new IllegalArgumentException("unknown bizType " + bizType);
        }
    }

    @Override
    public FiberFuture<Object> exec(long index, RaftInput input) {
        FiberFuture<Object> f = mainFiberGroup.newFuture("dtkv-exec");
        if (useSeparateExecutor) {
            dtkvExecutor.execute(() -> {
                try {
                    Object r = exec0(index, input);
                    f.fireComplete(r);
                } catch (Exception e) {
                    f.fireCompleteExceptionally(e);
                }
            });
        } else {
            try {
                Object r = exec0(index, input);
                f.complete(r);
            } catch (Exception e) {
                f.completeExceptionally(e);
            }
        }
        return f;
    }

    private Object exec0(long index, RaftInput input) {
        if (kvStatus.installSnapshot) {
            throw new DtBugException("dtkv is install snapshot");
        }
        ByteArrayEncoder key = (ByteArrayEncoder) input.getHeader();
        String ks = key == null ? null : new String(key.getData(), StandardCharsets.UTF_8);
        switch (input.getBizType()) {
            case BIZ_TYPE_PUT:
                ByteArrayEncoder body = (ByteArrayEncoder) input.getBody();
                byte[] bs = body == null ? null : body.getData();
                return kvStatus.kvImpl.put(index, ks, bs);
            case BIZ_TYPE_REMOVE:
                return kvStatus.kvImpl.remove(index, ks);
            case BIZ_TYPE_MKDIR:
                return kvStatus.kvImpl.mkdir(index, ks);
            default:
                throw new IllegalArgumentException("unknown bizType " + input.getBizType());
        }
    }

    /**
     * raft lease read, can read in any threads.
     * <p>
     * For simplification, this method reads the latest snapshot, rather than the one specified by
     * the raftIndex parameter, and this does not violate linearizability.
     * @see com.github.dtprj.dongting.raft.server.RaftGroup#getLeaseReadIndex(DtTime)
     */
    public KvResult get(String key) {
        KvStatus kvStatus = this.kvStatus;
        if (kvStatus.installSnapshot) {
            return new KvResult(KvCodes.CODE_INSTALL_SNAPSHOT);
        }
        return kvStatus.kvImpl.get(key);
    }

    /**
     * raft lease read, can read in any threads.
     * <p>
     * For simplification, this method reads the latest snapshot, rather than the one specified by
     * the raftIndex parameter, and this does not violate linearizability.
     * @see com.github.dtprj.dongting.raft.server.RaftGroup#getLeaseReadIndex(DtTime)
     */
    public Pair<Integer, List<KvNode>> list(String key) {
        KvStatus kvStatus = this.kvStatus;
        if (kvStatus.installSnapshot) {
            return new Pair<>(KvCodes.CODE_INSTALL_SNAPSHOT, null);
        }
        return kvStatus.kvImpl.list(key);
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
            KvImpl kvImpl = new KvImpl(config.getTs(), config.getGroupId(), kvConfig.getInitMapCapacity(),
                    kvConfig.getLoadFactor());
            updateStatus(true, kvImpl);
            encodeStatus = new EncodeStatus();
        } else if (!kvStatus.installSnapshot) {
            throw new DtBugException("current status is not install snapshot");
        }
        KvImpl kvImpl = kvStatus.kvImpl;
        if (data != null && data.hasRemaining()) {
            while (data.hasRemaining()) {
                if (encodeStatus.readFromBuffer(data)) {
                    kvImpl.installSnapshotPut(encodeStatus);
                    encodeStatus.reset();
                } else {
                    break;
                }
            }
        }
        if (done) {
            updateStatus(false, kvImpl);
            encodeStatus = null;
        }
    }

    @Override
    public Snapshot takeSnapshot(SnapshotInfo si) {
        int currentEpoch = kvStatus.epoch;
        Supplier<Boolean> cancel = () -> kvStatus.epoch != currentEpoch;
        KvSnapshot snapshot = new KvSnapshot(si, kvStatus.kvImpl, cancel, this::closeSnapshot);
        openSnapshots.add(snapshot);
        updateMinMax();
        return snapshot;
    }

    private void closeSnapshot(Snapshot snapshot) {
        openSnapshots.remove(snapshot);
        updateMinMax();
        Supplier<Boolean> task = kvStatus.kvImpl.createGcTask();
        if (useSeparateExecutor) {
            doGcInExecutor(task);
        } else {
            mainFiberGroup.fireFiber("task" + config.getGroupId(), new FiberFrame<>() {
                @Override
                public FrameCallResult execute(Void input) {
                    if (task.get()) {
                        return Fiber.yield(this);
                    } else {
                        return Fiber.frameReturn();
                    }
                }
            });
        }
    }

    private void doGcInExecutor(Supplier<Boolean> gc) {
        dtkvExecutor.execute(() -> {
            if (gc.get()) {
                doGcInExecutor(gc);
            }
        });
    }

    private void updateMinMax() {
        long max = 0;
        long min = Long.MAX_VALUE;
        for (Snapshot s : openSnapshots) {
            long idx = s.getSnapshotInfo().getLastIncludedIndex();
            max = Math.max(max, idx);
            min = Math.min(min, idx);
        }
        if (min == Long.MAX_VALUE) {
            min = 0;
        }
        KvImpl kvImpl = kvStatus.kvImpl;
        if (kvImpl != null) {
            kvImpl.maxOpenSnapshotIndex = max;
            kvImpl.minOpenSnapshotIndex = min;
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

    /**
     * may be called in other threads.
     */
    @Override
    protected void doStop(DtTime timeout, boolean force) {
        if (dtkvExecutor != null) {
            stopExecutor(dtkvExecutor);
        }
    }

    private synchronized void updateStatus(boolean installSnapshot, KvImpl kvImpl) {
        if (kvStatus == null) {
            kvStatus = new KvStatus(installSnapshot, kvImpl, 0);
        } else {
            kvStatus = new KvStatus(installSnapshot, kvImpl, kvStatus.epoch + 1);
        }
    }
}
