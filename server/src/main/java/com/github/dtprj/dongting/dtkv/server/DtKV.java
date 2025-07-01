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

import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.codec.DecoderCallbackCreator;
import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.ByteArray;
import com.github.dtprj.dongting.common.DtBugException;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.FutureCallback;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.dtkv.KvCodes;
import com.github.dtprj.dongting.dtkv.KvReq;
import com.github.dtprj.dongting.dtkv.KvResult;
import com.github.dtprj.dongting.dtkv.WatchNotifyReq;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.EncodableBodyWritePacket;
import com.github.dtprj.dongting.net.NioServer;
import com.github.dtprj.dongting.net.RpcCallback;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.DecodeContextEx;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.sm.Snapshot;
import com.github.dtprj.dongting.raft.sm.SnapshotInfo;
import com.github.dtprj.dongting.raft.sm.StateMachine;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class DtKV extends AbstractLifeCircle implements StateMachine {
    private static final DtLog log = DtLogs.getLogger(DtKV.class);

    // since we not implements raft log-read, all read biz type of read operation are reserved and not used
    public static final int BIZ_TYPE_PUT = 0;
    // public static final int BIZ_TYPE_GET = 1;
    public static final int BIZ_TYPE_REMOVE = 2;
    public static final int BIZ_TYPE_MKDIR = 3;
    // public static final int BIZ_TYPE_LIST = 4;
    // public static final int BIZ_TYPE_BATCH_GET = 5;
    public static final int BIZ_TYPE_BATCH_PUT = 6;
    public static final int BIZ_TYPE_BATCH_REMOVE = 7;
    public static final int BIZ_TYPE_CAS = 8;
    // Watch operations may update the state machine, but not persist to raft log, treat as read-only.
    // After leadership transfer, the client should re-execute the watch operation (idempotent) to new leader.
    // public static final int BIZ_TYPE_WATCH = 9;
    // public static final int BIZ_TYPE_UNWATCH = 10;

    private final Timestamp ts;
    final ScheduledExecutorService dtkvExecutor;

    private final FiberGroup mainFiberGroup;
    private final RaftGroupConfigEx config;
    private final KvConfig kvConfig;
    final boolean useSeparateExecutor;

    volatile KvStatus kvStatus;
    private EncodeStatus encodeStatus;

    final WatchManager watchManager;

    public DtKV(RaftGroupConfigEx config, KvConfig kvConfig) {
        this.mainFiberGroup = config.fiberGroup;
        this.config = config;
        this.useSeparateExecutor = kvConfig.useSeparateExecutor;
        this.kvConfig = kvConfig;
        if (useSeparateExecutor) {
            dtkvExecutor = createExecutor();
            this.ts = new Timestamp();
        } else {
            dtkvExecutor = null;
            this.ts = config.ts;
        }
        watchManager = new WatchManager(config.groupId, ts, kvConfig) {
            @Override
            protected void sendRequest(ChannelInfo ci, WatchNotifyReq req, ArrayList<ChannelWatch> watchList,
                                       int requestEpoch, boolean fireNext) {
                EncodableBodyWritePacket r = new EncodableBodyWritePacket(Commands.DTKV_WATCH_NOTIFY_PUSH, req);
                DtTime timeout = new DtTime(5, TimeUnit.SECONDS);
                DecoderCallbackCreator<WatchNotifyRespCallback> decoder =
                        ctx -> ctx.toDecoderCallback(new WatchNotifyRespCallback(req.notifyList.size()));
                RpcCallback<WatchNotifyRespCallback> c = (result, ex) -> execute(() ->
                        watchManager.processNotifyResult(ci, watchList, result, ex, requestEpoch, fireNext));
                ((NioServer) ci.channel.getOwner()).sendRequest(ci.channel, r, decoder, timeout, c);
            }
        };
        KvImpl kvImpl = new KvImpl(watchManager, ts, config.groupId, kvConfig.initMapCapacity, kvConfig.loadFactor);
        updateStatus(false, kvImpl);
    }

    void execute(Runnable r) {
        Executor executor = dtkvExecutor == null ? mainFiberGroup.getExecutor() : dtkvExecutor;
        try {
            executor.execute(r);
        } catch (RejectedExecutionException e) {
            log.error("", e);
        }
    }

    @Override
    public DecoderCallback<? extends Encodable> createHeaderCallback(int bizType, DecodeContext context) {
        return null;
    }

    @Override
    public DecoderCallback<? extends Encodable> createBodyCallback(int bizType, DecodeContext context) {
        DecodeContextEx e = (DecodeContextEx) context;
        return context.toDecoderCallback(e.kvReqCallback());
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
        KvReq req = (KvReq) input.getBody();
        switch (input.getBizType()) {
            case BIZ_TYPE_PUT: {
                ByteArray key = req.key == null ? null : new ByteArray(req.key);
                return kvStatus.kvImpl.put(index, key, req.value);
            }
            case BIZ_TYPE_REMOVE: {
                ByteArray key = req.key == null ? null : new ByteArray(req.key);
                return kvStatus.kvImpl.remove(index, key);
            }
            case BIZ_TYPE_MKDIR: {
                ByteArray key = req.key == null ? null : new ByteArray(req.key);
                return kvStatus.kvImpl.mkdir(index, key);
            }
            case BIZ_TYPE_BATCH_PUT: {
                return kvStatus.kvImpl.batchPut(index, req.keys, req.values);
            }
            case BIZ_TYPE_BATCH_REMOVE: {
                return kvStatus.kvImpl.batchRemove(index, req.keys);
            }
            case BIZ_TYPE_CAS: {
                ByteArray key = req.key == null ? null : new ByteArray(req.key);
                return kvStatus.kvImpl.compareAndSet(index, key, req.expectValue, req.value);
            }
            default:
                throw new IllegalArgumentException("unknown bizType " + input.getBizType());
        }
    }

    /**
     * raft lease read, can read in any threads.
     * <p>
     * For simplification, this method reads the latest snapshot, rather than the one specified by
     * the raftIndex parameter, and this does not violate linearizability.
     *
     * @see com.github.dtprj.dongting.raft.server.RaftGroup#leaseRead(DtTime, FutureCallback)
     */
    public KvResult get(ByteArray key) {
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
     *
     * @see com.github.dtprj.dongting.raft.server.RaftGroup#leaseRead(DtTime, FutureCallback)
     */
    public Pair<Integer, List<KvResult>> batchGet(List<byte[]> keys) {
        KvStatus kvStatus = this.kvStatus;
        if (kvStatus.installSnapshot) {
            return new Pair<>(KvCodes.CODE_INSTALL_SNAPSHOT, null);
        }
        return kvStatus.kvImpl.batchGet(keys);
    }

    /**
     * raft lease read, can read in any threads.
     * <p>
     * For simplification, this method reads the latest snapshot, rather than the one specified by
     * the raftIndex parameter, and this does not violate linearizability.
     *
     * @see com.github.dtprj.dongting.raft.server.RaftGroup#leaseRead(DtTime, FutureCallback)
     */
    public Pair<Integer, List<KvResult>> list(ByteArray key) {
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
            watchManager.reset();
            KvImpl kvImpl = new KvImpl(watchManager, ts, config.groupId, kvConfig.initMapCapacity, kvConfig.loadFactor);
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
        if (kvStatus.installSnapshot) {
            throw new RaftException("dtkv is install snapshot");
        }
        KvStatus currentKvStatus = kvStatus;
        int currentEpoch = currentKvStatus.epoch;
        Supplier<Boolean> cancel = () -> kvStatus.epoch != currentEpoch;
        return new KvSnapshot(config.groupId, si, currentKvStatus.kvImpl, cancel, dtkvExecutor);
    }

    protected ScheduledExecutorService createExecutor() {
        return Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setName("DtKV-" + config.groupId);
            return t;
        });
    }

    protected void stopExecutor(ScheduledExecutorService executor) {
        executor.shutdownNow();
    }

    @Override
    protected void doStart() {
        if (useSeparateExecutor) {
            dtkvExecutor.scheduleWithFixedDelay(ts::refresh, 1, 1, TimeUnit.MILLISECONDS);
            dtkvExecutor.execute(this::watchDispatchInExecutor);
        } else {
            Fiber f = new Fiber("watch-dispatch", mainFiberGroup, new WatchDispatchFrame(), true);
            f.start();
        }
    }

    private boolean dispatchWatchTask() {
        if (kvStatus.installSnapshot) {
            return true;
        }
        boolean b = watchManager.dispatch();
        watchManager.cleanTimeoutChannel(120_000_000_000L); // 120 seconds
        return b;
    }

    private void watchDispatchInExecutor() {
        if (kvStatus.installSnapshot || dispatchWatchTask()) {
            dtkvExecutor.schedule(this::watchDispatchInExecutor, kvConfig.watchDispatchIntervalMillis, TimeUnit.MILLISECONDS);
        } else {
            dtkvExecutor.execute(this::watchDispatchInExecutor);
        }
    }

    private class WatchDispatchFrame extends FiberFrame<Void> {

        @Override
        public FrameCallResult execute(Void input) {
            if (status > AbstractLifeCircle.STATUS_RUNNING || isGroupShouldStopPlain()) {
                return Fiber.frameReturn();
            }
            if (kvStatus.installSnapshot || dispatchWatchTask()) {
                return Fiber.sleepUntilShouldStop(kvConfig.watchDispatchIntervalMillis, this);
            } else {
                return Fiber.yield(this);
            }
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
