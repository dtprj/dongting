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
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.EncodableBodyWritePacket;
import com.github.dtprj.dongting.net.NioServer;
import com.github.dtprj.dongting.net.RpcCallback;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.DecodeContextEx;
import com.github.dtprj.dongting.raft.impl.RaftGroupImpl;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftCallback;
import com.github.dtprj.dongting.raft.server.RaftGroup;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.sm.Snapshot;
import com.github.dtprj.dongting.raft.sm.SnapshotInfo;
import com.github.dtprj.dongting.raft.sm.StateMachine;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class DtKV extends AbstractLifeCircle implements StateMachine {

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
    public static final int BIZ_TYPE_EXPIRE = 11;
    public static final int BIZ_TYPE_UPDATE_TTL = 12;

    private final Timestamp ts;
    final DtKVExecutor dtkvExecutor;

    private final FiberGroup mainFiberGroup;
    private final RaftGroupConfigEx config;
    private final KvConfig kvConfig;
    final boolean useSeparateExecutor;

    volatile KvStatus kvStatus;
    private EncodeStatus encodeStatus;

    final WatchManager watchManager;
    final TtlManager ttlManager;

    private RaftGroupImpl raftGroup;

    public DtKV(RaftGroupConfigEx config, KvConfig kvConfig) {
        this.mainFiberGroup = config.fiberGroup;
        this.config = config;
        this.useSeparateExecutor = kvConfig.useSeparateExecutor;
        this.kvConfig = kvConfig;
        if (useSeparateExecutor) {
            this.ts = new Timestamp();
        } else {
            this.ts = config.ts;
        }
        this.dtkvExecutor = new DtKVExecutor(config.groupId, ts, useSeparateExecutor ? null : mainFiberGroup);
        watchManager = new WatchManager(config.groupId, ts, kvConfig) {
            @Override
            protected void sendRequest(ChannelInfo ci, WatchNotifyReq req, ArrayList<ChannelWatch> watchList,
                                       int requestEpoch, boolean fireNext) {
                EncodableBodyWritePacket p = new EncodableBodyWritePacket(Commands.DTKV_WATCH_NOTIFY_PUSH, req);
                DtTime timeout = new DtTime(5, TimeUnit.SECONDS);
                DecoderCallbackCreator<WatchNotifyRespCallback> decoder =
                        ctx -> ctx.toDecoderCallback(new WatchNotifyRespCallback(req.notifyList.size()));
                RpcCallback<WatchNotifyRespCallback> c = (result, ex) -> {
                    Runnable r = () -> watchManager.processNotifyResult(
                            ci, watchList, result, ex, requestEpoch, fireNext);
                    // ignore submit failure (stopped)
                    DtKV.this.dtkvExecutor.submitTaskInAnyThread(r);
                };
                ((NioServer) ci.channel.getOwner()).sendRequest(ci.channel, p, decoder, timeout, c);
            }
        };
        this.ttlManager = new TtlManager(config.groupId, ts, dtkvExecutor, this::expire);
        KvImpl kvImpl = new KvImpl(watchManager, ttlManager, ts, config.groupId,
                kvConfig.initMapCapacity, kvConfig.loadFactor);
        updateStatus(false, kvImpl);
    }

    @Override
    public void setRaftGroup(RaftGroup raftGroup) {
        this.raftGroup = (RaftGroupImpl) raftGroup;
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
            // assert submit success
            dtkvExecutor.submitTaskInFiberThread(() -> {
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
        ByteArray key = req.key == null ? null : new ByteArray(req.key);
        switch (input.getBizType()) {
            case BIZ_TYPE_PUT:
                return kvStatus.kvImpl.put(index, key, req.value, req.ownerUuid, req.ttlMillis);
            case BIZ_TYPE_REMOVE:
                return kvStatus.kvImpl.remove(index, key, req.ownerUuid);
            case BIZ_TYPE_MKDIR:
                return kvStatus.kvImpl.mkdir(index, key, req.ownerUuid, req.ttlMillis);
            case BIZ_TYPE_BATCH_PUT:
                return kvStatus.kvImpl.batchPut(index, req.keys, req.values, req.ownerUuid, req.ttlMillis);
            case BIZ_TYPE_BATCH_REMOVE:
                return kvStatus.kvImpl.batchRemove(index, req.keys, req.ownerUuid);
            case BIZ_TYPE_CAS:
                return kvStatus.kvImpl.compareAndSet(index, key, req.expectValue, req.value, req.ownerUuid);
            case BIZ_TYPE_EXPIRE:
                return kvStatus.kvImpl.expire(index, key, req.ttlMillis);
            case BIZ_TYPE_UPDATE_TTL:
                return kvStatus.kvImpl.updateTtl(index, key, req.ownerUuid, req.ttlMillis);
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
        dtkvExecutor.submitTaskInFiberThread(f, () -> {
            try {
                install0(offset, done, data);
                f.fireComplete(null);
            } catch (Exception ex) {
                f.fireCompleteExceptionally(ex);
            }
        });
        return f;
    }

    private void install0(long offset, boolean done, ByteBuffer data) {
        if (offset == 0) {
            watchManager.reset();
            KvImpl kvImpl = new KvImpl(watchManager, ttlManager, ts, config.groupId, kvConfig.initMapCapacity,
                    kvConfig.loadFactor);
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
    public FiberFuture<Snapshot> takeSnapshot(SnapshotInfo si) {
        if (kvStatus.installSnapshot) {
            throw new RaftException("dtkv is install snapshot");
        }
        KvStatus currentKvStatus = kvStatus;
        int currentEpoch = currentKvStatus.epoch;
        Supplier<Boolean> cancel = () -> kvStatus.epoch != currentEpoch;
        KvSnapshot s = new KvSnapshot(config.groupId, si, currentKvStatus.kvImpl, cancel, dtkvExecutor);
        FiberFuture<Snapshot> f = mainFiberGroup.newFuture("take-snapshot-" + config.groupId);
        s.init(f);
        return f;
    }

    @Override
    protected void doStart() {
        dtkvExecutor.start();
        // ignore submit failure (stopped)
        dtkvExecutor.startDaemonTask("watch-dispatch", new DtKVExecutor.DtKVExecutorTask(dtkvExecutor) {
            final long defaultDelayNanos = kvConfig.watchDispatchIntervalMillis * 1_000_000L;

            @Override
            protected long execute() {
                return dispatchWatchTask() ? defaultDelayNanos : 0;
            }

            @Override
            protected boolean shouldPause() {
                return kvStatus.installSnapshot;
            }

            @Override
            protected boolean shouldStop() {
                return DtKV.this.status > AbstractLifeCircle.STATUS_RUNNING;
            }

            @Override
            protected long defaultDelayNanos() {
                return defaultDelayNanos;
            }
        });

        ((RaftStatusImpl) config.raftStatus).roleChangeListener = ttlManager::roleChange;
        ttlManager.start();
    }

    private boolean dispatchWatchTask() {
        if (kvStatus.installSnapshot) {
            return true;
        }
        boolean b = watchManager.dispatch();
        watchManager.cleanTimeoutChannel(120_000_000_000L); // 120 seconds
        return b;
    }

    /**
     * may be called in other threads.
     */
    @Override
    protected void doStop(DtTime timeout, boolean force) {
        ttlManager.stop();
        dtkvExecutor.stop();
    }

    private synchronized void updateStatus(boolean installSnapshot, KvImpl kvImpl) {
        if (kvStatus == null) {
            kvStatus = new KvStatus(installSnapshot, kvImpl, 0);
        } else {
            kvStatus = new KvStatus(installSnapshot, kvImpl, kvStatus.epoch + 1);
        }
    }

    private void expire(RaftInput input, RaftCallback callback) {
        // no flow control here
        raftGroup.groupComponents.linearTaskRunner.submitRaftTaskInBizThread(LogItem.TYPE_NORMAL, input, callback);
    }
}
