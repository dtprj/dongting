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
package com.github.dtprj.dongting.dtmq.server;

import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.common.DtBugException;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.LongObjMap;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.SimpleFrame;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.DtChannel;
import com.github.dtprj.dongting.raft.impl.RaftGroupImpl;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.server.RaftGroup;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftReqData;
import com.github.dtprj.dongting.raft.sm.Snapshot;
import com.github.dtprj.dongting.raft.sm.SnapshotInfo;
import com.github.dtprj.dongting.raft.sm.StateMachine;
import com.github.dtprj.dongting.raft.store.LogHeader;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashSet;

/**
 * @author huangli
 */
public class DtMQ implements StateMachine {

    private static final DtLog log = DtLogs.getLogger(DtMQ.class);

    private static final String MQ_IDX_DIR = "mqIdx";

    private static final long LOG_RETENTION_CHECK_MILLIS = 60_000;

    private final RaftGroupConfigEx groupConfig;
    final MqServerConfig serverConfig;

    private File mqIdxDir;

    MqIdxManager manager;
    final HashSet<MqPullFrame> activePulls = new HashSet<>();
    final LongObjMap<HashSet<MqPullFrame>> waitingPulls = new LongObjMap<>();
    long changeVersion;

    private boolean installing;

    private RaftGroup raftGroup;

    public DtMQ(RaftGroupConfigEx groupConfig, MqServerConfig serverConfig) {
        DtUtil.checkPositive(serverConfig.maxBody, "maxBody");
        DtUtil.checkPositive(serverConfig.maxPullLimit, "maxPullLimit");
        DtUtil.checkPositive(serverConfig.maxPullBytesLimit, "maxPullBytesLimit");
        this.groupConfig = groupConfig;
        this.serverConfig = serverConfig;
        // mq deletes raft logs by time, so snapshots are kept while the logs exist
        groupConfig.maxKeepSnapshots = 0;
        groupConfig.installOldestSnapshot = true;
    }

    @Override
    public void setRaftGroup(RaftGroup raftGroup) {
        this.raftGroup = raftGroup;
    }

    @Override
    public void onChannelDisconnected(DtChannel channel) {
    }

    /** Raft thread only; limits are shared by all queues, and waitMillis=0 disables long polling.
     * The frame retains queueIds/fromSeqs until completion; callers must not modify them.
     * The caller owns the linearizability point: validate lease read before calling; the frame never re-checks. */
    public MqPullFrame pull(long[] queueIds, long[] fromSeqs, int limit, int bytesLimit,
                            long waitMillis, DtTime deadline) {
        return new MqPullFrame(this, groupConfig,
                ((RaftGroupImpl) raftGroup).groupComponents.raftLog,
                queueIds, fromSeqs, Math.min(limit, serverConfig.maxPullLimit),
                Math.min(bytesLimit, serverConfig.maxPullBytesLimit), waitMillis, deadline);
    }

    /** Raft thread only; individual requests can also be cancelled through MqPullFrame.cancel(). */
    public void cancelPendingPulls() {
        for (MqPullFrame pull : activePulls) {
            pull.cancel();
        }
    }

    private void afterAppend(long queueId) {
        changeVersion++;
        HashSet<MqPullFrame> pulls = waitingPulls.get(queueId);
        if (pulls != null) {
            for (MqPullFrame pull : pulls) {
                pull.signal();
            }
        }
    }

    @Override
    public DecoderCallback<?> createHeaderCallback(int bizType, DecodeContext context) {
        return null;
    }

    @Override
    public DecoderCallback<?> createBodyCallback(int bizType, DecodeContext context) {
        return null;
    }

    @Override
    public FiberFuture<Object> exec(RaftInput input) {
        RaftReqData reqData = input.reqData;
        if (reqData.type != LogHeader.TYPE_NORMAL) {
            return FiberFuture.completedFuture(groupConfig.fiberGroup, null);
        }
        if (installing) {
            throw new DtBugException("dtmq exec during install snapshot");
        }
        RaftTask rt = (RaftTask) input;
        @SuppressWarnings("unchecked")
        FiberFuture<Object> f = (FiberFuture<Object>) (FiberFuture<?>) manager.appendAsync(
                reqData.bizKey, rt.raftLogPosition, reqData.timestamp, reqData.totalLen, reqData.index);
        long queueId = reqData.bizKey;
        if (f.isDone()) {
            if (f.getEx() == null) {
                afterAppend(queueId);
            }
        } else {
            f.registerCallback((v, ex) -> {
                if (ex == null) {
                    afterAppend(queueId);
                }
            });
        }
        return f;
    }

    @Override
    public FiberFuture<Snapshot> takeSnapshot(SnapshotInfo snapshotInfo) {
        MqSnapshot s = new MqSnapshot(snapshotInfo, manager.queues, groupConfig.fiberGroup);
        FiberFuture<Snapshot> f = groupConfig.fiberGroup.newFuture("dtmqSnapshot");
        manager.flusher.flushAll().registerCallback((v, ex) -> {
            if (ex != null) {
                s.close();
                f.completeExceptionally(ex);
            } else {
                f.complete(s);
            }
        });
        return f;
    }

    @Override
    public FiberFuture<Void> startInstall(boolean clean) {
        installing = true;
        cancelPendingPulls();
        FiberFuture<Void> f;
        if (clean) {
            f = manager.destroyAllBeforeInstallSnapshot();
        } else {
            f = manager.close();
        }
        return f.convert("dtmqStartInstall", v -> {
            manager = new MqIdxManager(groupConfig, mqIdxDir);
            return null;
        });
    }

    @Override
    public FiberFuture<Void> installSnapshot(long lastIncludeIndex, int lastIncludeTerm, long offset,
                                             boolean done, ByteBuffer data) {
        if (!installing) {
            throw new DtBugException("mq installSnapshot before startInstall");
        }
        if (data != null) {
            MqSnapshot.parseChunk(data, manager);
        }
        if (done) {
            manager.start();
            installing = false;
            log.info("dtmq install/recover done, queues={}, groupId={}",
                    manager.queues.size(), groupConfig.groupId);
        }
        return FiberFuture.completedFuture(groupConfig.fiberGroup, null);
    }

    @Override
    public FiberFuture<Void> start() {
        mqIdxDir = new File(groupConfig.dataDir, MQ_IDX_DIR);
        manager = new MqIdxManager(groupConfig, mqIdxDir);
        manager.start();
        String fiberName = "mqMarkDelete" + groupConfig.groupId;
        Fiber f = new Fiber(fiberName, groupConfig.fiberGroup,
                new SimpleFrame<>(fiberName, frame -> {
            RaftStatusImpl rs = (RaftStatusImpl) groupConfig.raftStatus;
            if (rs.isInitFinished() && !rs.installSnapshot) {
                long bound = groupConfig.ts.wallClockMillis - serverConfig.logRetentionMinutes * 60_000L;
                raftGroup.markTruncateByTimestamp(bound,
                        groupConfig.autoDeleteLogDelaySeconds * 1000L);
            }
            return Fiber.sleep(LOG_RETENTION_CHECK_MILLIS, frame);
        }));
        f.setDaemon(true).start();
        return FiberFuture.completedFuture(groupConfig.fiberGroup, null);
    }

    @Override
    public FiberFuture<Void> stop() {
        cancelPendingPulls();
        MqIdxManager m = manager;
        if (m == null) {
            return FiberFuture.completedFuture(groupConfig.fiberGroup, null);
        }
        return m.close();
    }
}
