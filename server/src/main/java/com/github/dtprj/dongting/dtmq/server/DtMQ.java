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
import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.DtBugException;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
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

/**
 * @author huangli
 */
public class DtMQ extends AbstractLifeCircle implements StateMachine {

    private static final DtLog log = DtLogs.getLogger(DtMQ.class);

    private static final String MQ_IDX_DIR = "mqIdx";

    private static final long LOG_RETENTION_CHECK_MILLIS = 60_000;

    private final RaftGroupConfigEx groupConfig;
    private final MQServerConfig serverConfig;

    private File mqIdxDir;

    MqIdxManager manager;

    private boolean installing;

    private RaftGroup raftGroup;

    public DtMQ(RaftGroupConfigEx groupConfig, MQServerConfig serverConfig) {
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
                reqData.bizKey, rt.raftLogPosition, reqData.timestamp, reqData.totalLen);
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
    protected void doStart() {
        mqIdxDir = new File(groupConfig.dataDir, MQ_IDX_DIR);
        manager = new MqIdxManager(groupConfig, mqIdxDir);
        manager.start();
        Fiber f = new Fiber("mq-mark-delete-" + groupConfig.groupId, groupConfig.fiberGroup,
                new MarkDeleteFrame());
        f.setDaemon(true).start();
    }

    private class MarkDeleteFrame extends FiberFrame<Void> {
        @Override
        public FrameCallResult execute(Void input) {
            RaftStatusImpl rs = (RaftStatusImpl) groupConfig.raftStatus;
            if (!rs.installSnapshot) {
                long bound = groupConfig.ts.wallClockMillis - serverConfig.logRetentionMinutes * 60_000L;
                raftGroup.markTruncateByTimestamp(bound,
                        groupConfig.autoDeleteLogDelaySeconds * 1000L);
            }
            return Fiber.sleep(LOG_RETENTION_CHECK_MILLIS, this);
        }
    }

    @Override
    protected void doStop(DtTime timeout, boolean force) {
        MqIdxManager m = manager;
        if (m != null) {
            m.close().registerCallback((v, ex) -> {
                if (ex != null) {
                    log.error("mq idx close fail, groupId={}", groupConfig.groupId, ex);
                }
            });
        }
    }
}
