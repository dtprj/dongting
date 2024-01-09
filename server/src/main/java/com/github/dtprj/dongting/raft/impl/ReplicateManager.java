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
package com.github.dtprj.dongting.raft.impl;

import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.sm.StateMachine;
import com.github.dtprj.dongting.raft.store.RaftLog;
import com.github.dtprj.dongting.raft.store.StatusManager;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class ReplicateManager {

    private static final DtLog log = DtLogs.getLogger(ReplicateManager.class);
    private static final long FAIL_TIMEOUT = Duration.ofSeconds(1).toNanos();

    private final int groupId;
    private final RaftStatusImpl raftStatus;
    private final RaftServerConfig config;
    private final RaftLog raftLog;
    private final StateMachine stateMachine;
    private final NioClient client;
    private final CommitManager commitManager;
    private final Timestamp ts;

    private final int maxReplicateItems;
    private final int restItemsToStartReplicate;
    private final long maxReplicateBytes;
    private final StatusManager statusManager;

    private long installSnapshotFailTime;


    public ReplicateManager(RaftServerConfig config, RaftGroupConfig groupConfig, RaftStatusImpl raftStatus, RaftLog raftLog,
                            StateMachine stateMachine, NioClient client,
                            CommitManager commitManager, StatusManager statusManager) {
        this.groupId = groupConfig.getGroupId();
        this.raftStatus = raftStatus;
        this.config = config;
        this.raftLog = raftLog;
        this.stateMachine = stateMachine;
        this.client = client;
        this.commitManager = commitManager;
        this.ts = raftStatus.getTs();

        this.maxReplicateItems = config.getMaxReplicateItems();
        this.maxReplicateBytes = config.getMaxReplicateBytes();
        this.statusManager = statusManager;
        this.restItemsToStartReplicate = (int) (maxReplicateItems * 0.1);

        this.installSnapshotFailTime = ts.getNanoTime() - TimeUnit.SECONDS.toNanos(10);
    }

}
