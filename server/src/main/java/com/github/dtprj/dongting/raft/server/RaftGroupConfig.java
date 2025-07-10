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
package com.github.dtprj.dongting.raft.server;

import com.github.dtprj.dongting.common.NoopPerfCallback;
import com.github.dtprj.dongting.common.PerfCallback;

/**
 * @author huangli
 */
public class RaftGroupConfig {
    public final int groupId;
    public final String nodeIdOfMembers;
    public final String nodeIdOfObservers;
    public String dataDir = "./data";
    public String statusFile = "raft.status";
    public int[] ioRetryInterval = new int[]{100, 1000, 3000, 5000, 10000, 20000};
    public boolean syncForce = true;
    public int raftPingCheck = 0;
    public boolean disableConfigChange;

    public int maxReplicateItems = 50000;
    public long maxReplicateBytes = 16 * 1024 * 1024;
    public int singleReplicateLimit = 1800 * 1024;

    public int maxPendingRaftTasks = 50000;
    public long maxPendingTaskBytes = 256 * 1024 * 1024;

    public int idxCacheSize = 16 * 1024;
    public int idxFlushThreshold = 8 * 1024;

    public boolean ioCallbackUseGroupExecutor = false;

    public PerfCallback perfCallback = NoopPerfCallback.INSTANCE;

    // leader replicate/install read concurrency, or recovering write concurrency.
    // greater than 1 require state machine support.
    public int snapshotConcurrency = 1;
    public int diskSnapshotConcurrency = 4; // disk snapshot read/write concurrency
    public int diskSnapshotBufferSize = 64 * 1024;
    public int replicateSnapshotConcurrency = 4;
    public int replicateSnapshotBufferSize = 64 * 1024;

    public int saveSnapshotSeconds = 3600;
    public int maxKeepSnapshots = 2;
    public boolean saveSnapshotWhenClose = true;
    public int autoDeleteLogDelaySeconds = 60;

    public boolean deleteLogsAfterTakeSnapshot = true;

    RaftGroupConfig(int groupId, String nodeIdOfMembers, String nodeIdOfObservers) {
        this.groupId = groupId;
        this.nodeIdOfMembers = nodeIdOfMembers;
        this.nodeIdOfObservers = nodeIdOfObservers;
    }

    public static RaftGroupConfig newInstance(int groupId, String nodeIdOfMembers, String nodeIdOfObservers) {
        return new RaftGroupConfigEx(groupId, nodeIdOfMembers, nodeIdOfObservers);
    }
}
