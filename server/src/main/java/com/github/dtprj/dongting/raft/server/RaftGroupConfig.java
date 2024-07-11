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
    private final int groupId;
    private final String nodeIdOfMembers;
    private final String nodeIdOfObservers;
    private String dataDir = "./data";
    private String statusFile = "raft.status";
    private int[] ioRetryInterval = new int[]{100, 1000, 3000, 5000, 10000, 20000};
    private boolean syncForce = true;
    private boolean staticConfig = true;

    private int maxReplicateItems = 50000;
    private long maxReplicateBytes = 16 * 1024 * 1024;
    private int singleReplicateLimit = 1800 * 1024;

    private int maxPendingRaftTasks = 50000;
    private long maxPendingTaskBytes = 256 * 1024 * 1024;

    private int idxCacheSize = 16 * 1024;
    private int idxFlushThreshold = 8 * 1024;

    private boolean ioCallbackUseGroupExecutor = false;

    private PerfCallback perfCallback = NoopPerfCallback.INSTANCE;

    private long saveSnapshotMillis = 60 * 1000;
    // greater than 1 require state machine support
    private int snapshotConcurrency = 1;
    private int diskSnapshotConcurrency = 4;
    private int diskSnapshotBufferSize = 64 * 1024;
    private int replicateSnapshotConcurrency = 4;
    private int replicateSnapshotBufferSize = 64 * 1024;

    RaftGroupConfig(int groupId, String nodeIdOfMembers, String nodeIdOfObservers) {
        this.groupId = groupId;
        this.nodeIdOfMembers = nodeIdOfMembers;
        this.nodeIdOfObservers = nodeIdOfObservers;
    }

    public static RaftGroupConfig newInstance(int groupId, String nodeIdOfMembers, String nodeIdOfObservers) {
        return new RaftGroupConfigEx(groupId, nodeIdOfMembers, nodeIdOfObservers);
    }

    public int getGroupId() {
        return groupId;
    }

    public String getNodeIdOfMembers() {
        return nodeIdOfMembers;
    }

    public String getNodeIdOfObservers() {
        return nodeIdOfObservers;
    }

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public String getStatusFile() {
        return statusFile;
    }

    public void setStatusFile(String statusFile) {
        this.statusFile = statusFile;
    }

    public int[] getIoRetryInterval() {
        return ioRetryInterval;
    }

    public void setIoRetryInterval(int[] ioRetryInterval) {
        this.ioRetryInterval = ioRetryInterval;
    }

    public boolean isSyncForce() {
        return syncForce;
    }

    public void setSyncForce(boolean syncForce) {
        this.syncForce = syncForce;
    }

    public int getMaxReplicateItems() {
        return maxReplicateItems;
    }

    public void setMaxReplicateItems(int maxReplicateItems) {
        this.maxReplicateItems = maxReplicateItems;
    }

    public long getMaxReplicateBytes() {
        return maxReplicateBytes;
    }

    public void setMaxReplicateBytes(long maxReplicateBytes) {
        this.maxReplicateBytes = maxReplicateBytes;
    }

    public int getSingleReplicateLimit() {
        return singleReplicateLimit;
    }

    public void setSingleReplicateLimit(int singleReplicateLimit) {
        this.singleReplicateLimit = singleReplicateLimit;
    }

    public int getMaxPendingRaftTasks() {
        return maxPendingRaftTasks;
    }

    public void setMaxPendingRaftTasks(int maxPendingRaftTasks) {
        this.maxPendingRaftTasks = maxPendingRaftTasks;
    }

    public long getMaxPendingTaskBytes() {
        return maxPendingTaskBytes;
    }

    public void setMaxPendingTaskBytes(long maxPendingTaskBytes) {
        this.maxPendingTaskBytes = maxPendingTaskBytes;
    }

    public boolean isStaticConfig() {
        return staticConfig;
    }

    public void setStaticConfig(boolean staticConfig) {
        this.staticConfig = staticConfig;
    }

    public int getIdxCacheSize() {
        return idxCacheSize;
    }

    public void setIdxCacheSize(int idxCacheSize) {
        this.idxCacheSize = idxCacheSize;
    }

    public int getIdxFlushThreshold() {
        return idxFlushThreshold;
    }

    public void setIdxFlushThreshold(int idxFlushThreshold) {
        this.idxFlushThreshold = idxFlushThreshold;
    }

    public boolean isIoCallbackUseGroupExecutor() {
        return ioCallbackUseGroupExecutor;
    }

    public void setIoCallbackUseGroupExecutor(boolean ioCallbackUseGroupExecutor) {
        this.ioCallbackUseGroupExecutor = ioCallbackUseGroupExecutor;
    }

    public PerfCallback getPerfCallback() {
        return perfCallback;
    }

    public void setPerfCallback(PerfCallback perfCallback) {
        this.perfCallback = perfCallback;
    }

    public long getSaveSnapshotMillis() {
        return saveSnapshotMillis;
    }

    public void setSaveSnapshotMillis(long saveSnapshotMillis) {
        this.saveSnapshotMillis = saveSnapshotMillis;
    }

    public int getDiskSnapshotConcurrency() {
        return diskSnapshotConcurrency;
    }

    public void setDiskSnapshotConcurrency(int diskSnapshotConcurrency) {
        this.diskSnapshotConcurrency = diskSnapshotConcurrency;
    }

    public int getDiskSnapshotBufferSize() {
        return diskSnapshotBufferSize;
    }

    public void setDiskSnapshotBufferSize(int diskSnapshotBufferSize) {
        this.diskSnapshotBufferSize = diskSnapshotBufferSize;
    }

    public int getReplicateSnapshotConcurrency() {
        return replicateSnapshotConcurrency;
    }

    public void setReplicateSnapshotConcurrency(int replicateSnapshotConcurrency) {
        this.replicateSnapshotConcurrency = replicateSnapshotConcurrency;
    }

    public int getReplicateSnapshotBufferSize() {
        return replicateSnapshotBufferSize;
    }

    public void setReplicateSnapshotBufferSize(int replicateSnapshotBufferSize) {
        this.replicateSnapshotBufferSize = replicateSnapshotBufferSize;
    }

    public int getSnapshotConcurrency() {
        return snapshotConcurrency;
    }

    public void setSnapshotConcurrency(int snapshotConcurrency) {
        this.snapshotConcurrency = snapshotConcurrency;
    }
}
