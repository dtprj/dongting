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

import com.github.dtprj.dongting.buf.RefBufferFactory;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.raft.sm.RaftCodecFactory;

import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class RaftGroupConfig {
    private final int groupId;
    private final String nodeIdOfMembers;
    private final String nodeIdOfObservers;
    private String dataDir = "./data";
    private String statusFile = "raft.status";
    private long[] ioRetryInterval = new long[]{100, 1000, 3000, 5000, 10000, 20000};

    private Timestamp ts;
    private RefBufferFactory heapPool;
    private RefBufferFactory directPool;
    private ExecutorService ioExecutor;
    private FiberGroup fiberGroup;
    private Supplier<Boolean> stopIndicator;
    private RaftStatus raftStatus;
    private RaftCodecFactory codecFactory;

    public RaftGroupConfig(int groupId, String nodeIdOfMembers, String nodeIdOfObservers) {
        this.groupId = groupId;
        this.nodeIdOfMembers = nodeIdOfMembers;
        this.nodeIdOfObservers = nodeIdOfObservers;
    }

    public String getNodeIdOfMembers() {
        return nodeIdOfMembers;
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

    public int getGroupId() {
        return groupId;
    }


    public String getNodeIdOfObservers() {
        return nodeIdOfObservers;
    }

    public Timestamp getTs() {
        return ts;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public RefBufferFactory getHeapPool() {
        return heapPool;
    }

    public void setHeapPool(RefBufferFactory heapPool) {
        this.heapPool = heapPool;
    }

    public RefBufferFactory getDirectPool() {
        return directPool;
    }

    public void setDirectPool(RefBufferFactory directPool) {
        this.directPool = directPool;
    }

    public FiberGroup getFiberGroup() {
        return fiberGroup;
    }

    public void setFiberGroup(FiberGroup fiberGroup) {
        this.fiberGroup = fiberGroup;
    }

    public Supplier<Boolean> getStopIndicator() {
        return stopIndicator;
    }

    public void setStopIndicator(Supplier<Boolean> stopIndicator) {
        this.stopIndicator = stopIndicator;
    }

    public RaftStatus getRaftStatus() {
        return raftStatus;
    }

    public void setRaftStatus(RaftStatus raftStatus) {
        this.raftStatus = raftStatus;
    }

    public RaftCodecFactory getCodecFactory() {
        return codecFactory;
    }

    public void setCodecFactory(RaftCodecFactory codecFactory) {
        this.codecFactory = codecFactory;
    }

    public ExecutorService getIoExecutor() {
        return ioExecutor;
    }

    public void setIoExecutor(ExecutorService ioExecutor) {
        this.ioExecutor = ioExecutor;
    }

    public long[] getIoRetryInterval() {
        return ioRetryInterval;
    }

    public void setIoRetryInterval(long[] ioRetryInterval) {
        this.ioRetryInterval = ioRetryInterval;
    }
}
