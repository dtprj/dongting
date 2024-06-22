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

import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.fiber.FiberGroup;

import java.util.concurrent.ExecutorService;

/**
 * @author huangli
 */
public class RaftGroupConfigEx extends RaftGroupConfig {

    private Timestamp ts;
    private RaftStatus raftStatus;
    private ExecutorService blockIoExecutor;
    private FiberGroup fiberGroup;

    public RaftGroupConfigEx(int groupId, String nodeIdOfMembers, String nodeIdOfObservers) {
        super(groupId, nodeIdOfMembers, nodeIdOfObservers);
    }

    public Timestamp getTs() {
        return ts;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public FiberGroup getFiberGroup() {
        return fiberGroup;
    }

    public void setFiberGroup(FiberGroup fiberGroup) {
        this.fiberGroup = fiberGroup;
    }

    public RaftStatus getRaftStatus() {
        return raftStatus;
    }

    public void setRaftStatus(RaftStatus raftStatus) {
        this.raftStatus = raftStatus;
    }

    public ExecutorService getBlockIoExecutor() {
        return blockIoExecutor;
    }

    public void setBlockIoExecutor(ExecutorService blockIoExecutor) {
        this.blockIoExecutor = blockIoExecutor;
    }
}
