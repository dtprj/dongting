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

import com.github.dtprj.dongting.buf.ByteBufferPool;
import com.github.dtprj.dongting.buf.RefBufferFactory;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.raft.sm.RaftCodecFactory;

import java.util.concurrent.ExecutorService;

/**
 * @author huangli
 */
public class RaftGroupConfigEx extends RaftGroupConfig {

    private Timestamp ts;
    private RefBufferFactory heapPool;
    private ByteBufferPool directPool;
    private RaftStatus raftStatus;
    private ExecutorService ioExecutor;
    private FiberGroup fiberGroup;
    private RaftCodecFactory codecFactory;

    public RaftGroupConfigEx(int groupId, String nodeIdOfMembers, String nodeIdOfObservers) {
        super(groupId, nodeIdOfMembers, nodeIdOfObservers);
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

    public ByteBufferPool getDirectPool() {
        return directPool;
    }

    public void setDirectPool(ByteBufferPool directPool) {
        this.directPool = directPool;
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
}
