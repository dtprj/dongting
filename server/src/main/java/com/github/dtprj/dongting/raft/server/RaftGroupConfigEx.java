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

import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class RaftGroupConfigEx extends RaftGroupConfig {

    private Timestamp ts;
    private RefBufferFactory heapPool;
    private ByteBufferPool directPool;
    private Executor raftExecutor;
    private Supplier<Boolean> stopIndicator;

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

    public Executor getRaftExecutor() {
        return raftExecutor;
    }

    public void setRaftExecutor(Executor raftExecutor) {
        this.raftExecutor = raftExecutor;
    }

    public Supplier<Boolean> getStopIndicator() {
        return stopIndicator;
    }

    public void setStopIndicator(Supplier<Boolean> stopIndicator) {
        this.stopIndicator = stopIndicator;
    }
}
