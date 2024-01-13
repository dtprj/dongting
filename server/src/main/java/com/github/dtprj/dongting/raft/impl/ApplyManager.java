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

import com.github.dtprj.dongting.buf.RefBufferFactory;
import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.common.RefCount;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.server.RaftExecTimeoutException;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftOutput;
import com.github.dtprj.dongting.raft.sm.StateMachine;
import com.github.dtprj.dongting.raft.store.RaftLog;
import com.github.dtprj.dongting.raft.store.StatusManager;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class ApplyManager {
    private static final DtLog log = DtLogs.getLogger(ApplyManager.class);

    private final int selfNodeId;
    private final RaftLog raftLog;
    private final StateMachine stateMachine;
    private final Timestamp ts;
    private final EventBus eventBus;
    private final RaftStatusImpl raftStatus;
    private final StatusManager statusManager;

    private final DecodeContext decodeContext;

    private boolean configChanging = false;

    private boolean waiting;

    private RaftLog.LogIterator logIterator;

    public ApplyManager(int selfNodeId, RaftLog raftLog, StateMachine stateMachine,
                        RaftStatusImpl raftStatus, EventBus eventBus,
                        RefBufferFactory heapPool, StatusManager statusManager) {
        this.selfNodeId = selfNodeId;
        this.raftLog = raftLog;
        this.stateMachine = stateMachine;
        this.ts = raftStatus.getTs();
        this.raftStatus = raftStatus;
        this.eventBus = eventBus;
        this.decodeContext = new DecodeContext();
        this.decodeContext.setHeapPool(heapPool);
        this.statusManager = statusManager;
    }

    public void execRead(long index, RaftTask rt) {
        RaftInput input = rt.getInput();
        CompletableFuture<RaftOutput> future = rt.getFuture();
        try {
            if (input.getDeadline() != null && input.getDeadline().isTimeout(ts)) {
                future.completeExceptionally(new RaftExecTimeoutException("timeout "
                        + input.getDeadline().getTimeout(TimeUnit.MILLISECONDS) + "ms"));
            }
            Object r = stateMachine.exec(index, input);
            future.complete(new RaftOutput(index, r));
        } catch (Throwable e) {
            log.error("exec read failed. {}", e);
            future.completeExceptionally(e);
        } finally {
            // for read task, no LogItem generated
            if (input.getHeader() instanceof RefCount) {
                ((RefCount) input.getHeader()).release();
            }
            if (input.getBody() instanceof RefCount) {
                ((RefCount) input.getBody()).release();
            }
        }
    }

}
