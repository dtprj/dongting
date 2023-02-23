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
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftExecTimeoutException;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftOutput;
import com.github.dtprj.dongting.raft.server.StateMachine;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
class ApplyManager {

    private final StateMachine stateMachine;
    private final Timestamp ts;

    ApplyManager(StateMachine stateMachine, Timestamp ts) {
        this.stateMachine = stateMachine;
        this.ts = ts;
    }


    public void execChain(long index, RaftTask rt) {
        exec(index, rt);
        if (rt.nextReaders == null) {
            return;
        }
        for (RaftTask readerTask : rt.nextReaders) {
            exec(index, readerTask);
        }
    }

    public void exec(long index, RaftTask rt) {
        if (rt.type == LogItem.TYPE_HEARTBEAT) {
            return;
        }
        RaftInput input = rt.input;
        CompletableFuture<RaftOutput> future = rt.future;
        if (input.isReadOnly() && input.getDeadline().isTimeout(ts)) {
            if (future != null) {
                future.completeExceptionally(new RaftExecTimeoutException("timeout "
                        + input.getDeadline().getTimeout(TimeUnit.MILLISECONDS) + "ms"));
            }
            return;
        }
        try {
            Object result = stateMachine.exec(index, input);
            if (future != null) {
                future.complete(new RaftOutput(index, result));
            }
        } catch (RuntimeException e) {
            if (input.isReadOnly()) {
                if (future != null) {
                    future.completeExceptionally(e);
                }
            } else {
                throw e;
            }
        }

    }
}
