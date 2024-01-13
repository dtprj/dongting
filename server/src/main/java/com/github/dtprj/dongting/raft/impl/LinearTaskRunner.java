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
import com.github.dtprj.dongting.fiber.FiberChannel;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftOutput;
import com.github.dtprj.dongting.raft.store.RaftLog;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * @author huangli
 */
public class LinearTaskRunner implements BiConsumer<EventType, Object> {

    private static final DtLog log = DtLogs.getLogger(LinearTaskRunner.class);

    private final ReplicateManager replicateManager;
    private final ApplyManager applyManager;

    private final RaftLog raftLog;
    private final RaftStatusImpl raftStatus;

    private final Timestamp ts;

    private FiberChannel<RaftTask> taskChannel;

    public LinearTaskRunner(RaftStatusImpl raftStatus, RaftLog raftLog, ApplyManager applyManager,
                            ReplicateManager replicateManager) {
        this.raftStatus = raftStatus;
        this.raftLog = raftLog;
        this.ts = raftStatus.getTs();

        this.applyManager = applyManager;
        this.replicateManager = replicateManager;
    }

    @Override
    public void accept(EventType eventType, Object o) {
    }

    public void init(FiberChannel<RaftTask> taskChannel) {
        this.taskChannel = taskChannel;
    }

    public CompletableFuture<RaftOutput> submitRaftTaskInBizThread(RaftInput input) {
        CompletableFuture f = new CompletableFuture<>();
        RaftTask t = new RaftTask(raftStatus.getTs(), LogItem.TYPE_NORMAL, input, f);
        taskChannel.fireOffer(t);
        return f;
    }
}
