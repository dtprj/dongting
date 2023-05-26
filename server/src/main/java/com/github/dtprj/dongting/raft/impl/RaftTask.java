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
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftOutput;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
public class RaftTask {

    public final int type;
    @SuppressWarnings("rawtypes")
    public final RaftInput input;
    final CompletableFuture<RaftOutput> future;
    public final long createTimeNanos;

    LogItem item;

    ArrayList<RaftTask> nextReaders;

    public RaftTask(Timestamp ts, int type, @SuppressWarnings("rawtypes") RaftInput input,
                    CompletableFuture<RaftOutput> future) {
        this.createTimeNanos = ts.getNanoTime();
        this.type = type;
        this.input = input;
        this.future = future;
    }



    public void addNext(RaftTask t) {
        if (nextReaders == null) {
            nextReaders = new ArrayList<>();
        }
        nextReaders.add(t);
    }
}
