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

import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
public class RaftTask {

    private final int type;
    private final RaftInput input;
    private final CompletableFuture<RaftOutput> future;
    private final long createTimeNanos;

    private LogItem item;

    private RaftTask nextReader;

    private long index;

    public RaftTask(Timestamp ts, int type, RaftInput input,
                    CompletableFuture<RaftOutput> future) {
        this.createTimeNanos = ts.getNanoTime();
        this.type = type;
        this.input = input;
        this.future = future;
    }

    public void setNextReader(RaftTask nextReader) {
        this.nextReader = nextReader;
    }

    public RaftTask getNextReader() {
        return nextReader;
    }

    public CompletableFuture<RaftOutput> getFuture() {
        return future;
    }

    public LogItem getItem() {
        return item;
    }

    public void setItem(LogItem item) {
        this.item = item;
    }

    public long getCreateTimeNanos() {
        return createTimeNanos;
    }

    public RaftInput getInput() {
        return input;
    }

    public int getType() {
        return type;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }
}
