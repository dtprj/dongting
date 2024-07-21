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
import com.github.dtprj.dongting.raft.server.RaftCallback;
import com.github.dtprj.dongting.raft.server.RaftInput;

/**
 * @author huangli
 */
public class RaftTask {

    private final int type;
    private final RaftInput input;
    private final RaftCallback callback;
    private final long createTimeNanos;

    private LogItem item;

    public RaftTask(Timestamp ts, int type, RaftInput input, RaftCallback callback) {
        this.createTimeNanos = ts.getNanoTime();
        this.type = type;
        this.input = input;
        this.callback = callback;
    }

    public RaftCallback getCallback() {
        return callback;
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
}
