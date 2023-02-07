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

import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftLog;
import com.github.dtprj.dongting.raft.server.StateMachine;

import java.util.List;

/**
 * @author huangli
 */
public class MemRaftLog extends RaftLog {
    @Override
    public Pair<Integer, Long> init(StateMachine stateMachine) {
        return null;
    }

    @Override
    public void append(long prevLogIndex, int prevLogTerm, List<LogItem> logs) {
    }

    @Override
    public LogItem load(long index) {
        return null;
    }

    @Override
    public LogItem[] load(long index, int limit, long bytesLimit) {
        return new LogItem[0];
    }

    @Override
    public int getTermOf(long index) {
        return 0;
    }

    @Override
    public long findMaxIndexByTerm(int term) {
        return 0;
    }

    @Override
    public int findLastTermLessThan(int term) {
        return 0;
    }
}
