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

import com.github.dtprj.dongting.common.Pair;

import java.util.List;

/**
 * @author huangli
 */
public interface RaftLog {

    Pair<Integer, Long> init();

    void shutdown();

    void append(long prevLogIndex, int prevLogTerm, List<LogItem> logs);

    /**
     * load logs.
     *
     * @param index the index of the first log to be loaded
     * @param limit max number of logs to return
     * @param bytesLimit max bytes of logs to return, 0 means no limit
     * @return return log items, don't return null or empty array
     */
    LogItem[] load(long index, int limit, long bytesLimit);

    /**
     * if there is no such index, return -1.
     */
    int getTermOf(long index);

    /**
     * if there is no such term, return -1.
     */
    long findMaxIndexByTerm(int term);

    /**
     * if there is no such term, return -1.
     */
    int findLastTermLessThan(int term);

    /**
     * delete logs before the index(included), this method may invoke by other thread.
     */
    void truncate(long index);
}
