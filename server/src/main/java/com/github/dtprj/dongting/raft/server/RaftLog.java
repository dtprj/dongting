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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/**
 * Operations except init() of this interface should invoked in raft thread.
 *
 * @author huangli
 */
public interface RaftLog extends AutoCloseable {

    Pair<Integer, Long> init(ExecutorService ioExecutor) throws Exception;

    /**
     * Batch append logs. In some conditions, the commitIndex may be smaller than commitIndex
     * in previous append invoke, but it ensures that logs at index(<=commitIndex) never be re-write.
     */
    void append(long commitIndex, List<LogItem> logs) throws Exception;

    LogIterator openIterator(Supplier<Boolean> epochChange);

    /**
     * return -1 if the index can't find.
     */
    CompletableFuture<Long> nextIndexToReplicate(int remoteMaxTerm, long remoteMaxIndex, Supplier<Boolean> epochChange);

    /**
     * try to delete logs before the index(included).
     * @param index the index of the last log to be deleted
     * @param delayMillis delay millis to delete the logs, to wait read complete
     */
    void markTruncateByIndex(long index, long delayMillis);

    /**
     * try to delete logs before the timestamp(included).
     * @param timestampMillis the timestamp of the log
     * @param delayMillis delay millis to delete the logs, to wait read complete
     */
    void markTruncateByTimestamp(long timestampMillis, long delayMillis);

    interface LogIterator extends AutoCloseable {

        /**
         * load logs.
         *
         * @param index the index of the first log to be loaded
         * @param limit max number of logs to return
         * @param bytesLimit max bytes of logs to return, 0 means no limit
         * @return return log items, don't return null or empty array
         */
        CompletableFuture<List<LogItem>> next(long index, int limit, int bytesLimit);
    }
}
