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
package com.github.dtprj.dongting.raft.store;

import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.raft.impl.RaftExecutor;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftLog;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class MemRaftLog implements RaftLog {

    private final IndexedQueue<LogItem> logs;
    private boolean closed;
    private final RaftExecutor raftExecutor;

    public MemRaftLog(RaftExecutor raftExecutor, int maxItems) {
        this.raftExecutor = raftExecutor;
        logs = new IndexedQueue<>();
    }

    @Override
    public Pair<Integer, Long> init(Supplier<Boolean> cancelInit) throws Exception {
        return new Pair<>(0, 0L);
    }

    @Override
    public void append(List<LogItem> logs) throws Exception {
    }

    @Override
    public LogIterator openIterator(Supplier<Boolean> epochChange) {
        return null;
    }

    @Override
    public CompletableFuture<Long> nextIndexToReplicate(int remoteMaxTerm, long remoteMaxIndex,
                                                        Supplier<Boolean> epochChange) {
        return CompletableFuture.completedFuture(nextIndexToReplicate0(remoteMaxTerm, remoteMaxIndex));
    }

    private long nextIndexToReplicate0(int remoteMaxTerm, long remoteMaxIndex) {
        IndexedQueue<LogItem> logs = this.logs;
        if (logs.size() == 0) {
            return -1L;
        }
        LogItem first = logs.get(0);
        LogItem last = logs.get(logs.size() - 1);
        int c = LogFileQueue.compare(last.getTerm(), last.getIndex(), remoteMaxTerm, remoteMaxIndex);
        if (c < 0) {
            return last.getIndex();
        }
        c = LogFileQueue.compare(first.getTerm(), first.getIndex(), remoteMaxTerm, remoteMaxIndex);
        if (c < 0) {
            return -1L;
        }
        int left = 0;
        int right = logs.size() - 1;
        while (left < right) {
            int mid = (left + right + 1) >>> 1;
            LogItem i = logs.get(mid);
            c = LogFileQueue.compare(i.getTerm(), i.getIndex(), remoteMaxTerm, remoteMaxIndex);
            if (c < 0) {
                left = mid;
            } else if (c > 0) {
                right = mid - 1;
            } else {
                return i.getIndex() < last.getIndex() ? i.getIndex() + 1 : i.getIndex();
            }
        }
        return left;
    }

    @Override
    public void markTruncateByIndex(long index, long delayMillis) {
        raftExecutor.schedule(() -> this.markTruncateByIndex0(index), delayMillis);
    }

    private void markTruncateByIndex0(long index) {
        IndexedQueue<LogItem> logs = this.logs;
        while (logs.size() > 0) {
            LogItem i = logs.get(0);
            if (i.getIndex() < index) {
                logs.removeFirst();
                i.release();
            } else {
                return;
            }
        }
    }

    @Override
    public void markTruncateByTimestamp(long timestampMillis, long delayMillis) {
        raftExecutor.schedule(() -> this.markTruncateByTimestamp0(timestampMillis), delayMillis);
    }

    private void markTruncateByTimestamp0(long timestampMillis) {
        IndexedQueue<LogItem> logs = this.logs;
        while (logs.size() > 0) {
            LogItem i = logs.get(0);
            if (i.getTimestamp() < timestampMillis) {
                logs.removeFirst();
                i.release();
            } else {
                return;
            }
        }
    }

    @Override
    public void close() throws Exception {
        closed = true;
        LogItem i;
        while ((i = logs.removeFirst()) != null) {
            i.release();
        }
    }

}
