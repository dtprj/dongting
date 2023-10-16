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

import com.github.dtprj.dongting.codec.Encoder;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.impl.TailCache;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class MemRaftLog implements RaftLog {

    private final IndexedQueue<MemLog> logs;
    private final Timestamp ts;
    private final RaftStatusImpl raftStatus;
    private final RaftGroupConfig groupConfig;
    private final int maxItems;
    private boolean closed;
    private AppendCallback appendCallback;

    static final class MemLog {
        LogItem item;
        long deleteTimestamp;
        int flowControlSize;
    }


    public MemRaftLog(RaftGroupConfig groupConfig, int maxItems) {
        this.ts = groupConfig.getTs();
        this.raftStatus = (RaftStatusImpl) groupConfig.getRaftStatus();
        this.groupConfig = groupConfig;
        this.maxItems = maxItems;
        logs = new IndexedQueue<>(1024);
    }

    @Override
    public Pair<Integer, Long> init(AppendCallback appendCallback) throws Exception {
        this.appendCallback = appendCallback;
        return new Pair<>(0, 0L);
    }

    @Override
    public void append() {
        TailCache tailCache = raftStatus.getTailCache();
        if (tailCache.size() == 0) {
            BugLog.getLog().error("tailCache.size() == 0");
            return;
        }
        IndexedQueue<MemLog> logs = this.logs;
        long lastPersistIndex = -1;
        int lastPersistTerm = -1;
        for (long i = raftStatus.getLastLogIndex() + 1; i <= tailCache.getLastIndex(); i++) {
            RaftTask rt = tailCache.get(i);
            LogItem logItem = rt.getItem();
            MemLog it = new MemLog();
            it.item = logItem;
            it.item.retain();
            logs.addLast(it);
            lastPersistIndex = logItem.getIndex();
            lastPersistTerm = logItem.getTerm();
        }
        if (logs.size() > maxItems) {
            doDelete();
        }
        if (lastPersistIndex != -1) {
            raftStatus.setLastLogIndex(lastPersistIndex);
            raftStatus.setLastLogTerm(lastPersistTerm);
            appendCallback.finish(lastPersistTerm, lastPersistIndex);
        }
    }

    @Override
    public void truncateTail(long index) {
        TailCache tailCache = raftStatus.getTailCache();
        tailCache.truncate(index);
    }

    @Override
    public LogIterator openIterator(Supplier<Boolean> cancelIndicator) {
        return new LogIterator() {
            @Override
            public CompletableFuture<List<LogItem>> next(long index, int limit, int bytesLimit) {
                if (closed || cancelIndicator.get()) {
                    return CompletableFuture.failedFuture(new CancellationException());
                }
                IndexedQueue<MemLog> logs = MemRaftLog.this.logs;
                if (logs.size() == 0) {
                    return CompletableFuture.completedFuture(Collections.emptyList());
                }
                MemLog first = logs.get(0);
                long logIndex = index - first.item.getIndex();
                if (logIndex < 0 || logIndex >= logs.size()) {
                    return CompletableFuture.failedFuture(new RaftException("bad index " + index +
                            ", fist index is " + first.item.getIndex()));
                }
                ArrayList<LogItem> list = new ArrayList<>(limit);
                while (logIndex < logs.size()) {
                    MemLog it = logs.get((int) logIndex);
                    LogItem li = it.item;
                    if (it.flowControlSize == 0 && li.getType() == LogItem.TYPE_NORMAL) {
                        @SuppressWarnings("rawtypes")
                        Encoder encoder = groupConfig.getCodecFactory().createBodyEncoder(li.getBizType());
                        //noinspection unchecked
                        it.flowControlSize = encoder.actualSize(li.getBody());
                    }
                    bytesLimit -= it.flowControlSize;
                    if (!list.isEmpty() && bytesLimit < 0) {
                        break;
                    }
                    li.retain();
                    list.add(li);
                    logIndex++;
                    if (list.size() >= limit) {
                        break;
                    }
                }
                return CompletableFuture.completedFuture(list);
            }

            @Override
            public void close() {
            }
        };
    }

    @Override
    public CompletableFuture<Pair<Integer, Long>> tryFindMatchPos(int suggestTerm, long suggestIndex,
                                                                  Supplier<Boolean> cancelIndicator) {
        return CompletableFuture.completedFuture(tryFindMatchPos0(suggestTerm, suggestIndex));
    }

    private Pair<Integer, Long> tryFindMatchPos0(int suggestTerm, long suggestIndex) {
        IndexedQueue<MemLog> logs = this.logs;
        if (logs.size() == 0) {
            return null;
        }
        int left = 0;
        int right = logs.size() - 1;
        while (left <= right) {
            int mid = (left + right + 1) >>> 1;
            MemLog memLog = logs.get(mid);
            if (memLog.deleteTimestamp > 0) {
                left = mid + 1;
                continue;
            }
            LogItem i = memLog.item;
            if (i.getIndex() == suggestIndex && i.getTerm() == suggestTerm) {
                return new Pair<>(i.getTerm(), i.getIndex());
            } else if (i.getIndex() < suggestIndex && i.getTerm() <= suggestTerm) {
                if (left == right) {
                    return new Pair<>(i.getTerm(), i.getIndex());
                } else {
                    left = mid;
                }
            } else {
                right = mid - 1;
            }
        }
        return null;
    }

    @Override
    public void markTruncateByIndex(long index, long delayMillis) {
        markDelete(delayMillis, li -> li.getIndex() < index
                && li.getIndex() < raftStatus.getLastApplied());
    }

    @Override
    public void markTruncateByTimestamp(long timestampBound, long delayMillis) {
        markDelete(delayMillis, li -> li.getTimestamp() < timestampBound
                && li.getIndex() < raftStatus.getLastApplied());
    }

    private void markDelete(long delayMillis, Predicate<LogItem> predicate) {
        IndexedQueue<MemLog> logs = this.logs;
        int len = logs.size();
        long deleteTimestamp = ts.getWallClockMillis() + delayMillis;
        for (int i = 0; i < len; i++) {
            MemLog memLog = logs.get(i);
            if (predicate.test(memLog.item)) {
                if (memLog.deleteTimestamp == 0) {
                    memLog.deleteTimestamp = deleteTimestamp;
                } else {
                    memLog.deleteTimestamp = Math.min(deleteTimestamp, memLog.deleteTimestamp);
                }
            } else {
                return;
            }
        }
    }

    @Override
    public void doDelete() {
        IndexedQueue<MemLog> logs = this.logs;
        while (logs.size() > 0) {
            MemLog memLog = logs.get(0);
            if (memLog.deleteTimestamp > 0 && memLog.deleteTimestamp < ts.getWallClockMillis()) {
                memLog.item.release();
                logs.removeFirst();
            } else if (logs.size() > maxItems && memLog.item.getIndex() < raftStatus.getLastApplied()) {
                memLog.item.release();
                logs.removeFirst();
            } else {
                break;
            }
        }
    }

    @Override
    public void syncClear(long nextLogIndex, long nextLogPos) {
        IndexedQueue<MemLog> logs = this.logs;
        while (logs.size() > 0) {
            MemLog memLog = logs.get(0);
            memLog.item.release();
            logs.removeFirst();
        }
    }

    @Override
    public long syncLoadNextItemPos(long index) {
        return 0;
    }

    @Override
    public void close() throws Exception {
        closed = true;
        MemLog i;
        while ((i = logs.removeFirst()) != null) {
            i.item.release();
        }
    }

    // for test
    IndexedQueue<MemLog> getLogs() {
        return logs;
    }
}
