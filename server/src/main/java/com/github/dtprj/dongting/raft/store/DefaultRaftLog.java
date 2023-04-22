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

import com.github.dtprj.dongting.buf.ByteBufferPool;
import com.github.dtprj.dongting.buf.RefByteBuffer;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.client.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftExecutor;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;
import com.github.dtprj.dongting.raft.server.RaftLog;

import java.io.File;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class DefaultRaftLog implements RaftLog {
    private static final DtLog log = DtLogs.getLogger(DefaultRaftLog.class);
    private static final String KNOWN_MAX_COMMIT_INDEX_KEY = "knownMaxCommitIndex";
    private static final String DELETE_MARK_INDEX_KEY = "deleteMarkIndex";

    private final RaftGroupConfig groupConfig;
    private final Timestamp ts;
    private final ByteBufferPool heapPool;
    private final ByteBufferPool directPool;
    private final Executor ioExecutor;
    private final RaftExecutor raftExecutor;
    private final Supplier<Boolean> stopIndicator;
    private LogFileQueue logFiles;
    private IdxFileQueue idxFiles;
    private long knownMaxCommitIndex;
    private long deleteMarkIndex;
    private StatusFile checkpointFile;

    private long lastDeleteNanos;
    private static final long DELETE_INTERVAL_NANOS = 10 * 1000 * 1000 * 1000L;

    public DefaultRaftLog(RaftGroupConfig groupConfig, Timestamp ts, ByteBufferPool heapPool, ByteBufferPool directPool,
                          Executor ioExecutor, RaftExecutor raftExecutor, Supplier<Boolean> stopIndicator) {
        this.groupConfig = groupConfig;
        this.ts = ts;
        this.heapPool = heapPool;
        this.directPool = directPool;
        this.ioExecutor = ioExecutor;
        this.raftExecutor = raftExecutor;
        this.stopIndicator = stopIndicator;

        this.lastDeleteNanos = ts.getNanoTime();
    }

    @Override
    public Pair<Integer, Long> init() throws Exception {
        File dataDir = FileUtil.ensureDir(groupConfig.getDataDir());
        checkpointFile = new StatusFile(new File(dataDir, "checkpoint"));
        checkpointFile.init();
        knownMaxCommitIndex = Long.parseLong(checkpointFile.getProperties().getProperty(KNOWN_MAX_COMMIT_INDEX_KEY, "0"));
        deleteMarkIndex = Long.parseLong(checkpointFile.getProperties().getProperty(DELETE_MARK_INDEX_KEY, "0"));
        idxFiles = new IdxFileQueue(FileUtil.ensureDir(dataDir, "idx"), ioExecutor, raftExecutor, stopIndicator, heapPool, directPool);
        logFiles = new LogFileQueue(FileUtil.ensureDir(dataDir, "log"), ioExecutor, raftExecutor, stopIndicator, idxFiles, heapPool, directPool);
        logFiles.init();
        idxFiles.init();
        idxFiles.initWithCommitIndex(knownMaxCommitIndex);
        long commitIndexPos;
        if (knownMaxCommitIndex > 0) {
            commitIndexPos = idxFiles.findLogPosInMemCache(knownMaxCommitIndex);
            if (commitIndexPos < 0) {
                commitIndexPos = idxFiles.syncLoadLogPos(knownMaxCommitIndex);
            }
        } else {
            commitIndexPos = 0;
        }
        int lastTerm = logFiles.restore(knownMaxCommitIndex, commitIndexPos);
        if (idxFiles.getNextIndex() == 1) {
            return new Pair<>(0, 0L);
        } else {
            long lastIndex = idxFiles.getNextIndex() - 1;
            return new Pair<>(lastTerm, lastIndex);
        }
    }

    @Override
    public void close() {
        logFiles.close();
        idxFiles.close();
    }

    @Override
    public void append(long commitIndex, List<LogItem> logs) throws Exception {
        if (logs == null || logs.size() == 0) {
            BugLog.getLog().error("append log with empty logs");
            return;
        }
        knownMaxCommitIndex = Math.max(knownMaxCommitIndex, commitIndex);
        long firstIndex = logs.get(0).getIndex();
        DtUtil.checkPositive(firstIndex, "firstIndex");
        if (firstIndex == idxFiles.getNextIndex()) {
            logFiles.append(logs);
        } else if (firstIndex < idxFiles.getNextIndex()) {
            long dataPosition = idxFiles.truncateTail(firstIndex);
            logFiles.truncateTail(dataPosition);
            logFiles.append(logs);
        } else {
            throw new RaftException("bad index: " + firstIndex);
        }
        if (ts.getNanoTime() - lastDeleteNanos > DELETE_INTERVAL_NANOS) {
            delete();
        }
    }

    @Override
    public LogIterator openIterator(Supplier<Boolean> epochChange) {
        return new DefaultLogIterator(this,
                RefByteBuffer.createPlain(directPool, 1024 * 1024, 0),
                () -> stopIndicator.get() || epochChange.get());
    }

    public CompletableFuture<List<LogItem>> next(DefaultLogIterator it, long index, int limit, int bytesLimit) {
        try {
            if (index != it.nextIndex) {
                it.resetBuffer();
                it.nextIndex = index;
            }
            long pos = idxFiles.syncLoadLogPos(index);
            CompletableFuture<List<LogItem>> result = logFiles.loadLog(pos, it, limit, bytesLimit);
            result.exceptionally(e -> {
                it.resetBuffer();
                return null;
            });
            return result;
        } catch (Throwable e) {
            it.resetBuffer();
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Long> nextIndexToReplicate(int remoteMaxTerm, long remoteMaxIndex) {
        return null;
    }

    @Override
    public void markTruncateByIndex(long index, long delayMillis) {
        index = Math.min(index, knownMaxCommitIndex - 1);
        if (index <= 0) {
            return;
        }
        this.deleteMarkIndex = index;
        long deleteTimestamp = ts.getWallClockMillis() + delayMillis;
        logFiles.markDeleteByIndex(index, deleteTimestamp);
    }

    @Override
    public void markTruncateByTimestamp(long timestampMillis, long delayMillis) {
        long deleteTimestamp = ts.getWallClockMillis() + delayMillis;
        logFiles.markDeleteByTimestamp(knownMaxCommitIndex, timestampMillis, deleteTimestamp);
    }

    private void delete() {
        logFiles.submitDeleteTask(ts.getWallClockMillis());
        idxFiles.submitDeleteTask(logFiles.getFirstIndex());
    }

}
