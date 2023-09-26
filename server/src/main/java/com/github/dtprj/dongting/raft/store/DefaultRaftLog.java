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

import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.raft.impl.FileUtil;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.impl.TailCache;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;
import com.github.dtprj.dongting.raft.server.UnrecoverableException;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class DefaultRaftLog implements RaftLog {

    private final RaftGroupConfig groupConfig;
    private final Timestamp ts;
    private final RaftStatusImpl raftStatus;
    private final StatusManager statusManager;
    private LogAppender2 logAppender2;
    LogFileQueue logFiles;
    IdxFileQueue idxFiles;

    private long lastTaskNanos;
    private static final long TASK_INTERVAL_NANOS = 10 * 1000 * 1000 * 1000L;

    private static final String KEY_TRUNCATE = "truncate";

    int idxItemsPerFile = IdxFileQueue.DEFAULT_ITEMS_PER_FILE;
    int idxMaxCacheItems = IdxFileQueue.DEFAULT_MAX_CACHE_ITEMS;
    long logFileSize = LogFileQueue.DEFAULT_LOG_FILE_SIZE;
    int logWriteBufferSize = LogFileQueue.DEFAULT_WRITE_BUFFER_SIZE;


    public DefaultRaftLog(RaftGroupConfig groupConfig, StatusManager statusManager) {
        this.groupConfig = groupConfig;
        this.ts = groupConfig.getTs();
        this.raftStatus = (RaftStatusImpl) groupConfig.getRaftStatus();
        this.statusManager = statusManager;

        this.lastTaskNanos = ts.getNanoTime();
    }

    @Override
    public Pair<Integer, Long> init(Supplier<Boolean> stopIndicator) throws Exception {
        try {
            File dataDir = FileUtil.ensureDir(groupConfig.getDataDir());
            ByteBuffer buffer = ByteBuffer.allocateDirect(logWriteBufferSize);

            idxFiles = new IdxFileQueue(FileUtil.ensureDir(dataDir, "idx"),
                    statusManager, groupConfig, idxItemsPerFile, idxMaxCacheItems);
            logFiles = new LogFileQueue(FileUtil.ensureDir(dataDir, "log"),
                    groupConfig, idxFiles, logFileSize, logWriteBufferSize);
            logFiles.init();
            RaftUtil.checkStop(stopIndicator);
            idxFiles.init();
            RaftUtil.checkStop(stopIndicator);

            logAppender2 = new LogAppender2(idxFiles, logFiles, groupConfig, buffer);

            Pair<Long, Long> p = idxFiles.initRestorePos();

            String truncateStatus = statusManager.getProperties().getProperty(KEY_TRUNCATE);
            if (truncateStatus != null) {
                String[] parts = truncateStatus.split(",");
                if (parts.length == 2) {
                    long start = Long.parseLong(parts[0]);
                    long end = Long.parseLong(parts[1]);
                    logFiles.syncTruncateTail(start, end);
                    statusManager.getProperties().remove(KEY_TRUNCATE);
                    statusManager.persistSync();
                }
            }
            RaftUtil.checkStop(stopIndicator);

            int lastTerm = logFiles.restore(p.getLeft(), p.getRight(), stopIndicator);
            RaftUtil.checkStop(stopIndicator);

            if (idxFiles.getNextIndex() == 1) {
                logAppender2.setNextPersistIndex(1);
                return new Pair<>(0, 0L);
            } else {
                long lastIndex = idxFiles.getNextIndex() - 1;
                logAppender2.setNextPersistIndex(idxFiles.getNextIndex());
                return new Pair<>(lastTerm, lastIndex);
            }
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    @Override
    public void close() {
        DtUtil.close(idxFiles, logFiles);
    }

    public void tryPersist(TailCache tailCache) throws Exception {
        logAppender2.append(tailCache);
    }

    @Override
    public CompletableFuture<Void> append(List<LogItem> logs) throws Exception {
        if (logs == null || logs.size() == 0) {
            BugLog.getLog().error("append log with empty logs");
            return CompletableFuture.completedFuture(null);
        }
        try {
            long firstIndex = logs.get(0).getIndex();
            DtUtil.checkPositive(firstIndex, "firstIndex");
            if (firstIndex == idxFiles.getNextIndex()) {
                logFiles.append(logs);
            } else if (firstIndex < idxFiles.getNextIndex()) {
                if (firstIndex < idxFiles.getFirstIndex()) {
                    throw new UnrecoverableException("index too small: " + firstIndex);
                }
                Long firstIndexPos = idxFiles.loadLogPos(firstIndex).get();
                if (firstIndexPos < logFiles.queueStartPosition) {
                    throw new UnrecoverableException("position too small: " + firstIndexPos);
                }
                truncateTail(firstIndex, firstIndexPos);
                logFiles.append(logs);
            } else {
                throw new UnrecoverableException("index too large: " + firstIndex);
            }
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private void truncateTail(long firstIndex, long firstPos) throws Exception {
        idxFiles.truncateTail(firstIndex);
        Properties props = statusManager.getProperties();
        props.setProperty(KEY_TRUNCATE, firstPos + "," + logFiles.getWritePos());
        statusManager.persistSync();
        logFiles.syncTruncateTail(firstPos, logFiles.getWritePos());
        props.remove(KEY_TRUNCATE);
        statusManager.persistSync();
    }

    @Override
    public LogIterator openIterator(Supplier<Boolean> cancelIndicator) {
        return new FileLogLoader(idxFiles, logFiles, groupConfig, cancelIndicator);
    }

    @Override
    public CompletableFuture<Pair<Integer, Long>> tryFindMatchPos(int suggestTerm, long suggestIndex,
                                                                  Supplier<Boolean> cancelIndicator) {
        suggestIndex = Math.min(suggestIndex, raftStatus.getLastLogIndex());
        return logFiles.tryFindMatchPos(suggestTerm, suggestIndex, cancelIndicator);
    }

    @Override
    public void markTruncateByIndex(long index, long delayMillis) {
        long bound = Math.min(raftStatus.getLastApplied(), idxFiles.getNextPersistIndex());
        bound = Math.min(bound, index);
        logFiles.markDelete(bound, Long.MAX_VALUE, delayMillis);
    }

    @Override
    public void markTruncateByTimestamp(long timestampBound, long delayMillis) {
        long bound = Math.min(raftStatus.getLastApplied(), idxFiles.getNextPersistIndex());
        logFiles.markDelete(bound, timestampBound, delayMillis);
    }

    @Override
    public void doDelete() {
        if (ts.getNanoTime() - lastTaskNanos > TASK_INTERVAL_NANOS) {
            logFiles.submitDeleteTask(ts.getWallClockMillis());
            idxFiles.submitDeleteTask(logFiles.getFirstIndex());
            lastTaskNanos = ts.getNanoTime();
        }
    }

}
