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
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.FileUtil;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.impl.TailCache;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;

import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.github.dtprj.dongting.raft.store.IdxFileQueue.KEY_INSTALL_SNAPSHOT;
import static com.github.dtprj.dongting.raft.store.IdxFileQueue.KEY_NEXT_IDX_AFTER_INSTALL_SNAPSHOT;
import static com.github.dtprj.dongting.raft.store.IdxFileQueue.KEY_NEXT_POS_AFTER_INSTALL_SNAPSHOT;

/**
 * @author huangli
 */
public class DefaultRaftLog implements RaftLog {

    private final RaftGroupConfig groupConfig;
    private final Timestamp ts;
    private final RaftStatusImpl raftStatus;
    private final StatusManager statusManager;
    LogFileQueue logFiles;
    IdxFileQueue idxFiles;

    private long lastTaskNanos;
    private static final long TASK_INTERVAL_NANOS = 10 * 1000 * 1000 * 1000L;

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
    public Pair<Integer, Long> init(AppendCallback appendCallback) throws Exception {
        try {
            Supplier<Boolean> stopIndicator = raftStatus::isStop;
            File dataDir = FileUtil.ensureDir(groupConfig.getDataDir());

            idxFiles = new IdxFileQueue(FileUtil.ensureDir(dataDir, "idx"),
                    statusManager, groupConfig, idxItemsPerFile, idxMaxCacheItems);
            logFiles = new LogFileQueue(FileUtil.ensureDir(dataDir, "log"),
                    groupConfig, idxFiles, appendCallback, logFileSize, logWriteBufferSize);
            logFiles.init();
            RaftUtil.checkStop(stopIndicator);
            idxFiles.init();
            RaftUtil.checkStop(stopIndicator);

            Pair<Long, Long> p = idxFiles.initRestorePos();
            if (p == null) {
                raftStatus.setInstallSnapshot(true);
                return new Pair<>(0, 0L);
            }
            long restoreIndex = p.getLeft();
            long restoreIndexPos = p.getRight();
            long firstValidPos = Long.parseLong(statusManager.getProperties()
                    .getProperty(KEY_NEXT_POS_AFTER_INSTALL_SNAPSHOT, "0"));
            int lastTerm = logFiles.restore(restoreIndex, restoreIndexPos, firstValidPos, stopIndicator);
            RaftUtil.checkStop(stopIndicator);

            if (idxFiles.getNextIndex() == 1) {
                return new Pair<>(0, 0L);
            } else {
                long lastIndex = idxFiles.getNextIndex() - 1;
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

    @Override
    public void append() {
        try {
            TailCache tailCache = raftStatus.getTailCache();
            logFiles.append(tailCache);
        } catch (InterruptedException e) {
            throw new RaftException(e);
        }
    }

    public void truncateTail(long index) {
        TailCache tailCache = raftStatus.getTailCache();
        tailCache.truncate(index);

        if (index < idxFiles.getNextIndex()) {
            idxFiles.truncateTail(index);
        }
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

    @Override
    public void beginInstall() throws Exception {
        statusManager.getProperties().setProperty(KEY_INSTALL_SNAPSHOT, "true");
        statusManager.persistSync();
        logFiles.forceDeleteAll();
        idxFiles.beginInstall();
    }

    @Override
    public void finishInstall(long nextLogIndex, long nextLogPos) throws Exception {
        logFiles.finishInstall(nextLogIndex, nextLogPos);
        idxFiles.finishInstall(nextLogIndex);
        statusManager.getProperties().remove(KEY_INSTALL_SNAPSHOT);
        statusManager.getProperties().setProperty(KEY_NEXT_IDX_AFTER_INSTALL_SNAPSHOT, String.valueOf(nextLogIndex));
        statusManager.getProperties().setProperty(KEY_NEXT_POS_AFTER_INSTALL_SNAPSHOT, String.valueOf(nextLogPos));
        statusManager.persistSync();
    }

    @Override
    public long syncLoadNextItemPos(long index) throws Exception {
        long pos = idxFiles.loadLogPos(index).get();
        return logFiles.syncGetNextIndexPos(pos);
    }
}
