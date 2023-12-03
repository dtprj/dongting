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
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.raft.impl.FileUtil;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;

import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

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
    public FiberFrame<Pair<Integer, Long>> init(AppendCallback appendCallback) throws Exception {
        Supplier<Boolean> stopIndicator = raftStatus::isStop;
        return new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) throws Exception {
                File dataDir = FileUtil.ensureDir(groupConfig.getDataDir());

                idxFiles = new IdxFileQueue(FileUtil.ensureDir(dataDir, "idx"),
                        statusManager, groupConfig, idxItemsPerFile, idxMaxCacheItems);
                logFiles = new LogFileQueue(FileUtil.ensureDir(dataDir, "log"),
                        groupConfig, idxFiles, appendCallback, logFileSize, logWriteBufferSize);
                logFiles.init();
                RaftUtil.checkStop(stopIndicator);
                idxFiles.init();
                RaftUtil.checkStop(stopIndicator);

                return Fiber.call(idxFiles.initRestorePos(), this::afterIdxFileQueueInit);
            }

            private FrameCallResult afterIdxFileQueueInit(Pair<Long, Long> p) {
                if (p == null) {
                    raftStatus.setInstallSnapshot(true);
                    setResult(new Pair<>(0, 0L));
                    return Fiber.frameReturn();
                }
                long restoreIndex = p.getLeft();
                long restoreIndexPos = p.getRight();
                long firstValidPos = Long.parseLong(statusManager.getProperties()
                        .getProperty(KEY_NEXT_POS_AFTER_INSTALL_SNAPSHOT, "0"));
                return Fiber.call(logFiles.restore(restoreIndex, restoreIndexPos, firstValidPos, stopIndicator),
                        this::afterLogRestore);
            }

            private FrameCallResult afterLogRestore(int lastTerm) {
                RaftUtil.checkStop(stopIndicator);
                if (idxFiles.getNextIndex() == 1) {
                    setResult(new Pair<>(0, 0L));
                } else {
                    long lastIndex = idxFiles.getNextIndex() - 1;
                    setResult(new Pair<>(lastTerm, lastIndex));
                }
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) throws Throwable {
                close();
                throw ex;
            }
        };
    }

    @Override
    public void append() {

    }

    @Override
    public void truncateTail(long index) {

    }

    @Override
    public LogIterator openIterator(Supplier<Boolean> cancelIndicator) {
        return null;
    }

    @Override
    public CompletableFuture<Pair<Integer, Long>> tryFindMatchPos(int suggestTerm, long suggestIndex, Supplier<Boolean> cancelIndicator) {
        return null;
    }

    @Override
    public void markTruncateByIndex(long index, long delayMillis) {

    }

    @Override
    public void markTruncateByTimestamp(long timestampBound, long delayMillis) {

    }

    @Override
    public void doDelete() {

    }

    @Override
    public void beginInstall() throws Exception {

    }

    @Override
    public void finishInstall(long nextLogIndex, long nextLogPos) throws Exception {

    }

    @Override
    public long syncLoadNextItemPos(long index) throws Exception {
        return 0;
    }

    @Override
    public void close() throws Exception {

    }
}
