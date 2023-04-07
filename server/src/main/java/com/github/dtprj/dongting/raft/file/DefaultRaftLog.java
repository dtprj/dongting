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
package com.github.dtprj.dongting.raft.file;

import com.github.dtprj.dongting.common.ObjUtil;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.client.RaftException;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;
import com.github.dtprj.dongting.raft.server.RaftLog;

import java.io.File;
import java.io.IOException;
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

    private final RaftGroupConfig groupConfig;
    private final Timestamp ts;
    private final Executor ioExecutor;
    private final Supplier<Boolean> stopIndicator;
    private LogFileQueue logFiles;
    private IdxFileQueue idxFiles;
    private long knownMaxCommitIndex;
    private StatusFile checkpointFile;

    public DefaultRaftLog(RaftGroupConfig groupConfig, Timestamp ts,
                          Executor ioExecutor, Supplier<Boolean> stopIndicator) {
        this.groupConfig = groupConfig;
        this.ts = ts;
        this.ioExecutor = ioExecutor;
        this.stopIndicator = stopIndicator;
    }

    @Override
    public Pair<Integer, Long> init() throws IOException {
        File dataDir = FileUtil.ensureDir(groupConfig.getDataDir());
        checkpointFile = new StatusFile(new File(dataDir, "checkpoint"));
        checkpointFile.init();
        knownMaxCommitIndex = Long.parseLong(checkpointFile.getProperties().getProperty(KNOWN_MAX_COMMIT_INDEX_KEY, "0"));
        idxFiles = new IdxFileQueue(FileUtil.ensureDir(dataDir, "idx"), ioExecutor, stopIndicator);
        logFiles = new LogFileQueue(FileUtil.ensureDir(dataDir, "log"), ioExecutor, idxFiles);
        logFiles.init();
        idxFiles.init();
        idxFiles.initWithCommitIndex(knownMaxCommitIndex);
        long commitIndexPos;
        if (knownMaxCommitIndex > 0) {
            commitIndexPos = idxFiles.findLogPosByItemIndex(knownMaxCommitIndex);
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
    public void append(long commitIndex, List<LogItem> logs) throws IOException {
        if (logs == null || logs.size() == 0) {
            BugLog.getLog().error("append log with empty logs");
            return;
        }
        knownMaxCommitIndex = Math.max(knownMaxCommitIndex, commitIndex);
        long firstIndex = logs.get(0).getIndex();
        ObjUtil.checkPositive(firstIndex, "firstIndex");
        if (firstIndex == idxFiles.getNextIndex()) {
            logFiles.append(logs);
        } else if (firstIndex < idxFiles.getNextIndex()) {
            long dataPosition = idxFiles.truncateTail(firstIndex);
            logFiles.truncateTail(dataPosition);
            logFiles.append(logs);
        } else {
            throw new RaftException("bad index: " + firstIndex);
        }
    }

    @Override
    public CompletableFuture<LogItem[]> load(long index, int limit, long bytesLimit) {
        return null;
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

    @Override
    public void truncate(long index) {

    }
}
