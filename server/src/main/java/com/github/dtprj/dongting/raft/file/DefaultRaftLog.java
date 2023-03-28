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

import com.github.dtprj.dongting.common.CloseUtil;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.client.RaftException;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;
import com.github.dtprj.dongting.raft.server.RaftLog;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author huangli
 */
public class DefaultRaftLog implements RaftLog {
    private static final DtLog log = DtLogs.getLogger(DefaultRaftLog.class);
    private static final long MAX_LOG_FILE_SIZE = 1024 * 1024 * 1024;
    private static final int IDX_FILE_SIZE_STEP = 1024 * 1024;

    private final RaftGroupConfig groupConfig;
    private ArrayDeque<LogFile> logFiles = new ArrayDeque<>();
    private ArrayDeque<LogFile> idxFiles = new ArrayDeque<>();
    private long checkPoint;

    public DefaultRaftLog(RaftGroupConfig groupConfig) {
        this.groupConfig = groupConfig;
    }

    @Override
    public Pair<Integer, Long> init() {
        try {
            File dataDir = FileUtil.ensureDir(groupConfig.getDataDir());
            File logDir = FileUtil.ensureDir(dataDir, "log");
            File[] files = logDir.listFiles();
            Pattern logPattern = Pattern.compile("^(\\d{20})\\.log$");
            Pattern idxPattern = Pattern.compile("^(\\d{20})\\.idx$");
            for (File f : files) {
                if (!f.isFile()) {
                    continue;
                }
                if (buildLogFile(logPattern, f, logFiles)) {
                    continue;
                }
                buildLogFile(idxPattern, f, idxFiles);
            }
            return null;
        } catch (IOException e) {
            throw new RaftException(e);
        }
    }

    private boolean buildLogFile(Pattern logPattern, File f, ArrayDeque<LogFile> queue) throws IOException {
        Matcher matcher = logPattern.matcher(f.getName());
        if (matcher.matches()) {
            long index = Long.parseLong(matcher.group(1));
            log.info("load file: {}", f.getPath());
            FileChannel channel = FileChannel.open(f.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
            queue.addLast(new LogFile(index, channel));
            return true;
        }
        return false;
    }

    @Override
    public void close() throws Exception {
        for (LogFile logFile : logFiles) {
            CloseUtil.close(logFile.channel);
        }
        for (LogFile logFile : idxFiles) {
            CloseUtil.close(logFile.channel);
        }
    }

    @Override
    public void append(long prevLogIndex, int prevLogTerm, List<LogItem> logs) {

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

    @Override
    public void truncate(long index) {

    }
}
