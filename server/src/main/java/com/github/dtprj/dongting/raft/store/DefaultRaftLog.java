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

import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;

/**
 * @author huangli
 */
public class DefaultRaftLog {
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
}
