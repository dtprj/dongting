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
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.FileUtil;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;
import com.github.dtprj.dongting.raft.server.RaftStatus;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
public class StatusManager implements AutoCloseable {

    private static final String CURRENT_TERM_KEY = "currentTerm";
    private static final String VOTED_FOR_KEY = "votedFor";
    private static final String COMMIT_INDEX_KEY = "commitIndex";

    private final RaftGroupConfig groupConfig;
    private final RaftStatus raftStatus;
    private final StatusFile statusFile;

    private CompletableFuture<Void> asyncFuture;

    private boolean closed;

    public StatusManager(RaftGroupConfig groupConfig, RaftStatus raftStatus) {
        this.groupConfig = groupConfig;
        this.raftStatus = raftStatus;
        File dir = FileUtil.ensureDir(groupConfig.getDataDir());
        File file = new File(dir, groupConfig.getStatusFile());
        this.statusFile = new StatusFile(file, groupConfig.getIoExecutor());
    }

    public void initStatusFile() {
        statusFile.init();

        Properties loadedProps = statusFile.getProperties();

        raftStatus.setCurrentTerm(Integer.parseInt(loadedProps.getProperty(CURRENT_TERM_KEY, "0")));
        raftStatus.setVotedFor(Integer.parseInt(loadedProps.getProperty(VOTED_FOR_KEY, "0")));
        raftStatus.setCommitIndex(Integer.parseInt(loadedProps.getProperty(COMMIT_INDEX_KEY, "0")));
    }

    public void close() {
        if (!closed) {
            DtUtil.close(statusFile);
            closed = true;
        }
    }


    public CompletableFuture<Void> persistAsync() {
        try {
            if (asyncFuture != null && !asyncFuture.isDone()) {
                try {
                    asyncFuture.get();
                } finally {
                    asyncFuture = null;
                }
            }
            copyWriteData();
            asyncFuture = persist(false);
            return asyncFuture;
        } catch (Exception e) {
            throw new RaftException(e);
        }
    }

    protected CompletableFuture<Void> persist(boolean flush) {
        return statusFile.update(flush);
    }

    private void copyWriteData() {
        Properties destProps = statusFile.getProperties();

        destProps.setProperty(CURRENT_TERM_KEY, String.valueOf(raftStatus.getCurrentTerm()));
        destProps.setProperty(VOTED_FOR_KEY, String.valueOf(raftStatus.getVotedFor()));
        destProps.setProperty(COMMIT_INDEX_KEY, String.valueOf(raftStatus.getCommitIndex()));
    }

    public void persistSync() {
        copyWriteData();
        try {
            FileUtil.doWithRetry(() -> {
                if (asyncFuture != null) {
                    try {
                        asyncFuture.get();
                    } finally {
                        asyncFuture = null;
                    }
                    CompletableFuture<Void> f = persist(true);
                    f.get();
                }
            }, raftStatus::isStop, false, groupConfig.getIoRetryInterval());
        } catch (InterruptedException e) {
            throw new RaftException(e);
        }
    }

    public Properties getProperties() {
        return statusFile.getProperties();
    }
}
