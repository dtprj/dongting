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
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.FileUtil;
import com.github.dtprj.dongting.raft.impl.StoppedException;
import com.github.dtprj.dongting.raft.server.RaftStatus;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author huangli
 */
public class StatusManager {
    private static final DtLog log = DtLogs.getLogger(StatusManager.class);

    private static final String CURRENT_TERM_KEY = "currentTerm";
    private static final String VOTED_FOR_KEY = "votedFor";
    private static final String COMMIT_INDEX_KEY = "commitIndex";

    private final Executor ioExecutor;

    private CompletableFuture<Void> updateFuture;

    public StatusManager(Executor ioExecutor) {
        this.ioExecutor = ioExecutor;
    }

    public void initStatusFileChannel(String dataDir, String filename, RaftStatus raftStatus) {
        File dir = FileUtil.ensureDir(dataDir);
        File file = new File(dir, filename);
        StatusFile sf = new StatusFile(file);
        sf.init();
        raftStatus.setStatusFile(sf);
        Properties loadedProps = sf.getProperties();

        raftStatus.getExtraPersistProps().putAll(loadedProps);
        raftStatus.getExtraPersistProps().remove(CURRENT_TERM_KEY);
        raftStatus.getExtraPersistProps().remove(VOTED_FOR_KEY);
        raftStatus.getExtraPersistProps().remove(COMMIT_INDEX_KEY);

        raftStatus.setCurrentTerm(Integer.parseInt(loadedProps.getProperty(CURRENT_TERM_KEY, "0")));
        raftStatus.setVotedFor(Integer.parseInt(loadedProps.getProperty(VOTED_FOR_KEY, "0")));
        raftStatus.setCommitIndex(Integer.parseInt(loadedProps.getProperty(COMMIT_INDEX_KEY, "0")));
    }

    public void persistAsync(RaftStatus raftStatus, boolean flush) {
        try {
            waitFinish();
            persist(raftStatus, flush);
        } catch (Exception e) {
            throwIfStop(e);
            throw new RaftException(e);
        }
    }

    private void persist(RaftStatus raftStatus, boolean flush) {
        StatusFile sf = raftStatus.getStatusFile();

        Properties destProps = new Properties();

        destProps.putAll(raftStatus.getExtraPersistProps());

        destProps.setProperty(CURRENT_TERM_KEY, String.valueOf(raftStatus.getCurrentTerm()));
        destProps.setProperty(VOTED_FOR_KEY, String.valueOf(raftStatus.getVotedFor()));
        destProps.setProperty(COMMIT_INDEX_KEY, String.valueOf(raftStatus.getCommitIndex()));

        updateFuture = CompletableFuture.runAsync(() -> {
            try {
                sf.update(destProps, flush);
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RaftException(e);
            }
        }, ioExecutor);
    }

    private void waitFinish() throws InterruptedException, ExecutionException, TimeoutException {
        if (updateFuture != null) {
            try {
                updateFuture.get(60, TimeUnit.SECONDS);
            } finally {
                updateFuture = null;
            }
        }
    }

    public void persistSync(RaftStatus raftStatus) {
        while (!raftStatus.isStop()) {
            try {
                waitFinish();
                persist(raftStatus, true);
                waitFinish();
                return;
            } catch (Exception e) {
                throwIfStop(e);
                log.error("persist raft status file failed", e);
                try {
                    //noinspection BusyWait
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    throw new RaftException(ex);
                }
            }
        }
        throw new StoppedException();
    }

    private static void throwIfStop(Exception e) {
        Throwable root = DtUtil.rootCause(e);
        if ((root instanceof InterruptedException) || (root instanceof StoppedException)) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new RaftException(e);
            }
        }
    }

}
