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
package com.github.dtprj.dongting.raft.impl;

import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;

import java.io.File;
import java.util.Properties;

/**
 * @author huangli
 */
public class StatusUtil {
    private static final DtLog log = DtLogs.getLogger(StatusUtil.class);

    private static final String CURRENT_TERM_KEY = "currentTerm";
    private static final String VOTED_FOR_KEY = "votedFor";
    private static final String COMMIT_INDEX_KEY = "commitIndex";

    public static void initStatusFileChannel(String dataDir, String filename, RaftStatusImpl raftStatus) {
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

    public static void persist(RaftStatusImpl raftStatus, boolean flush) {
        // may run in io executor
        // noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (raftStatus) {
            StatusFile sf = raftStatus.getStatusFile();

            Properties destProps = sf.getProperties();
            destProps.clear();

            destProps.putAll(raftStatus.getExtraPersistProps());

            destProps.setProperty(CURRENT_TERM_KEY, String.valueOf(raftStatus.getCurrentTerm()));
            destProps.setProperty(VOTED_FOR_KEY, String.valueOf(raftStatus.getVotedFor()));
            destProps.setProperty(COMMIT_INDEX_KEY, String.valueOf(raftStatus.getCommitIndex()));

            sf.update(flush);
        }
    }

    public static void persistUntilSuccess(RaftStatusImpl raftStatus) {
        try {
            while (!raftStatus.isStop()) {
                try {
                    persist(raftStatus, true);
                    return;
                } catch (Exception e) {
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
        } catch (Exception e) {
            log.error("persist failed", e);
        }
    }

}
