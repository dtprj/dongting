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

import java.io.File;
import java.util.Properties;

/**
 * @author huangli
 */
public class StatusUtil {

    private static final String CURRENT_TERM_KEY = "currentTerm";
    private static final String VOTED_FOR_KEY = "votedFor";
    private static final String COMMIT_INDEX_KEY = "commitIndex";

    public static void initStatusFileChannel(String dataDir, String filename, RaftStatusImpl raftStatus) {
        File dir = FileUtil.ensureDir(dataDir);
        File file = new File(dir, filename);
        StatusFile sf = new StatusFile(file);
        sf.init();
        raftStatus.setStatusFile(sf);
        Properties p = sf.getProperties();
        raftStatus.setCurrentTerm(Integer.parseInt(p.getProperty(CURRENT_TERM_KEY, "0")));
        raftStatus.setVotedFor(Integer.parseInt(p.getProperty(VOTED_FOR_KEY, "0")));
        raftStatus.setCommitIndex(Integer.parseInt(p.getProperty(COMMIT_INDEX_KEY, "0")));
    }

    public static void persist(RaftStatusImpl raftStatus) {
        if (raftStatus.isStop()) {
            return;
        }

        StatusFile sf = raftStatus.getStatusFile();

        sf.getProperties().setProperty(CURRENT_TERM_KEY, String.valueOf(raftStatus.getCurrentTerm()));
        sf.getProperties().setProperty(VOTED_FOR_KEY, String.valueOf(raftStatus.getVotedFor()));
        sf.getProperties().setProperty(COMMIT_INDEX_KEY, String.valueOf(raftStatus.getCommitIndex()));

        if (!sf.update() && !raftStatus.isRetrying()) {
            // prevent concurrent saving or saving actions more and more
            raftStatus.setRetrying(true);
            raftStatus.getRaftExecutor().schedule(() -> {
                raftStatus.setRetrying(false);
                persist(raftStatus);
            }, 1000);
        }
    }

}
