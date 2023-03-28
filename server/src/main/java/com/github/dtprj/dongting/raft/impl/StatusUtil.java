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

import com.github.dtprj.dongting.raft.file.FileUtil;
import com.github.dtprj.dongting.raft.file.StatusFile;

import java.io.File;

/**
 * @author huangli
 */
public class StatusUtil {

    private static final String currentTermKey = "currentTerm";
    private static final String votedForKey = "votedFor";

    public static void initStatusFileChannel(String dataDir, String filename, RaftStatus raftStatus) {
        File dir = FileUtil.ensureDir(dataDir);
        File file = new File(dir, filename);
        StatusFile sf = new StatusFile(file);
        sf.init();
        raftStatus.setStatusFile(sf);
        raftStatus.setCurrentTerm(Integer.parseInt(currentTermKey));
        raftStatus.setVotedFor(Integer.parseInt(votedForKey));
    }

    public static void updateStatusFile(RaftStatus raftStatus) {
        if (raftStatus.isSaving()) {
            // prevent concurrent saving or saving actions more and more
            return;
        }
        if (raftStatus.isStop()) {
            return;
        }
        raftStatus.setSaving(true);
        StatusFile sf = raftStatus.getStatusFile();

        sf.getProperties().setProperty(currentTermKey, String.valueOf(raftStatus.getCurrentTerm()));
        sf.getProperties().setProperty(votedForKey, String.valueOf(raftStatus.getVotedFor()));

        if (sf.update()) {
            raftStatus.setSaving(false);
        } else {
            raftStatus.getRaftExecutor().schedule(() -> updateStatusFile(raftStatus), 1000);
        }
    }
}
