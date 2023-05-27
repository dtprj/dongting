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
package com.github.dtprj.dongting.raft.sm;

import com.github.dtprj.dongting.raft.impl.FileUtil;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class DefaultSnapshotManager implements SnapshotManager {

    private final RaftServerConfig serverConfig;
    private final RaftGroupConfigEx groupConfig;

    public DefaultSnapshotManager(RaftServerConfig serverConfig, RaftGroupConfigEx groupConfig) {
        this.serverConfig = serverConfig;
        this.groupConfig = groupConfig;
    }

    @Override
    public void init(Supplier<Boolean> cancelIndicator) throws IOException {
        File dataDir = FileUtil.ensureDir(groupConfig.getDataDir());
        File snapshotDir = FileUtil.ensureDir(dataDir, "snapshot");
        File[] files = snapshotDir.listFiles();
    }

    @Override
    public List<Snapshot> list() {
        return null;
    }

    @Override
    public boolean delete(Snapshot snapshot) {
        return false;
    }

    @Override
    public void saveSnapshot(Snapshot snapshot, Supplier<Boolean> cancelIndicator) {
    }
}
