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

import java.io.File;

/**
 * @author huangli
 */
public class DefaultSnapshot extends Snapshot {
    private final File idxFile;
    private final File dataFile;

    public DefaultSnapshot(long lastIncludedIndex, int lastIncludedTerm, File idxFile, File dataFile) {
        super(lastIncludedIndex, lastIncludedTerm);
        this.idxFile = idxFile;
        this.dataFile = dataFile;
    }

    @Override
    public SnapshotIterator openIterator() {
        return null;
    }

    public File getIdxFile() {
        return idxFile;
    }

    public File getDataFile() {
        return dataFile;
    }
}
