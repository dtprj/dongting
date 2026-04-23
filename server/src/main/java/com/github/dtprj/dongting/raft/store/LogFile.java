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

import com.github.dtprj.dongting.fiber.FiberGroup;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * @author huangli
 */
class LogFile extends DtFile {
    final long startPos;
    final long endPos;

    // idx file not set below 4 fields
    long firstTimestamp;
    long firstIndex;
    int firstTerm;
    long deleteTimestamp;

    boolean deleted;

    public LogFile(long startPos, long endPos, File file, FiberGroup group,
                   Set<OpenOption> openOptions, ExecutorService executor) throws IOException {
        super(file, group, openOptions, executor);
        this.startPos = startPos;
        this.endPos = endPos;
    }

    public boolean shouldDelete() {
        return deleteTimestamp > 0;
    }

    public boolean isDeleted() {
        return deleted;
    }
}
