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

import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.store.AsyncIoTask;
import com.github.dtprj.dongting.raft.store.DtFile;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Set;

/**
 * @author huangli
 */
public class FileSnapshot extends Snapshot {

    private final DtFile dtFile;
    private final FiberGroup fiberGroup;
    private final long fileSize;

    private long filePos;

    private final int bufferSize;

    public FileSnapshot(RaftGroupConfigEx groupConfig, SnapshotInfo si, File dataFile, int bufferSize) throws IOException {
        super(si);
        this.fiberGroup = groupConfig.fiberGroup;
        this.fileSize = dataFile.length();
        this.bufferSize = bufferSize;

        Set<OpenOption> options = Set.of(StandardOpenOption.READ);
        this.dtFile = new DtFile(dataFile, groupConfig.fiberGroup, options, groupConfig.blockIoExecutor);
        this.dtFile.open();
    }

    @Override
    public FiberFuture<Integer> readNext(ByteBuffer buffer) {
        if (filePos >= fileSize) {
            return FiberFuture.completedFuture(fiberGroup, 0);
        }
        long rest = fileSize - filePos;
        if (rest < buffer.remaining()) {
            buffer.limit(buffer.position() + (int) rest);
        }
        AsyncIoTask t = new AsyncIoTask(fiberGroup, dtFile);
        int readBytes = buffer.remaining();
        FiberFuture<Void> f = t.read(buffer, filePos);
        filePos += readBytes;
        return f.convert("FileSnapshotReadNext", v -> readBytes);
    }

    @Override
    protected void doClose() {
        dtFile.close();
    }

    public int getBufferSize() {
        return bufferSize;
    }
}
