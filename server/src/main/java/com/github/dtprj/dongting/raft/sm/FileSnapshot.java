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
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.store.AsyncIoTask;
import com.github.dtprj.dongting.raft.store.DtFile;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
public class FileSnapshot extends Snapshot {

    private static final DtLog log = DtLogs.getLogger(FileSnapshot.class);

    private final DtFile dtFile;
    private final FiberGroup fiberGroup;
    private final long fileSize;

    private long filePos;

    private final int bufferSize;

    DefaultSnapshotManager.FileSnapshotInfo fsi;

    public FileSnapshot(RaftGroupConfigEx groupConfig, SnapshotInfo si, File dataFile, int bufferSize) {
        super(si);
        this.fiberGroup = groupConfig.fiberGroup;
        this.fileSize = dataFile.length();
        this.bufferSize = bufferSize;

        Set<OpenOption> options = Set.of(StandardOpenOption.READ);
        this.dtFile = new DtFile(dataFile, groupConfig.fiberGroup, options, groupConfig.blockIoExecutor);
    }

    public void syncOpen() throws IOException {
        dtFile.syncOpen();
    }

    FiberFuture<Void> asyncOpen() {
        return dtFile.ensureOpen();
    }

    void attachTo(DefaultSnapshotManager.FileSnapshotInfo fsi) {
        this.fsi = fsi;
        fsi.openReads.add(this);
    }

    @Override
    public FiberFuture<Integer> readNext(ByteBuffer buffer) {
        DefaultSnapshotManager.FileSnapshotInfo fsi = this.fsi;
        if (fsi != null && fsi.closing) {
            return FiberFuture.failedFuture(fiberGroup, new RaftException("snapshot is closing"));
        }
        if (filePos >= fileSize) {
            return FiberFuture.completedFuture(fiberGroup, 0);
        }
        int startPos = buffer.position();
        if (buffer.remaining() < bufferSize) {
            return FiberFuture.failedFuture(fiberGroup, new RaftException(
                    "buffer too small for snapshot block: remaining=" + buffer.remaining()
                            + ", bufferSize=" + bufferSize));
        }
        int blockBytes = (int) Math.min(bufferSize, fileSize - filePos);
        buffer.limit(startPos + blockBytes);
        // AsyncIoTask require buffer position is 0
        ByteBuffer readSlice = buffer.slice();
        AsyncIoTask t = new AsyncIoTask(fiberGroup, dtFile);
        FiberFuture<Void> f = t.read(readSlice, filePos);
        if (fsi != null) {
            fsi.busy++;
            f.registerCallback((v, ex) -> {
                if (--fsi.busy == 0) {
                    fsi.busyCond.signalAll();
                }
                if (ex != null) {
                    markBad(ex);
                }
            });
        }
        filePos += blockBytes;
        return f.convert("FileSnapshotReadNext", v -> {
            try {
                int size = buffer.getInt(startPos);
                if (size <= 0 || size > blockBytes - 8) {
                    throw new RaftException("bad snapshot data size: " + size);
                }
                CRC32C crc32c = new CRC32C();
                RaftUtil.updateCrc(crc32c, buffer, startPos, size + 4);
                if (buffer.getInt(startPos + size + 4) != (int) crc32c.getValue()) {
                    throw new RaftException("snapshot data crc error");
                }
                buffer.position(startPos + 4);
                buffer.limit(startPos + size + 4);
                return size;
            } catch (Throwable e) {
                markBad(e);
                throw e;
            }
        });
    }

    private void markBad(Throwable ex) {
        DefaultSnapshotManager.FileSnapshotInfo fsi = this.fsi;
        if (fsi != null && !fsi.closing && !fsi.bad) {
            fsi.bad = true;
            log.warn("mark snapshot bad: {}, {}", dtFile.getFile().getPath(), ex.toString());
        }
    }

    @Override
    protected void doClose() {
        dtFile.destroy();
        if (fsi != null) {
            fsi.openReads.remove(this);
        }
    }

    public int getBufferSize() {
        return bufferSize;
    }
}
