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

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.buf.RefBufferFactory;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.store.AsyncIoTask;
import com.github.dtprj.dongting.raft.store.DtFile;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
public class FileSnapshot extends Snapshot {
    static final int MAX_BLOCK_SIZE = 1024 * 1024 * 1024;

    private final DtFile dtFile;
    private final RaftGroupConfigEx groupConfig;
    private final FiberGroup fiberGroup;
    private final ByteBuffer headerBuffer = ByteBuffer.allocate(8);
    private final long fileSize;
    private final CRC32C crc32C = new CRC32C();
    private final RefBufferFactory directRefBufferFactory;

    private long filePos;

    public FileSnapshot(RaftGroupConfigEx groupConfig, SnapshotInfo si, File dataFile) throws IOException {
        super(si);
        this.fiberGroup = groupConfig.getFiberGroup();
        this.groupConfig = groupConfig;
        this.fileSize = dataFile.length();
        this.directRefBufferFactory = new RefBufferFactory(groupConfig.getDirectPool(), 0);

        HashSet<StandardOpenOption> options = new HashSet<>();
        options.add(StandardOpenOption.READ);
        AsynchronousFileChannel channel = AsynchronousFileChannel.open(dataFile.toPath(), options, fiberGroup.getExecutor());

        this.dtFile = new DtFile(dataFile, channel, groupConfig.getFiberGroup());
    }

    @Override
    public FiberFuture<RefBuffer> readNext() {
        if (filePos >= fileSize) {
            return FiberFuture.completedFuture(fiberGroup, null);
        }
        FiberFuture<RefBuffer> result = fiberGroup.newFuture("readNext");

        AsyncIoTask t = new AsyncIoTask(groupConfig, dtFile);
        headerBuffer.clear();
        FiberFuture<Void> fu = t.read(headerBuffer, filePos);
        fu.registerCallback((v, ex) -> {
            if (ex != null) {
                result.completeExceptionally(ex);
                return;
            }
            headerReadFinished(result);
        });
        return result;
    }

    private void headerReadFinished(FiberFuture<RefBuffer> future) {
        filePos += headerBuffer.capacity();
        headerBuffer.flip();
        int size = headerBuffer.getInt();
        if (size <= 0 || size > MAX_BLOCK_SIZE) {
            future.completeExceptionally(new RaftException("illegal size " + size));
            return;
        }
        if (filePos + size > fileSize) {
            future.completeExceptionally(new RaftException("block size " + size + " exceed file size " + fileSize));
            return;
        }
        int crc = headerBuffer.getInt();

        RefBuffer refBuffer = directRefBufferFactory.create(size);
        refBuffer.getBuffer().limit(size);
        AsyncIoTask t = new AsyncIoTask(groupConfig, dtFile);

        t.read(refBuffer.getBuffer(), filePos).registerCallback((v, ex) -> {
            if (ex != null) {
                future.completeExceptionally(ex);
                return;
            }
            blockReadFinished(future, crc, refBuffer);
        });
    }

    private void blockReadFinished(FiberFuture<RefBuffer> future, int crc, RefBuffer refBuffer) {
        ByteBuffer dataBuffer = refBuffer.getBuffer();
        filePos += dataBuffer.limit();
        dataBuffer.flip();
        crc32C.reset();
        RaftUtil.updateCrc(crc32C, dataBuffer, 0, dataBuffer.limit());
        if (crc != crc32C.getValue()) {
            future.completeExceptionally(new RaftException("crc mismatch"));
            return;
        }
        future.complete(refBuffer);
    }

    @Override
    protected void doClose() {
        DtUtil.close(dtFile.getChannel());
    }
}
