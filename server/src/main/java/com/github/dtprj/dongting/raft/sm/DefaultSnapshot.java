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

import com.github.dtprj.dongting.buf.ByteBufferPool;
import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.buf.TwoLevelPool;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.RaftException;
import com.github.dtprj.dongting.raft.store.AsyncIoTask;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
public class DefaultSnapshot extends Snapshot {
    private static final DtLog log = DtLogs.getLogger(DefaultSnapshot.class);
    static final int MAX_BLOCK_SIZE = 1024 * 1024 * 1024;

    private final AsynchronousFileChannel channel;
    private final ExecutorService ioExecutor;
    private final ByteBuffer headerBuffer = ByteBuffer.allocate(8);
    private final long fileSize;
    private final AsyncIoTask task;
    private final CRC32C crc32C = new CRC32C();
    private final ByteBufferPool dataBufferPool;
    private final ByteBuffer dataBuffer;

    private long filePos;

    public DefaultSnapshot(long lastIncludedIndex, int lastIncludedTerm, File dataFile,
                           ExecutorService ioExecutor, int maxBlock) throws IOException {
        super(lastIncludedIndex, lastIncludedTerm);
        this.ioExecutor = ioExecutor;

        HashSet<StandardOpenOption> options = new HashSet<>();
        options.add(StandardOpenOption.READ);
        this.channel = AsynchronousFileChannel.open(dataFile.toPath(), options, ioExecutor);
        this.fileSize = channel.size();
        this.task = new AsyncIoTask(channel, () -> false);

        log.info("open snapshot file, maxBlock={}, size={}, file={}", maxBlock, fileSize, dataFile);

        // thread in io executor have no byte buffer pool
        ByteBufferPool pool = TwoLevelPool.getDefaultFactory().apply(new Timestamp(), true);
        if (pool instanceof TwoLevelPool) {
            // large pool should be thread safe
            dataBufferPool = ((TwoLevelPool) pool).getLargePool();
            this.dataBuffer = dataBufferPool.allocate(maxBlock);
        } else {
            this.dataBufferPool = null;
            this.dataBuffer = ByteBuffer.allocateDirect(maxBlock);
        }
    }

    @Override
    public CompletableFuture<RefBuffer> readNext() {
        CompletableFuture<RefBuffer> future = new CompletableFuture<>();
        ioExecutor.execute(() -> this.readHeader(future));
        return future;
    }

    public void readHeader(CompletableFuture<RefBuffer> future) {
        if (filePos >= fileSize) {
            future.complete(null);
            return;
        }
        CompletableFuture<Void> f = task.read(headerBuffer, filePos);
        f.whenComplete((v, ex) -> {
            if (ex != null) {
                future.completeExceptionally(ex);
                return;
            }
            headerReadFinished(future);
        });
    }

    private void headerReadFinished(CompletableFuture<RefBuffer> future) {
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

        dataBuffer.clear();
        dataBuffer.limit(size);
        task.read(dataBuffer, filePos).whenComplete((v, ex) -> {
            if (ex != null) {
                future.completeExceptionally(ex);
                return;
            }
            blockReadFinished(future, crc);
        });
    }

    private void blockReadFinished(CompletableFuture<RefBuffer> future, int crc) {
        filePos += dataBuffer.limit();
        dataBuffer.flip();
        crc32C.reset();
        RaftUtil.updateCrc(crc32C, dataBuffer, 0, dataBuffer.limit());
        if (crc != crc32C.getValue()) {
            future.completeExceptionally(new RaftException("crc mismatch"));
            return;
        }
        // use unpooled buffer, so the caller should not retain it.
        // and caller should invoke readNext() after previous process finished, because dataBuffer is reused.
        future.complete(RefBuffer.wrapUnpooled(dataBuffer));
    }

    @Override
    protected void doClose() {
        if (dataBufferPool != null && dataBuffer != null) {
            dataBufferPool.release(dataBuffer);
        }
        DtUtil.close(channel);
    }
}
