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

import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.common.VersionFactory;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.log.BugLog;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/**
 * @author huangli
 */
public class LogFile extends AbstractFile<Pair<FileChannel, MappedByteBuffer>> {
    final long startPos;
    final long endPos;

    // idx file not set below 4 fields
    long firstTimestamp;
    long firstIndex;
    int firstTerm;
    long deleteTimestamp;

    boolean deleted;

    long lastAccessTime;
    private final Consumer<LogFile> accessCallback;
    LogFile lruPrev;
    LogFile lruNext;

    private int readers;
    private int writers;
    private final FiberCondition noRwCond;

    private FileChannel fileChannel;
    private MappedByteBuffer mappedBuffer;

    public LogFile(long startPos, long endPos, File file, FiberGroup group,
                   Set<OpenOption> openOptions, ExecutorService ioExecutor,
                   Consumer<LogFile> accessCallback, long currentTimeMillis) {
        super(file, group, openOptions, ioExecutor);
        this.lastAccessTime = currentTimeMillis;
        this.accessCallback = accessCallback;
        this.startPos = startPos;
        this.endPos = endPos;
        this.noRwCond = group.newCondition("noRw-" + file.getName());
    }

    @Override
    public boolean isOpen() {
        return mappedBuffer != null;
    }

    @Override
    protected Pair<FileChannel, MappedByteBuffer> doSyncOpen() throws IOException {
        FileChannel fc = FileChannel.open(file.toPath(), openOptions);
        try {
            MappedByteBuffer buf = fc.map(FileChannel.MapMode.READ_WRITE, 0, fc.size());
            return new Pair<>(fc, buf);
        } catch (Throwable t) {
            DtUtil.close(fc);
            throw t;
        }
    }

    @Override
    protected void afterSyncOpen(Pair<FileChannel, MappedByteBuffer> openResult) {
        fileChannel = openResult.getLeft();
        mappedBuffer = openResult.getRight();
    }

    @Override
    protected void dropOpenResult(Pair<FileChannel, MappedByteBuffer> o) {
        if (o != null) {
            VersionFactory.getInstance().releaseDirectBuffer(o.getRight());
            DtUtil.close(o.getLeft());
        }
    }

    @Override
    protected void doClose() {
        if (isOpen()) {
            MappedByteBuffer buf = mappedBuffer;
            mappedBuffer = null;
            VersionFactory.getInstance().releaseDirectBuffer(buf);
            DtUtil.close(fileChannel);
            fileChannel = null;
        }
    }

    public ByteBuffer duplicateMmap() {
        return mappedBuffer.duplicate();
    }

    @Override
    public void doForce(boolean meta) {
        if (mappedBuffer != null) {
            mappedBuffer.force();
        }
    }

    public void close() {
        if (inUse() || openFuture != null) {
            BugLog.log(new IllegalStateException("close file while in use: " + file.getPath()));
            return;
        }
        doClose();
    }

    public void incReaders() {
        readers++;
        updateAccessTime();
    }

    public void decReaders() {
        readers--;
        if (readers <= 0 && writers <= 0) {
            noRwCond.signalAll();
        }
    }

    public int getReaders() {
        return readers;
    }

    public void incWriters() {
        writers++;
        updateAccessTime();
    }

    public void decWriters() {
        writers--;
        if (readers <= 0 && writers <= 0) {
            noRwCond.signalAll();
        }
    }

    public int getWriters() {
        return writers;
    }

    public boolean inUse() {
        return readers > 0 || writers > 0;
    }

    public FiberCondition getNoRwCond() {
        return noRwCond;
    }

    public void updateAccessTime() {
        if (accessCallback != null) {
            accessCallback.accept(this);
        }
    }

    public boolean shouldDelete() {
        return deleteTimestamp > 0;
    }

    public boolean isDeleted() {
        return deleted;
    }
}
