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
import com.github.dtprj.dongting.common.VersionFactory;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.raft.RaftException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * @author huangli
 */
public class DtFile {
    protected final File file;
    AsynchronousFileChannel channel;
    private MappedByteBuffer mappedBuffer;

    final Set<OpenOption> openOptions;
    final ExecutorService ioExecutor;
    final FiberGroup fiberGroup;

    boolean destroyed;
    FiberFuture<Void> openFuture;
    FiberFuture<Void> mmapOpenFuture;

    public DtFile(File file, FiberGroup fiberGroup, Set<OpenOption> openOptions,
                  ExecutorService ioExecutor) {
        this.file = file;
        this.openOptions = openOptions;
        this.ioExecutor = ioExecutor;
        this.fiberGroup = fiberGroup;
    }

    public File getFile() {
        return file;
    }

    public AsynchronousFileChannel getChannel() {
        return channel;
    }

    public boolean isRwChannelOpen() {
        return channel != null;
    }

    public boolean isMmapOpen() {
        return mappedBuffer != null;
    }

    /**
     * This method performs blocking IO, should only be used in IO thread or initialization code.
     * For raft main thread, use ensureOpen() instead.
     */
    public final void syncOpen() throws IOException {
        if (isRwChannelOpen()) {
            return;
        }
        channel = doSyncOpen();
    }

    protected AsynchronousFileChannel doSyncOpen() throws IOException {
        return AsynchronousFileChannel.open(file.toPath(), openOptions, ioExecutor);
    }

    public final void destroy() {
        destroyed = true;
        FiberFuture<Void> pendingOpenFuture = openFuture;
        FiberFuture<Void> pendingMmapFuture = mmapOpenFuture;
        openFuture = null;
        mmapOpenFuture = null;
        doClose();
        closeMmap();
        if (pendingOpenFuture != null) {
            pendingOpenFuture.completeExceptionally(new RaftException("DtFile is destroyed: " + file.getPath()));
        }
        if (pendingMmapFuture != null) {
            pendingMmapFuture.completeExceptionally(new RaftException("DtFile is destroyed: " + file.getPath()));
        }
    }

    protected void doClose() {
        DtUtil.close(channel);
        channel = null;
    }

    public void doForce(boolean meta) throws IOException {
        channel.force(meta);
    }

    protected void dropOpenResult(Object o) {
        if (o != null) {
            DtUtil.close((AsynchronousFileChannel) o);
        }
    }

    public FiberFuture<Void> ensureOpen() {
        if (destroyed) {
            return FiberFuture.failedFuture(fiberGroup, new RaftException("DtFile is destroyed: " + file.getPath()));
        }
        if (isRwChannelOpen()) {
            return FiberFuture.completedFuture(fiberGroup, null);
        }
        if (openFuture != null) {
            return openFuture;
        }
        FiberFuture<Void> result = fiberGroup.newFuture("OpenFile-" + file.getName());
        openFuture = result;
        FiberFuture<AsynchronousFileChannel> f = fiberGroup.newFuture("asyncOpenFile");
        try {
            ioExecutor.execute(() -> {
                try {
                    f.fireComplete(doSyncOpen());
                } catch (Throwable e) {
                    f.fireCompleteExceptionally(e);
                }
            });
        } catch (Throwable e) {
            f.completeExceptionally(e);
        }
        registerCallback(f);
        return result;
    }

    private void registerCallback(FiberFuture<AsynchronousFileChannel> f) {
        f.registerCallback((ch, ex) -> {
            FiberFuture<Void> oldOpenFuture = this.openFuture;
            this.openFuture = null;
            if (destroyed) {
                dropOpenResult(ch);
                return;
            }
            if (ex != null) {
                if (oldOpenFuture != null) {
                    oldOpenFuture.completeExceptionally(ex);
                }
            } else {
                channel = ch;
                if (oldOpenFuture != null) {
                    oldOpenFuture.complete(null);
                }
            }
        });
    }

    /**
     * This method performs blocking IO, should only be used in IO thread or initialization code.
     * For raft main thread, use ensureMmapOpen() instead.
     */
    public final void syncOpenMmap() throws IOException {
        if (mappedBuffer != null) {
            return;
        }
        FileChannel fc = null;
        try {
            fc = FileChannel.open(file.toPath(), StandardOpenOption.READ);
            mappedBuffer = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
        } finally {
            DtUtil.close(fc);
        }
    }

    public FiberFuture<Void> ensureMmapOpen() {
        if (destroyed) {
            return FiberFuture.failedFuture(fiberGroup, new RaftException("DtFile is destroyed: " + file.getPath()));
        }
        if (mappedBuffer != null) {
            return FiberFuture.completedFuture(fiberGroup, null);
        }
        if (mmapOpenFuture != null) {
            return mmapOpenFuture;
        }
        FiberFuture<Void> result = fiberGroup.newFuture("openMmap-" + file.getName());
        mmapOpenFuture = result;
        FiberFuture<Object> f = fiberGroup.newFuture("asyncOpenMmap");
        try {
            ioExecutor.execute(() -> {
                FileChannel fc = null;
                try {
                    fc = FileChannel.open(file.toPath(), StandardOpenOption.READ);
                    MappedByteBuffer buf = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
                    DtUtil.close(fc);
                    f.fireComplete(buf);
                } catch (Throwable e) {
                    DtUtil.close(fc);
                    f.fireCompleteExceptionally(e);
                }
            });
        } catch (Throwable e) {
            f.completeExceptionally(e);
        }
        registerMmapCallback(f);
        return result;
    }

    private void registerMmapCallback(FiberFuture<Object> f) {
        f.registerCallback((buf, ex) -> {
            FiberFuture<Void> oldMmapFuture = this.mmapOpenFuture;
            this.mmapOpenFuture = null;
            if (destroyed) {
                if (buf instanceof MappedByteBuffer) {
                    VersionFactory.getInstance().releaseDirectBuffer((MappedByteBuffer) buf);
                }
                return;
            }
            if (ex != null) {
                if (oldMmapFuture != null) {
                    oldMmapFuture.completeExceptionally(ex);
                }
            } else {
                mappedBuffer = (MappedByteBuffer) buf;
                if (oldMmapFuture != null) {
                    oldMmapFuture.complete(null);
                }
            }
        });
    }

    public ByteBuffer duplicateMmap() {
        return mappedBuffer.duplicate();
    }

    public void closeMmap() {
        if (mappedBuffer != null) {
            MappedByteBuffer buf = mappedBuffer;
            mappedBuffer = null;
            VersionFactory.getInstance().releaseDirectBuffer(buf);
        }
    }
}
