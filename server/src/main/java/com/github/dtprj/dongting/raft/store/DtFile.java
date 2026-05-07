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
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.raft.RaftException;

import java.io.File;
import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.OpenOption;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * @author huangli
 */
public class DtFile {
    protected final File file;
    AsynchronousFileChannel channel;
    private final Set<OpenOption> openOptions;
    final ExecutorService ioExecutor;
    final FiberGroup fiberGroup;

    private boolean destroyed;
    FiberFuture<Void> openFuture;

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

    public boolean isOpen() {
        return channel != null;
    }

    /**
     * This method performs blocking IO, should only be used in IO thread or initialization code.
     * For raft main thread, use ensureOpen() instead.
     */
    public void syncOpen() throws IOException {
        if (channel != null) {
            return;
        }
        channel = AsynchronousFileChannel.open(file.toPath(), openOptions, ioExecutor);
    }

    public void destroy() {
        destroyed = true;
        FiberFuture<Void> pendingFuture = openFuture;
        openFuture = null;
        DtUtil.close(channel);
        channel = null;
        if (pendingFuture != null) {
            pendingFuture.completeExceptionally(new RaftException("DtFile is destroyed: " + file.getPath()));
        }
    }

    public FiberFuture<Void> ensureOpen() {
        if (destroyed) {
            return FiberFuture.failedFuture(fiberGroup, new RaftException("DtFile is destroyed: " + file.getPath()));
        }
        if (channel != null) {
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
                    AsynchronousFileChannel ch = AsynchronousFileChannel.open(file.toPath(), openOptions, ioExecutor);
                    f.fireComplete(ch);
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
                DtUtil.close(ch);
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

}
