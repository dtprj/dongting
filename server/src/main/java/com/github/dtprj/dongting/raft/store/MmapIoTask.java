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

import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.raft.RaftException;

import java.io.IOException;
import java.util.Objects;

/**
 * Performs mmap read operations without retry. Read failures are propagated to the caller.
 * Unlike {@link AsyncIoTask} which retries on write failures (to prevent raft log stall),
 * mmap reads can safely fail fast since the caller can retry at a higher level if needed.
 *
 * @author huangli
 */
public class MmapIoTask {
    private final DtFile dtFile;
    private final FiberFuture<Void> future;

    private IoCallback callback;
    private boolean used;

    public MmapIoTask(FiberGroup fiberGroup, DtFile dtFile) {
        this.future = Objects.requireNonNull(fiberGroup).newFuture("mmapIoTask");
        Objects.requireNonNull(dtFile);
        this.dtFile = dtFile;
    }

    public FiberFuture<Void> run(IoCallback callback) {
        if (used) {
            future.completeExceptionally(new RaftException("io task can't be reused"));
            return future;
        }
        this.callback = callback;
        used = true;
        if (!dtFile.isMmapOpen()) {
            FiberFuture<Void> openFut = dtFile.ensureMmapOpen();
            openFut.registerCallback((v, ex) -> {
                if (ex != null) {
                    future.fireCompleteExceptionally(ex);
                } else {
                    submitToIoExecutor();
                }
            });
        } else {
            submitToIoExecutor();
        }
        return future;
    }

    public FiberFuture<Void> getFuture() {
        return future;
    }

    private void submitToIoExecutor() {
        try {
            dtFile.ioExecutor.execute(() -> {
                try {
                    callback.run(dtFile.duplicateMmap());
                    future.fireComplete(null);
                } catch (Throwable e) {
                    completeWithError(e);
                }
            });
        } catch (Throwable e) {
            completeWithError(e);
        }
    }

    private void completeWithError(Throwable ex) {
        String s = "mmap io file=" + dtFile.getFile().getPath() + " fail. " + ex;
        future.fireCompleteExceptionally(new IOException(s, ex));
    }
}
