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

import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class MmapIoTask {
    private static final DtLog log = DtLogs.getLogger(MmapIoTask.class);

    private final LogFile logFile;
    private final FiberFuture<Void> future;
    private final FiberGroup fiberGroup;
    private final int[] retryInterval;
    private final boolean retryForever;
    private final Supplier<Boolean> cancelRetryIndicator;

    private IoCallback callback;
    private boolean used;
    private int retryCount;

    public MmapIoTask(FiberGroup fiberGroup, LogFile logFile) {
        this(fiberGroup, logFile, null, false, null);
    }

    public MmapIoTask(FiberGroup fiberGroup, LogFile logFile,
                      int[] retryInterval, boolean retryForever,
                      Supplier<Boolean> cancelRetryIndicator) {
        this.fiberGroup = Objects.requireNonNull(fiberGroup);
        Objects.requireNonNull(logFile);
        this.logFile = logFile;
        this.retryInterval = retryInterval;
        this.retryForever = retryForever;
        this.cancelRetryIndicator = cancelRetryIndicator;
        this.future = fiberGroup.newFuture("mmapIoTask");
    }

    public FiberFuture<Void> run(IoCallback callback) {
        if (used) {
            future.completeExceptionally(new RaftException("io task can't be reused"));
            return future;
        }
        this.callback = callback;
        used = true;
        if (!logFile.isOpen()) {
            FiberFuture<Void> openFut = logFile.ensureOpen();
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

    public LogFile getLogFile() {
        return logFile;
    }

    private void submitToIoExecutor() {
        try {
            logFile.ioExecutor.execute(() -> {
                try {
                    callback.run(logFile.duplicateMmap());
                    future.fireComplete(null);
                } catch (Throwable e) {
                    retryOrComplete(e);
                }
            });
        } catch (Throwable e) {
            retryOrComplete(e);
        }
    }

    private void retryOrComplete(Throwable ex) {
        long sleepTime = StoreUtil.calcRetryInterval(retryCount, retryInterval);
        if (sleepTime <= 0 || retryInterval == null) {
            completeWithError(ex);
            return;
        }
        if (retryCount >= retryInterval.length && !retryForever) {
            completeWithError(ex);
            return;
        }
        Fiber retryFiber = new Fiber("mmap-io-retry", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                log.warn("mmap io error, retry after {} ms", sleepTime, ex);
                return Fiber.sleepUntilShouldStop(sleepTime, this::resume);
            }

            private FrameCallResult resume(Void v) {
                if (shouldCancelRetry()) {
                    completeWithError(ex);
                    return Fiber.frameReturn();
                }
                retryCount++;
                submitToIoExecutor();
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                log.error("unexpected retry error", ex);
                completeWithError(ex);
                return Fiber.frameReturn();
            }

            private boolean shouldCancelRetry() {
                if (isGroupShouldStopPlain()) return true;
                if (cancelRetryIndicator != null && cancelRetryIndicator.get()) return true;
                return false;
            }
        });
        if (!fiberGroup.fireFiber(retryFiber)) {
            future.fireCompleteExceptionally(new RaftException("retry failed: fiber group stopped"));
        }
    }

    private void completeWithError(Throwable ex) {
        String s = "mmap io file=" + logFile.getFile().getPath() + " fail. " + ex.toString();
        future.fireCompleteExceptionally(new IOException(s, ex));
    }
}
