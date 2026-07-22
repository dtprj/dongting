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
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class AsyncIoTask {
    private static final DtLog log = DtLogs.getLogger(AsyncIoTask.class);
    private final DtFile dtFile;
    private final Supplier<Boolean> cancelRetryIndicator;
    private final FiberFuture<Void> future;

    private final int[] retryInterval;
    private final boolean retryForever;
    private final FiberGroup fiberGroup;

    private ByteBuffer ioBuffer;
    private long filePos;

    private boolean write;

    private int retryCount = 0;

    private boolean rwCalled;

    public AsyncIoTask(FiberGroup fiberGroup, DtFile dtFile) {
        this(fiberGroup, dtFile, null, false, null);
    }

    public AsyncIoTask(FiberGroup fiberGroup, DtFile dtFile, int[] retryInterval, boolean retryForever,
                       Supplier<Boolean> cancelRetryIndicator) {
        this.fiberGroup = fiberGroup;
        Objects.requireNonNull(dtFile);
        this.dtFile = dtFile;
        this.retryInterval = retryInterval;
        this.retryForever = retryForever;
        this.cancelRetryIndicator = cancelRetryIndicator;
        this.future = fiberGroup.newFuture("asyncIoTaskFuture");
    }

    public FiberFuture<Void> read(ByteBuffer ioBuffer, long filePos) {
        return exec(ioBuffer, filePos, false);
    }

    public FiberFuture<Void> write(ByteBuffer ioBuffer, long filePos) {
        return exec(ioBuffer, filePos, true);
    }

    private FiberFuture<Void> exec(ByteBuffer ioBuffer, long filePos, boolean write) {
        if (rwCalled) {
            future.completeExceptionally(new RaftException("io task can't reused"));
            return future;
        }
        Fiber f = Fiber.currentFiber();
        if (f == null || f.isDaemon()) {
            throw new RaftException("io task should not run in daemon fiber");
        }
        if (ioBuffer.position() != 0) {
            throw new RaftException("buffer position must be 0: " + ioBuffer.position());
        }
        this.ioBuffer = ioBuffer;
        this.filePos = filePos;
        this.write = write;
        if (!dtFile.isRwChannelOpen()) {
            FiberFuture<Void> openFut = dtFile.ensureOpen();
            openFut.registerCallback((v, ex) -> {
                if (ex != null) {
                    future.fireCompleteExceptionally(ex);
                } else {
                    exec(filePos);
                }
            });
        } else {
            exec(filePos);
        }
        rwCalled = true;
        return future;
    }

    protected void fireComplete(Throwable ex) {
        if (ex == null) {
            future.fireComplete(null);
        } else {
            String op = write ? "write" : "read";
            String s = op + " file=" + dtFile.getFile().getPath() + ", filePos=" + filePos + " fail. " + ex.getMessage();
            future.fireCompleteExceptionally(new IOException(s, ex));
        }
    }

    void retry(Throwable ioEx) {
        long sleepTime = StoreUtil.calcRetryInterval(retryCount, retryInterval);
        if (sleepTime <= 0) {
            fireComplete(ioEx);
            return;
        }
        // assert retryInterval is not null since StoreUtil.calcRetryInterval checked it
        if (retryCount >= retryInterval.length && !retryForever) {
            fireComplete(ioEx);
            return;
        }

        Fiber retryFiber = new Fiber("io-retry-fiber", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                log.warn("io error, retry after {} ms", sleepTime, ioEx);
                return Fiber.sleepUntilShouldStop(sleepTime, this::resume);
            }

            private FrameCallResult resume(Void v) {
                if (shouldCancelRetry()) {
                    fireComplete(ioEx);
                    return Fiber.frameReturn();
                }
                retryCount++;
                ioBuffer.rewind();
                exec(filePos);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                log.error("unexpected retry error", ex);
                fireComplete(ex);
                return Fiber.frameReturn();
            }

            private boolean shouldCancelRetry() {
                if (isGroupShouldStopPlain()) {
                    // if fiber group is stopped, ignore cancelIndicator and retryForever
                    return true;
                }
                if (cancelRetryIndicator != null && cancelRetryIndicator.get()) {
                    log.warn("retry canceled by cancelIndicator");
                    return true;
                }
                return false;
            }
        });
        if (!fiberGroup.fireFiber(retryFiber)) {
            future.fireCompleteExceptionally(new RaftException("retry failed because fiber group is stopped"));
        }
    }

    private void exec(long pos) {
        try {
            dtFile.ioExecutor.execute(() -> doExec(pos));
        } catch (Throwable e) {
            fireComplete(e);
        }
    }

    // this method set to protected for mock error in unit test
    protected void doExec(long pos) {
        try {
            while (ioBuffer.hasRemaining()) {
                int n;
                if (write) {
                    n = dtFile.getChannel().write(ioBuffer, pos);
                } else {
                    n = dtFile.getChannel().read(ioBuffer, pos);
                }
                if (n < 0) {
                    fireComplete(new RaftException("read end of file"));
                    return;
                }
                if (n == 0) {
                    throw new IOException((write ? "write" : "read") + " returned 0");
                }
                pos = filePos + ioBuffer.position();
            }
            fireComplete(null);
        } catch (Throwable e) {
            retry(e);
        }
    }

    public FiberFuture<Void> getFuture() {
        return future;
    }

    public DtFile getDtFile() {
        return dtFile;
    }
}
