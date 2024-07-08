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
import java.nio.channels.CompletionHandler;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class AsyncIoTask implements CompletionHandler<Integer, Void>, BiConsumer<Void, Throwable> {
    private static final DtLog log = DtLogs.getLogger(AsyncIoTask.class);
    private final DtFile dtFile;
    private final Supplier<Boolean> cancelRetryIndicator;
    private final FiberFuture<Void> future;

    private final int[] retryInterval;
    private final boolean retryForever;
    private final FiberGroup fiberGroup;

    private Executor forceExecutor;

    private ByteBuffer ioBuffer;
    private long filePos;
    private int position;

    private boolean write;
    private boolean force;
    private boolean flushMeta;

    private int retryCount = 0;

    private boolean rwCalled;

    public AsyncIoTask(FiberGroup fiberGroup, DtFile dtFile) {
        this(fiberGroup, dtFile, null, false, null);
    }

    public AsyncIoTask(FiberGroup fiberGroup, DtFile dtFile, int[] retryInterval, boolean retryForever) {
        this(fiberGroup, dtFile, retryInterval, retryForever, null);
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
        this.future.registerCallback(this);
    }

    public FiberFuture<Void> read(ByteBuffer ioBuffer, long filePos) {
        if (rwCalled) {
            future.completeExceptionally(new RaftException("io task can't reused"));
            return future;
        }
        dtFile.incReaders();
        this.ioBuffer = ioBuffer;
        this.filePos = filePos;
        this.position = ioBuffer.position();
        exec(filePos);
        rwCalled = true;
        return future;
    }

    public FiberFuture<Void> write(ByteBuffer ioBuffer, long filePos) {
        return write(ioBuffer, filePos, false, false);
    }

    public FiberFuture<Void> writeAndForce(ByteBuffer ioBuffer, long filePos, boolean flushMeta, Executor forceExecutor) {
        this.forceExecutor = forceExecutor;
        return write(ioBuffer, filePos, true, flushMeta);
    }

    private FiberFuture<Void> write(ByteBuffer ioBuffer, long filePos, boolean force, boolean syncMeta) {
        if (rwCalled) {
            future.completeExceptionally(new RaftException("io task can't reused"));
            return future;
        }
        dtFile.incWriters();
        this.ioBuffer = ioBuffer;
        this.filePos = filePos;
        this.position = ioBuffer.position();
        this.write = true;
        this.force = force;
        this.flushMeta = syncMeta;
        exec(filePos);
        rwCalled = true;
        return future;
    }

    @Override
    public void accept(Void unused, Throwable throwable) {
        ioBuffer = null;
        if (write) {
            dtFile.decWriters();
        } else {
            dtFile.decReaders();
        }
    }

    protected void fireComplete(Throwable ex) {
        if (ex == null) {
            future.fireComplete(null);
        } else {
            String op = ioBuffer == null ? "force" : write ? "write" : "read";
            String s = op + " file=" + dtFile.getFile().getPath() + ", filePos=" + filePos + " fail. " + ex.getMessage();
            future.fireCompleteExceptionally(new IOException(s, ex));
        }
    }

    private void retry(Throwable ioEx) {
        if (retryInterval == null) {
            fireComplete(ioEx);
            return;
        }
        if (shouldCancelRetry()) {
            fireComplete(ioEx);
            return;
        }
        long sleepTime;
        if (retryCount >= retryInterval.length) {
            if (retryForever) {
                sleepTime = retryInterval[retryInterval.length - 1];
                retryCount++;
            } else {
                fireComplete(ioEx);
                return;
            }
        } else {
            sleepTime = retryInterval[retryCount++];
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
                if (ioBuffer == null) {
                    submitForceTask();
                } else {
                    ioBuffer.position(position);
                    exec(filePos);
                }
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                log.error("unexpected retry error", ex);
                fireComplete(ex);
                return Fiber.frameReturn();
            }
        });
        fiberGroup.fireFiber(retryFiber);
    }

    private boolean shouldCancelRetry() {
        if (fiberGroup.isShouldStop()) {
            // if fiber group is stopped, ignore cancelIndicator and retryForever
            return true;
        }
        if (cancelRetryIndicator != null && cancelRetryIndicator.get()) {
            log.warn("retry canceled by cancelIndicator");
            return true;
        }
        return false;
    }

    // this method set to protected for mock error in unit test
    protected void exec(long pos) {
        try {
            if (write) {
                dtFile.getChannel().write(ioBuffer, pos, null, this);
            } else {
                dtFile.getChannel().read(ioBuffer, pos, null, this);
            }
        } catch (Throwable e) {
            fireComplete(e);
        }
    }

    @Override
    public void completed(Integer result, Void v) {
        if (result < 0) {
            fireComplete(new RaftException("read end of file"));
            return;
        }
        if (ioBuffer.hasRemaining()) {
            int bytes = ioBuffer.position() - position;
            exec(filePos + bytes);
        } else {
            if (force) {
                submitForceTask();
            } else {
                fireComplete(null);
            }
        }
    }

    private void submitForceTask() {
        try {
            forceExecutor.execute(() -> {
                try {
                    doForce();
                    fireComplete(null);
                } catch (Throwable e) {
                    // retry should run in fiber dispatcher thread
                    fiberGroup.getExecutor().execute(() -> retry(e));
                }
            });
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

    // this method set to protected for mock error in unit test
    protected void doForce() throws IOException {
        dtFile.getChannel().force(flushMeta);
    }

    @Override
    public void failed(Throwable exc, Void v) {
        retry(exc);
    }
}
