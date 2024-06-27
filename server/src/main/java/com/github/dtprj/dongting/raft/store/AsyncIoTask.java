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

import com.github.dtprj.dongting.fiber.DoInLockFrame;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class AsyncIoTask implements CompletionHandler<Integer, Void> {
    private static final DtLog log = DtLogs.getLogger(AsyncIoTask.class);
    private final DtFile dtFile;
    private final Supplier<Boolean> cancelRetryIndicator;
    private final RaftGroupConfigEx groupConfig;
    private final FiberFuture<Void> future;

    private final boolean retry;
    private final boolean retryForever;

    private ByteBuffer ioBuffer;
    private long filePos;
    private int position;

    private boolean write;
    private boolean force;
    private boolean flushMeta;

    private int retryCount = 0;

    public AsyncIoTask(RaftGroupConfigEx groupConfig, DtFile dtFile) {
        this(groupConfig, dtFile, false, false, null);
    }

    public AsyncIoTask(RaftGroupConfigEx groupConfig, DtFile dtFile, boolean retry, boolean retryForever) {
        this(groupConfig, dtFile, retry, retryForever, null);
    }

    public AsyncIoTask(RaftGroupConfigEx groupConfig, DtFile dtFile, boolean retry, boolean retryForever,
                       Supplier<Boolean> cancelRetryIndicator) {
        Objects.requireNonNull(groupConfig.getFiberGroup());
        Objects.requireNonNull(dtFile);
        this.groupConfig = groupConfig;
        this.dtFile = dtFile;
        this.retry = retry;
        this.retryForever = retryForever;
        this.cancelRetryIndicator = cancelRetryIndicator;
        this.future = groupConfig.getFiberGroup().newFuture("asyncIoTaskFuture");
    }

    public FiberFuture<Void> read(ByteBuffer ioBuffer, long filePos) {
        if (this.ioBuffer != null) {
            future.completeExceptionally(new RaftException("io task can't reused"));
            return future;
        }
        this.ioBuffer = ioBuffer;
        this.filePos = filePos;
        this.position = ioBuffer.position();
        exec(filePos);
        return future;
    }

    public FiberFrame<Void> lockRead(ByteBuffer ioBuffer, long filePos) {
        return new DoInLockFrame<>(dtFile.getLock().readLock()) {
            @Override
            protected FrameCallResult afterGetLock() {
                return read(ioBuffer, filePos).await(this::justReturn);
            }
        };
    }

    public FiberFuture<Void> write(ByteBuffer ioBuffer, long filePos) {
        return write(ioBuffer, filePos, false, false);
    }

    public FiberFuture<Void> writeAndForce(ByteBuffer ioBuffer, long filePos, boolean flushMeta) {
        return write(ioBuffer, filePos, true, flushMeta);
    }

    private FiberFuture<Void> write(ByteBuffer ioBuffer, long filePos, boolean force, boolean syncMeta) {
        if (this.ioBuffer != null) {
            future.completeExceptionally(new RaftException("io task can't reused"));
            return future;
        }
        this.ioBuffer = ioBuffer;
        this.filePos = filePos;
        this.position = ioBuffer.position();
        this.write = true;
        this.force = force;
        this.flushMeta = syncMeta;
        exec(filePos);
        return future;
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
        if (!retry) {
            fireComplete(ioEx);
            return;
        }
        if (shouldCancelRetry()) {
            fireComplete(ioEx);
            return;
        }
        long sleepTime;
        long[] retryInterval = groupConfig.getIoRetryInterval();
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

        Fiber retryFiber = new Fiber("io-retry-fiber", groupConfig.getFiberGroup(), new FiberFrame<>() {
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
        groupConfig.getFiberGroup().fireFiber(retryFiber);
    }

    private boolean shouldCancelRetry() {
        if (groupConfig.getFiberGroup().isShouldStop()) {
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
            ExecutorService executor = groupConfig.getBlockIoExecutor();
            executor.execute(() -> {
                try {
                    doForce();
                    fireComplete(null);
                } catch (Throwable e) {
                    // retry should run in fiber dispatcher thread
                    executor.execute(() -> retry(e));
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
