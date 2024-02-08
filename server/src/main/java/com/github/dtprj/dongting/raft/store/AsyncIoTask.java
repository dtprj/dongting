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
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class AsyncIoTask implements CompletionHandler<Integer, Void> {
    private static final DtLog log = DtLogs.getLogger(AsyncIoTask.class);
    private final DtFile dtFile;
    private final Supplier<Boolean> cancelRetryIndicator;
    private final FiberGroup fiberGroup;
    private final long[] retryInterval;
    private final FiberFuture<Void> future;

    private final boolean retryForever;

    private ByteBuffer ioBuffer;
    private long filePos;
    boolean write;
    private int position;
    private boolean flush;
    private boolean flushMeta;

    private int retryCount = 0;

    public AsyncIoTask(FiberGroup fiberGroup, DtFile dtFile) {
        this(fiberGroup, dtFile, null, false, null);
    }

    public AsyncIoTask(FiberGroup fiberGroup, DtFile dtFile, long[] retryInterval, boolean retryForever) {
        this(fiberGroup, dtFile, retryInterval, retryForever, null);
    }

    public AsyncIoTask(FiberGroup fiberGroup, DtFile dtFile, long[] retryInterval, boolean retryForever,
                       Supplier<Boolean> cancelRetryIndicator) {
        Objects.requireNonNull(fiberGroup);
        Objects.requireNonNull(dtFile);
        this.retryInterval = retryInterval;
        this.fiberGroup = fiberGroup;
        this.dtFile = dtFile;
        this.retryForever = retryForever;
        this.cancelRetryIndicator = cancelRetryIndicator;
        this.future = fiberGroup.newFuture();
    }

    public FiberFuture<Void> read(ByteBuffer ioBuffer, long filePos) {
        if (this.ioBuffer != null) {
            future.completeExceptionally(new RaftException("io task can't reused"));
            return future;
        }
        this.ioBuffer = ioBuffer;
        this.filePos = filePos;
        this.position = ioBuffer.position();
        this.write = false;
        this.flush = false;
        this.flushMeta = false;
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

    public FiberFuture<Void> writeAndSync(ByteBuffer ioBuffer, long filePos, boolean flushMeta) {
        return write(ioBuffer, filePos, true, flushMeta);
    }

    private FiberFuture<Void> write(ByteBuffer ioBuffer, long filePos, boolean sync, boolean syncMeta) {
        if (this.ioBuffer != null) {
            future.completeExceptionally(new RaftException("io task can't reused"));
            return future;
        }
        this.ioBuffer = ioBuffer;
        this.filePos = filePos;
        this.position = ioBuffer.position();
        this.write = true;
        this.flush = sync;
        this.flushMeta = syncMeta;
        exec(filePos);
        return future;
    }

    public FiberFrame<Void> lockWrite(ByteBuffer ioBuffer, long filePos) {
        // use read lock, so not block read operation.
        // because we never read file block that is being written.
        return new DoInLockFrame<>(dtFile.getLock().readLock()) {
            @Override
            protected FrameCallResult afterGetLock() {
                return write(ioBuffer, filePos, false, false).await(this::justReturn);
            }
        };
    }

    public FiberFrame<Void> lockWriteAndSync(ByteBuffer ioBuffer, long filePos, boolean flushMeta) {
        // use read lock, so not block read operation.
        // because we never read file block that is being written.
        return new DoInLockFrame<>(dtFile.getLock().readLock()) {
            @Override
            protected FrameCallResult afterGetLock() {
                return write(ioBuffer, filePos, true, flushMeta).await(this::justReturn);
            }
        };
    }

    private void complete(Throwable ex) {
        if (ex == null) {
            future.fireComplete(null);
        } else {
            future.fireCompleteExceptionally(ex);
        }
    }

    private void retry(Throwable ioEx) {
        if (retryInterval == null || retryInterval.length == 0) {
            complete(ioEx);
            return;
        }
        if (shouldCancelRetry()) {
            complete(ioEx);
            return;
        }
        long sleepTime;
        if (retryCount >= retryInterval.length) {
            if (retryForever) {
                sleepTime = retryInterval[retryInterval.length - 1];
                retryCount++;
            } else {
                complete(ioEx);
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
                    complete(ioEx);
                    return Fiber.frameReturn();
                }
                ioBuffer.position(position);
                exec(filePos);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                log.error("unexpected retry error", ex);
                complete(ex);
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
            complete(e);
        }
    }

    @Override
    public void completed(Integer result, Void v) {
        if (result < 0) {
            complete(new RaftException("read end of file"));
            return;
        }
        if (ioBuffer.hasRemaining()) {
            int bytes = ioBuffer.position() - position;
            exec(filePos + bytes);
        } else {
            try {
                doFlush();
                complete(null);
            } catch (Throwable e) {
                retry(e);
            }
        }
    }

    public FiberFuture<Void> getFuture() {
        return future;
    }

    public ByteBuffer getIoBuffer() {
        return ioBuffer;
    }

    public void setIoBuffer(ByteBuffer ioBuffer) {
        this.ioBuffer = ioBuffer;
    }

    public DtFile getDtFile() {
        return dtFile;
    }

    // this method set to protected for mock error in unit test
    protected void doFlush() throws IOException {
        if (flush) {
            dtFile.getChannel().force(flushMeta);
        }
    }

    @Override
    public void failed(Throwable exc, Void v) {
        retry(exc);
    }
}
