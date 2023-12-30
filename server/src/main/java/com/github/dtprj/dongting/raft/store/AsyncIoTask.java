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
import com.github.dtprj.dongting.fiber.FiberCancelException;
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
    private final long opId;
    private final DtFile dtFile;
    private final Supplier<Boolean> cancelIndicator;
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

    public AsyncIoTask(FiberGroup fiberGroup, DtFile dtFile,
                       long[] retryInterval, boolean retryForever) {
        this(fiberGroup, dtFile, retryInterval, retryForever, null);
    }

    public AsyncIoTask(FiberGroup fiberGroup, DtFile dtFile,
                       long[] retryInterval, boolean retryForever, Supplier<Boolean> cancelIndicator) {
        Objects.requireNonNull(fiberGroup);
        Objects.requireNonNull(dtFile);
        this.retryInterval = retryInterval;
        this.fiberGroup = fiberGroup;
        this.dtFile = dtFile;
        this.retryForever = retryForever;
        this.cancelIndicator = cancelIndicator;
        this.future = fiberGroup.newFuture();
        this.opId = dtFile.nextOperationId();
    }

    public FiberFuture<Void> read(ByteBuffer ioBuffer, long filePos) {
        if (this.ioBuffer != null) {
            throw new RaftException("io task can't reused");
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

    public FiberFuture<Void> write(ByteBuffer ioBuffer, long filePos) {
        if (this.ioBuffer != null) {
            throw new RaftException("io task can't reused");
        }
        this.ioBuffer = ioBuffer;
        this.filePos = filePos;
        this.position = ioBuffer.position();
        this.write = true;
        this.flush = false;
        this.flushMeta = false;
        exec(filePos);
        return future;
    }

    public FiberFuture<Void> writeAndFlush(ByteBuffer ioBuffer, long filePos, boolean flushMeta) {
        if (this.ioBuffer != null) {
            throw new RaftException("io task can't reused");
        }
        this.ioBuffer = ioBuffer;
        this.filePos = filePos;
        this.position = ioBuffer.position();
        this.write = true;
        this.flush = true;
        this.flushMeta = flushMeta;
        exec(filePos);
        return future;
    }

    private void retry(Throwable ioEx) {
        if (retryInterval == null || retryInterval.length == 0) {
            future.fireCompleteExceptionally(ioEx);
            return;
        }
        if (shouldCancelRetry()) {
            future.fireCompleteExceptionally(ioEx);
            return;
        }
        long sleepTime;
        if (retryCount >= retryInterval.length) {
            if (retryForever) {
                sleepTime = retryInterval[retryInterval.length - 1];
                retryCount++;
            } else {
                future.fireCompleteExceptionally(ioEx);
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
                    future.fireCompleteExceptionally(ioEx);
                    return Fiber.frameReturn();
                }
                ioBuffer.position(position);
                exec(filePos);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                log.error("unexpected retry error", ex);
                future.fireCompleteExceptionally(ex);
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
        if (cancelIndicator != null && cancelIndicator.get()) {
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
            future.fireCompleteExceptionally(e);
        }
    }

    @Override
    public void completed(Integer result, Void v) {
        if (result < 0) {
            future.fireCompleteExceptionally(new RaftException("read end of file"));
            return;
        }
        if (ioBuffer.hasRemaining()) {
            if (cancelIndicator != null && cancelIndicator.get()) {
                future.fireCompleteExceptionally(new FiberCancelException());
                return;
            }
            int bytes = ioBuffer.position() - position;
            exec(filePos + bytes);
        } else {
            try {
                doFlush();
                future.fireComplete(null);
            } catch (Throwable e) {
                retry(e);
            }
        }
    }

    public FiberFuture<Void> getFuture() {
        return future;
    }

    public long getOpId() {
        return opId;
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
