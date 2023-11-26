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

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class AsyncIoTask implements CompletionHandler<Integer, Void> {
    private static final DtLog log = DtLogs.getLogger(AsyncIoTask.class);
    private final AsynchronousFileChannel channel;
    private final Supplier<Boolean> cancelIndicator;
    private final FiberGroup fiberGroup;
    private final long[] retryInterval;
    private final FiberFuture<Void> future;

    private ByteBuffer ioBuffer;
    private long filePos;
    boolean write;
    private int position;
    private boolean flush;
    private boolean flushMeta;

    private int retryCount = 0;

    public AsyncIoTask(FiberGroup fiberGroup, AsynchronousFileChannel channel,
                       Supplier<Boolean> cancelIndicator, long[] retryInterval) {
        Objects.requireNonNull(fiberGroup);
        Objects.requireNonNull(channel);
        this.retryInterval = retryInterval;
        this.fiberGroup = fiberGroup;
        this.channel = channel;
        this.cancelIndicator = cancelIndicator;
        this.future = fiberGroup.newFuture();
    }

    public FiberFuture<Void> read(ByteBuffer ioBuffer, long filePos) {
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
        if (retryInterval == null) {
            future.completeExceptionally(ioEx);
            return;
        }
        if (shouldCancelRetry(ioEx)) {
            return;
        }
        if(retryCount >= retryInterval.length) {
            future.completeExceptionally(ioEx);
            return;
        }
        long sleepTime = retryInterval[retryCount++];

        Fiber retryFiber = new Fiber("io-retry-fiber", fiberGroup, new FiberFrame<Void>() {
            @Override
            public FrameCallResult execute(Void input) {
                log.warn("io error, retry after {} ms", sleepTime);
                return Fiber.sleep(sleepTime, this::resume);
            }

            private FrameCallResult resume(Void v) {
                if (shouldCancelRetry(ioEx)) {
                    return Fiber.frameReturn();
                }
                ioBuffer.position(position);
                exec(filePos);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                log.error("unexpected retry error", ex);
                future.completeExceptionally(ex);
                return Fiber.frameReturn();
            }
        });
        retryFiber.start(ex -> {
            log.error("start io retry fiber fail", ex);
            future.completeExceptionally(ioEx);
        });
    }

    private boolean shouldCancelRetry(Throwable e) {
        if (cancelIndicator != null && cancelIndicator.get()) {
            log.warn("retry canceled");
            future.completeExceptionally(new FiberCancelException());
            return true;
        }
        return false;
    }

    private void exec(long pos) {
        try {
            if (write) {
                channel.write(ioBuffer, pos, null, this);
            } else {
                channel.read(ioBuffer, pos, null, this);
            }
        } catch (Throwable e) {
            future.completeExceptionally(e);
        }
    }

    @Override
    public void completed(Integer result, Void v) {
        if (result < 0) {
            future.completeExceptionally(new RaftException("read end of file"));
            return;
        }
        if (ioBuffer.hasRemaining()) {
            if (cancelIndicator != null && cancelIndicator.get()) {
                future.completeExceptionally(new FiberCancelException());
                return;
            }
            int bytes = ioBuffer.position() - position;
            exec(filePos + bytes);
        } else {
            try {
                if (flush) {
                    channel.force(flushMeta);
                }
                future.complete(null);
            } catch (Throwable e) {
                retry(e);
            }
        }
    }

    @Override
    public void failed(Throwable exc, Void v) {
        retry(exc);
    }
}
