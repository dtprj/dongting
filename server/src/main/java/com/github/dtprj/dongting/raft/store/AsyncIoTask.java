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

import com.github.dtprj.dongting.fiber.FiberCancelException;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.raft.RaftException;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class AsyncIoTask implements CompletionHandler<Integer, FiberFuture<Void>> {
    private final AsynchronousFileChannel channel;
    private final Supplier<Boolean> cancelIndicator;
    private final FiberGroup fiberGroup;

    private ByteBuffer ioBuffer;
    private long filePos;
    private boolean write;
    private int position;
    private boolean flush;
    private boolean flushMeta;

    public AsyncIoTask(FiberGroup fiberGroup, AsynchronousFileChannel channel, Supplier<Boolean> cancelIndicator) {
        Objects.requireNonNull(channel);
        this.fiberGroup = fiberGroup;
        this.channel = channel;
        this.cancelIndicator = cancelIndicator;
    }

    public FiberFuture<Void> read(ByteBuffer ioBuffer, long filePos) {
        this.ioBuffer = ioBuffer;
        this.filePos = filePos;
        this.position = ioBuffer.position();
        this.write = false;
        this.flush = false;
        this.flushMeta = false;
        FiberFuture<Void> f = fiberGroup.newFuture();
        exec(f, filePos);
        return f;
    }

    public FiberFuture<Void> write(ByteBuffer ioBuffer, long filePos) {
        this.ioBuffer = ioBuffer;
        this.filePos = filePos;
        this.position = ioBuffer.position();
        this.write = true;
        this.flush = false;
        this.flushMeta = false;
        FiberFuture<Void> f = fiberGroup.newFuture();
        exec(f, filePos);
        return f;
    }

    public FiberFuture<Void> writeAndFlush(ByteBuffer ioBuffer, long filePos, boolean flushMeta) {
        this.ioBuffer = ioBuffer;
        this.filePos = filePos;
        this.position = ioBuffer.position();
        this.write = true;
        this.flush = true;
        this.flushMeta = flushMeta;
        FiberFuture<Void> f = fiberGroup.newFuture();
        exec(f, filePos);
        return f;
    }

    public FiberFuture<Void> retry() {
        FiberFuture<Void> f = fiberGroup.newFuture();
        ioBuffer.position(position);
        exec(f, filePos);
        return f;
    }

    private void exec(FiberFuture<Void> f, long pos) {
        try {
            if (write) {
                channel.write(ioBuffer, pos, f, this);
            } else {
                channel.read(ioBuffer, pos, f, this);
            }
        } catch (Throwable e) {
            f.completeExceptionally(e);
        }
    }

    @Override
    public void completed(Integer result, FiberFuture<Void> f) {
        if (result < 0) {
            f.completeExceptionally(new RaftException("read end of file"));
            return;
        }
        if (ioBuffer.hasRemaining()) {
            if (cancelIndicator != null && cancelIndicator.get()) {
                f.completeExceptionally(new FiberCancelException());
                return;
            }
            int bytes = ioBuffer.position() - position;
            exec(f, filePos + bytes);
        } else {
            try {
                if (flush) {
                    channel.force(flushMeta);
                }
                f.complete(null);
            } catch (Throwable e) {
                f.completeExceptionally(e);
            }
        }
    }

    @Override
    public void failed(Throwable exc, FiberFuture<Void> f) {
        f.completeExceptionally(exc);
    }
}
